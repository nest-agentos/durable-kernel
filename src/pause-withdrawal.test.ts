import { afterEach, beforeEach, expect, it } from 'vitest';
import { createVFS, MemoryBackend, type IVFSManager } from '@nestos/vfs-core';
import { SeqFileKernelStore, type EffectClaim } from './infrastructure/seqfile/store';
import { addEffect } from './application/effect-utils';
import type { ResolvedStorageBinding, TaskRecord } from './domain/types';
import { taskStat } from './domain/status';
import { parseKernelEvent } from './public/events';

let manager: IVFSManager, binding: ResolvedStorageBinding, store: SeqFileKernelStore;
const spec = { program: { kind: 'held-effect', version: '1' }, input: null };
beforeEach(async () => {
    ({ manager } = await createVFS({ rootBackend: new MemoryBackend() }));
    const fs = await manager.openFileSystem('/pause'); binding = { fs, rootPath: '/session' };
    store = new SeqFileKernelStore({ fs, rootPath: '/catalog' }, async () => binding);
    await store.initialize(); await store.createSession('s', { kind: 'memory', locator: null });
});
afterEach(async () => { await manager.dispose(); });

async function runningTree(): Promise<{ ancestor: TaskRecord; root: TaskRecord; worker: TaskRecord; claim: EffectClaim }> {
    const ancestor = await store.createTask(binding, 's', { ...spec, deferStart: true });
    const root = await store.createTask(binding, 's', { ...spec, parent: ancestor.id, deferStart: true });
    const worker = await store.createTask(binding, 's', { ...spec, parent: root.id });
    const claim = (await store.claimReady(binding, 'worker', 60000))!;
    const next = addEffect({ ...claim.task, initialized: true, state: null, status: 'waiting' as const,
        currentAttempt: undefined, wait: { type: 'effect' as const, id: 'remote' } },
    { id: 'remote', kind: 'remote', version: '1', request: null, idempotencyKey: 'remote' });
    await store.commitTask(binding, claim, next, 'task.waiting');
    return { ancestor, root, worker, claim: (await store.claimEffect(binding, worker.id, 'remote', 'worker', 60000))! };
}
const read = (id: string) => store.readTask(binding, id);
async function pause(id: string, requestId = 'pause') { return store.controlTask(binding, id, 'pause', { requestId }); }

it('withdraws only an unconfirmed pause without cancelling or re-claiming the original Effect', async () => {
    const { root, worker } = await runningTree(), paused = await pause(root.id);
    expect(paused.acknowledged).toBe(false);
    await expect(store.controlTask(binding, root.id, 'run', { requestId: 'premature-resume' })).rejects.toMatchObject({ code: 'CONTROL_NOT_SETTLED' });
    const before = (await read(worker.id)).effects.remote;
    const options = { requestId: 'withdraw', expectedEpoch: paused.epoch };
    const control = await store.controlTask(binding, root.id, 'withdraw-pause', options);
    expect(control).toMatchObject({ mode: 'run', acknowledged: true, requestId: 'withdraw' });
    expect((await read(worker.id)).effects.remote).toEqual(before);
    expect((await read(worker.id)).controlHolds).toEqual([]);
    const event = (await store.events(binding)).find(value => value.taskId === root.id && value.type === 'task.control.withdraw-pause')!;
    expect(parseKernelEvent(event)).toMatchObject({type: 'task.control.withdraw-pause', payload: JSON.parse(JSON.stringify(control))});
    expect(await store.claimEffect(binding, worker.id, 'remote', 'second-worker', 60000)).toBeUndefined();
    await store.cancelTask(binding, root.id);
    expect(await store.controlTask(binding, root.id, 'withdraw-pause', options)).toEqual(control);
    await expect(store.controlTask(binding, root.id, 'run', options)).rejects.toThrow('Control request conflict');
});

it('preserves ancestor, Session and independent child pause ownership', async () => {
    const { ancestor, root, worker } = await runningTree();
    await pause(worker.id, 'child-pause'); await pause(root.id, 'root-pause'); await pause(ancestor.id, 'ancestor-pause');
    await store.setSessionStatus(binding, 'suspended');
    const pending = (await read(root.id)).control!;
    await store.controlTask(binding, root.id, 'withdraw-pause', { requestId: 'withdraw-root', expectedEpoch: pending.epoch });
    const current = await read(worker.id);
    expect(current.control).toMatchObject({ mode: 'pause', requestId: 'child-pause', acknowledged: false });
    expect(current.controlHolds).toEqual([ancestor.id]); expect(current.sessionPaused).toBe(true);
    const heldRoot = await read(root.id);
    expect(heldRoot.controlHolds).toEqual([ancestor.id]); expect(heldRoot.control?.acknowledged).toBe(false);
    expect(taskStat(heldRoot).stopped).toBe(false);
});

it('rejects missing or stale epochs and refuses an already-confirmed pause', async () => {
    const { root } = await runningTree(), pending = await pause(root.id);
    await expect(store.controlTask(binding, root.id, 'withdraw-pause', { requestId: 'missing' })).rejects.toMatchObject({ code: 'INVALID_SPEC' });
    await expect(store.controlTask(binding, root.id, 'withdraw-pause', { requestId: 'stale', expectedEpoch: pending.epoch - 1 })).rejects.toThrow('Control epoch conflict');
    const quiet = await store.createTask(binding, 's', { ...spec, deferStart: true }), confirmed = await pause(quiet.id, 'quiet-pause');
    expect(confirmed.acknowledged).toBe(true);
    await expect(store.controlTask(binding, quiet.id, 'withdraw-pause', { requestId: 'confirmed', expectedEpoch: confirmed.epoch })).rejects.toThrow('not awaiting confirmation');
});

it('cannot use withdrawal to override interrupt or descendant cleanup responsibility', async () => {
    const { root, worker } = await runningTree();
    const interrupted = await store.controlTask(binding, worker.id, 'interrupt', { requestId: 'interrupt' });
    await expect(store.controlTask(binding, worker.id, 'withdraw-pause', { requestId: 'wrong-kind', expectedEpoch: interrupted.epoch })).rejects.toThrow('not awaiting confirmation');
    const pending = await pause(root.id);
    await expect(store.controlTask(binding, root.id, 'withdraw-pause', { requestId: 'cleanup', expectedEpoch: pending.epoch })).rejects.toMatchObject({ code: 'CONTROL_NOT_SETTLED' });
    expect((await read(worker.id)).effects.remote.cleanupPending).toBe(true);
});

it('linearizes a continuation journal write with the pause barrier while preserving settlement writes', async () => {
    const { root, worker, claim } = await runningTree();
    await store.setShared(binding, 'admitted-before-pause', { requestId: 'first' }, {}, claim, true);
    const pending = await pause(root.id), before = await store.events(binding);
    expect(await store.setShared(binding, 'held-response', { private: 'answer' }, {}, claim, true)).toBeUndefined();
    expect(await store.getShared(binding, 'held-response')).toBeUndefined(); expect(await store.events(binding)).toEqual(before);
    await store.setShared(binding, 'observation', { terminal: false }, {}, claim);
    await store.controlTask(binding, root.id, 'withdraw-pause', { requestId: 'withdraw', expectedEpoch: pending.epoch });
    expect(await store.setShared(binding, 'held-response', { requestId: 'same-operation' }, {}, claim, true)).toMatchObject({ version: 1 });
    await store.cancelTask(binding, root.id);
    await expect(store.setShared(binding, 'late-response', null, {}, claim, true)).rejects.toThrow('Stale effect claim');
    await store.sweep(binding);
    expect((await read(worker.id)).effects.remote.cleanupPending).toBe(true);
});

it.each(['session', 'ancestor', 'task'] as const)('holds new continuation writes under %s pause', async owner => {
    const { ancestor, worker, claim } = await runningTree();
    if (owner === 'session') await store.setSessionStatus(binding, 'suspended');
    else await pause(owner === 'ancestor' ? ancestor.id : worker.id);
    expect(await store.setShared(binding, 'response', null, {}, claim, true)).toBeUndefined();
    expect(await store.getShared(binding, 'response')).toBeUndefined();
});
