import { afterEach, expect, it, vi } from 'vitest';
import { createVFS, MemoryBackend, type ISeqFileTransaction } from '@nestos/vfs-core';
import { Kernel, type DurableTaskProgram, type TaskRecord, type WaitSpec } from './index';
import { hydrateTaskWaitEventsTx } from './infrastructure/seqfile/store-helpers';
import { taskPath } from './infrastructure/seqfile/seqfile-core';

const releases: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of releases.splice(0).reverse()) await close(); });
const echo: DurableTaskProgram<null, string, string> = {
    manifest: { kind: 'hydration.echo', version: '1' },
    init: input => ({ state: null, next: { type: 'complete', output: input } }),
    reduce() { throw new Error('Unexpected echo input'); },
};
type CollectorState = { ids: string[]; seen: string[] };
const collector: DurableTaskProgram<CollectorState, string[], string[]> = {
    manifest: { kind: 'hydration.collector', version: '1' },
    init: ids => ({ state: { ids, seen: [] }, next: { type: 'wait', on: {
        type: 'all', waits: [...ids.map(id => ({ type: 'task' as const, id })), { type: 'signal', id: 'go' }],
    } } }),
    reduce: (state, event) => {
        const seen = event.type === 'task-exited' ? [...state.seen, String(event.exit.output)] : state.seen;
        return { state: { ...state, seen }, next: seen.length === 3
            ? { type: 'complete', output: seen }
            : { type: 'wait', on: { type: 'all', waits: state.ids.map(id => ({ type: 'task', id })) } } };
    },
};

async function setup() {
    const { manager } = await createVFS({ rootBackend: new MemoryBackend() });
    const fs = await manager.openFileSystem('/');
    releases.push(() => manager.dispose());
    const make = async () => {
        const kernel = new Kernel({ catalog: { fs, rootPath: '/catalog' }, pollMs: 0 });
        kernel.registerStorageResolver({ kind: 'test', resolve: async () => ({ fs, rootPath: '/s' }) });
        kernel.registerProgram(echo); kernel.registerProgram(collector); await kernel.initialize();
        releases.push(async () => { kernel.dispose(); await kernel.waitIdle(); });
        return kernel;
    };
    const kernel = await make();
    const session = await kernel.createSession({ id: 's', storage: { kind: 'test', locator: null } });
    const first = await session.submit({ program: echo.manifest, input: 'first' });
    const second = await session.submit({ program: echo.manifest, input: 'second' });
    await Promise.all([first.wait({ timeoutMs: 2000 }), second.wait({ timeoutMs: 2000 })]);
    const waiter = await session.submit({ program: collector.manifest, input: [first.id, second.id], deferStart: true,
        dependsOn: [{ task: first.id }, { task: second.id }] });
    return { kernel, make, session, fs, first, second, waiter };
}

function trackedReads(tx: ISeqFileTransaction) {
    const reads = vi.fn(tx.getEntry.bind(tx));
    const tracked = new Proxy(tx, { get(target, key) {
        if (key === 'getEntry') return reads;
        const value = Reflect.get(target, key);
        return typeof value === 'function' ? value.bind(target) : value;
    } });
    return { reads, tracked };
}

it.each(['any', 'all', 'quorum'] as const)('reuses existing durable exit receipts without target reads for %s waits', async type => {
    const { fs, first, second, waiter } = await setup();
    const task = (await waiter.status()).task;
    const waits = [first.id, second.id, first.id].map(id => ({ type: 'task' as const, id }));
    const wait: WaitSpec = type === 'quorum' ? { type, required: 2, waits } : { type, waits };
    await fs.meta.seq!.transaction!(async tx => {
        const { reads, tracked } = trackedReads(tx);
        const result = await hydrateTaskWaitEventsTx(tracked, '/s', { ...task, wait });
        expect(reads).not.toHaveBeenCalled();
        expect(result.pendingEvents).toEqual(task.pendingEvents);
        expect(result.pendingEvents).toHaveLength(2);
    });
});

it('reads only a missing receipt target and preserves complete outputs through nested waits', async () => {
    const { fs, first, second, waiter } = await setup();
    const task = (await waiter.status()).task;
    const pendingEvents = task.pendingEvents.filter(event => event.type === 'task-exited' && event.taskId === first.id);
    const wait: WaitSpec = { type: 'all', waits: [{ type: 'task', id: first.id },
        { type: 'any', waits: [{ type: 'task', id: second.id }, { type: 'task', id: first.id }] }] };
    await fs.meta.seq!.transaction!(async tx => {
        const { reads, tracked } = trackedReads(tx);
        const result = await hydrateTaskWaitEventsTx(tracked, '/s', { ...task, pendingEvents, wait });
        expect(reads.mock.calls).toEqual([[taskPath('/s', second.id), 'record']]);
        expect(result.pendingEvents).toEqual(task.pendingEvents);
    });
});

it('still rejects self waits before trusting an existing receipt and rejects absent targets', async () => {
    const { fs, waiter } = await setup();
    const task = (await waiter.status()).task;
    const receipt = task.pendingEvents.find(event => event.type === 'task-exited')!;
    const self: TaskRecord = { ...task, wait: { type: 'task', id: task.id },
        pendingEvents: [{ ...receipt, taskId: task.id } as typeof receipt] };
    await fs.meta.seq!.transaction!(async tx => {
        const { reads, tracked } = trackedReads(tx);
        await expect(hydrateTaskWaitEventsTx(tracked, '/s', self)).rejects.toThrow('Task cannot wait for itself');
        expect(reads).not.toHaveBeenCalled();
        await expect(hydrateTaskWaitEventsTx(tracked, '/s', { ...task, wait: { type: 'task', id: 'missing' } }))
            .rejects.toThrow('Wait target not found: missing');
    });
});

it('preserves pending receipts across Kernel reconstruction and hydrates a consumed target on a later await', async () => {
    const { kernel, make, first, second, waiter } = await setup();
    await waiter.start();
    await expect.poll(async () => (await waiter.status()).task.status).toBe('waiting');
    const before = (await waiter.status()).task.pendingEvents;
    const targets = await Promise.all([first.status(), second.status()]);
    kernel.dispose(); await kernel.waitIdle();
    const reopened = await make();
    await reopened.recoverSession('s', { takeover: true });
    const restored = await (await reopened.openSession('s')).attachTask(waiter.id);
    expect((await restored.status()).task.pendingEvents).toEqual(before);
    await restored.signal({ type: 'go' });
    expect((await restored.wait({ timeoutMs: 2000 })).output).toEqual(['first', 'second', 'first']);
    const sourceTasks = await Promise.all([reopened.inspectTask(first.id), reopened.inspectTask(second.id)]);
    expect(sourceTasks.map(snapshot => snapshot.task)).toEqual(targets.map(snapshot => snapshot.task));
});
