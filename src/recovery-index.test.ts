import {afterEach, expect, it, vi} from 'vitest';
import {createVFS, MemoryBackend} from '@nestos/vfs-core';
import {Kernel, type DurableTaskProgram} from './index';

const releases: Array<() => Promise<void>> = [];
afterEach(async () => {for (const release of releases.splice(0).reverse()) await release(); vi.restoreAllMocks();});
const program: DurableTaskProgram<null, string> = {manifest: {kind: 'recovery.fixture', version: '1'},
    init: input => ({state: null, next: input === 'wait' ? {type: 'wait', on: {type: 'signal'}} : {type: 'complete', output: input}}),
    reduce: () => ({state: null, next: {type: 'complete', output: 'done'}})};
async function fixture() {
    const backend = new MemoryBackend(), {manager} = await createVFS({rootBackend: backend}), fs = await manager.openFileSystem('/');
    releases.push(() => manager.dispose());
    const make = async () => {
        const kernel = new Kernel({catalog: {fs, rootPath: '/catalog'}});
        kernel.registerStorageResolver({kind: 'fixture', resolve: async () => ({fs, rootPath: '/s'})});
        kernel.registerProgram(program); await kernel.initialize();
        releases.push(async () => {kernel.dispose(); await kernel.waitIdle();}); return kernel;
    };
    const kernel = await make(), session = await kernel.createSession({id: 's', storage: {kind: 'fixture', locator: null}});
    return {backend, fs, kernel, session, make};
}

it('publishes an exact recovery reference atomically at admission and removes it at the terminal transition', async () => {
    const f = await fixture(), task = await f.session.submit({program: program.manifest, input: 'wait',
        recovery: {scope: 'fixture.scheduler', until: 'terminal'}});
    expect(await f.kernel.listRecoveryTasks('fixture.scheduler')).toEqual([{sessionId: 's', taskId: task.id}]);
    f.kernel.dispose(); await f.kernel.waitIdle(); const observer = await f.make();
    const sessions = vi.spyOn(observer, 'listSessions'), tasks = vi.spyOn(observer, 'listSessionTasks');
    expect(await observer.listRecoveryTasks('fixture.scheduler')).toEqual([{sessionId: 's', taskId: task.id}]);
    expect(sessions).not.toHaveBeenCalled(); expect(tasks).not.toHaveBeenCalled();
    await (await observer.attachTask('s', task.id)).cancel();
    expect(await observer.listRecoveryTasks('fixture.scheduler')).toEqual([]);
});

it('keeps terminal history responsibility until explicit matching acknowledgement, including across restart', async () => {
    const f = await fixture(), task = await f.session.submit({program: program.manifest, input: 'done',
        recovery: {scope: 'fixture.history', key: 'operation', until: 'acknowledged'}});
    await task.wait({until: 'settled', timeoutMs: 2000});
    f.kernel.dispose(); await f.kernel.waitIdle(); const observer = await f.make();
    expect(await observer.listRecoveryTasks('fixture.history')).toEqual([{sessionId: 's', taskId: task.id, key: 'operation'}]);
    await expect(observer.acknowledgeTaskRecovery('s', task.id, 'other')).rejects.toMatchObject({code: 'CONFLICT'});
    await observer.acknowledgeTaskRecovery('s', task.id, 'fixture.history');
    await observer.acknowledgeTaskRecovery('s', task.id, 'fixture.history');
    expect(await observer.listRecoveryTasks('fixture.history')).toEqual([]);
});

it('does not acknowledge an unfinished Task or lose its recovery responsibility', async () => {
    const f = await fixture(), task = await f.session.submit({program: program.manifest, input: 'wait',
        recovery: {scope: 'fixture.history', until: 'acknowledged'}});
    await expect(f.kernel.acknowledgeTaskRecovery('s', task.id, 'fixture.history')).rejects.toMatchObject({code: 'CONFLICT'});
    expect(await f.kernel.listRecoveryTasks('fixture.history')).toHaveLength(1);
});

it('rolls back both the Task admission and responsibility when its index write fails', async () => {
    const f = await fixture(), write = f.backend.records.setRecordField.bind(f.backend.records);
    const spy = vi.spyOn(f.backend.records, 'setRecordField').mockImplementation(async (path, field, value) => {
        if (field.includes('recovery/')) throw new Error('INDEX_WRITE_REJECTED');
        return write(path, field, value);
    });
    await expect(f.session.submit({program: program.manifest, input: 'wait', requestId: 'same-operation',
        recovery: {scope: 'fixture.scheduler', until: 'terminal'}})).rejects.toMatchObject({code: 'EIO'});
    spy.mockRestore();
    expect(await f.kernel.listRecoveryTasks('fixture.scheduler')).toEqual([]);
    expect(await f.session.listTasks()).toEqual([]);
    const retry = await f.session.submit({program: program.manifest, input: 'wait', requestId: 'same-operation',
        recovery: {scope: 'fixture.scheduler', until: 'terminal'}});
    expect(await f.kernel.listRecoveryTasks('fixture.scheduler')).toEqual([{sessionId: 's', taskId: retry.id}]);
});

it('indexes shared admission intents in the same transaction and clears only the intended operation', async () => {
    const f = await fixture();
    for (const key of ['first', 'second']) await f.session.setShared(key, {phase: 'prepared'},
        {expectedVersion: null, recovery: {scope: 'fixture.admission', pending: true}});
    await expect(f.session.setShared('first', {phase: 'accepted'},
        {expectedVersion: 0, recovery: {scope: 'fixture.admission', pending: false}})).rejects.toMatchObject({code: 'CONFLICT'});
    expect(await f.kernel.listRecoveryShared('fixture.admission')).toHaveLength(2);
    await f.session.setShared('first', {phase: 'accepted'}, {recovery: {scope: 'fixture.admission', pending: false}});
    expect(await f.kernel.listRecoveryShared('fixture.admission')).toEqual([{sessionId: 's', key: 'second'}]);
    await f.session.setShared('second', {phase: 'still-prepared'});
    expect(await f.kernel.listRecoveryShared('fixture.admission')).toEqual([{sessionId: 's', key: 'second'}]);
    await f.session.deleteShared('second');
    expect(await f.kernel.listRecoveryShared('fixture.admission')).toEqual([]);
});

it('removes a deleted Session from both recovery indexes without leaving dangling responsibilities', async () => {
    const f = await fixture();
    await f.session.submit({program: program.manifest, input: 'wait', recovery: {scope: 'fixture.history', until: 'acknowledged'}});
    await f.session.setShared('intent', {}, {recovery: {scope: 'fixture.admission', pending: true}});
    await f.kernel.closeSession('s', true); await f.kernel.removeSession('s');
    expect(await f.kernel.listRecoveryTasks('fixture.history')).toEqual([]);
    expect(await f.kernel.listRecoveryShared('fixture.admission')).toEqual([]);
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual([]);
});

it('rebinds exported responsibilities to the destination catalog and preserves paused execution', async () => {
    const f = await fixture(), task = await f.session.submit({program: program.manifest, input: 'wait',
        recovery: {scope: 'fixture.scheduler', until: 'terminal'}});
    await f.session.setShared('intent', {}, {recovery: {scope: 'fixture.admission', pending: true}});
    await f.session.suspend();
    await expect.poll(async () => (await f.session.stat()).acknowledged).toBe(true);
    const lines: string[] = []; for await (const line of f.kernel.archives.exportSession('s', {purpose: 'recover'})) lines.push(line);
    const target = new Kernel({catalog: {fs: f.fs, rootPath: '/destination'}});
    target.registerStorageResolver({kind: 'fixture', resolve: async () => ({fs: f.fs, rootPath: '/imported'})});
    target.registerProgram(program); await target.initialize();
    releases.push(async () => {target.dispose(); await target.waitIdle();});
    await target.archives.importSession((async function* () {yield* lines;})(),
        {mode: 'restore', requestId: 'restore', storage: {kind: 'fixture', locator: null}, assertExclusive: async () => {}});
    expect((await target.sessionStat('s')).phase).toBe('paused');
    expect(await target.listRuntimeRecoverySessions()).toEqual(['s']);
    expect(await target.listRecoveryTasks('fixture.scheduler')).toEqual([{sessionId: 's', taskId: task.id}]);
    expect(await target.listRecoveryShared('fixture.admission')).toEqual([{sessionId: 's', key: 'intent'}]);
    await (await target.attachTask('s', task.id)).cancel();
    expect(await target.listRecoveryTasks('fixture.scheduler')).toEqual([]);
    expect(await f.kernel.listRecoveryTasks('fixture.scheduler')).toHaveLength(1);
});

it('reads empty pending indexes without enumerating a large unrelated historical catalog', async () => {
    const f = await fixture();
    await f.fs.meta.seq!.transaction!(async tx => {
        for (let i = 0; i < 2000; i++) await tx.setEntry('/catalog/catalog.seq', `session/history-${i}`, JSON.stringify({id: `history-${i}`, status: 'open'}));
    });
    const sessions = vi.spyOn(f.kernel, 'listSessions'), inspect = vi.spyOn(f.kernel, 'inspectSession');
    expect(await f.kernel.listRecoveryTasks('fixture.scheduler')).toEqual([]);
    expect(await f.kernel.listRecoveryShared('fixture.admission')).toEqual([]);
    expect(sessions).not.toHaveBeenCalled(); expect(inspect).not.toHaveBeenCalled();
});

it('reports corrupt indexes without falling back to historical enumeration', async () => {
    const f = await fixture(), sessions = vi.spyOn(f.kernel, 'listSessions');
    await f.fs.meta.seq!.setEntry('/catalog/catalog.seq', 'recovery/fixture.scheduler/broken', '{');
    await expect(f.kernel.listRecoveryTasks('fixture.scheduler')).rejects.toMatchObject({code: 'STORAGE_CORRUPT'});
    expect(sessions).not.toHaveBeenCalled();
});

it('tracks ordinary waiting Tasks across restart and removes their runtime duty on settlement', async () => {
    const f = await fixture(), task = await f.session.submit({program: program.manifest, input: 'wait'});
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual(['s']);
    f.kernel.dispose(); await f.kernel.waitIdle(); const observer = await f.make();
    const history = vi.spyOn(observer, 'listSessions');
    expect(await observer.listRuntimeRecoverySessions()).toEqual(['s']);
    expect(history).not.toHaveBeenCalled();
    await (await observer.attachTask('s', task.id)).cancel();
    expect(await observer.listRuntimeRecoverySessions()).toEqual([]);
});

it('does not index deferred admission until started and rolls back failed runtime indexing', async () => {
    const f = await fixture();
    const deferred = await f.session.submit({program: program.manifest, input: 'wait', deferStart: true});
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual([]);
    await deferred.start();
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual(['s']);
    await deferred.cancel();
    const write = f.backend.records.setRecordField.bind(f.backend.records);
    const spy = vi.spyOn(f.backend.records, 'setRecordField').mockImplementation(async (path, field, value) => {
        if (field.includes('runtime-recovery/')) throw new Error('RUNTIME_INDEX_REJECTED');
        return write(path, field, value);
    });
    await expect(f.session.submit({program: program.manifest, input: 'wait'})).rejects.toMatchObject({code: 'EIO'});
    spy.mockRestore();
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual([]);
    expect(await f.session.listTasks()).toHaveLength(1);
});

it('retains cross-Session delivery responsibility through the settlement acknowledgement gap', async () => {
    const f = await fixture();
    const {SeqFileKernelStore} = await import('./index');
    const store = new SeqFileKernelStore({fs: f.fs, rootPath: '/catalog'}, async () => ({fs: f.fs, rootPath: '/s'}));
    const binding = await store.inspectSessionBinding('s');
    const message = {id: 'delivery', sourceSessionId: 's', targetSessionId: 'target', topic: 'fixture', payload: null,
        status: 'pending' as const, createdAt: Date.now()};
    await store.createOutboxMessage(binding, message);
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual(['s']);
    const delivered = await store.markMessageDelivered(binding, message.id);
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual(['s']);
    await store.acknowledgeMessageSettlement(binding, delivered, 'outbox');
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual([]);
});

it('indexes Session handles for shared Kernel resources and clears closed bindings', async () => {
    const f = await fixture(), {resourceResult} = await import('./public/resources');
    const task = await f.session.submit({program: program.manifest, input: 'wait', deferStart: true});
    const pool = await resourceResult(f.kernel.resources.create({requestId: 'pool', kind: 'pool', name: 'pool', capacity: 1}));
    await resourceResult(f.kernel.resources.share(pool.ref, {requestId: 'share', toSessionId: 's', rights: ['execute']}));
    const handle = await resourceResult(task.resources.open(pool.ref, {requestId: 'open', name: 'pool', rights: ['execute']}));
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual(['s']);
    const claim = await resourceResult(task.resources.acquire(handle, {requestId: 'take', quantity: 1}));
    await resourceResult(task.resources.release(claim, {requestId: 'release'}));
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual(['s']);
    await resourceResult(task.resources.close(handle, {requestId: 'close'}));
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual([]);
});

it('keeps a paused timer discoverable without resuming it during cold index reads', async () => {
    const f = await fixture();
    const timer: DurableTaskProgram = {manifest: {kind: 'fixture.timer', version: '1'},
        init: () => ({state: null, next: {type: 'wait', on: {type: 'timer', id: 'later', at: Date.now() + 60_000}}}),
        reduce: () => ({state: null, next: {type: 'complete', output: null}})};
    f.kernel.registerProgram(timer);
    const task = await f.session.submit({program: timer.manifest, input: null});
    await expect.poll(async () => (await task.snapshot()).task.wait?.type).toBe('timer');
    await f.session.suspend();
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual(['s']);
    expect((await f.session.stat()).phase).toBe('paused');
    await task.cancel();
    expect(await f.kernel.listRuntimeRecoverySessions()).toEqual([]);
});

it('rejects corrupt runtime references without scanning Session history', async () => {
    const f = await fixture(), history = vi.spyOn(f.kernel, 'listSessions');
    await f.fs.meta.seq!.setEntry('/catalog/catalog.seq', 'runtime-recovery/s/task/broken', 'other-session');
    await expect(f.kernel.listRuntimeRecoverySessions()).rejects.toMatchObject({code: 'STORAGE_CORRUPT'});
    expect(history).not.toHaveBeenCalled();
});
