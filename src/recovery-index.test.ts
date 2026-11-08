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
