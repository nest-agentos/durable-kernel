import {afterEach, expect, it, vi} from 'vitest';
import {createVFS, MemoryBackend} from '@nestos/vfs-core';
import {Kernel, type DurableTaskProgram} from './index';
import {indexPath, taskPath, eventsPath, appendEventTx} from './infrastructure/seqfile/seqfile-core';
import {ACTIVITY_REVISION, activityKey} from './infrastructure/seqfile/activity-index';
import {SeqFileKernelStore} from './infrastructure/seqfile/store';
import {CommitRescanner} from './runtime/commit-rescanner';

const releases: Array<() => Promise<void>> = [];
afterEach(async () => {for (const close of releases.splice(0).reverse()) await close();});
const interaction: DurableTaskProgram<null, 'approval' | 'input'> = {
    manifest: {kind: 'question', version: '1'},
    init: kind => ({state: null, actions: [{type: 'request-interaction', interaction: {id: 'q', kind, prompt: 'private prompt'}}],
        next: {type: 'wait', on: {type: 'interaction', id: 'q'}}}),
    reduce: () => ({state: null, next: {type: 'complete', output: 'answered'}}),
};
async function setup() {
    const backend = new MemoryBackend();
    const {manager} = await createVFS({rootBackend: backend}), fs = await manager.openFileSystem('/');
    releases.push(() => manager.dispose());
    const make = async () => {
        const kernel = new Kernel({catalog: {fs, rootPath: '/catalog'}});
        kernel.registerStorageResolver({kind: 'local', resolve: async () => ({fs, rootPath: '/s'})});
        kernel.registerProgram(interaction); await kernel.initialize();
        releases.push(async () => {kernel.dispose(); await kernel.waitIdle();}); return kernel;
    };
    const kernel = await make(), session = await kernel.createSession({id: 's', storage: {kind: 'local', locator: null}});
    const question = async (kind: 'approval' | 'input') => {
        const task = await session.submit({program: interaction.manifest, input: kind});
        await expect.poll(async () => (await task.status()).task.status).toBe('waiting'); return task;
    };
    return {backend, fs, kernel, session, make, question};
}

it('counts all pending interactions and retains the remaining approval after one answer', async () => {
    const {kernel, question} = await setup(), a = await question('approval'), b = await question('approval'), c = await question('input');
    expect(await kernel.sessionActivity('s')).toMatchObject({counts: {waiting: 3}, pending: {approval: 2, input: 1}});
    await a.respond({interactionId: 'q', value: 'approved'}); await a.wait({timeoutMs: 2000});
    expect(await kernel.sessionActivity('s')).toMatchObject({counts: {waiting: 2, succeeded: 1}, pending: {approval: 1, input: 1}});
    await b.cancel(); await c.cancel();
    expect(await kernel.sessionActivity('s')).toMatchObject({counts: {cancelled: 2}, pending: {approval: 0, input: 0}});
});

it('reconstructs paused pending questions on a cold observer without opening or executing a Session', async () => {
    const {kernel, question, session, make} = await setup(); await question('input');
    await session.suspend(); await expect.poll(async () => (await session.stat()).acknowledged).toBe(true);
    kernel.dispose(); await kernel.waitIdle(); const observer = await make();
    const open = vi.spyOn(observer, 'openSession'), reduce = vi.spyOn(interaction, 'reduce');
    try {
        expect(await observer.sessionActivity('s')).toMatchObject({sessionStatus: 'suspended', counts: {paused: 1}, pending: {input: 1}});
        expect(open).not.toHaveBeenCalled(); expect(reduce).not.toHaveBeenCalled();
    } finally {reduce.mockRestore();}
});

it('reads compact projections without Task bodies and batches missing legacy bodies without writing', async () => {
    const {backend, fs, kernel, question} = await setup(), active = await question('input'), done = await question('approval'); await done.cancel();
    await kernel.waitIdle();
    const bodies = vi.spyOn(backend.records, 'getRecordFieldsMany'), writes = vi.spyOn(backend.records, 'setRecordField');
    try {
        expect((await kernel.sessionActivity('s')).pending.input).toBe(1);
        expect(bodies.mock.calls.flatMap(([rows]) => rows).filter(row => row.path.endsWith('/task.seq'))).toEqual([]);
        expect(writes).not.toHaveBeenCalled();
        await fs.meta.seq!.transaction!(async tx => {for (const id of [active.id, done.id]) await tx.deleteEntry(indexPath('/s'), activityKey(id));});
        bodies.mockClear(); writes.mockClear();
        expect((await kernel.sessionActivity('s')).pending.input).toBe(1);
        expect(bodies.mock.calls.flatMap(([rows]) => rows).filter(row => row.path.endsWith('/task.seq')).map(row => row.path).sort()).toEqual([taskPath('/s', active.id), taskPath('/s', done.id)].sort());
        expect(writes).not.toHaveBeenCalled();
    } finally {bodies.mockRestore(); writes.mockRestore();}
});

it('repairs damaged activity projections during recovery and preserves a monotonic revision', async () => {
    const {fs, kernel, question, make} = await setup(), task = await question('input'), before = await kernel.sessionActivity('s');
    kernel.dispose(); await kernel.waitIdle();
    await fs.meta.seq!.transaction!(tx => tx.setEntry(indexPath('/s'), activityKey(task.id), '{'));
    await expect(kernel.sessionActivity('s')).rejects.toMatchObject({code: 'STORAGE_CORRUPT'});
    await fs.meta.seq!.transaction!(tx => tx.setEntry(indexPath('/s'), activityKey(task.id), '{"broken":true}'));
    const restarted = await make(); await restarted.recoverSession('s', {takeover: true});
    const after = await restarted.sessionActivity('s'); expect(after.pending.input).toBe(1); expect(after.revision).toBeGreaterThan(before.revision);
});

it('does not overwrite a concurrent Task completion with the recovery enumeration snapshot', async () => {
    const {fs, kernel, question} = await setup(), task = await question('input'), binding = {fs, rootPath: '/s'};
    const store = new SeqFileKernelStore({fs, rootPath: '/catalog'}, async () => binding), original = store.listTasks.bind(store);
    const list = vi.spyOn(store, 'listTasks').mockImplementationOnce(async current => {
        const stale = await original(current);
        await task.respond({interactionId: 'q', value: 'answer'}); await task.wait({timeoutMs: 2000});
        return stale;
    });
    try {
        await store.recover(binding);
        expect(await kernel.sessionActivity('s')).toMatchObject({counts: {succeeded: 1, waiting: 0}, pending: {input: 0}});
        expect((await task.snapshot()).task.status).toBe('succeeded');
    } finally {list.mockRestore();}
});

it('retains Tasks created after recovery enumerated the directory', async () => {
    const {fs, kernel, question} = await setup(), binding = {fs, rootPath: '/s'};
    const store = new SeqFileKernelStore({fs, rootPath: '/catalog'}, async () => binding), original = store.listTasks.bind(store);
    const list = vi.spyOn(store, 'listTasks').mockImplementationOnce(async current => {
        const stale = await original(current); await question('input'); return stale;
    });
    try {
        await store.recover(binding);
        expect(await kernel.sessionActivity('s')).toMatchObject({counts: {waiting: 1}, pending: {input: 1}});
        expect((await kernel.listSessionTaskPage('s')).items).toHaveLength(1);
    } finally {list.mockRestore();}
});

it('keeps content streaming off the activity revision and labels remote structural changes separately', async () => {
    const {fs, kernel, question} = await setup(); await question('input'); await kernel.waitIdle();
    const before = await kernel.sessionActivity('s');
    const binding = {fs, rootPath: '/s'}, store = new SeqFileKernelStore({fs, rootPath: '/catalog'}, async () => binding);
    for (let i = 0; i < 5; i++) await store.appendEvent(binding, 's', undefined, 'fixture.delta', {index: i});
    const after = await kernel.sessionActivity('s'); expect(after.revision).toBe(before.revision); expect(after.asOfSequence).toBeGreaterThan(before.asOfSequence);
    await fs.driver.createFile({name: 'markers', type: 'seqfile'});
    const changed = vi.fn(), failed = vi.fn(), scanner = new CommitRescanner(10, () => [{id: 's', fs, structureEntries: [1], entries: [
        {fileIdOrPath: '/markers', key: 'content'}, {fileIdOrPath: indexPath('/s'), key: ACTIVITY_REVISION}]}], changed, failed);
    try {
        await expect.poll(() => changed.mock.calls.length).toBeGreaterThan(0); changed.mockClear();
        await fs.meta.seq!.transaction!(tx => tx.increment('/markers', 'content'));
        await expect.poll(() => changed.mock.calls.length).toBe(1); expect(changed).toHaveBeenLastCalledWith('s', 'content');
        changed.mockClear(); await fs.meta.seq!.transaction!(tx => tx.increment(indexPath('/s'), ACTIVITY_REVISION));
        await expect.poll(() => changed.mock.calls.length).toBe(1); expect(changed).toHaveBeenLastCalledWith('s', 'structure'); expect(failed).not.toHaveBeenCalled();
    } finally {scanner.dispose();}
});
it('persists execution activity time without reading bodies, and excludes reads and log pruning',async()=>{
    const {kernel,fs,backend,question,make}=await setup(),task=await question('input');await kernel.waitIdle();
    const before=await kernel.sessionActivity('s');expect(before.lastActivityAt).toBeGreaterThan(0);
    const binding={fs,rootPath:'/s'},store=new SeqFileKernelStore({fs,rootPath:'/catalog'},async()=>binding);
    await store.appendEvent(binding,'s',undefined,'fixture.read');
    await fs.meta.seq!.transaction!(tx=>appendEventTx(tx,'/s','s',task.id,'task.events.pruned'));
    const bodies=vi.spyOn(backend.records,'getRecordFieldsMany');
    const after=await kernel.sessionActivity('s');expect(after.lastActivityAt).toBe(before.lastActivityAt);
    expect(bodies.mock.calls.flatMap(([rows])=>rows).filter(row=>row.path.endsWith('/task.seq'))).toEqual([]);bodies.mockRestore();
    const observer=await make();expect((await observer.sessionActivity('s')).lastActivityAt).toBe(before.lastActivityAt);
    await fs.meta.seq!.deleteEntry(eventsPath('/s'),'last-execution-activity-at');expect((await observer.sessionActivity('s')).lastActivityAt).toBeUndefined();
});
