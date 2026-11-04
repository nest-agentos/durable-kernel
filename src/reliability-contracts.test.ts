import {afterEach, expect, it, vi} from 'vitest';
import {createVFS, MemoryBackend} from '@nestos/vfs-core';
import {Kernel, type KernelOptions} from './index';
import {CommitRescanner, type CommitScope} from './runtime/commit-rescanner';
import {ArchiveHash} from './application/archive-hash';
import {scheduleProgram} from './application/schedule-program';

const releases: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of releases.splice(0).reverse()) await close(); vi.useRealTimers(); });
const program = {manifest: {kind: 'echo', version: '1'}, init: (input: unknown) => ({state: null,
    next: {type: 'complete' as const, output: input}}), reduce: () => ({state: null, next: {type: 'complete' as const, output: 'done'}})};
async function setup(options: Partial<KernelOptions> = {}) {
    const {manager} = await createVFS({rootBackend: new MemoryBackend()}); const fs = await manager.openFileSystem('/');
    releases.push(() => manager.dispose());
    const make = async (catalog = '/catalog') => {
        const k = new Kernel({catalog: {fs, rootPath: catalog}, ...options}); k.registerProgram(program);
        k.registerStorageResolver({kind: 'test', resolve: async ref => ({fs, rootPath: ref.locator as string})});
        await k.initialize(); releases.push(async () => {k.dispose(); await k.waitIdle();}); return k;
    };
    const kernel = await make();
    const session = await kernel.createSession({id: 's', storage: {kind: 'test', locator: '/session'}});
    return {fs, kernel, session, make};
}
const pause = async (session: Awaited<ReturnType<typeof setup>>['session']) => {
    await session.suspend(); await expect.poll(async () => (await session.stat()).acknowledged).toBe(true);
};
const chunks = async function* (text: string) { yield text.slice(0, 11); yield text.slice(11); };
async function exported(kernel: Kernel, purpose: 'inspect' | 'recover') {
    const result: string[] = []; for await (const part of kernel.archives.exportSession('s', {purpose})) result.push(part);
    return result.join('');
}

it('accepts a stable signal once across concurrent submissions and replays it after terminal completion', async () => {
    const {kernel, session} = await setup({maxConcurrent: 0});
    const task = await session.submit({program: program.manifest, input: null});
    const results = await Promise.all([task.signal({type: 'go', payload: {a: 1, b: 2}}, {requestId: 'once'}),
        task.signal({type: 'go', payload: {b: 2, a: 1}}, {requestId: 'once'})]);
    expect(results.map(r => r.replayed).sort()).toEqual([false, true]); expect((await task.stats()).pendingInputs).toBe(1);
    await task.cancel();
    expect((await task.signal({type: 'go', payload: {a: 1, b: 2}}, {requestId: 'once'})).replayed).toBe(true);
    await expect(task.signal({type: 'other'}, {requestId: 'once'})).rejects.toMatchObject({code: 'SIGNAL_IDENTITY_CONFLICT'});
    await expect(task.signal({type: 'new'})).rejects.toMatchObject({code: 'TASK_TERMINAL'});
    expect((await kernel.task('s', task.id)).pendingEvents).toHaveLength(1);
});

it('retires one worker without pausing a shared Session served by another worker', async () => {
    const {kernel, session, make} = await setup(); const replacement = await make(); await replacement.openSession('s');
    expect(await kernel.drain()).toEqual({status: 'drained', localIdle: true});
    expect((await session.stat()).phase).toBe('open');
    const task = await (await replacement.openSession('s')).submit({program: program.manifest, input: 'continued'});
    expect((await task.wait({timeoutMs: 1000})).output).toBe('continued');
});

it('keeps accepted external work and its heartbeat alive after a blocked drain', async () => {
    const {kernel, session} = await setup(); let finish!: () => void; let aborted = false;
    kernel.registerEffect({kind: 'hold', version: '1', execute: async (_request, context) => {
        context.abortSignal.addEventListener('abort', () => {aborted = true;}); await new Promise<void>(r => {finish = r;}); return 'done';
    }});
    kernel.registerProgram({manifest: {kind: 'fx', version: '1'}, init: () => ({state: null, actions: [
        {type: 'effect', effect: {id: 'e', kind: 'hold', version: '1', request: null, idempotencyKey: 'e'}}],
        next: {type: 'wait', on: {type: 'effect', id: 'e'}}}), reduce: program.reduce});
    await session.submit({program: {kind: 'fx', version: '1'}, input: null}); await expect.poll(() => !!finish).toBe(true);
    expect(await kernel.drain({timeoutMs: 10})).toEqual({status: 'blocked', localIdle: false});
    expect(aborted).toBe(false); expect((await session.stat()).phase).toBe('open'); finish();
    expect(await kernel.drain({timeoutMs: 1000})).toEqual({status: 'drained', localIdle: true});
});

it('runs a durable one-shot schedule after reconstruction and preserves occurrence identity', async () => {
    const {kernel, session, make} = await setup();
    const at = Date.now() + 120, schedule = await session.schedule({requestId: 'clock', at, task: {program: program.manifest, input: 'scheduled'}});
    await expect.poll(async () => (await schedule.status()).task.status).toBe('waiting');
    expect(await kernel.drain()).toMatchObject({status: 'drained'});
    const replacement = await make(); await replacement.recoverSession('s');
    const current = await replacement.openTask(schedule.id); expect((await current.wait({timeoutMs: 2000})).status).toBe('succeeded');
    const tasks = await replacement.listSessionTasks('s');
    expect(tasks.filter(t => t.parentTaskId === schedule.id)).toHaveLength(1);
    expect(tasks.find(t => t.parentTaskId === schedule.id)?.spawnKey).toBe(`scheduled:${at}`);
});

it('limits fixed-rate schedules, supports cancellation, and rejects unsafe timer specifications', async () => {
    const {kernel, session} = await setup();
    const schedule = await session.schedule({requestId: 'repeat', at: Date.now() + 10, everyMs: 15, maxRuns: 3,
        missed: 'catch-up', task: {program: program.manifest, input: null}});
    expect((await schedule.wait({timeoutMs: 2000})).status).toBe('succeeded');
    expect((await kernel.listSessionTasks('s')).filter(t => t.parentTaskId === schedule.id)).toHaveLength(3);
    const future = await session.schedule({requestId: 'cancel-clock', at: Date.now() + 10000, task: {program: program.manifest, input: null}});
    await future.cancel(); expect((await future.stat()).exit?.status).toBe('cancelled');
    await expect(session.schedule({requestId: 'bad', at: 0, everyMs: 0, task: {program: program.manifest, input: null}})).rejects.toMatchObject({code: 'INVALID_SPEC'});
});

it('skips missed fixed-rate slots deterministically from the persisted firing time', () => {
    const spec = {requestId: 'skip', at: 1000, everyMs: 100, task: {program: program.manifest, input: null}};
    const state = {spec, nextAt: 1000, runs: 0};
    const event = {type: 'timer-fired' as const, id: 'schedule', at: 1000, firedAt: 1480};
    const first = scheduleProgram.reduce(state, event), replay = scheduleProgram.reduce(state, event);
    expect(first).toEqual(replay);
    expect(first).toMatchObject({state: {nextAt: 1500, runs: 1}, actions: [{type: 'spawn', spawnKey: 'scheduled:1400'}]});
});

it('keeps the final occurrence wait when an earlier child or stale timer is consumed first', () => {
    const spec = {requestId: 'final', at: 1000, everyMs: 100, maxRuns: 2, task: {program: program.manifest, input: null}};
    const state = {spec, nextAt: 1200, runs: 2, child: 'scheduled:1100'};
    const older = scheduleProgram.reduce(state, {type: 'task-exited', taskId: 'older', spawnKey: 'scheduled:1000',
        exit: {taskId: 'older', status: 'succeeded', completedAt: 2000}});
    expect(older).toMatchObject({next: {type: 'wait', on: {type: 'child', spawnKey: 'scheduled:1100'}}});
    expect(scheduleProgram.reduce(state, {type: 'timer-fired', id: 'schedule', at: 1200, firedAt: 2000})).toEqual(older);
});

it('batch-reads only commit markers while unchanged scopes remain idle', async () => {
    vi.useFakeTimers(); const {manager} = await createVFS({rootBackend: new MemoryBackend()}); const fs = await manager.openFileSystem('/');
    const read = vi.fn(fs.meta.seq!.getEntriesMany.bind(fs.meta.seq!)), changed = vi.fn();
    const tx = vi.fn(() => {throw new Error('Writer transaction must not be used');});
    const observed = new Proxy(fs, {get(target, key) { return key === 'meta' ? {seq: {transaction: tx, getEntriesMany: read}} : Reflect.get(target, key, target); }});
    const scan = new CommitRescanner(50, () => Array.from({length: 200}, (_, i) => ({id: String(i), fs: observed,
        entries: [{fileIdOrPath: '/events.seq', key: String(i)}]})), changed, error => {throw error;});
    await vi.advanceTimersByTimeAsync(0); expect(read).toHaveBeenCalledTimes(1); expect(tx).not.toHaveBeenCalled(); expect(changed).toHaveBeenCalledTimes(200);
    changed.mockClear(); read.mockClear(); await vi.advanceTimersByTimeAsync(150);
    expect(read).toHaveBeenCalledTimes(3); expect(tx).not.toHaveBeenCalled(); expect(changed).not.toHaveBeenCalled();
    scan.dispose(); await manager.dispose();
});

it('requires a reliable host wake source before disabling safety scans', async () => {
    const {fs} = await setup();
    expect(() => new Kernel({catalog: {fs}, rescanMs: 0})).toThrow('wakeSource');
    const wake = vi.fn(() => () => {}); const kernel = new Kernel({catalog: {fs, rootPath: '/host'}, rescanMs: 0, wakeSource: {subscribe: wake}});
    await kernel.initialize(); expect(wake).toHaveBeenCalledTimes(1); kernel.dispose();
});

it('establishes newly registered Session markers immediately without waiting for the safety interval', async () => {
    vi.useFakeTimers();
    const {manager} = await createVFS({rootBackend: new MemoryBackend()}), fs = await manager.openFileSystem('/');
    const scopes: CommitScope[] = [], changed = vi.fn();
    const scan = new CommitRescanner(1000, () => scopes, changed, error => {throw error;});
    try {
        await vi.advanceTimersByTimeAsync(0);
        scopes.push({id: 'new', fs, entries: [{fileIdOrPath: '/events.seq', key: 'sequence'}]});
        scan.wake(); await vi.advanceTimersByTimeAsync(0);
        expect(changed).toHaveBeenCalledTimes(1); expect(changed.mock.calls[0][0]).toBe('new');
        changed.mockClear(); await vi.advanceTimersByTimeAsync(1000);
        expect(changed).not.toHaveBeenCalled();
    } finally {scan.dispose(); await manager.dispose();}
});

it.each(['','abc','a'.repeat(1000)])('matches standard SHA-256 across chunk boundaries', async text => {
    const {createHash} = await import('node:crypto'), hash = new ArchiveHash();
    for (let offset = 0; offset < text.length; offset += 13) hash.update(text.slice(offset, offset + 13));
    expect(hash.finish()).toBe(createHash('sha256').update(text).digest('hex'));
});

it('hashes a large Unicode archive using bounded blocks', async () => {
    const {createHash} = await import('node:crypto'), text = '中文🙂'.repeat(65536), hash = new ArchiveHash();
    hash.update(text);
    expect(hash.finish()).toBe(createHash('sha256').update(text).digest('hex'));
});

it('round-trips a recovery archive into a separate catalog without activating work and replays the import receipt', async () => {
    const {kernel, session, make} = await setup({maxConcurrent: 0});
    const task = await session.submit({program: program.manifest, input: 'original'});
    await task.signal({type: 'go'}, {requestId: 'signal'}); await pause(session);
    const text = await exported(kernel, 'recover'), target = await make('/new-catalog');
    const options = {mode: 'restore' as const, requestId: 'restore', storage: {kind: 'test', locator: '/restored'}, assertExclusive: async () => {}};
    const report = await target.archives.importSession(chunks(text), options);
    expect(report).toMatchObject({sessionId: 's', paused: true, replayed: false});
    expect((await target.archives.importSession(chunks(text), options)).replayed).toBe(true);
    const restored = await target.openSession('s'), current = await restored.attachTask(task.id);
    expect((await restored.stat()).phase).toBe('paused'); expect((await current.stats()).pendingInputs).toBe(1);
    expect((await current.signal({type: 'go'}, {requestId: 'signal'})).replayed).toBe(true);
});

it('forks fresh deferred Tasks without inheriting effects, pending inputs or messages', async () => {
    const {kernel, session, make} = await setup({maxConcurrent: 0});
    const old = await session.submit({program: program.manifest, input: 'fork'}); await old.signal({type: 'go'});
    const text = await exported(kernel, 'inspect'), target = await make('/fork-catalog');
    const report = await target.archives.importSession(chunks(text), {mode: 'fork', requestId: 'fork', sessionId: 'forked', storage: {kind: 'test', locator: '/fork'}});
    const task = await target.task('forked', report.taskIds[old.id]);
    expect(task.id).not.toBe(old.id); expect(task.status).toBe('created'); expect(task.pendingEvents).toEqual([]); expect(task.effects).toEqual({});
});

it('forks roots once, rebuilds dependency wakeups and lets root Programs create fresh descendants', async () => {
    const {kernel, session, make} = await setup({maxConcurrent: 0});
    const a = await session.submit({program: program.manifest, input: 'a', deferStart: true});
    const b = await session.submit({program: program.manifest, input: 'b', deferStart: true, dependsOn: [{task: a.id, condition: 'terminal'}]});
    const child = await session.submit({program: program.manifest, input: 'old-child', parent: a.id, deferStart: true});
    const text = await exported(kernel, 'inspect'), target = await make('/roots-catalog');
    const report = await target.archives.importSession(chunks(text), {mode: 'fork', requestId: 'roots', sessionId: 'roots',
        storage: {kind: 'test', locator: '/roots'}});
    expect(Object.keys(report.taskIds).sort()).toEqual([a.id, b.id].sort()); expect(report.taskIds[child.id]).toBeUndefined();
    await (await target.openTask(report.taskIds[a.id])).cancel();
    expect((await target.inspectTask(report.taskIds[b.id])).task.unresolvedDeps).toBe(0);
    expect((await (await target.inspectSession('roots')).listTaskPage()).items).toHaveLength(2);
});

it('continues a restored Task only after explicit recovery, resume and deferred start', async () => {
    const {kernel, session, make} = await setup();
    const task = await session.submit({program: program.manifest, input: 'restored-output', deferStart: true});
    await pause(session); const text = await exported(kernel, 'recover'); await kernel.drain();
    const target = await make('/execution-catalog');
    await target.archives.importSession(chunks(text), {mode: 'restore', requestId: 'execution',
        storage: {kind: 'test', locator: '/execution'}, assertExclusive: async () => {}});
    await target.recoverSession('s'); const restored = await target.openSession('s');
    expect((await target.task('s', task.id)).status).toBe('created');
    await restored.resume(); const current = await restored.attachTask(task.id); await current.start();
    expect((await current.wait({timeoutMs: 1000})).output).toBe('restored-output');
});

it('preserves pending Effect identities and deadlines while requiring their exact adapter version', async () => {
    const {kernel, session, make} = await setup({maxConcurrentEffects: 0});
    const fx = {manifest: {kind: 'archived-fx', version: '1'}, init: () => ({state: null,
        actions: [{type: 'effect' as const, effect: {id: 'e', kind: 'external', version: '1', request: null, idempotencyKey: 'operation', timeoutMs: 60000}}],
        next: {type: 'wait' as const, on: {type: 'effect' as const, id: 'e'}}}), reduce: program.reduce};
    kernel.registerProgram(fx);
    const task = await session.submit({program: fx.manifest, input: null});
    await expect.poll(async () => (await task.status()).task.status).toBe('waiting');
    const effect = (await task.status()).task.effects.e; await pause(session);
    const text = await exported(kernel, 'recover'), target = await make('/fx-catalog'); target.registerProgram(fx);
    const options = {mode: 'restore' as const, requestId: 'fx', storage: {kind: 'test', locator: '/fx-restore'}, assertExclusive: async () => {}};
    await expect(target.archives.importSession(chunks(text), options)).rejects.toMatchObject({code: 'ARCHIVE_UNSAFE'});
    target.registerEffect({kind: 'external', version: '1', execute: async () => null});
    await target.archives.importSession(chunks(text), options);
    expect((await target.inspectTask(task.id)).task.effects.e).toEqual(effect);
});

it('rejects partial, corrupted, oversized and unsafe recovery archives without creating a target Session', async () => {
    const {kernel, session, make} = await setup({maxConcurrent: 0}); await session.submit({program: program.manifest, input: 'private-body'});
    await expect(exported(kernel, 'recover')).rejects.toMatchObject({code: 'ARCHIVE_UNSAFE'});
    await pause(session); const text = await exported(kernel, 'recover'), target = await make('/validation');
    await expect(target.archives.validateArchive(chunks(text.slice(0, -1)))).rejects.toMatchObject({code: 'ARCHIVE_INVALID'});
    await expect(target.archives.validateArchive(chunks(text.replace('private-body','changed-body')))).rejects.toMatchObject({code: 'ARCHIVE_INVALID'});
    await expect(target.archives.validateArchive(chunks(text), {maxBytes: 10})).rejects.toMatchObject({code: 'ARCHIVE_INVALID'});
    await expect(target.archives.importSession(chunks(text), {mode: 'restore', requestId: 'no-owner', storage: {kind: 'test', locator: '/invalid'}})).rejects.toMatchObject({code: 'ARCHIVE_UNSAFE'});
    expect([...await collect(target.listSessions())]).toEqual([]);
});

it('requires host validation for artifact dependencies and never pretends to embed file content', async () => {
    const {kernel, session, fs, make} = await setup({maxConcurrent: 0});
    const task = await session.submit({program: program.manifest, input: null});
    await fs.driver.createFile({name: 'output.txt', parentPath: `/session/tasks/${task.id}/artifacts`, content: 'artifact-body'});
    await pause(session); const text = await exported(kernel, 'recover'), target = await make('/artifact-catalog');
    const validation = await target.archives.validateArchive(chunks(text));
    expect(validation.manifest.externalPaths).toEqual([`tasks/${task.id}/artifacts/output.txt`]);
    expect(text).not.toContain('artifact-body');
    await expect(target.archives.importSession(chunks(text), {mode: 'restore', requestId: 'artifact',
        storage: {kind: 'test', locator: '/artifact-restore'}, assertExclusive: async () => {}})).rejects.toMatchObject({code: 'ARCHIVE_UNSAFE'});
});
async function collect<T>(source: AsyncIterable<T>): Promise<T[]> { const values: T[] = []; for await (const value of source) values.push(value); return values; }
it('acknowledges a quiescent boundary without closing storage, then recovers the same Session', async () => {
    const {kernel, session, make} = await setup({pollMs: 0});
    const task = await session.submit({program: program.manifest, input: 'committed'});
    await task.wait({timeoutMs: 1000});
    expect(await kernel.quiesce()).toEqual({status: 'drained', localIdle: true});
    expect(kernel.isDisposed).toBe(false); expect((await kernel.task('s', task.id)).status).toBe('succeeded');
    expect(await kernel.drain()).toEqual({status: 'drained', localIdle: true});
    const recovered = await make(); await recovered.recoverSession('s');
    expect((await recovered.task('s', task.id)).exit).toEqual((await kernel.task('s', task.id)).exit);
});
