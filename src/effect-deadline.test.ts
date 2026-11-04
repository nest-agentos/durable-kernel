import {afterEach, expect, it, vi} from 'vitest';
import {createVFS, MemoryBackend} from '@itookit/vfs-core';
import {Kernel, taskStat, type TaskCallPlan, type EffectExecutionContext, type TaskRecord} from './index';
import {normalizeEffect, addEffect, executeEffectWithDeadline} from './application/effect-utils';
import {SeqFileKernelStore, type EffectClaim} from './infrastructure/seqfile/store';
import {archiveLines} from './infrastructure/seqfile/archive-store';
import type {ArchiveManifest, ArchiveRecord} from './domain/archive';

const releases: Array<() => Promise<unknown>> = [];
afterEach(async () => {vi.restoreAllMocks(); vi.useRealTimers(); for (const close of releases.splice(0).reverse()) await close();});
function deferred<T = void>() {
    let resolve!: (value: T | PromiseLike<T>) => void;
    const promise = new Promise<T>(done => {resolve = done;}); return {promise, resolve};
}
async function setup() {
    const {manager} = await createVFS({rootBackend: new MemoryBackend()}), fs = await manager.openFileSystem('/'); releases.push(() => manager.dispose());
    const execute = vi.fn(async () => 'done');
    const make = async () => {
        const kernel = new Kernel({catalog: {fs}, maxConcurrentEffects: 1, leaseMs: 120_000});
        kernel.registerStorageResolver({kind: 'test', resolve: async ref => ({fs, rootPath: String(ref.locator)})});
        kernel.registerEffect({kind: 'operation', version: '1', execute, cancel: async () => {}});
        kernel.registerProgram({manifest: {kind: 'operation', version: '1'}, init: () => ({state: null, actions: [{type: 'effect',
            effect: {id: 'op', kind: 'operation', version: '1', request: null, idempotencyKey: 'op'}}], next: {type: 'wait', on: {type: 'effect', id: 'op'}}}),
            reduce: () => ({state: null, next: {type: 'complete', output: 'done'}})});
        kernel.registerProgram({manifest: {kind: 'human', version: '1'}, init: () => ({state: null,
            actions: [{type: 'request-interaction', interaction: {id: 'answer', kind: 'input', prompt: 'Input', schema: {type: 'string'}}}],
            next: {type: 'wait', on: {type: 'interaction', id: 'answer'}}}),
            reduce: (_state, event) => ({state: null, next: event.type === 'interaction-resolved'
                ? {type: 'complete', output: event.value} : {type: 'wait', on: {type: 'interaction', id: 'answer'}}})});
        await kernel.initialize(); releases.push(async () => {kernel.dispose(); await kernel.waitIdle();}); return kernel;
    };
    const kernel = await make(); for (const id of ['source', 'target']) await kernel.createSession({id, storage: {kind: 'test', locator: `/${id}`}});
    return {kernel, make, fs, execute};
}
async function call(kernel: Kernel, version: string, timeoutMs?: number | null, program = 'human') {
    const plan: TaskCallPlan = {targetSessionId: 'target', task: {program: {kind: program, version: '1'}, input: null}, maxDepth: 3,
        ...(timeoutMs !== undefined ? {timeoutMs} : {})};
    const task = await kernel.submit('source', {program: {kind: 'kernel.call', version}, input: plan, deferStart: true});
    await kernel.bindTaskCall({sessionId: 'source', taskId: task.id}, plan); return task;
}
function fakeClock() {vi.useFakeTimers({toFake: ['Date', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval']});}

it('keeps omitted Effect deadlines bounded and persists explicit null without an invented deadline', () => {
    const request = {id: 'e', kind: 'x', version: '1', idempotencyKey: 'e', request: null};
    expect(normalizeEffect(request).timeoutMs).toBe(30_000);
    const effect = normalizeEffect({...request, timeoutMs: null}); expect(effect.timeoutMs).toBeNull();
    const task = addEffect({effects: {}} as TaskRecord, effect); expect(task.effects.e.deadlineAt).toBeUndefined();
    expect(() => normalizeEffect({...request, timeoutMs: 0})).toThrow();
});
it('lets explicit unbounded child approval wait beyond 30 seconds without losing execution or control', async () => {
    fakeClock(); const {kernel} = await setup(), task = await call(kernel, '2', null); await vi.advanceTimersByTimeAsync(10);
    const child = (await kernel.listSessionTasks('target'))[0]; expect(child.interactions.answer.status).toBe('pending');
    await vi.advanceTimersByTimeAsync(31_000);
    const parent = (await task.status()).task; expect(parent.exit).toBeUndefined(); expect(parent.effects.call.deadlineAt).toBeUndefined();
    await (await kernel.attachTask('target', child.id)).respond({interactionId: 'answer', value: 'approved'});
    await vi.advanceTimersByTimeAsync(500);
    expect((await task.status()).task.exit).toMatchObject({status: 'succeeded', output: {exit: {output: 'approved'}}});
});
it('retains the old call version and its 30-second deadline for already-frozen omitted policies', async () => {
    fakeClock(); const {kernel} = await setup(), task = await call(kernel, '1'); await vi.advanceTimersByTimeAsync(10);
    expect((await task.status()).task.effects.call.request.timeoutMs).toBe(30_000);
    await vi.advanceTimersByTimeAsync(31_000);
    expect((await task.status()).task.exit).toMatchObject({status: 'failed', error: {code: 'EFFECT_TIMEOUT'}});
    expect((await kernel.listSessionTasks('target'))[0].status).toBe('cancelled');
});
it.each([['1', null], ['2', undefined]] as const)('rejects an ambiguous policy for call version %s', async (version, timeout) => {
    const {kernel} = await setup(), task = await call(kernel, version, timeout);
    expect(await task.wait({timeoutMs: 1000})).toMatchObject({status: 'failed', error: {code: 'INVALID_SPEC'}});
    expect(await kernel.listSessionTasks('target')).toEqual([]);
});
it('keeps caller wait timeouts and worker drain separate from the unbounded execution', async () => {
    fakeClock(); const {kernel} = await setup(), task = await call(kernel, '2', null); await vi.advanceTimersByTimeAsync(10);
    const observed = task.wait({timeoutMs: 5}).catch(error => error); await vi.advanceTimersByTimeAsync(10);
    expect(await observed).toMatchObject({code: 'WAIT_TIMEOUT'}); expect((await task.status()).task.exit).toBeUndefined();
    const draining = kernel.drain({timeoutMs: 5}); await vi.advanceTimersByTimeAsync(10);
    expect(await draining).toMatchObject({status: 'blocked'});
    expect((await kernel.listSessionTasks('target'))[0].interactions.answer.status).toBe('pending');
});
it('promotes only actual unbounded Effect data and rolls the promotion back with a failed Decision', async () => {
    const {kernel} = await setup();
    kernel.registerProgram({manifest: {kind: 'rollback', version: '2'}, init: () => ({state: null, actions: [
        {type: 'effect', effect: {id: 'e', kind: 'operation', version: '1', request: null, idempotencyKey: 'e', timeoutMs: null}},
        {type: 'set-shared', key: 'guarded', value: 1, expectedVersion: 99}], next: {type: 'wait', on: {type: 'effect', id: 'e'}}}),
        reduce: () => {throw Error('Unexpected');}});
    const failed = await kernel.submit('source', {program: {kind: 'rollback', version: '2'}, input: null});
    expect((await failed.wait({timeoutMs: 1000})).status).toBe('failed');
    expect((await (await kernel.inspectSession('source')).snapshot()).session.layout!.recordSchemas.effect).toBe(1);
    const running = await call(kernel, '2', null); await vi.waitFor(async () => expect((await running.status()).task.effects.call?.status).toBe('leased'));
    const snapshot = await (await kernel.inspectSession('source')).snapshot();
    expect(snapshot.session.layout).toMatchObject({recordSchemas: {effect: 2}, requiredCapabilities: expect.arrayContaining(['effect-unbounded-deadline'])});
    expect((await (await kernel.inspectSession('target')).snapshot()).session.layout!.recordSchemas.effect).toBe(1);
});
it('keeps cleanup pending until an unbounded physical operation confirms stop', async () => {
    const {kernel} = await setup(), entered = deferred(), finish = deferred<string>(); let confirmed = false;
    const execute = vi.fn(async () => {entered.resolve(); return finish.promise;}); releases.push(async () => {finish.resolve('stopped');});
    kernel.registerEffect({kind: 'external', version: '1', execute, cancel: async () => {if (!confirmed) throw Error('Stop unknown');}});
    kernel.registerProgram({manifest: {kind: 'external', version: '2'}, init: () => ({state: null, actions: [{type: 'effect', effect: {
        id: 'e', kind: 'external', version: '1', request: null, idempotencyKey: 'e', timeoutMs: null}}], next: {type: 'wait', on: {type: 'effect', id: 'e'}}}),
        reduce: () => ({state: null, next: {type: 'complete', output: null}})});
    const task = await kernel.submit('source', {program: {kind: 'external', version: '2'}, input: null}); await entered.promise;
    await expect(task.cancel()).rejects.toBeInstanceOf(AggregateError); expect(taskStat((await task.status()).task).settled).toBe(false);
    confirmed = true; finish.resolve('stopped'); await task.cancel(); expect(taskStat((await task.status()).task).settled).toBe(true);
    expect(execute).toHaveBeenCalledOnce();
});
it('does not start a recovered deferred target after its original logical deadline, including a late old worker', async () => {
    fakeClock(); const {kernel, make, execute} = await setup(), entered = deferred(), release = deferred(); let first = true;
    releases.push(async () => {release.resolve();}); const start = SeqFileKernelStore.prototype.startTask;
    vi.spyOn(SeqFileKernelStore.prototype, 'startTask').mockImplementation(async function (this: SeqFileKernelStore, ...args) {
        if (args[0].rootPath === '/target' && first) {first = false; entered.resolve(); await release.promise;}
        return start.apply(this, args);
    });
    const task = await call(kernel, '1', 100, 'operation'); await vi.advanceTimersByTimeAsync(1); await entered.promise;
    kernel.dispose(); const idle = kernel.waitIdle(); await vi.advanceTimersByTimeAsync(200); await idle;
    const restored = await make(); await restored.recoverSession('source', {takeover: true}); await vi.advanceTimersByTimeAsync(500);
    const current = await restored.task('source', task.id); expect(current.exit?.status).toBe('failed'); expect(taskStat(current).settled).toBe(true);
    release.resolve(); await vi.advanceTimersByTimeAsync(10);
    const children = await restored.listSessionTasks('target'); expect(children).toHaveLength(1);
    expect(children[0]).toMatchObject({status: 'cancelled', attemptCount: 0}); expect(execute).not.toHaveBeenCalled();
});
it('replays an already accepted Task start even after the start guard expires, but refuses a new late start', async () => {
    const {kernel} = await setup(), session = await kernel.openSession('target'), deadlineAt = Date.now() + 1000;
    const task = await session.submit({program: {kind: 'human', version: '1'}, input: null, deferStart: true});
    await expect(task.start({deadlineAt: Date.now() - 1})).rejects.toHaveProperty('code', 'TASK_START_DEADLINE_EXCEEDED');
    expect((await task.status()).task.status).toBe('created'); await task.start({deadlineAt});
    await task.start({deadlineAt: Date.now() - 1}); expect((await task.status()).task.status).not.toBe('created');
});
it('segments numeric deadlines beyond the platform timer range without an early abort', async () => {
    fakeClock(); const controller = new AbortController(), timeoutMs = 2_147_483_647 + 100;
    const effect = normalizeEffect({id: 'e', kind: 'wait', version: '1', request: null, idempotencyKey: 'e', timeoutMs});
    const context: EffectExecutionContext = {sessionId: 's', taskId: 't', effectId: 'e', grants: [], abortSignal: controller.signal};
    const claim = {effect: {request: effect, deadlineAt: Date.now() + timeoutMs, attempts: []}} as unknown as EffectClaim;
    const result = executeEffectWithDeadline({kind: 'wait', version: '1', execute: () => new Promise(() => {})}, effect, claim, context, controller).catch(error => error);
    await vi.advanceTimersByTimeAsync(2_147_483_647); expect(controller.signal.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(100); expect(await result).toMatchObject({code: 'EFFECT_TIMEOUT'});
});
it('keeps the unbounded format contract through archive restore and rejects a validly sealed downgraded manifest', async () => {
    const {kernel} = await setup();
    kernel.registerProgram({manifest: {kind: 'short-unbounded', version: '2'}, init: () => ({state: null, actions: [{type: 'effect', effect: {
        id: 'op', kind: 'operation', version: '1', request: null, idempotencyKey: 'op', timeoutMs: null}}], next: {type: 'wait', on: {type: 'effect', id: 'op'}}}),
        reduce: () => ({state: null, next: {type: 'complete', output: 'done'}})});
    const task = await kernel.submit('source', {program: {kind: 'short-unbounded', version: '2'}, input: null}); await task.wait({timeoutMs: 1000});
    await kernel.closeSession('source', false); const lines: string[] = [];
    for await (const line of kernel.archives.exportSession('source', {purpose: 'recover'})) lines.push(line);
    const restored = await kernel.archives.importSession((async function* () {yield* lines;})(), {mode: 'restore', requestId: 'restore',
        storage: {kind: 'test', locator: '/restored'}, assertExclusive: async () => {await kernel.removeSession('source');}, validateBindings: async () => {}});
    expect((await (await kernel.inspectSession(restored.sessionId)).snapshot()).session.layout!.recordSchemas.effect).toBe(2);
    const manifest = JSON.parse(lines[0]) as ArchiveManifest, records = lines.slice(1, -1).map(line => JSON.parse(line) as ArchiveRecord);
    manifest.session.layout!.recordSchemas.effect = 1; manifest.session.layout!.requiredCapabilities = manifest.session.layout!.requiredCapabilities.filter(cap => cap !== 'effect-unbounded-deadline');
    records.find(row => row.path === 'session.seq' && row.key === 'record')!.value = JSON.stringify(manifest.session);
    await expect(kernel.archives.validateArchive(archiveLines({manifest, records}, {purpose: 'recover'}))).rejects.toHaveProperty('code', 'ARCHIVE_INVALID');
});
