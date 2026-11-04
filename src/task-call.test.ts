import {afterEach, expect, it, vi} from 'vitest';
import {createVFS, MemoryBackend} from '@itookit/vfs-core';
import {Kernel, KernelError, KernelErrorCode, parseKernelEvent, snapshotTaskCallPlan, bindCapabilities, taskCallCapabilityUri, taskStat, type TaskCallPlan, type TaskHandle, type KernelOptions, type KernelDiagnostic} from './index';
import {SeqFileKernelStore} from './infrastructure/seqfile/store';
import {TaskCallEffect} from './application/task-call-effect';

const releases: Array<() => Promise<void>> = [];
afterEach(async () => {vi.restoreAllMocks(); for (const close of releases.splice(0).reverse()) await close();});
function deferred<T = void>() {
    let resolve!: (value: T | PromiseLike<T>) => void;
    const promise = new Promise<T>(done => {resolve = done;}); return {promise, resolve};
}
const echo = {manifest: {kind: 'echo', version: '1'}, init: (input: unknown) => ({state: null, next: {type: 'complete' as const, output: input}}),
    reduce: () => {throw new Error('Unexpected echo event');}};
async function setup(options: Partial<KernelOptions> = {}) {
    const {manager} = await createVFS({rootBackend: new MemoryBackend()}), fs = await manager.openFileSystem('/');
    releases.push(() => manager.dispose()); const diagnostics: KernelDiagnostic[] = [];
    const make = async () => {
        const kernel = new Kernel({catalog: {fs}, maxConcurrentEffects: 1, ...options,
            onDiagnostic: event => {diagnostics.push(event); options.onDiagnostic?.(event);}});
        kernel.registerStorageResolver({kind: 'test', resolve: async ref => ({fs, rootPath: String(ref.locator)})}); kernel.registerProgram(echo);
        await kernel.initialize(); releases.push(async () => {kernel.dispose(); await kernel.waitIdle();}); return kernel;
    };
    const kernel = await make();
    for (const id of ['parent', 'child', 'grandchild', 'leaf']) await kernel.createSession({id, storage: {kind: 'test', locator: `/${id}`}});
    const plan = (targetSessionId = 'child', input: unknown = {answer: 42}): TaskCallPlan => ({targetSessionId,
        task: {program: echo.manifest, input}, maxDepth: 4});
    return {kernel, make, plan, fs, diagnostics};
}
async function call(kernel: Kernel, plan: TaskCallPlan, sessionId = 'parent', uri = taskCallCapabilityUri(plan)): Promise<TaskHandle> {
    const task = await kernel.submit(sessionId, {program: {kind: 'kernel.call', version: '1'}, input: plan, deferStart: true});
    await bindCapabilities(task, [{kind: 'task', uri, rights: ['execute'], signalKey: 'taskHandleId'}]); return task;
}
function nested(inner: TaskCallPlan, targetSessionId: string, maxDepth = 4): TaskCallPlan {
    return {targetSessionId, maxDepth, task: {program: {kind: 'kernel.call', version: '1'}, input: inner}, bindings: [
        {kind: 'task', uri: taskCallCapabilityUri(inner), rights: ['execute'], signalKey: 'taskHandleId'}]};
}
it('calls a Task in another Session and returns its actual structured exit and reference', async () => {
    const {kernel, plan} = await setup(), input = plan(), parent = await call(kernel, input);
    const exit = await parent.wait({timeoutMs: 2000}); expect(exit).toMatchObject({status: 'succeeded', output: {task: {sessionId: 'child'}, exit: {output: {answer: 42}}}});
    const children = await kernel.listSessionTasks('child'); expect(children).toHaveLength(1);
    expect(children[0]).toMatchObject({ownedBy: {sessionId: 'parent', taskId: parent.id, effectId: 'call', depth: 1, maxDepth: 4}});
    expect(taskStat(children[0]).settled).toBe(true);
    expect((await parent.status()).task.effects.call.status).toBe('succeeded');
});
it('runs nested cross-Session calls with one work slot and inherits the tighter depth budget', async () => {
    const {kernel, plan} = await setup();
    const root = await call(kernel, nested(nested(plan('leaf'), 'grandchild'), 'child', 3));
    expect(await root.wait({timeoutMs: 3000})).toMatchObject({status: 'succeeded'});
    expect((await kernel.listSessionTasks('leaf'))[0].ownedBy).toMatchObject({depth: 3, maxDepth: 3});
    const child = (await kernel.readTaskCallTarget({sessionId: 'parent', taskId: root.id, effectId: 'call'}))!;
    const grandchild = (await kernel.readTaskCallTarget({...child, effectId: 'call'}))!;
    expect(await kernel.readTaskCallTarget({...grandchild, effectId: 'call'})).toMatchObject({sessionId: 'leaf'});
    const exceeded = await call(kernel, nested(nested(plan('leaf'), 'grandchild'), 'child', 2));
    expect(await exceeded.wait({timeoutMs: 3000})).toMatchObject({status: 'failed', error: {code: 'BUDGET_EXCEEDED'}});
    expect(await kernel.listSessionTasks('leaf')).toHaveLength(1);
});
it('enforces the explicit host depth policy without substituting a fixed numeric maximum', async () => {
    const {kernel, plan} = await setup(), input = {...plan(), maxDepth: 100};
    const parent = await call(kernel, input); expect(await parent.wait({timeoutMs: 1000})).toMatchObject({status: 'succeeded'});
    expect((await kernel.listSessionTasks('child'))[0].ownedBy?.maxDepth).toBe(100);
});
it('binds authorization to the exact plan instead of authorizing arbitrary Tasks on a connection', async () => {
    const {kernel, plan} = await setup(), allowed = plan(), changed = {...allowed, task: {...allowed.task, input: 'different'}};
    const task = await call(kernel, changed, 'parent', taskCallCapabilityUri(allowed));
    expect(await task.wait({timeoutMs: 2000})).toMatchObject({status: 'failed', error: {code: 'TASK_CALL_PLAN_NOT_AUTHORIZED'}});
    expect(await kernel.listSessionTasks('child')).toEqual([]);
});
it('rejects caller-supplied target identity and parent metadata before any target is created', async () => {
    const {kernel, plan} = await setup(), invalid = plan(); (invalid.task as any).parent = 'foreign';
    const task = await call(kernel, invalid);
    expect(await task.wait({timeoutMs: 2000})).toMatchObject({status: 'failed', error: {code: 'INVALID_SPEC'}});
    expect(await kernel.listSessionTasks('child')).toEqual([]);
});
it('cancels a deferred target before a late creator returns, including from another Kernel instance', async () => {
    const {kernel, make, plan} = await setup({leaseMs: 100_000}), entered = deferred(), resume = deferred();
    releases.push(async () => {resume.resolve();}); let first = true, originalSignal: AbortSignal | undefined;
    const execute = TaskCallEffect.prototype.execute;
    vi.spyOn(TaskCallEffect.prototype, 'execute').mockImplementation(function (this: TaskCallEffect, request, context) {originalSignal = context.abortSignal; return execute.call(this, request, context);});
    const create = SeqFileKernelStore.prototype.createTask;
    vi.spyOn(SeqFileKernelStore.prototype, 'createTask').mockImplementation(async function (this: SeqFileKernelStore, ...args) {
        if (args[4] && first) {first = false; entered.resolve(); await resume.promise;}
        return create.apply(this, args);
    });
    const parent = await call(kernel, plan()); await entered.promise; const replacement = await make();
    await replacement.cancel('parent', parent.id, 'Stop original call');
    expect(originalSignal?.aborted).toBe(false);
    const targets = await replacement.listSessionTasks('child'); expect(targets).toHaveLength(1); expect(targets[0].status).toBe('cancelled');
    resume.resolve(); await kernel.waitIdle();
    expect(await replacement.listSessionTasks('child')).toHaveLength(1);
    expect((await replacement.task('child', targets[0].id)).attemptCount).toBe(0);
    expect(taskStat(await replacement.task('parent', parent.id)).settled).toBe(true);
});
it('keeps parent cleanup pending when the actual child cannot confirm physical stop', async () => {
    const {kernel, plan} = await setup(), entered = deferred(), physicalStop = deferred();
    releases.push(async () => {physicalStop.resolve();});
    kernel.registerProgram({manifest: {kind: 'external', version: '1'}, init: () => ({state: null, actions: [{type: 'effect', effect: {
        id: 'run', kind: 'external', version: '1', request: null, idempotencyKey: 'run'}}], next: {type: 'wait', on: {type: 'effect', id: 'run'}}}), reduce: () => {throw new Error('Unexpected');}});
    kernel.registerEffect({kind: 'external', version: '1', execute: async () => {entered.resolve(); await physicalStop.promise; return null;},
        cancel: async () => {throw Object.assign(new Error('PRIVATE EXTERNAL FAILURE'), {code: 'STOP_UNKNOWN'});}});
    const input = plan(); input.task.program = {kind: 'external', version: '1'}; const parent = await call(kernel, input); await entered.promise;
    await expect(parent.cancel('Stop')).rejects.toThrow();
    expect((await parent.status()).task.effects.call.cleanupPending).toBe(true);
    expect(taskStat((await parent.status()).task).settled).toBe(false);
    kernel.effects.resolve('external', '1').cancel = async () => {physicalStop.resolve();};
    await parent.cancel('Stop'); expect(taskStat((await parent.status()).task).settled).toBe(true);
});
it('reopens the original target after worker replacement without submitting a second Task', async () => {
    const {kernel, make, plan} = await setup();
    const program = {manifest: {kind: 'held', version: '1'}, init: () => ({state: null, next: {type: 'wait' as const, on: {type: 'signal' as const}}}),
        reduce: () => ({state: null, next: {type: 'complete' as const, output: 'original'}})};
    kernel.registerProgram(program); const input = plan(); input.task.program = program.manifest;
    const parent = await call(kernel, input);
    await expect.poll(async () => (await kernel.listSessionTasks('child')).length).toBe(1);
    const target = (await kernel.listSessionTasks('child'))[0]; kernel.dispose(); await kernel.waitIdle();
    const next = await make(); next.registerProgram(program); await next.recover({takeover: true});
    const child = await next.attachTask('child', target.id); await child.signal({type: 'finish'});
    const attached = await next.attachTask('parent', parent.id);
    expect(await attached.wait({timeoutMs: 3000})).toMatchObject({status: 'succeeded', output: {exit: {output: 'original'}}});
    expect(await next.listSessionTasks('child')).toHaveLength(1);
});
it('refuses a replacement Session with the same logical id', async () => {
    const {kernel, plan, diagnostics} = await setup(), entered = deferred(), proceed = deferred();
    releases.push(async () => {proceed.resolve();});
    const create = SeqFileKernelStore.prototype.createTask; let first = true;
    vi.spyOn(SeqFileKernelStore.prototype, 'createTask').mockImplementation(async function (this: SeqFileKernelStore, ...args) {
        if (args[4] && first) {first = false; entered.resolve(); await proceed.promise;}
        return create.apply(this, args);
    });
    const parent = await call(kernel, plan()); await entered.promise;
    await kernel.closeSession('child', true); await kernel.removeSession('child');
    await kernel.createSession({id: 'child', storage: {kind: 'test', locator: '/child'}}); proceed.resolve();
    await expect.poll(async () => (await parent.status()).task.effects.call.status).toBe('indeterminate');
    expect(await kernel.listSessionTasks('child')).toEqual([]);
    await expect(parent.cancel('Stop')).rejects.toThrow();
    expect((await parent.status()).task.effects.call.cleanupPending).toBe(true);
    expect(diagnostics).toContainEqual(expect.objectContaining({operation: 'task.call.cancel', stage: 'target', errorCode: 'SESSION_INSTANCE_CHANGED'}));
});
it.each(['requestId', 'parent', 'spawnKey', 'deferStart', 'ownedBy'])('rejects caller-controlled %s on the target spec', async field => {
    const {kernel, plan} = await setup(), input = plan(); (input.task as any)[field] = 'forged';
    const parent = await call(kernel, input);
    expect(await parent.wait({timeoutMs: 1000})).toMatchObject({status: 'failed', error: {code: 'INVALID_SPEC'}});
    expect(await kernel.listSessionTasks('child')).toEqual([]);
});
it('does not create a target when the original Effect has never been dispatched', async () => {
    const {kernel, plan} = await setup({maxConcurrentEffects: 0}), parent = await call(kernel, plan());
    await expect.poll(async () => (await parent.status()).task.effects.call?.status).toBe('pending');
    await parent.cancel('Before dispatch'); expect(taskStat((await parent.status()).task).settled).toBe(true);
    expect(await kernel.listSessionTasks('child')).toEqual([]);
});
it('keeps unrelated roots in the target Session outside the owned cancellation scope', async () => {
    const {kernel, plan} = await setup(), unrelated = await kernel.submit('child', {program: echo.manifest, input: 'unrelated', deferStart: true});
    const held = {manifest: {kind: 'wait', version: '1'}, init: () => ({state: null, next: {type: 'wait' as const, on: {type: 'signal' as const}}}),
        reduce: () => ({state: null, next: {type: 'complete' as const, output: 'done'}})};
    kernel.registerProgram(held); const input = plan(); input.task.program = held.manifest;
    const parent = await call(kernel, input);
    await expect.poll(async () => (await kernel.listSessionTasks('child')).filter(task => task.ownedBy).length).toBe(1);
    await parent.cancel('Stop owned'); expect(taskStat((await parent.status()).task).settled).toBe(true);
    expect((await unrelated.status()).task).toMatchObject({status: 'created', attemptCount: 0});
});
it('waits for descendants after a target root succeeds, then stops only its remaining tree', async () => {
    const {kernel, plan} = await setup(), entered = deferred(), finish = deferred(); releases.push(async () => {finish.resolve();});
    kernel.registerEffect({kind: 'wait', version: '1', execute: async () => {entered.resolve(); await finish.promise; return null;}, cancel: async () => {finish.resolve();}});
    kernel.registerProgram({manifest: {kind: 'worker', version: '1'}, init: () => ({state: null, actions: [{type: 'effect', effect: {
        id: 'wait', kind: 'wait', version: '1', request: null, idempotencyKey: 'wait'}}], next: {type: 'wait', on: {type: 'effect', id: 'wait'}}}),
        reduce: () => ({state: null, next: {type: 'complete', output: null}})});
    kernel.registerProgram({manifest: {kind: 'spawn-and-finish', version: '1'}, init: () => ({state: null, actions: [
        {type: 'spawn', spawnKey: 'worker', spec: {program: {kind: 'worker', version: '1'}, input: null}}], next: {type: 'complete', output: 'original'}}),
        reduce: () => {throw new Error('Unexpected');}});
    const input = plan(); input.task.program = {kind: 'spawn-and-finish', version: '1'};
    const parent = await call(kernel, input); await entered.promise;
    const target = (await kernel.listSessionTasks('child')).find(task => task.ownedBy)!;
    expect(target.exit).toMatchObject({status: 'succeeded', output: 'original'});
    expect((await parent.status()).task.effects.call.status).toBe('leased');
    await parent.cancel('Stop remaining tree'); expect(taskStat((await parent.status()).task).settled).toBe(true);
    expect((await kernel.task('child', target.id)).exit).toEqual(target.exit);
    expect((await kernel.listSessionTasks('child')).filter(task => task.id !== target.id).map(task => task.status)).toEqual(['cancelled']);
});
it('upgrades only the existing legacy Session incarnation before binding a call', async () => {
    const {kernel, plan, fs} = await setup(), path = '/child/session.seq';
    const record = JSON.parse((await fs.meta.seq!.getEntry(path, 'record'))!); delete record.instanceId;
    await fs.meta.seq!.setEntry(path, 'record', JSON.stringify(record));
    const parent = await call(kernel, plan()); expect(await parent.wait({timeoutMs: 1000})).toMatchObject({status: 'succeeded'});
    const session = await kernel.inspectSession('child'), snapshot = await session.snapshot(); expect(snapshot.session.instanceId).toBeTruthy();
    const page = await session.readEvents(), event = page.items.find(event => event.type === 'session.identity.assigned')!;
    expect(parseKernelEvent(event)?.type).toBe('session.identity.assigned');
    expect(() => parseKernelEvent({...event, payload: {...snapshot.session, instanceId: undefined}})).toThrow();
    expect(snapshot.tasks).toHaveLength(1);
});
it('keeps host authorization observations separate from the frozen executable plan', async () => {
    const {kernel, plan} = await setup({authorizeTaskCall: async request => {request.task.input = 'mutated'; request.targetSessionId = 'leaf';}});
    const parent = await call(kernel, plan());
    expect(await parent.wait({timeoutMs: 1000})).toMatchObject({status: 'succeeded', output: {task: {sessionId: 'child'}, exit: {output: {answer: 42}}}});
    expect(await kernel.listSessionTasks('leaf')).toEqual([]);
});
it('retains the exact failure category and result when diagnostics throw, without logging input', async () => {
    const {kernel, plan, diagnostics} = await setup({authorizeTaskCall: async () => {throw new KernelError(KernelErrorCode.HANDLE_REVOKED, 'PRIVATE DENIAL');},
        onDiagnostic: event => {if (event.operation.startsWith('task.call.')) throw new Error('PRIVATE LOGGER');}});
    const parent = await call(kernel, plan('child', 'PRIVATE INPUT'));
    expect(await parent.wait({timeoutMs: 1000})).toMatchObject({status: 'failed', error: {code: 'HANDLE_REVOKED'}});
    expect(await kernel.listSessionTasks('child')).toEqual([]);
    expect(diagnostics).toContainEqual(expect.objectContaining({operation: 'task.call.execute', stage: 'admission', errorCode: 'HANDLE_REVOKED'}));
    expect(JSON.stringify(diagnostics)).not.toContain('PRIVATE');
});
it('keeps physical resource capacity held after Task exit until the original release is confirmed', async () => {
    const {kernel, plan} = await setup(), stopped = deferred(), cleaning = deferred(); releases.push(async () => {stopped.resolve();});
    kernel.registerResourceAdapter({kind: 'scope', version: '1', cleanup: async context => {
        cleaning.resolve(); await stopped.promise; return {operationId: context.operationId, epoch: context.epoch, status: 'stopped'};
    }, destroy: async context => ({operationId: context.operationId, epoch: context.epoch, status: 'stopped'})});
    kernel.registerProgram({manifest: {kind: 'wait-resource', version: '1'}, init: () => ({state: null, next: {type: 'wait', on: {type: 'signal'}}}),
        reduce: () => ({state: null, next: {type: 'complete', output: 'done'}})});
    const input = plan(); input.task.program = {kind: 'wait-resource', version: '1'}; const parent = await call(kernel, input);
    await expect.poll(async () => (await kernel.listSessionTasks('child')).length).toBe(1);
    const target = await kernel.attachTask('child', (await kernel.listSessionTasks('child'))[0].id);
    await expect.poll(async () => (await target.status()).task.status).toBe('waiting');
    const pool = await (await target.resources.create({requestId: 'pool', kind: 'pool', name: 'scope', capacity: 1,
        physical: {kind: 'scope', version: '1', externalId: 'original'}})).wait();
    const handle = await (await target.resources.open(pool.ref, {requestId: 'open', name: 'capacity', rights: ['execute']})).wait();
    const claim = await (await target.resources.acquire(handle, {requestId: 'hold', quantity: 1})).wait();
    await target.signal({type: 'finish'}); await target.wait({timeoutMs: 1000});
    expect(taskStat((await target.status()).task).settled).toBe(true);
    expect((await parent.status()).task.effects.call.status).toBe('leased');
    await expect(parent.cancel('Stop owned scope')).rejects.toThrow();
    expect((await parent.status()).task.effects.call.cleanupPending).toBe(true);
    const release = await target.resources.release(claim, {requestId: 'release'}); await cleaning.promise;
    expect((await target.resources.stat(pool.ref)).held).toBe(1); stopped.resolve(); await release.wait({timeoutMs: 2000});
    await parent.cancel('Stop owned scope'); expect(taskStat((await parent.status()).task).settled).toBe(true);
    expect((await target.resources.stat(pool.ref)).held).toBe(0);
});
it('requires archive ownership validation and assigns a fresh storage incarnation on import', async () => {
    const {kernel, plan} = await setup(), parent = await call(kernel, plan()); await parent.wait({timeoutMs: 1000});
    const session = await kernel.openSession('child'), original = (await session.snapshot()).session.instanceId;
    const lines: string[] = []; for await (const line of kernel.archives.exportSession('child', {purpose: 'inspect'})) lines.push(line);
    const source = async function* () {yield* lines;};
    expect((await kernel.archives.validateArchive(source())).externalBindings).toBe(true);
    const options = {mode: 'fork' as const, requestId: 'fork', sessionId: 'forked', storage: {kind: 'test', locator: '/forked'}};
    await expect(kernel.archives.importSession(source(), options)).rejects.toMatchObject({code: 'ARCHIVE_UNSAFE'});
    await kernel.archives.importSession(source(), {...options, validateBindings: async () => {}});
    const forked = await (await kernel.inspectSession('forked')).snapshot();
    expect(forked.session.instanceId).not.toBe(original); expect(forked.session.status).toBe('suspended');
    expect(forked.tasks[0].ownedBy).toBeUndefined();
});

it('snapshots a detached call plan without rewriting its payload or changing capability identity', async () => {
    const {plan} = await setup(), original = plan(), snapshot = snapshotTaskCallPlan(original);
    const reordered = {maxDepth: original.maxDepth, task: {input: original.task.input, program: {version: '1', kind: 'echo'}}, targetSessionId: original.targetSessionId};
    expect(taskCallCapabilityUri(snapshotTaskCallPlan(reordered))).toBe(taskCallCapabilityUri(snapshot));
    expect(JSON.stringify(snapshot.task.input)).toBe(JSON.stringify(original.task.input));
    expect(taskCallCapabilityUri(snapshot)).toBe(taskCallCapabilityUri(original));
    original.task.input = 'changed'; expect(snapshot.task.input).toEqual({answer: 42});
});
it.each([NaN, Infinity, 1n, () => {}, new Date()])('refuses lossy call input normalization before a caller can be submitted: %s', async input => {
    const {plan} = await setup(); expect(() => snapshotTaskCallPlan(plan('child', input))).toThrow();
});
it('rejects unknown plan fields and injected resource handles instead of normalizing them away', async () => {
    const {plan} = await setup();
    expect(() => snapshotTaskCallPlan({...plan(), resourceHandleId: 'injected'} as never)).toThrow();
    expect(() => snapshotTaskCallPlan({...plan(), injected: true} as never)).toThrow();
});

it('preserves prepared payload representation when dispatching instead of rewriting it for a fingerprint', async () => {
    const {kernel, plan} = await setup();
    kernel.registerProgram({manifest: {kind: 'verify-seal', version: '1'}, init: (input: {payload: object; seal: string}) => ({state: null,
        next: JSON.stringify(input.payload) === input.seal ? {type: 'complete', output: input.payload}
            : {type: 'fail', error: {message: 'PREPARED_PAYLOAD_CHANGED'}}}), reduce: () => {throw Error('Unexpected');}});
    const payload = {z: 1, a: 2}, input = plan('child', {payload, seal: JSON.stringify(payload)}); input.task.program = {kind: 'verify-seal', version: '1'};
    const parent = await call(kernel, input);
    expect(await parent.wait({timeoutMs: 1000})).toMatchObject({status: 'succeeded', output: {exit: {output: payload}}});
});
