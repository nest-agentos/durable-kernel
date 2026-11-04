import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { createVFS, MemoryBackend } from '@itookit/vfs-core';
import { createHarness, defineTask, parseKernelEvent, snapshotTaskCallPlan } from '@itookit/durable-kernel/core';
import { Kernel } from '@itookit/durable-kernel';

const require = createRequire(import.meta.url);
assert.equal(typeof Kernel, 'function');
assert.equal(typeof require('@itookit/durable-kernel').Kernel, 'function');
assert.equal(typeof require('@itookit/durable-kernel/core').createHarness, 'function');
assert.equal(typeof require('@itookit/durable-kernel').snapshotTaskCallPlan, 'function');
assert.equal(typeof require('@itookit/durable-kernel/core').snapshotTaskCallPlan, 'function');
assert.deepEqual(Object.keys(require('@itookit/durable-kernel/contracts')), []);
assert.deepEqual(Object.keys(await import('@itookit/durable-kernel/contracts')), []);
const { manager } = await createVFS({ rootBackend: new MemoryBackend() });
const fs = await manager.openFileSystem('/consumer');
const harness = createHarness({ catalog: { fs, rootPath: '/catalog' } });
harness.registerStorageResolver({ kind: 'consumer', async resolve(ref) { return { fs, rootPath: ref.locator }; } });
harness.registerProgram(defineTask({ kind: 'echo', version: '1', state: null,
    step(state, event) { return { state, next: { type: 'complete', output: event.input } }; } }));
await harness.initialize();
try {
    const session = await harness.createSession({ id: 's', storage: { kind: 'consumer', locator: '/sessions/s' } });
    const task = await session.submit({ requestId: 'once', program: { kind: 'echo', version: '1' }, input: 'packaged' });
    assert.equal((await task.wait({ timeoutMs: 2000 })).output, 'packaged');
    assert.equal((await task.waitStopped()).settled, true);
    assert.equal((await harness.sessionActivity(session.id)).counts.succeeded, 1);
    const snapshot = await task.snapshot();
    assert.deepEqual(await harness.inspectTask(task.id), snapshot);
    const events = (await task.readEvents()).items;
    assert.equal(parseKernelEvent(events.find(event => event.type === 'task.succeeded')).payload.output, 'packaged');
    assert.equal((await (await harness.inspectSession(session.id)).snapshot()).session.id, session.id);
    const message = await session.send({ idempotencyKey: 'loop', targetSessionId: session.id, topic: 'work', payload: null });
    await session.acknowledgeMessage(message.id);
    assert.equal((await session.inboxPage()).items.length, 1);
    const schedule = await session.schedule({requestId: 'packaged-timer', at: Date.now() + 5,
        task: {program: {kind: 'echo', version: '1'}, input: 'clock'}});
    assert.equal((await schedule.wait({timeoutMs: 2000})).status, 'succeeded');
    const deferred = await session.submit({program: {kind: 'echo', version: '1'}, input: 'signal', deferStart: true});
    const receipt = await deferred.signal({type: 'go'}, {requestId: 'same'});
    assert.equal(receipt.replayed, false);
    assert.equal((await deferred.signal({type: 'go'}, {requestId: 'same'})).replayed, true);
    const lines = [];
    for await (const line of harness.archives.exportSession(session.id, {purpose: 'inspect'})) lines.push(line);
    const source = async function* () {yield* lines;};
    assert.ok((await harness.archives.validateArchive(source())).records > 0);
    const report = await harness.archives.importSession(source(), {mode: 'fork', requestId: 'packaged-fork',
        storage: {kind: 'consumer', locator: '/sessions/forked'}, validateBindings: async () => {}});
    assert.equal(report.paused, true);
    await deferred.cancel('stop', {requestId: 'packaged-cancel'});
    const reader = new Kernel({catalog: {fs, rootPath: '/catalog'}});
    reader.registerStorageResolver({kind: 'consumer', async resolve(ref) {return {fs, rootPath: ref.locator};}});
    try {
        const cancelled = await reader.readTaskCancellationReceipt(session.id, deferred.id, 'packaged-cancel');
        assert.equal(cancelled.requestId, 'packaged-cancel');
        assert.equal(cancelled.status, 'cancelled');
        assert.equal(cancelled.taskId, deferred.id);
    } finally {reader.dispose(); await reader.waitIdle();}
    await observationSmoke(Kernel, 'esm');
    await observationSmoke(require('@itookit/durable-kernel').Kernel, 'cjs');
    await taskCallSmoke(options => new Kernel(options), 'esm');
    await taskCallSmoke(options => new (require('@itookit/durable-kernel').Kernel)(options), 'cjs');
    await taskCallSmoke(createHarness, 'core');
    assert.equal((await harness.shutdown()).status, 'stopped');
} finally { harness.dispose(); await harness.waitIdle(); await manager.dispose(); }
console.log('Standalone ESM/CommonJS, types, Task, owned calls, activity, mailbox, observation capacity, idempotent signal, scheduling, archives and shutdown passed.');

async function observationSmoke(KernelConstructor, mode) {
    const prefix = `/observation-${mode}`, kernel = new KernelConstructor({catalog: {fs, rootPath: prefix}, maxConcurrentEffects: 1});
    kernel.registerStorageResolver({kind: 'observe', resolve: async ref => ({fs, rootPath: ref.locator})});
    const program = {manifest: {kind: 'observe', version: '1'},
        init: input => ({state: null, actions: [{type: 'effect', effect: {id: 'run', kind: 'observe', version: '1',
            request: input, idempotencyKey: 'run'}}], next: {type: 'wait', on: {type: 'effect', id: 'run'}}}),
        reduce: (state, event) => ({state, next: {type: 'complete', output: event.result}})};
    let childSession;
    kernel.registerProgram(program);
    kernel.registerEffect({kind: 'observe', version: '1', async execute(input, context) {
        if (input === 'child') return 'observed';
        const child = await childSession.submit({program: program.manifest, input: 'child'});
        assert.equal(typeof context.waitForObservation, 'function');
        return (await context.waitForObservation(signal => child.wait({signal, until: 'settled'}))).output;
    }});
    await kernel.initialize();
    try {
        childSession = await kernel.createSession({id: 'child', storage: {kind: 'observe', locator: `${prefix}/child`}});
        const parent = await kernel.createSession({id: 'parent', storage: {kind: 'observe', locator: `${prefix}/parent`}});
        const task = await parent.submit({program: program.manifest, input: 'parent'});
        assert.equal((await task.wait({timeoutMs: 2000})).output, 'observed');
    } finally {kernel.dispose(); await kernel.waitIdle();}
}
async function taskCallSmoke(make, mode) {
    const prefix = `/owned-${mode}`, engine = make({catalog: {fs, rootPath: prefix}, maxConcurrentEffects: 1});
    engine.registerStorageResolver({kind: 'owned', resolve: async ref => ({fs, rootPath: ref.locator})});
    engine.registerProgram(defineTask({kind: 'owned.echo', version: '1', state: null,
        step: (state, event) => ({state, next: {type: 'complete', output: event.input}})}));
    await engine.initialize();
    try {
        const child = await engine.createSession({id: 'child', storage: {kind: 'owned', locator: `${prefix}/child`}});
        const parent = await engine.createSession({id: 'parent', storage: {kind: 'owned', locator: `${prefix}/parent`}});
        const snapshotPlan = mode === 'cjs' ? require('@itookit/durable-kernel').snapshotTaskCallPlan : snapshotTaskCallPlan;
        const payload = {z: 1, a: 2};
        const plan = snapshotPlan({targetSessionId: child.id, task: {program: {kind: 'owned.echo', version: '1'}, input: payload}, maxDepth: 2});
        const spec = {program: {kind: 'kernel.call', version: '1'}, input: plan, requestId: 'owned-submission', deferStart: true};
        const task = await parent.submit(spec);
        assert.equal((await engine.readTaskSubmissionReceipt(parent.id, spec)).task.taskId, task.id);
        assert.equal(await engine.readTaskCallTarget({sessionId: parent.id, taskId: task.id, effectId: 'call'}), undefined);
        await engine.bindTaskCall({sessionId: parent.id, taskId: task.id}, plan);
        const result = await task.wait({timeoutMs: 2000}); assert.equal(result.status, 'succeeded');
        assert.deepEqual(await engine.readTaskCallTarget({sessionId: parent.id, taskId: task.id, effectId: 'call'}), result.output.task);
        assert.equal(result.output.task.sessionId, 'child'); assert.equal(JSON.stringify(result.output.exit.output), JSON.stringify(payload));
        const snapshot = await child.snapshot(); assert.equal(snapshot.tasks.length, 1); assert.equal(snapshot.tasks[0].ownedBy.taskId, task.id);
        await unboundedCallSmoke(engine, parent, child, plan);
    } finally {engine.dispose(); await engine.waitIdle();}
}

async function unboundedCallSmoke(engine, parent, child, original) {
    const plan = snapshotTaskCallPlan({...original, timeoutMs: null});
    const task = await parent.submit({program: {kind: 'kernel.call', version: '2'}, input: plan, deferStart: true});
    await engine.bindTaskCall({sessionId: parent.id, taskId: task.id}, plan);
    assert.equal((await task.wait({timeoutMs: 2000})).status, 'succeeded');
    const source = await parent.snapshot(), effect = source.tasks.find(item => item.id === task.id).effects.call;
    assert.equal(effect.request.timeoutMs, null); assert.equal(effect.deadlineAt, undefined);
    assert.equal(source.session.layout.recordSchemas.effect, 2);
    assert.equal((await child.snapshot()).tasks.length, 2);
    const guarded = await child.submit({program: {kind: 'owned.echo', version: '1'}, input: null, deferStart: true});
    await assert.rejects(guarded.start({deadlineAt: Date.now() - 1}), error => error.code === 'TASK_START_DEADLINE_EXCEEDED');
    await guarded.cancel();
}
