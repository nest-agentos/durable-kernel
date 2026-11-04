import { afterEach, expect, it, vi } from 'vitest';
import { createVFS, MemoryBackend } from '@itookit/vfs-core';
import { Kernel, taskStat, KernelErrorCode, parseKernelEvent, parseEventEnvelope, type EffectExecutionContext, type KernelOptions } from './index';
import { SeqFileKernelStore } from './infrastructure/seqfile/store';
import { createHarness, type Task } from './core';

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });
function gate() {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
}
async function setup(options: Partial<KernelOptions> = {}) {
    const { manager } = await createVFS({ rootBackend: new MemoryBackend() });
    const fs = await manager.openFileSystem('/contracts');
    const make = () => {
        const kernel = new Kernel({ catalog: { fs, rootPath: '/catalog' }, onDiagnostic: () => {}, ...options });
        kernel.registerStorageResolver({ kind: 'test', async resolve(ref) { return { fs, rootPath: ref.locator as string }; } });
        cleanups.push(async () => { kernel.dispose(); await kernel.waitIdle(); });
        return kernel;
    };
    cleanups.push(() => manager.dispose());
    const kernel = make(); await kernel.initialize();
    const session = (id: string) => kernel.createSession({ id, storage: { kind: 'test', locator: `/sessions/${id}` } });
    const store = new SeqFileKernelStore({ fs, rootPath: '/catalog' }, async ref => ({ fs, rootPath: ref.locator as string }));
    return { kernel, session, fs, make, store };
}
const doneProgram = { manifest: { kind: 'done', version: '1' },
    init() { return { state: null, next: { type: 'complete' as const, output: 'done' } }; },
    reduce() { throw new Error('unexpected'); } };
const effectProgram = { manifest: { kind: 'effect', version: '1' },
    init() { return { state: null, actions: [{ type: 'effect' as const, effect: { id: 'e', kind: 'external', version: '1', idempotencyKey: 'e', request: null } }],
        next: { type: 'wait' as const, on: { type: 'effect' as const, id: 'e' } } }; },
    reduce(_state: unknown, event: { type: string; result?: unknown }) { return { state: null, next: { type: 'complete' as const, output: event.result ?? 'done' } }; } };

it('exposes pending pause withdrawal through the canonical Task and keeps the original Effect attempt', async () => {
    const {kernel, session} = await setup(), held = gate(); let calls = 0;
    kernel.registerProgram(effectProgram);
    kernel.registerEffect({kind: 'external', version: '1', execute: async () => {calls++; await held.promise; return 'finished';}});
    const task: Task = await (await session('withdraw-public')).submit({program: effectProgram.manifest, input: null});
    try {
        await expect.poll(() => calls).toBe(1);
        const before = await task.snapshot(), pause = await task.pause({requestId: 'pause-public'});
        expect(pause.acknowledged).toBe(false);
        await task.withdrawPause({requestId: 'withdraw-public', expectedEpoch: pause.epoch});
        const after = await task.snapshot();
        expect(await kernel.readTaskControlReceipt('withdraw-public', task.id, 'pause-public')).toMatchObject({
            action: 'pause', accepted: {acknowledged: false}, current: {mode: 'run', requestId: 'withdraw-public'}});
        expect(await kernel.readTaskControlReceipt('withdraw-public', task.id, 'withdraw-public')).toMatchObject({
            action: 'withdraw-pause', expectedEpoch: pause.epoch, accepted: {mode: 'run'}});
        expect(after.task.effects.e.currentAttempt?.id).toBe(before.task.effects.e.currentAttempt?.id);
        expect(after.task.effects.e.attemptCount).toBe(before.task.effects.e.attemptCount);
        held.resolve(); expect((await task.wait({timeoutMs: 2000})).output).toBe('finished');
        expect(calls).toBe(1);
    } finally {held.resolve();}
});

it('correlates concurrent Effect start, progress and completion and publishes a terminal ExitRecord', async () => {
    const { kernel, session } = await setup();
    kernel.registerEffect({ kind: 'external', version: '1', async execute(_request, context) {
        await context.emit!({ type: 'app.progress', payload: { step: 1 } }); return 'same';
    } });
    kernel.registerProgram({ ...effectProgram, init() { const decision = effectProgram.init();
        return { ...decision, actions: ['a', 'b'].map(id => ({ type: 'effect' as const, effect: { ...decision.actions[0].effect, id, idempotencyKey: id } })),
            next: { type: 'wait', on: { type: 'all', waits: [{ type: 'effect', id: 'a' }, { type: 'effect', id: 'b' }] } } }; } });
    const s = await session('s'), task = await s.submit({ program: effectProgram.manifest, input: null });
    await task.wait({ until: 'settled', timeoutMs: 2000 });
    const events = (await task.readEvents()).items;
    for (const id of ['a', 'b']) {
        const related = events.filter(event => event.effectId === id);
        expect(related.map(event => event.type)).toEqual(['effect.leased', 'app.progress', 'effect.succeeded']);
        expect(new Set(related.map(event => event.attemptId)).size).toBe(1);
    }
    const terminal = events.find(event => event.type === 'task.succeeded')!;
    expect(parseKernelEvent(terminal)?.payload).toMatchObject({ taskId: task.id, status: 'succeeded', output: 'same' });
    expect(events.filter(event => event.type.startsWith('effect.')).map(parseKernelEvent)).not.toContain(undefined);
});

it('interrupts physically, fences late writes and reconciles before resuming the same Task', async () => {
    const { kernel, session } = await setup();
    const execution = gate(), cancellation = gate();
    let context!: EffectExecutionContext;
    const execute = vi.fn(async (_request, ctx) => { context = ctx; await execution.promise; return 'late'; });
    const reconcile = vi.fn(async () => ({ status: 'completed' as const, result: 'reconciled' }));
    const cancel = vi.fn(async () => { execution.resolve(); await cancellation.promise; });
    kernel.registerEffect({ kind: 'external', version: '1', execute, reconcile, cancel });
    kernel.registerProgram(effectProgram);
    const s = await session('s'), task = await s.submit({ program: effectProgram.manifest, input: null });
    await expect.poll(() => execute.mock.calls.length).toBe(1);
    expect((await task.interrupt({ requestId: 'stop' })).acknowledged).toBe(false);
    await expect.poll(() => cancel.mock.calls.length).toBe(1);
    expect(context.abortSignal.aborted).toBe(true);
    expect(context.attemptId).toBeTypeOf('string'); expect(context.attemptNumber).toBe(1);
    await expect(context.emit!({ type: 'app.late' })).rejects.toThrow('Stale effect');
    await expect(task.resume({ requestId: 'too-early' })).rejects.toMatchObject({ code: KernelErrorCode.CONTROL_NOT_SETTLED });
    cancellation.resolve();
    await task.waitStopped({ timeoutMs: 2000 });
    expect((await task.snapshot()).task.effects.e).toMatchObject({ status: 'pending', cleanupPending: false });
    await task.resume({ requestId: 'continue' });
    expect((await task.wait({ timeoutMs: 2000 })).output).toBe('reconciled');
    expect(execute).toHaveBeenCalledTimes(1); expect(reconcile).toHaveBeenCalledTimes(1);
});

it('keeps aggregate control receipts unacknowledged until the entire descendant tree stops', async () => {
    const { kernel, session } = await setup(); const execution = gate(), cancellation = gate();
    kernel.registerProgram(effectProgram);
    kernel.registerEffect({ kind: 'external', version: '1', async execute() { await execution.promise; return null; },
        async cancel() { execution.resolve(); await cancellation.promise; } });
    const s = await session('s'), parent = await s.submit({ program: effectProgram.manifest, input: null, deferStart: true });
    const middle = await s.submit({ program: effectProgram.manifest, input: null, parent: parent.id, deferStart: true });
    const child = await s.submit({ program: effectProgram.manifest, input: null, parent: middle.id });
    await expect.poll(async () => (await child.snapshot()).task.effects.e?.status).toBe('leased');
    await parent.interrupt({ requestId: 'tree-stop' });
    const receipt = (await parent.readEvents()).items.find(event => event.type === 'task.control.interrupt')!;
    expect(receipt.payload).toMatchObject({ acknowledged: false, requestId: 'tree-stop' });
    expect((await middle.stat()).stopped).toBe(false);
    await expect(s.submit({ program: effectProgram.manifest, input: null, parent: middle.id }))
        .rejects.toMatchObject({ code: KernelErrorCode.CONFLICT });
    await expect(parent.waitStopped({ timeoutMs: 20 })).rejects.toMatchObject({ code: KernelErrorCode.WAIT_TIMEOUT });
    cancellation.resolve();
    expect((await parent.waitStopped({ timeoutMs: 2000 })).control.acknowledged).toBe(true);
});

it('keeps tasks submitted during a Session pause observably stopped until explicit resume', async () => {
    const { kernel, session } = await setup(); kernel.registerProgram(doneProgram);
    const s = await session('s'); await s.suspend();
    const task = await s.submit({ program: doneProgram.manifest, input: null });
    expect((await task.waitStopped({ timeoutMs: 100 })).stopped).toBe(true);
    expect(await task.poll()).toBeUndefined();
    await s.resume(); expect((await task.wait({ timeoutMs: 2000 })).output).toBe('done');
});

it('recovers an interrupt committed before the cancelling worker ran', async () => {
    const { kernel, session, fs, store, make } = await setup({ maxConcurrentEffects: 0 });
    kernel.registerProgram(effectProgram);
    const s = await session('s'), task = await s.submit({ program: effectProgram.manifest, input: null });
    await expect.poll(async () => (await task.snapshot()).task.status).toBe('waiting');
    const binding = { fs, rootPath: '/sessions/s' };
    const claim = (await store.claimEffect(binding, task.id, 'e', 'external-worker', 30_000))!;
    await store.controlTask(binding, task.id, 'interrupt', { requestId: 'stop' });
    kernel.dispose(); await kernel.waitIdle();
    await expect(store.completeEffect(binding, task.id, 'e', claim.effect.currentAttempt!.leaseToken, { result: 'stale' })).rejects.toThrow('Stale effect');
    const next = make(), cancel = vi.fn(async () => {});
    next.registerProgram(effectProgram);
    next.registerEffect({ kind: 'external', version: '1', async execute() { throw new Error('must reconcile'); }, cancel });
    await next.initialize(); await next.recoverSession(s.id);
    const attached = await next.attachTask(s.id, task.id);
    await attached.waitStopped({ timeoutMs: 2000 });
    expect(cancel).toHaveBeenCalledTimes(1);
    expect((await attached.snapshot()).task.effects.e.attempts[0].outcome).toBe('lost');
    expect((await attached.snapshot()).task.effects.e.cleanupPending).toBe(false);
    expect(await attached.recoveryIssues()).toContainEqual({ sessionId: s.id, taskId: task.id, effectId: 'e', reason: 'effect-reconciliation' });
});

it('does not acknowledge an interrupt when its adapter cannot confirm physical stop', async () => {
    const { kernel, session } = await setup();
    kernel.registerProgram(effectProgram);
    kernel.registerEffect({ kind: 'external', version: '1', async execute() { return new Promise(() => {}); } });
    const s = await session('s'), task = await s.submit({ program: effectProgram.manifest, input: null });
    await expect.poll(async () => (await task.snapshot()).task.effects.e?.status).toBe('leased');
    await task.interrupt({ requestId: 'stop' });
    await expect(task.waitStopped({ timeoutMs: 30 })).rejects.toMatchObject({ code: KernelErrorCode.WAIT_TIMEOUT });
    expect((await task.stat()).control.acknowledged).toBe(false);
    expect((await task.snapshot()).task.effects.e.cleanupPending).toBe(true);
});

it('never starts a fenced attempt whose grant resolution finishes after interrupt confirmation', async () => {
    const { kernel, session } = await setup(), authorization = gate();
    const internal = kernel as unknown as { store: SeqFileKernelStore };
    const authorize = vi.spyOn(internal.store, 'authorizeResource').mockImplementation(async () => {
        await authorization.promise;
        return { id: 'r', sessionId: 's', kind: 'external', uri: 'logical:r', generation: 1, createdAt: 0 };
    });
    const execute = vi.fn(async () => 'must-not-start');
    kernel.registerEffect({ kind: 'external', version: '1', execute, async cancel() {} });
    kernel.registerProgram({ ...effectProgram, init() { const d = effectProgram.init();
        return { ...d, actions: [{ type: 'effect', effect: { ...d.actions[0].effect, grants: [{ handleId: 'held', right: 'execute' }] } }] }; } });
    const task = await (await session('s')).submit({ program: effectProgram.manifest, input: null });
    await expect.poll(() => authorize.mock.calls.length).toBe(1);
    await task.interrupt({ requestId: 'stop-before-execute' });
    await task.waitStopped({ timeoutMs: 2000 });
    authorization.resolve(); await kernel.waitIdle();
    expect(execute).not.toHaveBeenCalled();
});

it('keeps default watch and settled wait open until the physical cleanup confirmation', async () => {
    const { kernel, session } = await setup();
    const cancellation = gate();
    kernel.registerEffect({ kind: 'external', version: '1', async execute() { return new Promise(() => {}); }, async cancel() { await cancellation.promise; } });
    kernel.registerProgram({ ...effectProgram, init() { const d = effectProgram.init(); d.actions[0].effect = { ...d.actions[0].effect, timeoutMs: 40 } as typeof d.actions[0]['effect']; return d; } });
    const s = await session('s'), task = await s.submit({ program: effectProgram.manifest, input: null });
    await task.wait({ timeoutMs: 2000 });
    const types: string[] = [];
    let ended = false;
    const following = (async () => { for await (const event of task.watch()) types.push(event.type); ended = true; })();
    await expect.poll(() => types.includes('task.succeeded')).toBe(true);
    expect(ended).toBe(false);
    await expect(task.wait({ until: 'settled', timeoutMs: 20 })).rejects.toMatchObject({ code: KernelErrorCode.WAIT_TIMEOUT });
    const logical = []; for await (const event of task.watch({ until: 'exit' })) logical.push(event);
    expect(logical.some(event => event.type === 'effect.cleanup.confirmed')).toBe(false);
    cancellation.resolve(); await following;
    expect(types.at(-1)).toBe('effect.cleanup.confirmed');
    expect((await task.wait({ until: 'settled', timeoutMs: 2000 })).status).toBe('succeeded');
});

it('reads cold snapshots and events without registering execution or starting ready work', async () => {
    const { kernel, session, make } = await setup({ maxConcurrent: 0 });
    const s = await session('s'), task = await s.submit({ program: doneProgram.manifest, input: null });
    kernel.dispose(); await kernel.waitIdle();
    const reader = make(), init = vi.fn(doneProgram.init);
    reader.registerProgram({ ...doneProgram, init }); await reader.initialize();
    const snapshot = await reader.inspectTask(task.id), inspection = await reader.inspectSession(s.id);
    await inspection.snapshot(); await inspection.readEvents(); await inspection.listTaskPage();
    const handle = await inspection.attachTask(task.id); await handle.status(); await handle.snapshot(); await handle.readEvents();
    expect(snapshot.asOfSequence).toBeGreaterThan(0);
    expect((reader as unknown as { sessions: Map<string, unknown> }).sessions.size).toBe(0);
    expect(init).not.toHaveBeenCalled();
});

it('rejects expired Session and Task cursors and resumes after an atomic snapshot boundary', async () => {
    const { kernel, session, fs, store } = await setup(); kernel.registerProgram(doneProgram);
    const s = await session('s'), task = await s.submit({ program: doneProgram.manifest, input: null });
    await task.wait(); await kernel.pruneTaskEvents(s.id, task.id, { keepEvents: 1 });
    for (const read of [() => task.readEvents(), () => s.readEvents()]) {
        await expect(read()).rejects.toMatchObject({ code: KernelErrorCode.CURSOR_EXPIRED, details: { sessionId: s.id } });
    }
    await expect(task.watch()[Symbol.asyncIterator]().next()).rejects.toMatchObject({ code: KernelErrorCode.CURSOR_EXPIRED });
    await fs.meta.seq!.transaction!(async tx => {
        await tx.deleteEntry('/sessions/s/events.seq', 'pruned-through-sequence');
        await tx.deleteEntry('/sessions/s/events.seq', `task-event-pruned/${encodeURIComponent(task.id)}`);
    });
    await expect(s.readEvents()).rejects.toMatchObject({ code: KernelErrorCode.CURSOR_EXPIRED });
    await expect(task.readEvents()).rejects.toMatchObject({ code: KernelErrorCode.CURSOR_EXPIRED });
    const snapshot = await task.snapshot();
    await store.appendEvent({ fs, rootPath: '/sessions/s' }, s.id, task.id, 'app.after', { value: 1 });
    expect((await task.readEvents({ afterSequence: snapshot.asOfSequence })).items.map(event => event.type)).toEqual(['app.after']);
    expect((await s.snapshot()).tasks.map(t => t.id)).toEqual([task.id]);
});

it('reads legacy event schemas cold and declares v2 atomically on the first new write', async () => {
    const { kernel, session, fs, store, make } = await setup({ maxConcurrent: 0 });
    const s = await session('s'), task = await s.submit({ program: doneProgram.manifest, input: null });
    kernel.dispose(); await kernel.waitIdle();
    const path = '/sessions/s/session.seq', record = JSON.parse((await fs.meta.seq!.getEntry(path, 'record'))!);
    record.layout.recordSchemas.events = 1;
    record.layout.recordSchemas.index = 1;
    await fs.meta.seq!.setEntry(path, 'record', JSON.stringify(record));
    const reader = make(); await reader.initialize(); await reader.inspectTask(task.id);
    expect(JSON.parse((await fs.meta.seq!.getEntry(path, 'record'))!).layout.recordSchemas.events).toBe(1);
    expect(JSON.parse((await fs.meta.seq!.getEntry(path, 'record'))!).layout.recordSchemas.index).toBe(1);
    await store.appendEvent({ fs, rootPath: '/sessions/s' }, s.id, task.id, 'app.upgraded');
    expect(JSON.parse((await fs.meta.seq!.getEntry(path, 'record'))!).layout.recordSchemas.events).toBe(2);
    expect(JSON.parse((await fs.meta.seq!.getEntry(path, 'record'))!).layout.recordSchemas.index).toBe(2);
    record.layout.recordSchemas.events = 3;
    await fs.meta.seq!.setEntry(path, 'record', JSON.stringify(record));
    await expect(store.appendEvent({ fs, rootPath: '/sessions/s' }, s.id, task.id, 'app.downgrade')).rejects.toThrow('Unsupported Session record schema');
});

it('rejects invalid waits and caller cancellation without cancelling the Task', async () => {
    const { session } = await setup({ maxConcurrent: 0 });
    const task = await (await session('s')).submit({ program: doneProgram.manifest, input: null });
    await expect(task.wait({ timeoutMs: -1 })).rejects.toMatchObject({ code: KernelErrorCode.INVALID_SPEC });
    const controller = new AbortController(), waiting = task.wait({ signal: controller.signal }); controller.abort();
    await expect(waiting).rejects.toMatchObject({ code: KernelErrorCode.ABORTED });
    expect((await task.snapshot()).task.status).toBe('ready');
});

it('logs control failures with safe identities and classifications, without control reasons or payloads', async () => {
    const diagnostics: unknown[] = [];
    const { session } = await setup({ maxConcurrent: 0, onDiagnostic: event => diagnostics.push(event) });
    const task = await (await session('s')).submit({ program: doneProgram.manifest, input: 'secret-payload' });
    await task.pause({ requestId: 'pause', reason: 'secret-reason' });
    await expect(task.resume({ requestId: 'resume', expectedEpoch: 999 })).rejects.toMatchObject({ code: KernelErrorCode.CONFLICT });
    expect(diagnostics).toContainEqual(expect.objectContaining({ operation: 'task.run', stage: 'admission', outcome: 'failure',
        operationId: 'resume', errorCode: KernelErrorCode.CONFLICT, durationMs: expect.any(Number), sessionId: 's', taskId: task.id }));
    expect(JSON.stringify(diagnostics)).not.toContain('secret-');
});

it('pins event page membership while concurrent appends and Task filtering preserve Session sequences', async () => {
    const { kernel, session, fs, store } = await setup(); kernel.registerProgram(doneProgram);
    const s = await session('s'), tasks = await Promise.all([1, 2].map(() => s.submit({ program: doneProgram.manifest, input: null })));
    await Promise.all(tasks.map(t => t.wait()));
    const first = await tasks[0].readEvents({ limit: 1 });
    await store.appendEvent({ fs, rootPath: '/sessions/s' }, s.id, tasks[0].id, 'app.later');
    const sequences = first.items.map(e => e.sequence); let next = first.nextAfterSequence;
    while (next !== undefined) {
        const page = await tasks[0].readEvents({ afterSequence: next, throughSequence: first.throughSequence, limit: 1 });
        sequences.push(...page.items.map(e => e.sequence)); next = page.nextAfterSequence;
    }
    expect(sequences).toEqual((await tasks[0].readEvents({ throughSequence: first.throughSequence })).items.map(e => e.sequence));
    expect((await tasks[0].readEvents({ afterSequence: first.throughSequence })).items.map(e => e.type)).toEqual(['app.later']);
    await expect(s.readEvents({ limit: 501 })).rejects.toMatchObject({ code: KernelErrorCode.INVALID_SPEC });
});

it('cancels a pending watch without cancelling execution, and ends watchers at disposal or Session close', async () => {
    const { kernel, session } = await setup({ maxConcurrent: 0 });
    const s = await session('s'), task = await s.submit({ program: doneProgram.manifest, input: null });
    const controller = new AbortController(), after = (await s.snapshot()).asOfSequence;
    const iterator = s.watch({ after, signal: controller.signal })[Symbol.asyncIterator]();
    const pending = iterator.next(); controller.abort();
    await expect(pending).rejects.toMatchObject({ code: KernelErrorCode.ABORTED });
    expect((await task.snapshot()).task.status).toBe('ready');
    const watching = s.watch({ after })[Symbol.asyncIterator](), disposing = watching.next(); kernel.dispose();
    expect((await disposing).done).toBe(true);
});

it('ends a closed Session watch after draining its final transaction', async () => {
    const { session } = await setup(); const s = await session('s');
    await s.close(); const types = [];
    for await (const event of s.watch()) types.push(event.type);
    expect(types).toContain('session.closed');
});

it('validates known v2 payloads, preserves legacy identities and skips unknown event types', () => {
    const base = { schemaVersion: 2, sequence: 1, sessionId: 's', taskId: 't', type: 'effect.succeeded', occurredAt: 0, payload: { result: 'same' } };
    expect(() => parseKernelEvent(base)).toThrow('Invalid journal');
    expect(parseKernelEvent({ ...base, effectId: 'e', attemptId: 'a' })?.type).toBe('effect.succeeded');
    expect(parseKernelEvent({ ...base, schemaVersion: 1 })).toBeUndefined();
    expect(parseKernelEvent({ ...base, type: 'app.progress' })).toBeUndefined();
    expect(() => parseEventEnvelope({ ...base, schemaVersion: 3 })).toThrow('Invalid journal');
    expect(() => parseKernelEvent({ ...base, type: 'task.succeeded', payload: { taskId: 'other', status: 'failed', completedAt: 0 } })).toThrow('Invalid journal');
});

it('reports corrupt persisted JSON as a storage error rather than silently skipping it', async () => {
    const { kernel, session, fs } = await setup(); kernel.registerProgram(doneProgram);
    const task = await (await session('s')).submit({ program: doneProgram.manifest, input: null });
    await task.wait();
    const first = (await task.readEvents()).items[0];
    await fs.meta.seq!.setEntry('/sessions/s/events.seq', `event/${String(first.sequence).padStart(16, '0')}`, '{invalid');
    await expect(task.readEvents()).rejects.toMatchObject({ code: KernelErrorCode.STORAGE_CORRUPT });
});

it('rejects application attempts to spoof built-in event names with typed diagnostics', async () => {
    const { kernel, session } = await setup();
    kernel.registerProgram({ ...doneProgram, init() { return { ...doneProgram.init(), actions: [{ type: 'emit', eventType: 'task.succeeded', payload: null }] }; } });
    const task = await (await session('s')).submit({ program: doneProgram.manifest, input: null });
    expect((await task.wait()).error?.code).toBe(KernelErrorCode.EVENT_INVALID);
});

it('keeps retry recursively narrow and exposes cold inspection through the core entry', async () => {
    type Retried = Awaited<ReturnType<Task['retry']>>;
    const legacyHidden: 'sendMessage' extends keyof Retried ? false : true = true;
    expect(legacyHidden).toBe(true);
    const { fs } = await setup();
    const harness = createHarness({ catalog: { fs, rootPath: '/core' }, maxConcurrent: 0 });
    harness.registerStorageResolver({ kind: 'test', async resolve() { return { fs, rootPath: '/core-session' }; } });
    cleanups.push(async () => { harness.dispose(); await harness.waitIdle(); }); await harness.initialize();
    const s = await harness.createSession({ id: 'core', storage: { kind: 'test', locator: null } });
    const task = await s.submit({ program: doneProgram.manifest, input: null });
    expect((await harness.inspectTask(task.id)).task.id).toBe(task.id);
    expect((await (await harness.inspectSession(s.id)).snapshot()).session.id).toBe(s.id);
});

it('reads original control admissions after replacement and cold restart without replaying or leaking signals', async () => {
    const f = await setup(); f.kernel.registerProgram(doneProgram);
    const session = await f.session('receipts'), other = await f.session('other');
    const task = await session.submit({program: doneProgram.manifest, input: null, deferStart: true});
    const admitted = JSON.parse(JSON.stringify(await task.pause({requestId: 'pause/original', expectedEpoch: 0})));
    await task.resume({requestId: 'resume', expectedEpoch: admitted.epoch, signal: {type: 'private', payload: 'PRIVATE-SIGNAL'}});
    const before = (await task.status()).task;
    const history = await task.history();
    expect(await f.kernel.readTaskControlReceipt(session.id, task.id, 'pause/original')).toMatchObject({
        requestId: 'pause/original', action: 'pause', expectedEpoch: 0, accepted: admitted, current: {requestId: 'resume', mode: 'run'}});
    expect(JSON.stringify(await f.kernel.readTaskControlReceipt(session.id, task.id, 'resume'))).not.toContain('PRIVATE-SIGNAL');
    expect(await f.kernel.readTaskControlReceipt(session.id, task.id, 'missing')).toBeUndefined();
    expect((await task.status()).task.version).toBe(before.version);
    expect(await task.history()).toEqual(history);
    await expect(f.kernel.readTaskControlReceipt(other.id, task.id, 'pause/original')).rejects.toBeDefined();
    await expect(f.kernel.readTaskControlReceipt(session.id, task.id, '')).rejects.toMatchObject({code: 'INVALID_SPEC'});
    f.kernel.dispose(); await f.kernel.waitIdle();
    const reader = f.make(); await reader.initialize();
    const open = vi.spyOn(reader, 'openSession');
    expect(await reader.readTaskControlReceipt(session.id, task.id, 'pause/original')).toMatchObject({accepted: admitted});
    expect(open).not.toHaveBeenCalled();
});

it('persists cancellation admission atomically and replays after a cold restart without exposing the reason', async () => {
    const f = await setup({maxConcurrent: 0}), session = await f.session('cancel-receipts');
    const task = await session.submit({program: doneProgram.manifest, input: null, deferStart: true});
    const options = {requestId: 'cancel/original', expectedEpoch: 0};
    await task.cancel('PRIVATE-REASON', options);
    const receipt = {requestId: options.requestId, taskId: task.id, expectedEpoch: 0, controlEpoch: 0, status: 'cancelled'};
    expect(await f.kernel.readTaskCancellationReceipt(session.id, task.id, options.requestId)).toEqual(receipt);
    const before = await task.snapshot(), history = await task.history();
    await task.cancel('PRIVATE-REASON', options);
    expect(await task.snapshot()).toEqual(before); expect(await task.history()).toEqual(history);
    await expect(task.cancel('changed', options)).rejects.toMatchObject({code: KernelErrorCode.CONFLICT});
    await expect(task.cancel('PRIVATE-REASON', {...options, expectedEpoch: 1})).rejects.toMatchObject({code: KernelErrorCode.CONFLICT});
    f.kernel.dispose(); await f.kernel.waitIdle();
    const reader = f.make(); await reader.initialize(); const open = vi.spyOn(reader, 'openSession');
    expect(await reader.readTaskCancellationReceipt(session.id, task.id, options.requestId)).toEqual(receipt);
    expect(open).not.toHaveBeenCalled();
    await reader.cancel(session.id, task.id, 'PRIVATE-REASON', options);
    expect((await reader.task(session.id, task.id)).version).toBe(before.task.version);
});

it('records the true completion when it wins cancellation and does not rewrite its output', async () => {
    const f = await setup(); f.kernel.registerProgram(doneProgram);
    const session = await f.session('cancel-race'), task = await session.submit({program: doneProgram.manifest, input: null});
    await task.wait({timeoutMs: 2000}); const before = (await task.snapshot()).task;
    await task.cancel('late', {requestId: 'cancel', expectedEpoch: 0});
    expect(await f.kernel.readTaskCancellationReceipt(session.id, task.id, 'cancel')).toMatchObject({status: 'succeeded'});
    expect((await task.snapshot()).task).toEqual(before);
});

it('rejects stale cancellation epochs without saving a receipt and scopes cold reads to the Task owner', async () => {
    const f = await setup({maxConcurrent: 0}), session = await f.session('cancel-stale'), other = await f.session('other-cancel');
    const task = await session.submit({program: doneProgram.manifest, input: null, deferStart: true});
    const control = await task.pause({requestId: 'pause'}), before = await task.snapshot();
    await expect(task.cancel('stale', {requestId: 'cancel', expectedEpoch: 0})).rejects.toMatchObject({code: KernelErrorCode.CONFLICT});
    expect(await f.kernel.readTaskCancellationReceipt(session.id, task.id, 'cancel')).toBeUndefined();
    expect(await task.snapshot()).toEqual(before);
    await expect(task.cancel('invalid', {expectedEpoch: control.epoch})).rejects.toMatchObject({code: KernelErrorCode.INVALID_SPEC});
    await expect(f.kernel.readTaskCancellationReceipt(other.id, task.id, 'cancel')).rejects.toBeDefined();
    for (const invalid of ['', ' ', undefined, 1])
        await expect(f.kernel.readTaskCancellationReceipt(session.id, task.id, invalid as string)).rejects.toMatchObject({code: KernelErrorCode.INVALID_SPEC});
    await task.cancel('current', {requestId: 'current', expectedEpoch: control.epoch});
    expect(await f.kernel.readTaskCancellationReceipt(session.id, task.id, 'current')).toMatchObject({controlEpoch: control.epoch, status: 'cancelled'});
});

it('makes cancellation admission readable while physical cleanup is still unconfirmed', async () => {
    const diagnostics: import('./index').KernelDiagnostic[] = [], f = await setup({onDiagnostic: event => diagnostics.push(event)});
    const execution = gate(), cleanup = gate(); f.kernel.registerProgram(effectProgram);
    const cancel = vi.fn(async () => {execution.resolve(); await cleanup.promise;});
    f.kernel.registerEffect({kind: 'external', version: '1', execute: async () => {await execution.promise; return null;}, cancel});
    const session = await f.session('cancel-cleanup'), task = await session.submit({program: effectProgram.manifest, input: null});
    try {
        await expect.poll(async () => (await task.snapshot()).task.effects.e?.status).toBe('leased');
        const cancelling = task.cancel('PRIVATE-CLEANUP', {requestId: 'cancel-cleanup'});
        await expect.poll(() => cancel.mock.calls.length).toBe(1);
        expect(await f.kernel.readTaskCancellationReceipt(session.id, task.id, 'cancel-cleanup')).toMatchObject({status: 'cancelled'});
        expect((await task.stat()).settled).toBe(false);
        cleanup.resolve(); await cancelling; await task.waitStopped({timeoutMs: 2000});
        expect(diagnostics).toContainEqual(expect.objectContaining({operation: 'task.cancel', operationId: 'cancel-cleanup', stage: 'cleanup', outcome: 'success'}));
        expect(JSON.stringify(diagnostics)).not.toContain('PRIVATE-CLEANUP');
    } finally {execution.resolve(); cleanup.resolve();}
});

it('does not propagate a parent cancellation identity or epoch into independently controlled children', async () => {
    const f = await setup({maxConcurrent: 0}), session = await f.session('cancel-tree');
    const parent = await session.submit({program: doneProgram.manifest, input: null, deferStart: true});
    const child = await session.submit({program: doneProgram.manifest, input: null, parent: parent.id, deferStart: true});
    await child.pause({requestId: 'child-pause'});
    await parent.cancel('tree', {requestId: 'parent-cancel', expectedEpoch: 0});
    expect((await child.snapshot()).task.status).toBe('cancelled');
    expect(await f.kernel.readTaskCancellationReceipt(session.id, child.id, 'parent-cancel')).toBeUndefined();
});

it('retains cancellation evidence on cleanup failure and diagnoses the actual failing stage', async () => {
    const diagnostics: import('./index').KernelDiagnostic[] = [], f = await setup({onDiagnostic: event => diagnostics.push(event)});
    const execution = gate(); let canStop = false; f.kernel.registerProgram(effectProgram);
    f.kernel.registerEffect({kind: 'external', version: '1', execute: async () => {await execution.promise; return null;},
        cancel: async () => {execution.resolve(); if (!canStop) throw new Error('PRIVATE-REMOTE-ERROR');}});
    const session = await f.session('cancel-failure'), task = await session.submit({program: effectProgram.manifest, input: null});
    try {
        await expect.poll(async () => (await task.snapshot()).task.effects.e?.status).toBe('leased');
        await expect(task.cancel('PRIVATE-REASON', {requestId: 'cancel-failed'})).rejects.toBeDefined();
        expect(await f.kernel.readTaskCancellationReceipt(session.id, task.id, 'cancel-failed')).toMatchObject({status: 'cancelled'});
        expect((await task.snapshot()).task.effects.e.cleanupPending).toBe(true);
        expect(diagnostics).toContainEqual(expect.objectContaining({operation: 'task.cancel', stage: 'cleanup', outcome: 'failure', operationId: 'cancel-failed'}));
        await expect(task.cancel('changed', {requestId: 'cancel-failed'})).rejects.toMatchObject({code: KernelErrorCode.CONFLICT});
        expect(diagnostics).toContainEqual(expect.objectContaining({operation: 'task.cancel', stage: 'admission', outcome: 'failure', errorCode: 'CONFLICT'}));
        expect(JSON.stringify(diagnostics)).not.toContain('PRIVATE-');
    } finally {canStop = true; execution.resolve(); await task.cancel('PRIVATE-REASON', {requestId: 'cancel-failed'});}
});

const answerProgram = {manifest: {kind: 'answer-receipt', version: '1'},
    init() {return {state: null, actions: ['answer', 'other'].map(id => ({type: 'request-interaction' as const,
        interaction: {id, kind: 'input' as const, prompt: 'PRIVATE-PROMPT'}})), next: {type: 'wait' as const, on: {type: 'interaction' as const, id: 'answer'}}};},
    reduce() {return {state: null, next: {type: 'complete' as const, output: 'finished'}};}};
async function answeringTask(options: Partial<KernelOptions> = {}) {
    const f = await setup(options); f.kernel.registerProgram(answerProgram); const session = await f.session('answer-receipts');
    const task = await session.submit({program: answerProgram.manifest, input: null});
    await expect.poll(async () => (await task.status()).task.interactions.answer?.status).toBe('pending');
    await task.pause({requestId: 'hold'});
    return {...f, session, task};
}
it('persists answer identity atomically, keeps pause and reads the original receipt after completion and cold restart', async () => {
    const f = await answeringTask(), response = {interactionId: 'answer', value: 'PRIVATE-ANSWER'};
    await f.task.respond(response, {requestId: 'answer-one'});
    const receipt = await f.kernel.readTaskInteractionResponseReceipt(f.session.id, f.task.id, 'answer-one');
    expect(receipt).toEqual({requestId: 'answer-one', taskId: f.task.id, interactionId: 'answer', resolvedAt: expect.any(Number)});
    expect((await f.task.status()).task.control?.mode).toBe('pause');
    const before = await f.task.snapshot(), history = await f.task.history();
    await f.session.respond(f.task.id, response, {requestId: 'answer-one'});
    expect(await f.task.snapshot()).toEqual(before); expect(await f.task.history()).toEqual(history);
    await f.task.resume({requestId: 'run'}); await f.task.wait();
    await f.task.respond(response, {requestId: 'answer-one'});
    f.kernel.dispose(); await f.kernel.waitIdle();
    const reader = f.make(); await reader.initialize(); const open = vi.spyOn(reader, 'openSession');
    expect(await reader.readTaskInteractionResponseReceipt(f.session.id, f.task.id, 'answer-one')).toEqual(receipt);
    expect(await reader.readTaskInteractionResponseReceipt(f.session.id, f.task.id, 'missing')).toBeUndefined();
    expect(open).not.toHaveBeenCalled(); expect(JSON.stringify(receipt)).not.toContain('PRIVATE');
});
it('rejects changed answer identities and never adopts a legacy or another client response', async () => {
    const f = await answeringTask();
    await f.task.respond({interactionId: 'answer', value: 'yes'}, {requestId: 'original'});
    const before = await f.task.snapshot(), history = await f.task.history();
    for (const [interactionId, value, requestId] of [['answer', 'no', 'original'], ['other', 'yes', 'original'], ['answer', 'yes', 'another']])
        await expect(f.task.respond({interactionId, value}, {requestId})).rejects.toMatchObject({code: 'CONFLICT'});
    expect(await f.task.snapshot()).toEqual(before); expect(await f.task.history()).toEqual(history);
    await f.task.respond({interactionId: 'other', value: 'legacy'});
    await expect(f.task.respond({interactionId: 'other', value: 'legacy'}, {requestId: 'adopt'})).rejects.toMatchObject({code: 'CONFLICT'});
    expect(await f.kernel.readTaskInteractionResponseReceipt(f.session.id, f.task.id, 'adopt')).toBeUndefined();
    await f.task.respond({interactionId: 'other', value: 'legacy'});
});
it('arbitrates competing answer identities in the answer transaction', async () => {
    const f = await answeringTask();
    const outcomes = await Promise.allSettled(['first', 'second'].map(requestId => f.task.respond({interactionId: 'answer', value: 'same'}, {requestId})));
    expect(outcomes.filter(item => item.status === 'fulfilled')).toHaveLength(1);
    const receipts = await Promise.all(['first', 'second'].map(id => f.kernel.readTaskInteractionResponseReceipt(f.session.id, f.task.id, id)));
    expect(receipts.filter(Boolean)).toHaveLength(1);
    expect((await f.store.events(await f.store.inspectSessionBinding(f.session.id))).filter(event => event.type === 'task.interaction.resolved')).toHaveLength(1);
});
it('rejects invalid answer identities without persisting an answer and logs no prompt or response', async () => {
    const logs: unknown[] = [], f = await answeringTask({onDiagnostic: entry => {logs.push(entry);}});
    for (const requestId of ['', '   ', 'x'.repeat(257)]) {
        await expect(f.task.respond({interactionId: 'answer', value: 'PRIVATE-ANSWER'}, {requestId})).rejects.toMatchObject({code: 'INVALID_SPEC'});
        await expect(f.kernel.readTaskInteractionResponseReceipt(f.session.id, f.task.id, requestId)).rejects.toMatchObject({code: 'INVALID_SPEC'});
    }
    expect((await f.task.status()).task.interactions.answer.status).toBe('pending');
    expect(logs).toContainEqual(expect.objectContaining({operation: 'task.interaction.respond', outcome: 'failure', errorCode: 'INVALID_SPEC'}));
    expect(JSON.stringify(logs)).not.toContain('PRIVATE');
});
it('does not retain answer admission after cancellation wins', async () => {
    const f = await answeringTask(); await f.task.cancel('stop');
    await expect(f.task.respond({interactionId: 'answer', value: 'yes'}, {requestId: 'too-late'})).rejects.toMatchObject({code: 'CONFLICT'});
    expect(await f.kernel.readTaskInteractionResponseReceipt(f.session.id, f.task.id, 'too-late')).toBeUndefined();
    const other = await f.kernel.createSession({id: 'another-session', storage: {kind: 'test', locator: '/sessions/another-session'}});
    await expect(f.kernel.readTaskInteractionResponseReceipt(other.id, f.task.id, 'too-late')).rejects.toBeDefined();
});


it('cancels descendants through a completed intermediate Task without rewriting its result', async () => {
    const f = await setup(); f.kernel.registerProgram(doneProgram);
    const session = await f.session('cancel-terminal-bridge');
    const root = await session.submit({program: doneProgram.manifest, input: null, deferStart: true});
    const bridge = await session.submit({program: doneProgram.manifest, input: null, parent: root.id, deferStart: true});
    const leaf = await session.submit({program: doneProgram.manifest, input: null, parent: bridge.id, deferStart: true});
    const unrelated = await session.submit({program: doneProgram.manifest, input: null, deferStart: true});
    await bridge.start(); await bridge.wait({timeoutMs: 2000});
    const before = (await bridge.snapshot()).task;
    await root.cancel('tree', {requestId: 'root-cancel', expectedEpoch: 0});
    expect((await leaf.snapshot()).task.status).toBe('cancelled');
    expect((await bridge.snapshot()).task).toEqual(before);
    expect((await unrelated.snapshot()).task.exit).toBeUndefined();
    expect(await f.kernel.readTaskCancellationReceipt(session.id, leaf.id, 'root-cancel')).toBeUndefined();
});


it('does not turn a late cancellation receipt for a completed root into descendant cancellation', async () => {
    const f = await setup(); f.kernel.registerProgram(doneProgram);
    const session = await f.session('cancel-completed-root');
    const root = await session.submit({program: doneProgram.manifest, input: null, deferStart: true});
    const child = await session.submit({program: doneProgram.manifest, input: null, parent: root.id, deferStart: true});
    await root.start(); await root.wait({timeoutMs: 2000});
    await root.cancel('late', {requestId: 'late-cancel', expectedEpoch: 0});
    expect(await f.kernel.readTaskCancellationReceipt(session.id, root.id, 'late-cancel')).toMatchObject({status: 'succeeded'});
    expect((await child.snapshot()).task.status).toBe('created');
});

it.each(['root', 'child'] as const)('does not let a hanging %s cleanup block cancellation of other descendants', async blocked => {
    const f = await setup({effectCleanupTimeoutMs: 10000}), cleanup = gate(), executions = new Map<string, ReturnType<typeof gate>>();
    let blockedId = ''; const stopped = new Set<string>(); f.kernel.registerProgram(effectProgram);
    f.kernel.registerEffect({kind: 'external', version: '1', execute: async (_input, context) => {
        const held = gate(); executions.set(context.taskId, held); await held.promise; return null;
    }, cancel: async (_input, context) => {stopped.add(context.taskId); executions.get(context.taskId)?.resolve(); if (context.taskId === blockedId) await cleanup.promise;}});
    const session = await f.session(`cancel-hanging-${blocked}`), root = await session.submit({program: effectProgram.manifest, input: null});
    const children = await Promise.all([0, 1].map(() => session.submit({program: effectProgram.manifest, input: null, parent: root.id})));
    const childIds = children.map(child => child.id).sort(); blockedId = blocked === 'root' ? root.id : childIds[0];
    await expect.poll(() => executions.size).toBe(3);
    const requests = vi.spyOn(f.kernel, 'cancel');
    const cancelling = root.cancel('tree', {requestId: 'cancel-tree'}).then(() => undefined, error => error);
    try {
        await expect.poll(() => childIds.every(id => requests.mock.calls.some(([, taskId]) => taskId === id)), {timeout: 1000}).toBe(true);
        await expect.poll(() => childIds.every(id => stopped.has(id)), {timeout: 1000}).toBe(true);
        expect((await f.kernel.task(session.id, blockedId)).effects.e.cleanupPending).toBe(true);
        expect(await f.kernel.readTaskCancellationReceipt(session.id, root.id, 'cancel-tree')).toMatchObject({status: 'cancelled'});
        cleanup.resolve(); expect(await cancelling).toBeUndefined();
    } finally {cleanup.resolve(); for (const held of executions.values()) held.resolve(); await cancelling;}
});

it('joins other stop confirmations after a cleanup failure and replays only the retained cleanup duties', async () => {
    const diagnostics: import('./index').KernelDiagnostic[] = [], f = await setup({onDiagnostic: event => diagnostics.push(event)});
    const finish = gate(), executions = new Map<string, ReturnType<typeof gate>>(), stopped = new Set<string>();
    let rootId = '', fail = true, completed = false, starts = 0; f.kernel.registerProgram(effectProgram);
    f.kernel.registerEffect({kind: 'external', version: '1', execute: async (_input, context) => {
        starts++; const held = gate(); executions.set(context.taskId, held); await held.promise; return null;
    }, cancel: async (_input, context) => {
        stopped.add(context.taskId); executions.get(context.taskId)?.resolve();
        if (context.taskId === rootId && fail) throw new Error('PRIVATE-CLEANUP-FAILURE');
        await finish.promise;
    }});
    const session = await f.session('cancel-tree-failure'), root = await session.submit({program: effectProgram.manifest, input: null}); rootId = root.id;
    const child = await session.submit({program: effectProgram.manifest, input: null, parent: root.id});
    await expect.poll(() => executions.size).toBe(2);
    const options = {requestId: 'tree-failure'}, cancelling = root.cancel('PRIVATE-REASON', options).then(() => {completed = true;}, error => {completed = true; return error;});
    try {
        await expect.poll(() => stopped.has(child.id)).toBe(true); expect(completed).toBe(false);
        expect((await root.snapshot()).task.effects.e.cleanupPending).toBe(true);
        finish.resolve(); expect(await cancelling).toBeInstanceOf(AggregateError);
        expect((await child.stat()).settled).toBe(true); expect((await root.stat()).settled).toBe(false);
        expect(await f.kernel.readTaskCancellationReceipt(session.id, root.id, options.requestId)).toMatchObject({status: 'cancelled'});
        expect(JSON.stringify(diagnostics)).not.toContain('PRIVATE');
        fail = false; await root.cancel('PRIVATE-REASON', options); expect((await root.stat()).settled).toBe(true);
        expect(starts).toBe(2);
    } finally {fail = false; finish.resolve(); for (const held of executions.values()) held.resolve(); await cancelling; await root.cancel('PRIVATE-REASON', options);}
});

it('takes cleanup duties from the cancellation transaction when an Effect is admitted concurrently', async () => {
    const f = await setup(), execution = gate(), cleanup = gate(); let started = false, cleaning = false, completed = false;
    const program = {manifest: {kind: 'late-cancel-effect', version: '1'}, init: () => ({state: null, next: {type: 'wait' as const, on: {type: 'signal' as const}}}), reduce: effectProgram.init};
    f.kernel.registerProgram(program);
    f.kernel.registerEffect({kind: 'external', version: '1', execute: async () => {started = true; await execution.promise; return null;},
        cancel: async () => {cleaning = true; execution.resolve(); await cleanup.promise;}});
    const session = await f.session('cancel-admission-race'), task = await session.submit({program: program.manifest, input: null});
    await expect.poll(async () => (await task.snapshot()).task.wait?.type).toBe('signal');
    const original = SeqFileKernelStore.prototype.cancelTask;
    const intercept = vi.spyOn(SeqFileKernelStore.prototype, 'cancelTask').mockImplementationOnce(async function (this: SeqFileKernelStore, ...args) {
        await task.signal({type: 'create-effect'}); await expect.poll(() => started).toBe(true);
        return original.apply(this, args);
    });
    const options = {requestId: 'race-cancel'}, cancelling = task.cancel('stop', options).then(() => {completed = true;}, error => {completed = true; return error;});
    try {
        await expect.poll(() => cleaning).toBe(true); expect(completed).toBe(false);
        cleanup.resolve(); expect(await cancelling).toBeUndefined(); expect((await task.stat()).settled).toBe(true);
    } finally {intercept.mockRestore(); execution.resolve(); cleanup.resolve(); await cancelling; await task.cancel('stop', options);}
});

async function pendingCleanupRecovery(mode: 'cancelled' | 'closing', options: Partial<KernelOptions> = {}) {
    const f = await setup({maxConcurrentEffects: 0, effectCleanupTimeoutMs: 10000, ...options});
    f.kernel.registerProgram(effectProgram); const session = await f.session('recover-cleanup-branches');
    const tasks = await Promise.all([0, 1].map(() => session.submit({program: effectProgram.manifest, input: null})));
    await expect.poll(async () => (await session.listTasks()).filter(task => task.effects.e?.status === 'pending').length).toBe(2);
    f.kernel.dispose(); await f.kernel.waitIdle();
    const binding = {fs: f.fs, rootPath: `/sessions/${session.id}`};
    for (const task of tasks) {
        await f.store.claimEffect(binding, task.id, 'e', 'previous-worker', 30000);
        if (mode === 'cancelled') await f.store.cancelTask(binding, task.id, 'stop');
    }
    if (mode === 'closing') await f.store.setSessionStatus(binding, 'closing', 'cancel');
    return {f, session, ids: tasks.map(task => task.id).sort()};
}

it.each(['cancelled', 'closing'] as const)('recovers %s cleanup duties without waiting for the first adapter confirmation', async mode => {
    const {f, session, ids} = await pendingCleanupRecovery(mode), held = gate();
    const next = f.make(), execute = vi.fn(), cancel = vi.fn(async (_input, context) => {if (context.taskId === ids[0]) await held.promise;});
    next.registerProgram(effectProgram); next.registerEffect({kind: 'external', version: '1', execute, cancel}); await next.initialize();
    let recovered = false; const recovery = next.recoverSession(session.id).then(value => {recovered = true; return value;});
    try {
        await expect.poll(() => cancel.mock.calls.some(([, context]) => context.taskId === ids[1]), {timeout: 1000}).toBe(true);
        await expect.poll(async () => taskStat((await next.inspectTask(ids[1])).task).settled).toBe(true);
        expect(recovered).toBe(false); expect((await next.inspectTask(ids[0])).task.effects.e.cleanupPending).toBe(true);
        held.resolve(); await recovery;
        await expect.poll(async () => taskStat((await next.inspectTask(ids[0])).task).settled).toBe(true);
        expect(execute).not.toHaveBeenCalled(); expect(cancel.mock.calls.map(([, context]) => context.taskId).sort()).toEqual(ids);
        if (mode === 'closing') expect((await next.sessionStat(session.id)).phase).toBe('closed');
    } finally {held.resolve(); await recovery;}
});

it('requests cancellation of every Session task without waiting for another task cleanup', async () => {
    const f = await setup(), cleanup = gate(), executions = new Map<string, ReturnType<typeof gate>>(); let blockedId = '';
    f.kernel.registerProgram(effectProgram);
    f.kernel.registerEffect({kind: 'external', version: '1', execute: async (_input, context) => {
        const held = gate(); executions.set(context.taskId, held); await held.promise; return null;
    }, cancel: async (_input, context) => {executions.get(context.taskId)?.resolve(); if (context.taskId === blockedId) await cleanup.promise;}});
    const session = await f.session('close-independent-cleanup');
    const tasks = await Promise.all([0, 1].map(() => session.submit({program: effectProgram.manifest, input: null})));
    const descendant = await session.submit({program: effectProgram.manifest, input: null, parent: tasks[0].id});
    const ids = tasks.map(task => task.id).sort(); blockedId = ids[0]; await expect.poll(() => executions.size).toBe(3);
    const requests = vi.spyOn(f.kernel, 'cancel'), closing = session.close({cancelRunning: true});
    try {
        await expect.poll(() => requests.mock.calls.some(([, id]) => id === ids[1]), {timeout: 1000}).toBe(true);
        expect((await f.kernel.sessionStat(session.id)).phase).not.toBe('closed');
        cleanup.resolve(); await closing;
        await expect.poll(async () => (await f.kernel.sessionStat(session.id)).phase).toBe('closed');
        for (const id of [...ids, descendant.id]) expect(requests.mock.calls.filter(([, target]) => target === id).length).toBeLessThanOrEqual(1);
    } finally {cleanup.resolve(); for (const held of executions.values()) held.resolve(); await closing;}
});


it('retains failed recovery cleanup while confirming independent duties and retrying only what remains', async () => {
    const diagnostics: import('./index').KernelDiagnostic[] = [];
    const {f, session, ids} = await pendingCleanupRecovery('cancelled', {onDiagnostic: event => diagnostics.push(event)});
    let fail = true; const next = f.make(), execute = vi.fn(), cancel = vi.fn(async (_input, context) => {
        if (context.taskId === ids[0] && fail) throw new Error('PRIVATE-RECOVERY-FAILURE');
    });
    next.registerProgram(effectProgram); next.registerEffect({kind: 'external', version: '1', execute, cancel}); await next.initialize();
    await next.recoverSession(session.id);
    expect(taskStat((await next.inspectTask(ids[0])).task).settled).toBe(false);
    expect(taskStat((await next.inspectTask(ids[1])).task).settled).toBe(true);
    expect(diagnostics).toContainEqual(expect.objectContaining({operation: 'effect.cleanup', taskId: ids[0], stage: 'adapter-confirmation', outcome: 'failure'}));
    expect(JSON.stringify(diagnostics)).not.toContain('PRIVATE');
    fail = false; await next.recoverSession(session.id);
    expect(taskStat((await next.inspectTask(ids[0])).task).settled).toBe(true);
    expect(cancel.mock.calls.filter(([, context]) => context.taskId === ids[1])).toHaveLength(1); expect(execute).not.toHaveBeenCalled();
});

async function remainingTreeFixture(effectOnExit = false) {
    const diagnostics: import('./index').KernelDiagnostic[] = [];
    const f = await setup({maxConcurrentEffects: 0, onDiagnostic: event => diagnostics.push(event)});
    const parent = {manifest: {kind: 'remaining-parent', version: '1'},
        init: () => ({state: null, next: {type: 'wait' as const, on: {type: 'signal' as const}}}),
        reduce: () => ({...doneProgram.init(), ...(effectOnExit ? {actions: effectProgram.init().actions} : {})})};
    f.kernel.registerProgram(parent); f.kernel.registerProgram(effectProgram);
    const session = await f.session('remaining-tree');
    const root = await session.submit({program: parent.manifest, input: null});
    const middle = await session.submit({program: parent.manifest, input: null, parent: root.id});
    const child = await session.submit({program: effectProgram.manifest, input: null, parent: middle.id});
    const sibling = await session.submit({program: effectProgram.manifest, input: null, parent: root.id});
    const unrelated = await session.submit({program: effectProgram.manifest, input: null});
    await expect.poll(async () => (await session.listTasks()).filter(task => task.effects.e?.status === 'pending').length).toBe(3);
    await middle.signal({type: 'finish'}); await middle.wait({timeoutMs: 2000});
    await root.signal({type: 'finish'}); await root.wait({timeoutMs: 2000});
    f.kernel.dispose(); await f.kernel.waitIdle();
    return {...f, diagnostics, session, root, middle, child, sibling, unrelated, binding: {fs: f.fs, rootPath: `/sessions/${session.id}`}};
}

it('atomically cancels remaining descendants, preserves terminal results and fences old Effect claims', async () => {
    const f = await remainingTreeFixture(), options = {requestId: 'remaining-stop', expectedEpoch: 0};
    const before = await f.store.readTask(f.binding, f.root.id);
    const claim = await f.store.claimEffect(f.binding, f.child.id, 'e', 'old-worker', 30000);
    const records = await f.store.cancelRemainingTaskTree(f.binding, f.root.id, options);
    expect(records.find(task => task.id === f.root.id)).toEqual(before);
    expect(records.find(task => task.id === f.middle.id)?.exit?.status).toBe('succeeded');
    for (const id of [f.child.id, f.sibling.id]) expect(records.find(task => task.id === id)).toMatchObject({status: 'cancelled', effects: {e: {cleanupPending: true, status: 'cancelled'}}});
    expect((await f.store.readTask(f.binding, f.unrelated.id)).status).not.toBe('cancelled');
    await expect(f.store.completeEffect(f.binding, f.child.id, 'e', claim!.effect.currentAttempt!.leaseToken, {result: 'stale'})).rejects.toThrow('Stale effect');
    const receipt = await f.store.readTreeCancellationReceipt(f.binding, f.root.id, options.requestId);
    expect(receipt).toMatchObject({rootStatus: 'succeeded', taskCount: 4, expectedEpoch: 0});
    expect(await f.store.cancelRemainingTaskTree(f.binding, f.root.id, options)).toEqual(records);
    await expect(f.store.cancelRemainingTaskTree(f.binding, f.root.id, {...options, reason: 'changed'})).rejects.toMatchObject({code: KernelErrorCode.CONFLICT});
    const events = await f.store.events(f.binding, 0), event = events.find(event => event.type === 'task.tree-cancellation.requested')!;
    expect(events.filter(item => item.type === event.type)).toHaveLength(1); expect(parseKernelEvent(event)?.payload).toEqual(receipt);
    expect(() => parseKernelEvent({...event, taskId: 'another'})).toThrow();
});

it('rejects active roots, stale control epochs and lost scheduler leases before any cancellation writes', async () => {
    const f = await remainingTreeFixture(), options = {requestId: 'rejected-tree', expectedEpoch: 0};
    const before = await f.store.listTasks(f.binding);
    await expect(f.store.cancelRemainingTaskTree(f.binding, f.unrelated.id, options)).rejects.toMatchObject({code: KernelErrorCode.CONFLICT});
    await expect(f.store.cancelRemainingTaskTree(f.binding, f.root.id, {...options, expectedEpoch: 1})).rejects.toMatchObject({code: KernelErrorCode.CONFLICT});
    await expect(f.store.cancelRemainingTaskTree(f.binding, f.root.id, {...options, lease: {key: 'flow-lease', ownerId: 'old', epoch: 1}}))
        .rejects.toMatchObject({code: KernelErrorCode.STALE_SHARED_LEASE});
    await expect(f.store.cancelRemainingTaskTree(f.binding, f.root.id, {...options, requestId: ''})).rejects.toMatchObject({code: KernelErrorCode.INVALID_SPEC});
    expect(await f.store.listTasks(f.binding)).toEqual(before);
    expect(await f.store.readTreeCancellationReceipt(f.binding, f.root.id, options.requestId)).toBeUndefined();
});

it('recovers admitted remaining-tree cleanup after restart without executing cancelled work or changing the root', async () => {
    const f = await remainingTreeFixture(), options = {requestId: 'recover-tree', expectedEpoch: 0};
    await f.store.cancelRemainingTaskTree(f.binding, f.root.id, options);
    const next = f.make(), execute = vi.fn(), cancel = vi.fn(); next.registerProgram(effectProgram);
    next.registerEffect({kind: 'external', version: '1', execute, cancel}); await next.initialize();
    const before = await next.inspectTask(f.root.id), receipt = await next.readTaskTreeCancellationReceipt(f.session.id, f.root.id, options.requestId);
    expect(receipt?.rootStatus).toBe('succeeded'); expect(cancel).not.toHaveBeenCalled();
    await next.cancelRemainingTaskTree(f.session.id, f.root.id, options);
    expect(cancel.mock.calls.map(([, context]) => context.taskId).sort()).toEqual([f.child.id, f.sibling.id].sort());
    expect(execute).not.toHaveBeenCalled(); expect((await next.inspectTask(f.root.id)).task).toEqual(before.task);
    for (const id of [f.child.id, f.sibling.id]) expect(taskStat((await next.inspectTask(id)).task).settled).toBe(true);
    await next.cancelRemainingTaskTree(f.session.id, f.root.id, options); expect(cancel).toHaveBeenCalledTimes(2);
});


it('fences Effects on already successful members without replacing their output or exit', async () => {
    const f = await remainingTreeFixture(true), originals = await f.store.listTasks(f.binding);
    const tasks = await f.store.cancelRemainingTaskTree(f.binding, f.root.id, {requestId: 'terminal-effects', expectedEpoch: 0});
    for (const id of [f.root.id, f.middle.id]) {
        const before = originals.find(task => task.id === id)!, after = tasks.find(task => task.id === id)!;
        expect(after.exit).toEqual(before.exit); expect(after.output).toEqual(before.output);
        expect(after.effects.e).toMatchObject({status: 'cancelled', cleanupPending: true});
    }
});

it('includes children admitted before the tree transaction and rejects new children after admission', async () => {
    const f = await remainingTreeFixture();
    const spec = {program: effectProgram.manifest, input: null, parent: f.child.id};
    const late = await f.store.createTask(f.binding, f.session.id, spec);
    const records = await f.store.cancelRemainingTaskTree(f.binding, f.root.id, {requestId: 'spawn-fence', expectedEpoch: 0});
    expect(records.find(task => task.id === late.id)?.status).toBe('cancelled');
    for (const parent of [f.root.id, f.middle.id, f.child.id, late.id])
        await expect(f.store.createTask(f.binding, f.session.id, {...spec, parent})).rejects.toMatchObject({code: KernelErrorCode.CONFLICT});
});

it('keeps remaining-tree admission observable while independent cleanup proceeds and failures remain recoverable', async () => {
    const f = await remainingTreeFixture(), held = gate();
    const next = f.make(), cancel = vi.fn(async (_input, context) => {
        if (context.taskId === f.child.id) {await held.promise; throw new Error('PRIVATE-ADAPTER');}
    });
    next.registerEffect({kind: 'external', version: '1', execute: vi.fn(), cancel}); await next.initialize();
    const options = {requestId: 'hanging-tree', expectedEpoch: 0, reason: 'PRIVATE-REASON'};
    const pending = next.cancelRemainingTaskTree(f.session.id, f.root.id, options).catch(error => error);
    try {
        await expect.poll(() => cancel.mock.calls.length).toBe(2);
        expect(await next.readTaskTreeCancellationReceipt(f.session.id, f.root.id, options.requestId)).toMatchObject({rootStatus: 'succeeded'});
        await expect.poll(async () => taskStat((await next.inspectTask(f.sibling.id)).task).settled).toBe(true);
        expect(taskStat((await next.inspectTask(f.child.id)).task).settled).toBe(false);
        held.resolve(); expect(await pending).toBeInstanceOf(AggregateError);
        expect((await next.inspectTask(f.child.id)).task.effects.e.cleanupPending).toBe(true);
        expect(f.diagnostics).toContainEqual(expect.objectContaining({operation: 'task.cancel-remaining-tree', stage: 'cleanup', outcome: 'failure', operationId: options.requestId}));
        expect(JSON.stringify(f.diagnostics)).not.toContain('PRIVATE');
        cancel.mockImplementation(async () => {}); await next.cancelRemainingTaskTree(f.session.id, f.root.id, options);
        expect(cancel.mock.calls.filter(([, context]) => context.taskId === f.sibling.id)).toHaveLength(1);
    } finally {held.resolve(); await pending;}
});

it('fences an in-flight reducer so it cannot publish a Decision or spawn after remaining-tree cancellation', async () => {
    const f = await remainingTreeFixture();
    const pending = await f.store.createTask(f.binding, f.session.id, {program: effectProgram.manifest, input: null, parent: f.child.id});
    const claim = (await f.store.claimReady(f.binding, 'old-reducer', 30000))!;
    expect(claim.task.id).toBe(pending.id);
    await f.store.cancelRemainingTaskTree(f.binding, f.root.id, {requestId: 'reducer-fence', expectedEpoch: 0});
    await expect(f.store.commitTask(f.binding, claim, {...claim.task, status: 'succeeded', output: 'late'}, 'task.succeeded'))
        .rejects.toThrow('Stale task claim');
    expect((await f.store.readTask(f.binding, pending.id)).status).toBe('cancelled');
});
