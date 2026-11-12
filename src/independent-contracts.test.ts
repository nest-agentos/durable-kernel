import { afterEach, expect, it, vi } from 'vitest';
import { createVFS, MemoryBackend, type IFileSystem } from '@nestos/vfs-core';
import { Kernel, KernelErrorCode, type KernelDiagnostic, type KernelOptions } from './index';
import { createHarness } from './core';
import { SeqFileKernelStore } from './infrastructure/seqfile/store';

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });
async function setup(options: Partial<KernelOptions> = {}) {
    const { manager } = await createVFS({ rootBackend: new MemoryBackend() });
    const fs = await manager.openFileSystem('/review');
    const diagnostics: KernelDiagnostic[] = [];
    const makeKernel = () => {
        const kernel = new Kernel({ catalog: { fs, rootPath: '/catalog' }, onDiagnostic: event => diagnostics.push(event), ...options });
        kernel.registerStorageResolver(resolver(fs));
        cleanups.push(async () => { kernel.dispose(); await kernel.waitIdle(); });
        return kernel;
    };
    cleanups.push(() => manager.dispose());
    const kernel = makeKernel();
    await kernel.initialize();
    const session = (id: string) => kernel.createSession({ id, storage: { kind: 'test', locator: `/sessions/${id}` } });
    return { kernel, fs, session, diagnostics, makeKernel };
}
function resolver(fs: IFileSystem) {
    return { kind: 'test', async resolve(ref: { locator: unknown }) { return { fs, rootPath: ref.locator as string }; } };
}
function deferred() {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
}
const waitProgram = { manifest: { kind: 'wait', version: '1' },
    init() { return { state: null, next: { type: 'wait' as const, on: { type: 'signal' as const } } }; },
    reduce() { return { state: null, next: { type: 'complete' as const, output: 'done' } }; } };

it('reserves reducer slots before concurrently claiming work from several Sessions', async () => {
    const { kernel, session } = await setup({ maxConcurrent: 1 });
    const gate = deferred();
    let active = 0, maximum = 0;
    kernel.registerProgram({ manifest: { kind: 'hold', version: '1' },
        async init() { maximum = Math.max(maximum, ++active); await gate.promise; active--;
            return { state: null, next: { type: 'complete', output: 'done' } }; }, reduce() { throw new Error('unexpected'); } });
    const sessions = await Promise.all(['a', 'b', 'c', 'd'].map(session));
    const tasks = await Promise.all(sessions.map(s => s.spawn({ program: { kind: 'hold', version: '1' }, input: null, deferStart: true })));
    await Promise.all(tasks.map(task => task.start()));
    await expect.poll(() => active).toBe(1);
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(maximum).toBe(1);
    gate.resolve();
    expect((await Promise.all(tasks.map(task => task.wait({ timeoutMs: 2000 })))).every(exit => exit.status === 'succeeded')).toBe(true);
    expect(maximum).toBe(1);
});

it('replays Session sends across reconstruction, rejects conflicts, and acknowledges consumption idempotently', async () => {
    const { kernel, session, makeKernel } = await setup({ maxConcurrent: 0 });
    const source = await session('source'), target = await session('target');
    const request = { idempotencyKey: 'once', targetSessionId: 'target', topic: 'work', payload: { a: 1, b: 2 } };
    const first = await source.send(request);
    kernel.dispose(); await kernel.waitIdle();
    const replacement = makeKernel(); await replacement.initialize();
    const reopened = await replacement.openSession('source');
    const replay = await reopened.send({ ...request, payload: { b: 2, a: 1 } });
    expect(replay.id).toBe(first.id);
    expect(await target.inbox()).toHaveLength(1);
    await expect(reopened.send({ ...request, payload: { a: 2, b: 2 } })).rejects.toThrow('identity conflict');
    const currentTarget = await replacement.openSession('target');
    const consumed = await currentTarget.acknowledgeMessage(first.id);
    expect(await currentTarget.acknowledgeMessage(first.id)).toEqual(consumed);
    expect(consumed.consumedAt).toBeTypeOf('number');
    expect(await replacement.pruneSessionMessages('target', Date.now() + 1)).toEqual({ outbox: 0, inbox: 1 });
});

it('uses receive sequences for late and same-time deliveries and pins page membership', async () => {
    const { fs, session } = await setup({ maxConcurrent: 0 });
    const target = await session('target'), binding = { fs, rootPath: '/sessions/target' };
    const store = new SeqFileKernelStore({ fs, rootPath: '/catalog' }, async () => binding);
    const base = { sourceSessionId: 'source', targetSessionId: 'target', topic: 'work', payload: null, status: 'pending' as const };
    await store.deliverMessage(binding, { ...base, id: 'first', createdAt: 20 });
    await store.deliverMessage(binding, { ...base, id: 'late', createdAt: 10 });
    const first = await target.inboxPage({ limit: 1 });
    await store.deliverMessage(binding, { ...base, id: 'same-time', createdAt: 20 });
    const second = await target.inboxPage({ afterSequence: first.nextAfterSequence, throughSequence: first.throughSequence });
    expect(second.items.map(message => message.id)).toEqual(['late']);
    expect((await target.inbox({ after: first.throughSequence })).map(message => message.id)).toEqual(['same-time']);
    await expect(target.inboxPage({ afterSequence: -1 })).rejects.toThrow('Invalid inbox page');
    await expect(target.inboxPage({ throughSequence: 999 })).rejects.toThrow('Invalid inbox cursor');
});

it('routes Session sends to Tasks and prevents the host from acknowledging Task-owned input', async () => {
    const { kernel, session } = await setup();
    const gate = deferred();
    kernel.registerProgram({ manifest: { kind: 'receive', version: '1' },
        init() { return { state: null, next: { type: 'wait', on: { type: 'message', topic: 'work' } } }; },
        async reduce(_state, event) { await gate.promise; return { state: null, next: { type: 'complete', output: event.type } }; } });
    const source = await session('source'), target = await session('target');
    const task = await target.spawn({ program: { kind: 'receive', version: '1' }, input: null });
    await expect.poll(async () => (await task.status()).task.status).toBe('waiting');
    const sent = await source.send({ idempotencyKey: 'work', targetSessionId: target.id, targetTaskId: task.id, topic: 'work', payload: null });
    await expect(target.acknowledgeMessage(sent.id)).rejects.toThrow('Session consumer');
    gate.resolve();
    expect((await task.wait({ timeoutMs: 2000 })).output).toBe('message');
    expect((await target.inbox())[0].consumedAt).toBeTypeOf('number');
});

it('keeps timed-out operations unconfirmed through Task pause and Session suspension until cleanup settles', async () => {
    const { kernel, session, diagnostics } = await setup();
    const cancellation = deferred();
    let externalRunning = false;
    kernel.registerEffect({ kind: 'hung', version: '1',
        async execute() { externalRunning = true; return new Promise(() => {}); },
        async cancel() { await cancellation.promise; externalRunning = false; } });
    kernel.registerProgram({ manifest: { kind: 'effect', version: '1' },
        init() { return { state: null, actions: [{ type: 'effect', effect: { id: 'e', kind: 'hung', version: '1', idempotencyKey: 'e', request: null, timeoutMs: 40 } }],
            next: { type: 'wait', on: { type: 'effect', id: 'e' } } }; },
        reduce() { return { state: null, next: { type: 'wait', on: { type: 'signal' } } }; } });
    const s = await session('effect'), task = await s.spawn({ program: { kind: 'effect', version: '1' }, input: null });
    await expect.poll(async () => (await task.status()).task.effects.e?.cleanupPending).toBe(true);
    expect((await task.pause({ requestId: 'pause' })).acknowledged).toBe(false);
    await s.suspend();
    expect(await s.stat()).toMatchObject({ phase: 'paused', acknowledged: false });
    expect((await kernel.sessionActivity(s.id)).pending.cleanup).toBe(1);
    expect(externalRunning).toBe(true);
    expect(await task.recoveryIssues()).toContainEqual({ sessionId: s.id, taskId: task.id, effectId: 'e', reason: 'effect-cleanup' });
    cancellation.resolve();
    await expect.poll(async () => (await s.stat()).acknowledged).toBe(true);
    await expect.poll(async () => (await task.stat()).control.acknowledged).toBe(true);
    expect(externalRunning).toBe(false);
    expect((await kernel.sessionActivity(s.id)).pending.cleanup).toBe(0);
    expect(diagnostics.some(event => event.operation === 'effect.cleanup' && event.outcome === 'success')).toBe(true);
});

it('persists an idle shutdown pause and requires explicit resume after reconstruction', async () => {
    const { kernel, session, makeKernel } = await setup();
    kernel.registerProgram(waitProgram);
    const s = await session('s'), task = await s.spawn({ program: waitProgram.manifest, input: null });
    await expect.poll(async () => (await task.status()).task.status).toBe('waiting');
    const stopping = kernel.shutdown({ timeoutMs: 1000 });
    expect(kernel.shutdown()).toBe(stopping);
    await expect(s.resume()).rejects.toMatchObject({ code: KernelErrorCode.DISPOSED });
    expect(await stopping).toMatchObject({ status: 'stopped', localIdle: true, issues: [], pendingSessions: [] });
    const next = makeKernel(); next.registerProgram(waitProgram); await next.initialize(); await next.recover();
    const reopened = await next.openSession('s');
    expect(await reopened.stat()).toMatchObject({ phase: 'paused', acknowledged: true });
    const attached = await reopened.attachTask(task.id);
    await attached.signal({ type: 'continue' });
    expect(await attached.poll()).toBeUndefined();
    await reopened.resume();
    expect((await attached.wait({ timeoutMs: 2000 })).output).toBe('done');
});

it('reports blocked shutdown for a running external operation and rejects silent idle timeouts', async () => {
    const { kernel, session } = await setup();
    kernel.registerEffect({ kind: 'hung', version: '1', async execute() { return new Promise(() => {}); } });
    kernel.registerProgram({ manifest: { kind: 'effect', version: '1' },
        init() { return { state: null, actions: [{ type: 'effect', effect: { id: 'e', kind: 'hung', version: '1', idempotencyKey: 'e', request: null, timeoutMs: 60_000 } }],
            next: { type: 'wait', on: { type: 'effect', id: 'e' } } }; }, reduce() { throw new Error('unexpected'); } });
    const s = await session('s'), task = await s.spawn({ program: { kind: 'effect', version: '1' }, input: null });
    await expect.poll(async () => (await task.status()).task.effects.e?.status).toBe('leased');
    await expect(kernel.waitIdle(0)).rejects.toMatchObject({ code: KernelErrorCode.IDLE_TIMEOUT });
    const report = await kernel.shutdown({ timeoutMs: 10 });
    expect(report.status).toBe('blocked');
    expect(report.pendingSessions).toEqual(['s']);
    expect(report.issues).toContainEqual({ sessionId: s.id, taskId: task.id, effectId: 'e', reason: 'effect-in-flight' });
});

it('exposes message, inspection, recovery and shutdown operations through the core entry', async () => {
    const { fs } = await setup();
    const harness = createHarness({ catalog: { fs, rootPath: '/core' }, maxConcurrent: 0 });
    harness.registerStorageResolver(resolver(fs)); await harness.initialize();
    cleanups.push(async () => { harness.dispose(); await harness.waitIdle(); });
    const s = await harness.createSession({ id: 'core', storage: { kind: 'test', locator: '/sessions/core' } });
    const task = await s.submit({ requestId: 'submit', program: waitProgram.manifest, input: null });
    expect((await s.listTaskPage()).items.map(item => item.id)).toEqual([task.id]);
    expect(await task.recoveryIssues()).toEqual([{ sessionId: s.id, taskId: task.id, reason: 'program-unavailable' }]);
    expect(typeof task.resolveEffect).toBe('function');
    const message = await s.send({ idempotencyKey: 'loop', targetSessionId: s.id, topic: 'loop', payload: null });
    await s.acknowledgeMessage(message.id);
    expect((await s.inboxPage()).items[0].consumedAt).toBeTypeOf('number');
    expect((await harness.shutdown()).status).toBe('stopped');
});

it('classifies rejected sends without logging message bodies or parameters', async () => {
    const { session, diagnostics } = await setup({ maxConcurrent: 0 });
    const s = await session('s');
    const request = { idempotencyKey: 'same', targetSessionId: 'missing', topic: 'secret-topic', payload: { secret: 'PRIVATE_BODY' } };
    await s.send(request);
    await expect(s.send({ ...request, payload: { secret: 'CHANGED_BODY' } })).rejects.toThrow('identity conflict');
    const failure = diagnostics.find(event => event.operation === 'message.send' && event.outcome === 'failure');
    expect(failure).toMatchObject({ operation: 'message.send', stage: 'enqueue', errorCode: 'MESSAGE_IDENTITY_CONFLICT' });
    expect(failure?.operationId).toBeTruthy();
    expect(failure?.durationMs).toBeGreaterThanOrEqual(0);
    expect(JSON.stringify(diagnostics)).not.toMatch(/PRIVATE_BODY|CHANGED_BODY|secret-topic/);
});

it('preserves Task message replay after sender completion while rejecting new sends', async () => {
    const { session } = await setup({ maxConcurrent: 0 });
    const s = await session('s');
    const sender = await s.spawn({ program: waitProgram.manifest, input: null, deferStart: true });
    const target = await s.spawn({ program: waitProgram.manifest, input: null, deferStart: true });
    const request = { idempotencyKey: 'once', targetSessionId: s.id, targetTaskId: target.id, topic: 'work', payload: null };
    const sent = await sender.send(request);
    await sender.cancel();
    expect(await sender.send(request)).toEqual(sent);
    await expect(sender.send({ ...request, idempotencyKey: 'new' })).rejects.toThrow('terminal');
    await expect(sender.send({ ...request, payload: 'changed' })).rejects.toThrow('identity conflict');
});

it('inspects and resolves an unknown external outcome after takeover using only the core API', async () => {
    const { fs } = await setup();
    const program = { manifest: { kind: 'manual', version: '1' },
        init() { return { state: null, actions: [{ type: 'effect' as const, effect: { id: 'e', kind: 'manual', version: '1', idempotencyKey: 'e', request: null } }],
            next: { type: 'wait' as const, on: { type: 'effect' as const, id: 'e' } } }; },
        reduce(_state: unknown, event: import('./core').TaskInputEvent) {
            if (event.type !== 'effect-completed') throw new Error('Unexpected input');
            return { state: null, next: { type: 'complete' as const, output: event.result } };
        } };
    const make = async () => {
        const harness = createHarness({ catalog: { fs, rootPath: '/manual' } });
        harness.registerStorageResolver(resolver(fs)); harness.registerProgram(program);
        harness.registerEffect({ kind: 'manual', version: '1', async execute() { return new Promise(() => {}); } });
        cleanups.push(async () => { harness.dispose(); await harness.waitIdle(); });
        await harness.initialize(); return harness;
    };
    const first = await make();
    const s = await first.createSession({ id: 'manual', storage: { kind: 'test', locator: '/sessions/manual' } });
    const task = await s.submit({ program: program.manifest, input: null });
    await expect.poll(async () => (await task.snapshot()).task.effects.e?.status).toBe('leased');
    first.dispose(); await first.waitIdle();
    const next = await make(); await next.recoverSession(s.id, { takeover: true });
    const attached = await next.openTask(task.id);
    await expect.poll(async () => (await attached.recoveryIssues()).some(issue => issue.reason === 'effect-indeterminate')).toBe(true);
    await attached.resolveEffect({ requestId: 'checked', effectId: 'e', outcome: { type: 'completed', result: 'verified' } });
    expect((await attached.wait({ timeoutMs: 2000 })).output).toBe('verified');
});

it('wakes another Session when an asynchronous empty claim releases its reserved slot', async () => {
    const { kernel, session } = await setup({ maxConcurrent: 1 });
    kernel.registerProgram({ ...waitProgram, init: () => ({ state: null, next: { type: 'complete', output: 'ran' } }) });
    const a = await session('empty'), b = await session('ready');
    const task = await b.spawn({ program: waitProgram.manifest, input: null, deferStart: true });
    await kernel.waitIdle();
    const gate = deferred();
    const store = (kernel as unknown as { store: SeqFileKernelStore }).store;
    const original = store.claimReady.bind(store);
    let held = false;
    const spy = vi.spyOn(store, 'claimReady').mockImplementation(async (binding, ...rest) => {
        if (!held && binding.rootPath === '/sessions/empty') { held = true; await gate.promise; }
        return original(binding, ...rest);
    });
    try {
        await a.resume();
        await expect.poll(() => held).toBe(true);
        await task.start();
        await new Promise(resolve => setTimeout(resolve, 20));
        gate.resolve();
        expect((await task.wait({ timeoutMs: 2000 })).output).toBe('ran');
    } finally { gate.resolve(); spy.mockRestore(); }
});

it('refuses to activate a Session whose asynchronous open finishes after shutdown', async () => {
    const { kernel, session, makeKernel } = await setup();
    await session('s');
    kernel.dispose(); await kernel.waitIdle();
    const next = makeKernel(); await next.initialize();
    const store = (next as unknown as { store: SeqFileKernelStore }).store;
    const original = store.openSession.bind(store), gate = deferred();
    let reading = false;
    const spy = vi.spyOn(store, 'openSession').mockImplementation(async (...args) => {
        reading = true; await gate.promise; return original(...args);
    });
    const opening = next.openSession('s');
    const rejected = expect(opening).rejects.toMatchObject({ code: KernelErrorCode.DISPOSED });
    try {
        await expect.poll(() => reading).toBe(true);
        expect(await next.shutdown({ timeoutMs: 10 })).toMatchObject({ status: 'blocked', localIdle: false });
        gate.resolve(); await rejected;
        await next.waitIdle();
        expect((next as unknown as { sessions: Map<string, unknown> }).sessions.size).toBe(0);
    } finally { gate.resolve(); spy.mockRestore(); }
});

it('preserves explicitly uncertain live Effect outcomes without automatic retry and allows verified resolution', async () => {
    const { kernel, session } = await setup({ pollMs: 5 });
    kernel.registerProgram(liveOutcomeProgram);
    const execute = vi.fn(async () => { throw new Error('External reply unavailable'); });
    kernel.registerEffect({ kind: 'external-live', version: '1', execute, shouldRetry: () => true,
        failureOutcome: () => 'indeterminate', cancel: async () => {} });
    const current = await session('live-unknown');
    const task = await current.spawn({ program: liveOutcomeProgram.manifest, input: null });
    await vi.waitFor(async () => expect((await task.status()).task.effects.call?.status).toBe('indeterminate'));
    expect(execute).toHaveBeenCalledOnce(); expect((await task.status()).task.effects.call.attemptCount).toBe(1);
    expect((await current.readEvents()).items.some(event => event.type === 'effect.indeterminate')).toBe(true);
    await task.resolveEffect({ requestId: 'verified-completion', effectId: 'call', outcome: { type: 'completed', result: 'verified' } });
    expect(await task.wait({ timeoutMs: 2000 })).toMatchObject({ status: 'succeeded', output: 'verified' });
    expect(execute).toHaveBeenCalledOnce();
});

it('retains the default failed live Effect behavior without classification opt-in', async () => {
    const { kernel, session } = await setup({ pollMs: 5 }); kernel.registerProgram(liveOutcomeProgram);
    kernel.registerEffect({ kind: 'external-live', version: '1', execute: async () => { throw new Error('Rejected'); }, shouldRetry: () => false });
    const task = await (await session('live-rejected')).spawn({ program: liveOutcomeProgram.manifest, input: null });
    expect(await task.wait({ timeoutMs: 2000 })).toMatchObject({ status: 'failed', error: { message: 'Rejected' } });
    expect((await task.status()).task.effects.call.status).toBe('failed');
});

const liveOutcomeProgram: import('./index').DurableTaskProgram<null, null, unknown> = {
    manifest: { kind: 'live-outcome', version: '1' },
    init: () => ({ state: null, actions: [{ type: 'effect', effect: { id: 'call', kind: 'external-live', version: '1',
        request: null, idempotencyKey: 'call', retry: { maxAttempts: 3 }, timeoutMs: 10_000 } }],
        next: { type: 'wait', on: { type: 'effect', id: 'call' } } }),
    reduce: (_, event) => event.type === 'effect-completed' ? { state: null, next: { type: 'complete', output: event.result } }
        : event.type === 'effect-failed' ? { state: null, next: { type: 'fail', error: event.error } }
        : { state: null, next: { type: 'wait', on: { type: 'effect', id: 'call' } } },
};

it.each([true,false])('reconciles an indeterminate original effect without granting replay (adapter=%s)',async supported=>{
 const {kernel,session}=await setup();let reads=0;
 const execute=vi.fn(async()=>{throw Object.assign(new Error('observation unavailable'),{code:'EIO'});});
 kernel.registerEffect({kind:'original-operation',version:'1',recoveryPolicy:'idempotent-retry',failureOutcome:()=> 'indeterminate',execute,
  ...(supported?{reconcile:async()=>++reads===1?{status:'indeterminate' as const,error:{code:'EIO',message:'still unavailable'}}:{status:'completed' as const,result:'original-result'}}:{})});
 kernel.registerProgram({manifest:{kind:'observe-original',version:'1'},init:()=>({state:null,actions:[{type:'effect',effect:{id:'original',kind:'original-operation',version:'1',request:null,idempotencyKey:'original',timeoutMs:null}}],next:{type:'wait',on:{type:'effect',id:'original'}}}),
  reduce:(_state,event)=>event.type==='effect-completed'?{state:null,next:{type:'complete',output:event.result}}:{state:null,next:{type:'wait',on:{type:'effect',id:'original'}}}});
 const s=await session('reconcile-'+supported),task=await s.spawn({program:{kind:'observe-original',version:'1'},input:null});
 await expect.poll(async()=>(await task.status()).task.effects.original?.status).toBe('indeterminate');
 const request={requestId:'observe-original-1',effectId:'original',outcome:{type:'reconcile' as const}};
 await task.resolveEffect(request);await task.resolveEffect(request);
 await expect.poll(async()=>(await task.status()).task.effects.original?.attemptCount).toBe(2);
 await expect.poll(async()=>(await task.status()).task.effects.original?.status).toBe('indeterminate');
 expect(execute).toHaveBeenCalledOnce();expect((await task.status()).task.effects.original.replayAuthorized).toBe(false);
 if(supported){
  await task.resolveEffect({...request,requestId:'observe-original-2'});
  expect((await task.wait({timeoutMs:2000})).output).toBe('original-result');expect(reads).toBe(2);expect(execute).toHaveBeenCalledOnce();
 }
});
