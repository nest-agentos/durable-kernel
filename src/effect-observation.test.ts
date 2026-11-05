import {afterEach, expect, it, vi} from 'vitest';
import {createVFS, MemoryBackend} from '@nestos/vfs-core';
import {Kernel, taskStat, type DurableTaskProgram, type KernelDiagnostic, type KernelOptions} from './index';

const releases: Array<() => Promise<void>> = [];
afterEach(async () => {for (const close of releases.splice(0).reverse()) await close();});
function deferred<T = void>() {
    let resolve!: (value: T | PromiseLike<T>) => void;
    const promise = new Promise<T>(done => {resolve = done;}); return {promise, resolve};
}
const program: DurableTaskProgram<number, number, number> = {manifest: {kind: 'observe-fixture', version: '1'},
    init: depth => ({state: depth, actions: [{type: 'effect', effect: {id: 'run', kind: 'observe-fixture', version: '1',
        request: depth, idempotencyKey: 'run'}}], next: {type: 'wait', on: {type: 'effect', id: 'run'}}}),
    reduce: (state, event) => event.type === 'effect-completed' ? {state, next: {type: 'complete', output: event.result as number}}
        : {state, next: {type: 'wait', on: {type: 'effect', id: 'run'}}}};
async function setup(options: Partial<KernelOptions> = {}) {
    const {manager} = await createVFS({rootBackend: new MemoryBackend()}), fs = await manager.openFileSystem('/');
    releases.push(() => manager.dispose()); const diagnostics: KernelDiagnostic[] = [];
    const make = async () => {
        const kernel = new Kernel({catalog: {fs}, maxConcurrentEffects: 1, onDiagnostic: event => diagnostics.push(event), ...options});
        kernel.registerStorageResolver({kind: 'test', resolve: async ref => ({fs, rootPath: String(ref.locator)})});
        kernel.registerProgram(program); await kernel.initialize(); releases.push(async () => {kernel.dispose(); await kernel.waitIdle();});
        return kernel;
    };
    const kernel = await make();
    const sessions = await Promise.all([0, 1, 2, 3].map(index => kernel.createSession({id: `s${index}`, storage: {kind: 'test', locator: `/s${index}`}})));
    return {kernel, sessions, diagnostics, make};
}
it('completes nested calls across four Sessions with one Effect work slot', async () => {
    const {kernel, sessions, diagnostics} = await setup(); let work = 0, maximum = 0;
    kernel.registerEffect({kind: 'observe-fixture', version: '1', async execute(depth: number, context) {
        maximum = Math.max(maximum, ++work);
        if (depth === 3) {work--; return 1;}
        const child = await sessions[depth + 1].submit({program: program.manifest, input: depth + 1});
        const read = (signal: AbortSignal) => child.wait({signal, until: 'settled', timeoutMs: 2000}); work--;
        // The fallback reproduces the pre-change wait while retaining its work slot.
        const exit = await (context.waitForObservation ? context.waitForObservation(read) : read(context.abortSignal));
        maximum = Math.max(maximum, ++work); work--; return Number(exit.output) + 1;
    }});
    const root = await sessions[0].submit({program: program.manifest, input: 0});
    expect(await root.wait({timeoutMs: 3000})).toMatchObject({status: 'succeeded', output: 4});
    expect(maximum).toBe(1);
    expect(diagnostics.filter(event => event.operation === 'effect.observe' && event.outcome === 'success')).toHaveLength(3);
});
it('keeps an observed Effect leased, non-idle and responsible for unconfirmed cleanup', async () => {
    const {kernel, sessions} = await setup({leaseMs: 60}), waiting = deferred(), observation = deferred(), cleanup = deferred();
    releases.push(async () => {observation.resolve(); cleanup.resolve();});
    kernel.registerEffect({kind: 'observe-fixture', version: '1', async execute(_request, context) {
        return context.waitForObservation!(async () => {waiting.resolve(); await observation.promise; return 1;});
    }, cancel: async () => {await cleanup.promise;}});
    const root = await sessions[0].submit({program: program.manifest, input: 0}); await waiting.promise;
    const initial = (await root.status()).task.effects.run;
    expect(initial.status).toBe('leased');
    await expect.poll(async () => (await root.status()).task.effects.run.currentAttempt!.leaseUntil).toBeGreaterThan(initial.currentAttempt!.leaseUntil);
    await expect(kernel.waitIdle(10)).rejects.toHaveProperty('code', 'IDLE_TIMEOUT');
    const cancellation = root.cancel('Stop owner');
    await expect.poll(async () => (await root.status()).task.effects.run.cleanupPending).toBe(true);
    expect(taskStat((await root.status()).task).settled).toBe(false); cleanup.resolve(); await cancellation;
    expect(await root.wait({until: 'settled', timeoutMs: 1000})).toMatchObject({status: 'cancelled'});
});
it('does not execute a post-observation continuation when its owner is cancelled waiting for capacity', async () => {
    const {kernel, sessions} = await setup(), read = deferred(), parentReading = deferred(), blockerEntered = deferred(), blocker = deferred();
    const continued = vi.fn(); releases.push(async () => {read.resolve(); blocker.resolve();});
    kernel.registerEffect({kind: 'observe-fixture', version: '1', async execute(depth: number, context) {
        if (depth === 1) {blockerEntered.resolve(); await blocker.promise; return 1;}
        await context.waitForObservation!(async () => {parentReading.resolve(); await read.promise;}); continued(); return 1;
    }, cancel: async () => {}});
    const parent = await sessions[0].submit({program: program.manifest, input: 0}); await parentReading.promise;
    const other = await sessions[1].submit({program: program.manifest, input: 1}); await blockerEntered.promise;
    read.resolve(); await Promise.resolve(); await parent.cancel('Stop owner'); blocker.resolve();
    await other.wait({timeoutMs: 1000}); await kernel.waitIdle(); expect(continued).not.toHaveBeenCalled();
    expect((await parent.status()).task.exit?.status).toBe('cancelled');
});
it('reports the concrete observation error without exposing its payload', async () => {
    const {kernel, sessions, diagnostics} = await setup();
    kernel.registerEffect({kind: 'observe-fixture', version: '1', async execute(_request, context) {
        await context.waitForObservation!(async () => {throw Object.assign(new Error('PRIVATE PAYLOAD'), {code: 'EACCES'});}); return 1;
    }});
    const root = await sessions[0].submit({program: program.manifest, input: 0});
    await expect.poll(async () => (await root.status()).task.effects.run.status).toBe('failed');
    expect(diagnostics).toContainEqual(expect.objectContaining({operation: 'effect.observe', outcome: 'failure', errorCode: 'EACCES', sessionId: 's0'}));
    expect(JSON.stringify(diagnostics)).not.toContain('PRIVATE PAYLOAD');
});
it('preserves observation results when the diagnostic observer throws', async () => {
    const {kernel, sessions} = await setup({onDiagnostic: event => {
        if (event.operation === 'effect.observe') throw new Error('PRIVATE DIAGNOSTIC');
    }});
    kernel.registerEffect({kind: 'observe-fixture', version: '1', async execute(_request, context) {
        return context.waitForObservation!(async () => 7);
    }});
    const root = await sessions[0].submit({program: program.manifest, input: 0});
    expect(await root.wait({timeoutMs: 1000})).toMatchObject({status: 'succeeded', output: 7});
});
it('recovers the original observation from durable state without replaying its execution', async () => {
    const {kernel, sessions, make} = await setup(), entered = deferred(), result = deferred<number>(), recovering = deferred();
    const execute = vi.fn(async (_request: unknown, context: import('./index').EffectExecutionContext) => {
        await context.sessionState!.setIfRunning!('original-operation', {operationId: 'original'}, null);
        return context.waitForObservation!(async () => {entered.resolve(); return result.promise;});
    });
    const adapter = {kind: 'observe-fixture', version: '1', execute,
        async reconcile(_request: unknown, context: import('./index').EffectExecutionContext) {
            expect((await context.sessionState!.get('original-operation'))?.value).toEqual({operationId: 'original'});
            const value = await context.waitForObservation!(async () => {recovering.resolve(); return result.promise;});
            return {status: 'completed' as const, result: value};
        }};
    kernel.registerEffect(adapter); const root = await sessions[0].submit({program: program.manifest, input: 0}); await entered.promise;
    kernel.dispose(); await kernel.waitIdle(); const replacement = await make(); replacement.registerEffect(adapter);
    await replacement.recover({takeover: true}); await recovering.promise; result.resolve(9);
    const attached = await replacement.attachTask('s0', root.id);
    expect(await attached.wait({timeoutMs: 2000})).toMatchObject({status: 'succeeded', output: 9}); expect(execute).toHaveBeenCalledOnce();
    expect((await attached.status()).task.effects.run.attempts.map(attempt => attempt.outcome)).toEqual(['lost', 'completed']);
});
it('reports a blocked worker drain while observation is active and drains after it finishes', async () => {
    const {kernel, sessions} = await setup(), entered = deferred(), complete = deferred();
    releases.push(async () => {complete.resolve();});
    kernel.registerEffect({kind: 'observe-fixture', version: '1', async execute(_request, context) {
        return context.waitForObservation!(async signal => {entered.resolve(); await complete.promise; signal.throwIfAborted(); return 1;});
    }});
    await sessions[0].submit({program: program.manifest, input: 0}); await entered.promise;
    expect(await kernel.drain({timeoutMs: 10})).toEqual({status: 'blocked', localIdle: false});
    expect(kernel.isDisposed).toBe(false); complete.resolve(); await kernel.waitIdle();
    expect(await kernel.drain({timeoutMs: 1000})).toEqual({status: 'drained', localIdle: true});
});

it('detaches an explicitly drained observation without completing or cancelling its durable Effect',async()=>{
    const {kernel,sessions}=await setup(),entered=deferred();
    kernel.registerEffect({kind:'observe-fixture',version:'1',async execute(_request,context){
        return context.waitForObservation!(()=>{entered.resolve();return new Promise(()=>{});});
    }});
    const task=await sessions[0].submit({program:program.manifest,input:0});await entered.promise;
    expect(await kernel.quiesce({timeoutMs:1000,detachObservations:true})).toEqual({status:'drained',localIdle:true});
    expect(kernel.isDisposed).toBe(false);
    expect((await kernel.task('s0',task.id)).effects.run.status).toBe('leased');
});
