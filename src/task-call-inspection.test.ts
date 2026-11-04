import {afterEach, expect, it, vi} from 'vitest';
import {createVFS, MemoryBackend} from '@itookit/vfs-core';
import {Kernel, SeqFileKernelStore, type TaskCallPlan, type KernelDiagnostic} from './index';
import {taskCallKey} from './application/task-call-identity';
import {sessionPath, sharedPath, sharedKey, taskPath} from './infrastructure/seqfile/store-helpers';

const releases: Array<() => Promise<unknown>> = [];
afterEach(async () => {vi.restoreAllMocks(); for (const release of releases.splice(0).reverse()) await release();});
async function fixture() {
    const {manager} = await createVFS({rootBackend: new MemoryBackend()}), fs = await manager.openFileSystem('/'); releases.push(() => manager.dispose());
    const diagnostics: KernelDiagnostic[] = [];
    const make = () => {
        const kernel = new Kernel({catalog: {fs}, maxConcurrentEffects: 1, onDiagnostic: event => diagnostics.push(event)});
        kernel.registerStorageResolver({kind: 'test', resolve: async ref => ({fs, rootPath: String(ref.locator)})});
        kernel.registerProgram({manifest: {kind: 'echo', version: '1'}, init: input => ({state: null, next: {type: 'complete', output: input}}), reduce: () => {throw Error('Unexpected');}});
        kernel.registerProgram({manifest: {kind: 'reject', version: '1'}, init: () => ({state: null, next: {type: 'fail', error: {message: 'Expected rejection'}}}), reduce: () => {throw Error('Unexpected');}});
        kernel.registerProgram({manifest: {kind: 'menu', version: '1'}, init: () => ({state: null,
            actions: [{type: 'request-interaction', interaction: {id: 'question', kind: 'input', prompt: 'Choose', payload: {}}}], next: {type: 'wait', on: {type: 'interaction', id: 'question'}}}),
            reduce: (_state, event) => ({state: null, next: {type: 'complete', output: event.type}})});
        releases.push(async () => {kernel.dispose(); await kernel.waitIdle();}); return kernel;
    };
    const kernel = make(); await kernel.initialize();
    for (const id of ['source', 'target', 'other']) await kernel.createSession({id, storage: {kind: 'test', locator: `/${id}`}});
    const admit = async (kind = 'echo', start = true) => {
        const input: TaskCallPlan = {targetSessionId: 'target', maxDepth: 3, timeoutMs: null, task: {program: {kind, version: '1'}, input: 'PRIVATE INPUT'}};
        const caller = await kernel.submit('source', {program: {kind: 'kernel.call', version: '2'}, input, deferStart: true});
        if (start) await kernel.bindTaskCall({sessionId: 'source', taskId: caller.id}, input);
        return {caller, owner: {sessionId: 'source', taskId: caller.id, effectId: 'call'}};
    };
    return {kernel, make, admit, fs, diagnostics};
}
it('cold-reads the actual target after reconstruction without workers, scans or request input', async () => {
    const {kernel, make, admit} = await fixture(), {caller, owner} = await admit();
    const exit = await caller.wait({until: 'settled', timeoutMs: 2000}); kernel.dispose(); await kernel.waitIdle();
    const reader = make(), open = vi.spyOn(reader, 'openSession'), recover = vi.spyOn(reader, 'recoverSession');
    const create = vi.spyOn(SeqFileKernelStore.prototype, 'createTask'), list = vi.spyOn(SeqFileKernelStore.prototype, 'listTasks');
    expect(await reader.readTaskCallTarget(owner)).toEqual((exit.output as {task: unknown}).task);
    expect(open).not.toHaveBeenCalled(); expect(recover).not.toHaveBeenCalled(); expect(create).not.toHaveBeenCalled(); expect(list).not.toHaveBeenCalled();
});
it('does not dispatch a deferred caller without an Effect', async () => {
    const {kernel, admit} = await fixture(), {owner} = await admit('echo', false);
    expect(await kernel.readTaskCallTarget(owner)).toBeUndefined(); expect(await kernel.listSessionTasks('target')).toEqual([]);
});
it('finds the target while the original caller is waiting for input', async () => {
    const {kernel, admit} = await fixture(), {owner} = await admit('menu');
    await vi.waitFor(async () => {const target = await kernel.readTaskCallTarget(owner); expect(target).toBeDefined();
        expect((await kernel.task(target!.sessionId, target!.taskId)).interactions.question?.status).toBe('pending');});
    expect((await kernel.task('source', owner.taskId)).exit).toBeUndefined();
});
it('retains the exact relation after failure without relying on caller success output', async () => {
    const {kernel, admit} = await fixture(), {caller, owner} = await admit('reject');
    expect((await caller.wait({until: 'settled', timeoutMs: 2000})).status).toBe('failed');
    const target = (await kernel.readTaskCallTarget(owner))!; expect((await kernel.task(target.sessionId, target.taskId)).status).toBe('failed');
});
it('retains the original target after owner cancellation and cleanup', async () => {
    const {kernel, admit} = await fixture(), {caller, owner} = await admit('menu');
    let target: Awaited<ReturnType<Kernel['readTaskCallTarget']>>;
    await vi.waitFor(async () => {target = await kernel.readTaskCallTarget(owner); expect(target).toBeDefined();});
    await caller.cancel(); expect(await kernel.readTaskCallTarget(owner)).toEqual(target);
});
it.each(['sessionId', 'taskId', 'effectId'] as const)('rejects an absent %s rather than guessing the owner', async key => {
    const {kernel, admit} = await fixture(), {owner} = await admit('echo', false);
    await expect(kernel.readTaskCallTarget({...owner, [key]: ''})).rejects.toHaveProperty('code', 'INVALID_SPEC');
});
it('distinguishes a missing source Task from a call not yet dispatched', async () => {
    const {kernel, admit} = await fixture(), {owner} = await admit('echo', false);
    await expect(kernel.readTaskCallTarget({...owner, taskId: 'absent'})).rejects.toHaveProperty('code', 'ENOENT');
    expect(await kernel.readTaskCallTarget(owner)).toBeUndefined();
});
it.each(['fingerprint', 'owner'] as const)('rejects a changed binding %s without logging its body', async field => {
    const {kernel, make, admit, fs, diagnostics} = await fixture(), {caller, owner} = await admit(); await caller.wait({until: 'settled', timeoutMs: 2000});
    kernel.dispose(); await kernel.waitIdle();
    await fs.meta.seq!.transaction!(async tx => {
        const path = sharedPath('/source'), key = sharedKey(taskCallKey(owner)), stored = JSON.parse((await tx.getEntry(path, key))!);
        if (field === 'fingerprint') stored.value.fingerprint = 'PRIVATE CORRUPTION'; else stored.value.owner.taskId = 'foreign';
        await tx.setEntry(path, key, JSON.stringify(stored));
    });
    await expect(make().readTaskCallTarget(owner)).rejects.toHaveProperty('code', 'TASK_CALL_BINDING_INVALID');
    expect(JSON.stringify(diagnostics)).not.toContain('PRIVATE');
});
it.each(['id', 'sessionId', 'parentTaskId', 'ownedBy'] as const)('rejects changed target %s', async field => {
    const {kernel, make, admit, fs} = await fixture(), {caller, owner} = await admit(); await caller.wait({until: 'settled', timeoutMs: 2000});
    const target = (await kernel.readTaskCallTarget(owner))!; kernel.dispose(); await kernel.waitIdle();
    await fs.meta.seq!.transaction!(async tx => {const path = taskPath('/target', target.taskId), record = JSON.parse((await tx.getEntry(path, 'record'))!);
        record[field] = field === 'ownedBy' ? {...record.ownedBy, effectId: 'foreign'} : 'foreign'; await tx.setEntry(path, 'record', JSON.stringify(record));});
    await expect(make().readTaskCallTarget(owner)).rejects.toHaveProperty('code', 'TASK_CALL_TARGET_INVALID');
});
it('rejects a replaced target Session incarnation even if the old Task remains', async () => {
    const {kernel, make, admit, fs} = await fixture(), {caller, owner} = await admit(); await caller.wait({until: 'settled', timeoutMs: 2000});
    kernel.dispose(); await kernel.waitIdle();
    await fs.meta.seq!.transaction!(async tx => {const path = sessionPath('/target'), record = JSON.parse((await tx.getEntry(path, 'record'))!);
        record.instanceId = 'replaced'; await tx.setEntry(path, 'record', JSON.stringify(record));});
    await expect(make().readTaskCallTarget(owner)).rejects.toHaveProperty('code', 'SESSION_INSTANCE_CHANGED');
});
it('does not downgrade a missing completed target to never dispatched', async () => {
    const {kernel, make, admit, fs} = await fixture(), {caller, owner} = await admit(); await caller.wait({until: 'settled', timeoutMs: 2000});
    const target = (await kernel.readTaskCallTarget(owner))!; kernel.dispose(); await kernel.waitIdle();
    await fs.meta.seq!.transaction!(tx => tx.deleteEntry(taskPath('/target', target.taskId), 'record'));
    await expect(make().readTaskCallTarget(owner)).rejects.toHaveProperty('code', 'TASK_CALL_TARGET_INVALID');
});
it('freezes the owner before asynchronous lookup and logs the original identity', async () => {
    const {kernel, admit, diagnostics} = await fixture(), {caller, owner} = await admit(); await caller.wait({until: 'settled', timeoutMs: 2000});
    const target = await kernel.readTaskCallTarget(owner), original = SeqFileKernelStore.prototype.inspectSessionBinding; let release!: () => void;
    const gate = new Promise<void>(resolve => {release = resolve;});
    vi.spyOn(SeqFileKernelStore.prototype, 'inspectSessionBinding').mockImplementationOnce(async function (this: SeqFileKernelStore, ...args) {await gate; return original.apply(this, args);});
    const pending = kernel.readTaskCallTarget(owner); owner.effectId = 'foreign'; release(); expect(await pending).toEqual(target);
    expect(diagnostics.at(-1)).toMatchObject({operation: 'task.call.inspect', outcome: 'success', effectId: 'call'});
    expect(JSON.stringify(diagnostics)).not.toContain('PRIVATE INPUT');
});
it('does not manufacture a target while its original admission is still in flight', async () => {
    const {kernel, admit} = await fixture(), original = SeqFileKernelStore.prototype.createTask;
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>(resolve => {enter = resolve;}), gate = new Promise<void>(resolve => {release = resolve;});
    const create = vi.spyOn(SeqFileKernelStore.prototype, 'createTask').mockImplementation(async function (this: SeqFileKernelStore, ...args) {
        if (args[4]) {enter(); await gate;} return original.apply(this, args);
    });
    const {caller, owner} = await admit(); await entered;
    try {const before = create.mock.calls.length; expect(await kernel.readTaskCallTarget(owner)).toBeUndefined(); expect(create).toHaveBeenCalledTimes(before);}
    finally {release();} await caller.wait({until: 'settled', timeoutMs: 2000});
});
it.each(['scope', 'kind'] as const)('rejects corrupted source %s instead of following unrelated execution', async field => {
    const {kernel, make, admit, fs} = await fixture(), {caller, owner} = await admit(); await caller.wait({until: 'settled', timeoutMs: 2000});
    kernel.dispose(); await kernel.waitIdle();
    await fs.meta.seq!.transaction!(async tx => {const path = taskPath('/source', caller.id), record = JSON.parse((await tx.getEntry(path, 'record'))!);
        if (field === 'scope') record.sessionId = 'foreign'; else record.effects.call.request.kind = 'other'; await tx.setEntry(path, 'record', JSON.stringify(record));});
    await expect(make().readTaskCallTarget(owner)).rejects.toHaveProperty('code', field === 'scope' ? 'TASK_CALL_BINDING_INVALID' : 'INVALID_SPEC');
});
