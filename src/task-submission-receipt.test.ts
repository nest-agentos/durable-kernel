import {afterEach, expect, it, vi} from 'vitest';
import {createVFS, MemoryBackend} from '@itookit/vfs-core';
import {Kernel, SeqFileKernelStore, type TaskSpec, type KernelDiagnostic} from './index';
import {sessionPath, taskPath} from './infrastructure/seqfile/seqfile-core';

const releases: Array<() => Promise<unknown>> = [];
afterEach(async () => {vi.restoreAllMocks(); for (const release of releases.splice(0).reverse()) await release();});
async function fixture() {
    const {manager} = await createVFS({rootBackend: new MemoryBackend()}), fs = await manager.openFileSystem('/'), diagnostics: KernelDiagnostic[] = [];
    releases.push(() => manager.dispose());
    const make = () => {
        const kernel = new Kernel({catalog: {fs}, onDiagnostic: event => diagnostics.push(event)});
        kernel.registerStorageResolver({kind: 'test', resolve: async ref => ({fs, rootPath: String(ref.locator)})});
        kernel.registerProgram({manifest: {kind: 'echo', version: '1'}, init: input => ({state: null, next: {type: 'complete', output: input}}),
            reduce: () => {throw Error('Unexpected');}});
        releases.push(async () => {kernel.dispose(); await kernel.waitIdle();}); return kernel;
    };
    const owner = make(); await owner.initialize();
    for (const id of ['source', 'other']) await owner.createSession({id, storage: {kind: 'test', locator: `/${id}`}});
    return {owner, make, fs, diagnostics};
}
function spec(): TaskSpec {return {program: {kind: 'echo', version: '1'}, input: {z: 1, a: 2}, requestId: 'original', deferStart: true};}
it('cold-reads the original admission without registering execution, scanning Tasks or returning the fingerprint body', async () => {
    const {owner, make} = await fixture(), original = spec(), task = await owner.submit('source', original), reader = make();
    const before = await owner.sessionSnapshot('source'), open = vi.spyOn(reader, 'openSession'), recover = vi.spyOn(reader, 'recoverSession');
    const submit = vi.spyOn(reader, 'submit'), list = vi.spyOn(SeqFileKernelStore.prototype, 'listTasks');
    const receipt = await reader.readTaskSubmissionReceipt('source', original);
    expect(receipt).toEqual({requestId: 'original', task: {sessionId: 'source', taskId: task.id}, acceptedAt: before.tasks[0].createdAt});
    expect(await owner.sessionSnapshot('source')).toEqual(before);
    expect(open).not.toHaveBeenCalled(); expect(recover).not.toHaveBeenCalled(); expect(submit).not.toHaveBeenCalled(); expect(list).not.toHaveBeenCalled();
    expect(JSON.stringify(receipt)).not.toContain('fingerprint'); expect(JSON.stringify(receipt)).not.toContain('"input"');
});
it('finds a deferred submission after the caller reply and worker were lost', async () => {
    const {owner, make} = await fixture(), original = spec();
    await expect((async () => {await owner.submit('source', original); throw Error('Reply lost');})()).rejects.toThrow('Reply lost');
    owner.dispose(); await owner.waitIdle(); const reader = make(), receipt = await reader.readTaskSubmissionReceipt('source', original);
    expect(receipt?.task.sessionId).toBe('source');
    const task = await reader.task('source', receipt!.task.taskId); expect(task).toMatchObject({status: 'created', attemptCount: 0});
});
it('keeps equal request IDs isolated by Session and returns missing without creating anything', async () => {
    const {owner, make} = await fixture(), original = spec(), a = await owner.submit('source', original), b = await owner.submit('other', original), reader = make();
    expect((await reader.readTaskSubmissionReceipt('source', original))?.task.taskId).toBe(a.id);
    expect((await reader.readTaskSubmissionReceipt('other', original))?.task.taskId).toBe(b.id);
    expect(await reader.readTaskSubmissionReceipt('source', {...original, requestId: 'missing'})).toBeUndefined();
    expect(await owner.listSessionTasks('source')).toHaveLength(1);
});
it('rejects a misdirected storage binding instead of returning another Session admission', async () => {
    const {owner} = await fixture(), original = spec(); await owner.submit('other', original);
    const lookup = SeqFileKernelStore.prototype.inspectSessionBinding;
    vi.spyOn(SeqFileKernelStore.prototype, 'inspectSessionBinding').mockImplementationOnce(function (this: SeqFileKernelStore) {return lookup.call(this, 'other');});
    await expect(owner.readTaskSubmissionReceipt('source', original)).rejects.toHaveProperty('code', 'TASK_SUBMISSION_RECEIPT_INVALID');
});
it('keeps an absent Session distinct from an existing Session without an admission', async () => {
    const {make} = await fixture(), reader = make();
    await expect(reader.readTaskSubmissionReceipt('absent', spec())).rejects.toHaveProperty('code', 'SESSION_NOT_FOUND');
    expect(await reader.readTaskSubmissionReceipt('source', spec())).toBeUndefined();
});
it('rejects changed content and serialization instead of returning a misleading missing admission', async () => {
    const {owner, make} = await fixture(), original = spec(); await owner.submit('source', original); const reader = make();
    await expect(reader.readTaskSubmissionReceipt('source', {...original, input: 'PRIVATE INPUT'})).rejects.toHaveProperty('code', 'CONFLICT');
    const reordered = {...original, input: {a: 2, z: 1}};
    await expect(reader.readTaskSubmissionReceipt('source', reordered)).rejects.toHaveProperty('code', 'CONFLICT');
    expect((await owner.listSessionTasks('source'))[0]).toMatchObject({status: 'created', attemptCount: 0});
});
it.each(['succeeded', 'cancelled'] as const)('retains %s admission independently of current execution status', async status => {
    const {owner} = await fixture(), original = spec(), task = await owner.submit('source', original);
    if (status === 'cancelled') await task.cancel(); else {await task.start(); await task.wait({until: 'settled', timeoutMs: 1000});}
    expect((await owner.readTaskSubmissionReceipt('source', original))?.task.taskId).toBe(task.id);
    expect((await task.status()).task.status).toBe(status);
});
it('freezes observation input before asynchronous binding lookup and reports safe diagnostics', async () => {
    const {owner, diagnostics} = await fixture(), original = spec(), task = await owner.submit('source', original);
    const lookup = SeqFileKernelStore.prototype.inspectSessionBinding; let release!: () => void;
    const gate = new Promise<void>(resolve => {release = resolve;});
    vi.spyOn(SeqFileKernelStore.prototype, 'inspectSessionBinding').mockImplementationOnce(async function (this: SeqFileKernelStore, ...args) {await gate; return lookup.apply(this, args);});
    const query = owner.readTaskSubmissionReceipt('source', original); original.input = 'PRIVATE INPUT'; original.requestId = 'changed'; release();
    expect((await query)?.task.taskId).toBe(task.id);
    expect(diagnostics).toContainEqual(expect.objectContaining({operation: 'task.submission.inspect', operationId: 'original', outcome: 'success'}));
    expect(JSON.stringify(diagnostics)).not.toContain('PRIVATE INPUT');
});
it.each(['missing', 'blank'] as const)('rejects %s request identity before returning a receipt', async mode => {
    const {owner} = await fixture(), original = spec(); if (mode === 'missing') delete original.requestId; else original.requestId = ' ';
    await expect(owner.readTaskSubmissionReceipt('source', original)).rejects.toHaveProperty('code', 'INVALID_SPEC');
    expect(await owner.listSessionTasks('source')).toEqual([]);
});
it.each(['malformed', 'shape', 'scope'] as const)('rejects %s admission storage without exposing raw data', async mode => {
    const {owner, fs, diagnostics} = await fixture(), original = spec(), task = await owner.submit('source', original);
    await fs.meta.seq!.transaction!(async tx => {
        if (mode === 'scope') {
            const path = taskPath('/source', task.id), record = JSON.parse((await tx.getEntry(path, 'record'))!); record.sessionId = 'foreign';
            await tx.setEntry(path, 'record', JSON.stringify(record));
        } else await tx.setEntry(sessionPath('/source'), 'submission/original', mode === 'malformed' ? 'PRIVATE DATA' : JSON.stringify({id: {}, fingerprint: 'PRIVATE DATA'}));
    });
    await expect(owner.readTaskSubmissionReceipt('source', original)).rejects.toHaveProperty('code', 'TASK_SUBMISSION_RECEIPT_INVALID');
    expect(JSON.stringify(diagnostics)).not.toContain('PRIVATE DATA');
});
