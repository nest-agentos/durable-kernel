import {indexSharedRecovery, indexTaskRecovery} from './task-recovery-index';
import type { ISeqFileTransaction } from '@nestos/vfs-core';
import {indexTaskActivity} from './activity-index';
import type { ArchiveExportOptions, ArchiveImportOptions, ArchiveImportReport, ArchiveManifest, ArchiveRecord, ParsedArchive } from '../../domain/archive';
import type { ResolvedStorageBinding, SessionRecord, TaskRecord } from '../../domain/types';
import { KernelErrorCode, kernelError } from '../../domain/errors';
import { canonical } from '../../application/canonical';
import { ArchiveHash } from '../../application/archive-hash';
import { archiveDefaults } from '../../application/archive-codec';
import { appendEventTx, catalogPath, createId, decode, encode, ensureSessionLayout, ensureTaskLayout, eventsPath, graphPath, indexPath, join, sessionPath, sessionSeqFiles, sharedPath, taskPath, transaction } from './seqfile-core';
import { indexTask, requireSessionTx, taskFromSpec } from './store-helpers';

export function assertQuiescent(session: SessionRecord, tasks: TaskRecord[]): void {
    if (!['suspended','closed','archived'].includes(session.status) || tasks.some(task => task.currentAttempt || task.status === 'running'
        || Object.values(task.effects).some(effect => effect.currentAttempt || effect.cleanupPending || effect.status === 'leased'))) {
        throw kernelError(KernelErrorCode.ARCHIVE_UNSAFE, 'Recovery archive requires a paused or closed Session with no live claims or pending cleanup');
    }
}

/** Freeze record membership and contents in one bounded transaction before yielding to a slow reader. */
export async function readArchive(binding: ResolvedStorageBinding, options: ArchiveExportOptions) {
    return transaction(binding.fs, async tx => {
        const session = await requireSessionTx(tx, binding.rootPath), ids: string[] = [];
        await tx.walkEntries(indexPath(binding.rootPath), row => {
            ids.push(row.key.slice(5));
            if (ids.length > (options.maxRecords ?? archiveDefaults.maxRecords)) throw kernelError(KernelErrorCode.ARCHIVE_INVALID, 'Archive Task limit exceeded');
            return true;
        }, {keyPrefix: 'task/'});
        const paths = [...sessionSeqFiles(binding.rootPath), ...ids.map(id => taskPath(binding.rootPath, id))];
        const records = await collectRecords(tx, binding, paths, options);
        if (options.purpose === 'recover') assertQuiescent(session, records.filter(row => row.path.startsWith('tasks/') && row.key === 'record')
            .map(row => decode<TaskRecord>(row.value)));
        const manifest: ArchiveManifest = {format: 'durable-kernel/session', schemaVersion: 1, purpose: options.purpose,
            archiveId: createId('archive'), session, paths: paths.map(path => path.slice(binding.rootPath.replace(/\/$/, '').length + 1)),
            asOfSequence: Number(await tx.getEntry(eventsPath(binding.rootPath), 'next-sequence') ?? 0), dependencies: options.dependencies};
        return {manifest, records};
    });
}

export async function* archiveLines(snapshot: Awaited<ReturnType<typeof readArchive>>, options: ArchiveExportOptions): AsyncGenerator<string> {
    const hash = new ArchiveHash(); let bytes = 0;
    for (const row of [snapshot.manifest, ...snapshot.records]) {
        options.signal?.throwIfAborted(); const line = canonical(row) + '\n';
        bytes += new TextEncoder().encode(line).length;
        if (bytes > (options.maxBytes ?? archiveDefaults.maxBytes)) throw kernelError(KernelErrorCode.ARCHIVE_INVALID, 'Archive byte limit exceeded');
        hash.update(line); yield line;
    }
    const footer = canonical({type: 'end', records: snapshot.records.length, checksum: hash.finish()}) + '\n';
    if (bytes + footer.length > (options.maxBytes ?? archiveDefaults.maxBytes)) throw kernelError(KernelErrorCode.ARCHIVE_INVALID, 'Archive byte limit exceeded');
    yield footer;
}

export function forkTasks(archive: ParsedArchive, id: string): {tasks: TaskRecord[]; taskIds: Record<string,string>} {
    const roots = archive.tasks.filter(task => !task.parentTaskId);
    const taskIds = Object.fromEntries(roots.map(task => [task.id, createId('task')]));
    const tasks = roots.map(task => {
        if ((task.dependencies ?? []).some(dep => !Object.hasOwn(taskIds, dep.task))) {
            throw kernelError(KernelErrorCode.ARCHIVE_UNSAFE, 'Fork root depends on an old descendant; application remapping is required');
        }
        const next = taskFromSpec(taskIds[task.id], id, {program: task.program, input: task.input, deferStart: true,
            retry: task.retry, priority: task.priority, labels: task.labels,
            dependsOn: (task.dependencies ?? []).map(dep => ({...dep, task: taskIds[dep.task]}))}, Date.now());
        return {...next, sessionPaused: true};
    });
    return {tasks, taskIds};
}

/** Catalog and all imported records use the same transaction; a receipt survives response loss. */
export async function writeArchive(catalog: ResolvedStorageBinding, target: ResolvedStorageBinding, archive: ParsedArchive,
    options: ArchiveImportOptions, id: string): Promise<ArchiveImportReport> {
    if (catalog.fs !== target.fs) throw kernelError(KernelErrorCode.ARCHIVE_UNSAFE, 'Archive import requires catalog and target in the same filesystem transaction');
    const fork = options.mode === 'fork' ? forkTasks(archive, id) : undefined;
    const taskIds = fork?.taskIds ?? Object.fromEntries(archive.tasks.map(task => [task.id, task.id]));
    await ensureSessionLayout(target);
    for (const taskId of Object.values(taskIds)) {options.signal?.throwIfAborted(); await ensureTaskLayout(target, taskId);}
    return transaction(catalog.fs, async tx => {
        options.signal?.throwIfAborted();
        const path = catalogPath(catalog.rootPath), key = `archive-import/${encodeURIComponent(options.requestId)}`;
        const fingerprint = canonical([archive.validation.checksum, options.mode, options.storage, options.sessionId]);
        const saved = await tx.getEntry(path, key);
        if (saved) {
            const receipt = decode<{fingerprint: string; report: ArchiveImportReport}>(saved);
            if (receipt.fingerprint !== fingerprint) throw kernelError(KernelErrorCode.CONFLICT, 'Archive import identity conflict');
            return {...receipt.report, replayed: true};
        }
        if (await tx.getEntry(path, `session/${id}`) || await tx.getEntry(sessionPath(target.rootPath), 'record')) {
            throw kernelError(KernelErrorCode.CONFLICT, 'Archive target Session or storage already exists');
        }
        await importRecordsTx(tx, catalog, target, archive, options, id, fork?.tasks);
        await appendEventTx(tx, target.rootPath, id, undefined, 'session.archive.imported', {mode: options.mode, archiveId: archive.validation.manifest.archiveId});
        const report: ArchiveImportReport = {sessionId: id, mode: options.mode, replayed: false, paused: true, taskIds};
        await tx.setEntry(path, key, encode({fingerprint, report}));
        return report;
    });
}

async function importRecordsTx(tx: ISeqFileTransaction, catalog: ResolvedStorageBinding, target: ResolvedStorageBinding,
    archive: ParsedArchive, options: ArchiveImportOptions, id: string, fresh?: TaskRecord[]): Promise<void> {
    const path = catalogPath(catalog.rootPath);
    const tasks = fresh ?? archive.tasks;
    for (const task of tasks) if (await tx.getEntry(path, `task/${task.id}`)) {
        throw kernelError(KernelErrorCode.CONFLICT, 'An imported Task identity already belongs to this catalog');
    }
    const session: SessionRecord = {...archive.validation.manifest.session, id, instanceId: createId('session-instance'), storage: options.storage,
        status: 'suspended', closeMode: undefined, registrationPending: false, version: archive.validation.manifest.session.version + 1};
    if (fresh) {session.createdAt = Date.now(); session.nextEventSeq = 1; session.version = 0;}
    session.updatedAt = Date.now();
    if (!fresh) for (const row of archive.records) {options.signal?.throwIfAborted(); await tx.setEntry(join(target.rootPath, row.path), row.key, row.value);}
    await tx.setEntry(sessionPath(target.rootPath), 'record', encode(session));
    await tx.setEntry(path, `session/${id}`, encode(session));
    if (fresh) await appendEventTx(tx, target.rootPath, id, undefined, 'session.created', session);
    await tx.walkEntries(sharedPath(target.rootPath), async row => {
        const entry = decode<import('../../domain/types').SharedStateEntry>(row.value);
        if (entry.recoveryScope) await indexSharedRecovery(tx, path, id, entry.key, entry.recoveryScope, true);
        return true;
    }, {keyPrefix: 'value/'});
    for (const task of tasks) {
        const current = {...task, sessionPaused: true, recoveryIndexPath: path};
        await indexTaskRecovery(tx, current);
        await tx.setEntry(taskPath(target.rootPath, current.id), 'record', encode(current));
        await indexTaskActivity(tx, target.rootPath, current);
        await indexTask(tx, target.rootPath, current); await tx.setEntry(path, `task/${current.id}`, id);
        if (fresh) await createForkRelations(tx, target, current);
    }
}

async function createForkRelations(tx: ISeqFileTransaction, target: ResolvedStorageBinding, task: TaskRecord) {
    for (const dependency of task.dependencies ?? []) await tx.setEntry(graphPath(target.rootPath), `edge/${dependency.task}/${task.id}`, encode(dependency));
    await appendEventTx(tx, target.rootPath, task.sessionId, task.id, 'task.created', task);
}

async function collectRecords(tx: ISeqFileTransaction, binding: ResolvedStorageBinding, paths: string[], options: ArchiveExportOptions) {
    const records: ArchiveRecord[] = []; let bytes = 0;
    for (const path of paths) await tx.walkEntries(path, row => {
        options.signal?.throwIfAborted();
        const record = {path: path.slice(binding.rootPath.replace(/\/$/, '').length + 1), key: row.key, value: row.value};
        bytes += new TextEncoder().encode(canonical(record)).length + 1; records.push(record);
        if (bytes > (options.maxBytes ?? archiveDefaults.maxBytes) || records.length > (options.maxRecords ?? archiveDefaults.maxRecords)) {
            throw kernelError(KernelErrorCode.ARCHIVE_INVALID, 'Archive exceeds configured capacity');
        }
        return true;
    });
    return records;
}
