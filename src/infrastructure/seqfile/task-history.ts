import type {ISeqFileReadTransaction} from '@nestos/vfs-core';
import type {ResolvedStorageBinding, TaskHistoryPage, TaskHistoryQuery, TaskRecord} from '../../domain/types';
import {KernelErrorCode, kernelError} from '../../domain/errors';
import {decode, seq, snapshotKey, taskPath} from './seqfile-core';

type Reader = Pick<ISeqFileReadTransaction, 'getEntry' | 'getEntries' | 'walkEntries'>;

export function withTaskHistory<T>(binding: ResolvedStorageBinding, taskId: string,
    read: (reader: Reader, path: string, current: TaskRecord) => Promise<T>): Promise<T> {
    const storage = seq(binding.fs), path = taskPath(binding.rootPath, taskId);
    const run = async (reader: Reader) => {
        const raw = await reader.getEntry(path, 'record');
        if (!raw) throw kernelError(KernelErrorCode.TASK_NOT_FOUND, 'Task not found: ' + taskId);
        return read(reader, path, decode<TaskRecord>(raw));
    };
    return storage.readTransaction ? storage.readTransaction(run, path) : run(storage);
}

export async function historyPage(reader: Reader, path: string, current: TaskRecord, query: TaskHistoryQuery): Promise<TaskHistoryPage> {
    const after = query.afterVersion ?? -1, limit = query.limit ?? 100;
    if (!Number.isSafeInteger(after) || after < -1 || !Number.isSafeInteger(limit) || limit < 1 || limit > 500) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task history page');
    const through = query.throughVersion ?? current.version;
    if (!Number.isSafeInteger(through) || through < 0 || through > current.version) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task history upper bound');
    const page = async (start: number) => {
        const end = Math.min(through, start + limit - 1), keys: string[] = [];
        for (let version = start; version <= end; version++) keys.push(snapshotKey(version));
        const rows = keys.length ? await reader.getEntries(path, keys) : {};
        return {items: keys.flatMap(key => rows[key] ? [decode<TaskRecord>(rows[key])] : key === snapshotKey(current.version) ? [current] : []),
            throughVersion: through, ...(end < through ? {nextAfterVersion: end} : {})};
    };
    const first = await page(after + 1);
    if (first.items.length || first.nextAfterVersion === undefined) return first;
    let next = through === current.version ? current.version : through + 1;
    await reader.walkEntries(path, row => {
        next = Number(row.key.slice('snapshot/'.length)); return false;
    }, {keyPrefix: 'snapshot/', after: snapshotKey(first.nextAfterVersion), order: 'key', limit: 1});
    return page(next);
}
