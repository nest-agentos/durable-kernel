import type {ISeqFileTransaction} from '@itookit/vfs-core';
import type {TaskRecord} from '../../domain/types';
import {activityKey} from './activity-index';
import {decode, taskPath} from './seqfile-core';

/** A concurrent writer may advance Tasks between directory enumeration and repair. */
export async function refreshRecoveryRecordsTx(tx: ISeqFileTransaction, root: string,
    records: Map<string, TaskRecord>, rows: ReadonlyMap<string, string>): Promise<void> {
    const ids = new Set(records.keys());
    for (const key of rows.keys()) if (key.startsWith('task/')) ids.add(key.slice(5));
    const changed = [...ids].filter(id => projectionVersion(rows.get(activityKey(id))) !== records.get(id)?.version);
    if (!changed.length) return;
    const values = await tx.getEntriesMany(changed.map(id => ({fileIdOrPath: taskPath(root, id), key: 'record'})));
    for (const [index, id] of changed.entries()) {
        const raw = values[index];
        if (raw) records.set(id, decode<TaskRecord>(raw)); else records.delete(id);
    }
}

function projectionVersion(raw?: string): number {
    try {
        const version = raw ? decode<{taskVersion: number}>(raw).taskVersion : -1;
        return Number.isSafeInteger(version) && version >= 0 ? version : -1;
    } catch {return -1;}
}
