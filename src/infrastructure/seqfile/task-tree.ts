import type {ISeqFileTransaction} from '@itookit/vfs-core';
import type {TaskRecord} from '../../domain/types';
import {indexPath, readTaskTx} from './store-helpers';

export async function readSessionTasksTx(tx: ISeqFileTransaction, root: string, taskId: string): Promise<TaskRecord[]> {
    const ids = new Set([taskId]), tasks: TaskRecord[] = [];
    await tx.walkEntries(indexPath(root), row => { ids.add(row.key.slice(5)); return true; }, { keyPrefix: 'task/' });
    for (const id of ids) { const task = await readTaskTx(tx, root, id); if (task) tasks.push(task); }
    return tasks;
}
