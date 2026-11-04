import type {ISeqFileTransaction} from '@nestos/vfs-core';
import type {TaskRecord} from '../../domain/types';
import {cleanupTaskCachesTx} from './cache-store';
import {advanceDependants, appendEventTx, cancelActiveEffects, finishAttemptTx, indexTask,
    unregisterTaskWaitTx, wakeTaskWaiters, writeTaskTx} from './store-helpers';

/** Caller owns the Session transaction and validates admission before terminalizing. */
export async function finishTaskWithoutClaimTx(tx: ISeqFileTransaction, root: string, task: TaskRecord,
    status: 'cancelled' | 'failed', output?: unknown, error?: {message: string}): Promise<TaskRecord> {
    const completedAt = Date.now();
    const next: TaskRecord = {
        ...task, status, output, wait: undefined, currentAttempt: undefined,
        effects: cancelActiveEffects(task.effects, completedAt),
        interactions: Object.fromEntries(Object.entries(task.interactions).map(([id, interaction]) => [id, interaction.status === 'pending' ? { ...interaction, status: 'cancelled' as const } : interaction])),
        exit: { taskId: task.id, status, output, error, completedAt },
        version: task.version + 1, updatedAt: completedAt,
    };
    await unregisterTaskWaitTx(tx, root, task);
    await finishAttemptTx(tx, root, task, 'cancelled');
    await writeTaskTx(tx, root, next);
    await indexTask(tx, root, next);
    await appendEventTx(tx, root, task.sessionId, task.id, `task.${status}`, next.exit);
    await cleanupTaskCachesTx(tx, root, task.sessionId, task.id);
    await advanceDependants(tx, root, next);
    await wakeTaskWaiters(tx, root, next);
    return next;
}
