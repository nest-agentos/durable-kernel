import type {ISeqFileTransaction} from '@itookit/vfs-core';
import type {TaskRecord, TaskTreeCancellationOptions, TaskTreeCancellationReceipt} from '../../domain/types';
import {KernelErrorCode, kernelError} from '../../domain/errors';
import {taskTreeIds} from '../../domain/task-tree';
import {assertSharedLeaseTx} from './shared-lease';
import {finishTaskWithoutClaimTx} from './task-terminalization';
import {readSessionTasksTx} from './task-tree';
import {appendEventTx, cancelActiveEffects, decode, encode, isTerminal, requireTaskTx, taskPath, writeTaskTx} from './store-helpers';

type StoredReceipt = {fingerprint: string; receipt: TaskTreeCancellationReceipt};
const key = (requestId: string) => `tree-cancellation/${encodeURIComponent(requestId)}`;

export function validateTreeCancellation(options: TaskTreeCancellationOptions): void {
    if (!options || typeof options.requestId !== 'string' || !options.requestId.trim()
        || !Number.isSafeInteger(options.expectedEpoch) || options.expectedEpoch < 0
        || options.reason !== undefined && typeof options.reason !== 'string')
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Tree cancellation requires requestId, control epoch and optional text reason');
}

/** All members are fenced before any adapter cleanup starts. No new tree state machine. */
export async function cancelRemainingTaskTreeTx(tx: ISeqFileTransaction, root: string, taskId: string,
    options: TaskTreeCancellationOptions): Promise<TaskRecord[]> {
    await assertSharedLeaseTx(tx, root, options.lease);
    const task = await requireTaskTx(tx, root, taskId), path = taskPath(root, taskId);
    const reason = options.reason ?? 'Remaining task tree cancelled';
    const fingerprint = encode({reason, expectedEpoch: options.expectedEpoch});
    const previous = await tx.getEntry(path, key(options.requestId));
    if (previous && decode<StoredReceipt>(previous).fingerprint !== fingerprint)
        throw kernelError(KernelErrorCode.CONFLICT, 'Tree cancellation request conflict');
    if (!isTerminal(task.status) || !task.exit)
        throw kernelError(KernelErrorCode.CONFLICT, 'Remaining tree cancellation requires a terminal root');
    if (!previous && options.expectedEpoch !== (task.control?.epoch ?? 0))
        throw kernelError(KernelErrorCode.CONFLICT, 'Tree cancellation control epoch conflict');
    const ids = taskTreeIds(await readSessionTasksTx(tx, root, taskId), taskId);
    if (!previous) {
        for (const id of ids) await cancelMemberTx(tx, root, await requireTaskTx(tx, root, id), reason);
        const receipt: TaskTreeCancellationReceipt = {requestId: options.requestId, taskId,
            expectedEpoch: options.expectedEpoch, controlEpoch: task.control?.epoch ?? 0,
            rootStatus: task.exit.status, acceptedAt: Date.now(), taskCount: ids.size};
        await tx.setEntry(path, key(options.requestId), encode({fingerprint, receipt}));
        await appendEventTx(tx, root, task.sessionId, taskId, 'task.tree-cancellation.requested', receipt);
    }
    // Dependency propagation can update another member; return the final transaction records.
    const tasks: TaskRecord[] = [];
    for (const id of ids) tasks.push(await requireTaskTx(tx, root, id));
    return tasks;
}

async function cancelMemberTx(tx: ISeqFileTransaction, root: string, task: TaskRecord, reason: string): Promise<void> {
    if (!isTerminal(task.status)) {
        await finishTaskWithoutClaimTx(tx, root, task, 'cancelled', undefined, {message: reason});
        return;
    }
    if (!Object.values(task.effects).some(effect => ['pending', 'leased', 'indeterminate'].includes(effect.status))) return;
    const now = Date.now();
    await writeTaskTx(tx, root, {...task, effects: cancelActiveEffects(task.effects, now),
        version: task.version + 1, updatedAt: now});
}

/** Cold admission read, independent of physical cleanup. */
export async function readTreeCancellationReceiptTx(tx: ISeqFileTransaction, root: string, taskId: string,
    requestId: string): Promise<TaskTreeCancellationReceipt | undefined> {
    if (typeof requestId !== 'string' || !requestId.trim())
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Tree cancellation requestId is required');
    await requireTaskTx(tx, root, taskId);
    const raw = await tx.getEntry(taskPath(root, taskId), key(requestId));
    return raw ? decode<StoredReceipt>(raw).receipt : undefined;
}
