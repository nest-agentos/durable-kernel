import type {ISeqFileTransaction} from '@itookit/vfs-core';
import type {TaskCancellationOptions, TaskCancellationReceipt, TaskRecord} from '../../domain/types';
import {KernelErrorCode, kernelError} from '../../domain/errors';
import {decode, encode, requireTaskTx, taskPath} from './store-helpers';

type StoredReceipt = {fingerprint: string; receipt: TaskCancellationReceipt};
const key = (requestId: string) => `cancellation/${encodeURIComponent(requestId)}`;

export function validateCancellation(options?: TaskCancellationOptions): void {
    if (options?.requestId === undefined && options?.expectedEpoch === undefined) return;
    if (typeof options?.requestId !== 'string' || !options.requestId.trim()
        || options.expectedEpoch !== undefined && (!Number.isSafeInteger(options.expectedEpoch) || options.expectedEpoch < 0))
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Cancellation requires a requestId and a valid optional control epoch');
}

/** The caller commits this receipt in the same transaction as the terminal Task record. */
export async function prepareCancellationTx(tx: ISeqFileTransaction, root: string, task: TaskRecord,
    reason: string, options?: TaskCancellationOptions) {
    const requestId = options?.requestId;
    if (!options || requestId === undefined) return;
    const fingerprint = encode({reason, expectedEpoch: options.expectedEpoch});
    const path = taskPath(root, task.id), previous = await tx.getEntry(path, key(requestId));
    if (previous) {
        if (decode<StoredReceipt>(previous).fingerprint !== fingerprint)
            throw kernelError(KernelErrorCode.CONFLICT, 'Cancellation request conflict');
        return {replayed: true, commit: async (_task: TaskRecord) => {}};
    }
    if (options.expectedEpoch !== undefined && options.expectedEpoch !== (task.control?.epoch ?? 0))
        throw kernelError(KernelErrorCode.CONFLICT, 'Cancellation control epoch conflict');
    return {replayed: false, commit: async (result: TaskRecord) => {
        if (!result.exit) throw kernelError(KernelErrorCode.CONFLICT, 'Cancellation outcome is not terminal');
        const receipt: TaskCancellationReceipt = {requestId, taskId: task.id, status: result.exit.status,
            controlEpoch: task.control?.epoch ?? 0, ...(options.expectedEpoch === undefined ? {} : {expectedEpoch: options.expectedEpoch})};
        await tx.setEntry(path, key(requestId), encode({fingerprint, receipt}));
    }};
}

/** Receipt reads never activate a scheduler or repeat cancellation. */
export async function readCancellationReceiptTx(tx: ISeqFileTransaction, root: string, taskId: string,
    requestId: string): Promise<TaskCancellationReceipt | undefined> {
    if (typeof requestId !== 'string' || !requestId.trim())
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Cancellation requestId is required');
    await requireTaskTx(tx, root, taskId);
    const raw = await tx.getEntry(taskPath(root, taskId), key(requestId));
    return raw ? decode<StoredReceipt>(raw).receipt : undefined;
}
