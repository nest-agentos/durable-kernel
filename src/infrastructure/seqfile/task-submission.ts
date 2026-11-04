import type {ISeqFileTransaction} from '@nestos/vfs-core';
import type {TaskSpec, TaskSubmissionReceipt} from '../../domain/types';
import {KernelErrorCode, kernelError} from '../../domain/errors';
import {assertDurableValue} from '../../application/durability';
import {decode, encode, sessionPath} from './seqfile-core';
import {requireSessionTx, requireTaskTx} from './store-helpers';

/** The original admission key and Task are read in one transaction; inspection never creates a Task. */
export async function readTaskSubmissionReceiptTx(tx: ISeqFileTransaction, root: string, sessionId: string, spec: TaskSpec): Promise<TaskSubmissionReceipt | undefined> {
    if (!spec || typeof spec.requestId !== 'string' || !spec.requestId.trim())
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Task submission observation requires the original requestId and spec');
    assertDurableValue(spec, 'Task submission observation');
    const session = await requireSessionTx(tx, root);
    if (session.id !== sessionId) throw invalid();
    const raw = await tx.getEntry(sessionPath(root), `submission/${encodeURIComponent(spec.requestId)}`);
    if (!raw) return;
    const saved = readReceipt(raw);
    if (saved.fingerprint !== encode(spec)) throw kernelError(KernelErrorCode.CONFLICT, 'Task submission content differs from the admitted request');
    const task = await requireTaskTx(tx, root, saved.id);
    if (task.id !== saved.id || task.sessionId !== session.id || !Number.isFinite(task.createdAt)) throw invalid();
    return {requestId: spec.requestId, task: {sessionId: session.id, taskId: task.id}, acceptedAt: task.createdAt};
}
function readReceipt(raw: string): {id: string; fingerprint: string} {
    let value: {id?: unknown; fingerprint?: unknown};
    try {value = decode(raw);} catch {throw invalid();}
    if (!value || typeof value.id !== 'string' || !value.id.trim() || typeof value.fingerprint !== 'string') throw invalid();
    return {id: value.id, fingerprint: value.fingerprint};
}
function invalid(): Error {return kernelError(KernelErrorCode.TASK_SUBMISSION_RECEIPT_INVALID, 'Task submission receipt is invalid');}
