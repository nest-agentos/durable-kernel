import type { SignalOptions, SignalReceipt, TaskRecord, TaskSignal } from '../../domain/types';
import type { ISeqFileTransaction } from '@itookit/vfs-core';
import { assertDurableValue } from '../../application/durability';
import { canonical } from '../../application/canonical';
import { KernelErrorCode, kernelError } from '../../domain/errors';
import { assertSharedLeaseTx } from './shared-lease';
import { appendEventTx, decode, encode, taskPath } from './seqfile-core';
import { hasCancelledAncestorTx, indexTask, isTerminal, requireTaskTx, unregisterTaskWaitTx, wakeFromPendingEvents, writeTaskTx } from './store-helpers';

export function validateSignal(signal: TaskSignal, options?: SignalOptions): void {
    if (!signal || typeof signal.type !== 'string' || !signal.type.trim()
        || options?.requestId !== undefined && (typeof options.requestId !== 'string' || !options.requestId.trim())) {
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Signal type and supplied requestId must be nonempty strings');
    }
    try {assertDurableValue(signal, 'Signal');}
    catch {throw kernelError(KernelErrorCode.INVALID_SPEC, 'Signal must be JSON serializable');}
}

export async function signalTaskTx(tx: ISeqFileTransaction, root: string, id: string,
    signal: TaskSignal, options?: SignalOptions): Promise<SignalReceipt> {
    const path = taskPath(root, id), fingerprint = canonical(signal);
    const key = options?.requestId === undefined ? undefined : `signal-request/${encodeURIComponent(options.requestId)}`;
    const saved = key ? await tx.getEntry(path, key) : null;
    if (saved) {
        const previous = decode<{fingerprint: string; receipt: SignalReceipt}>(saved);
        if (previous.fingerprint !== fingerprint) throw kernelError(KernelErrorCode.SIGNAL_IDENTITY_CONFLICT, 'Signal request identity conflict');
        return {...previous.receipt, replayed: true};
    }
    await assertSharedLeaseTx(tx, root, options?.lease);
    const task = await requireTaskTx(tx, root, id);
    if (isTerminal(task.status)) throw kernelError(KernelErrorCode.TASK_TERMINAL, 'Terminal Task cannot accept a new signal');
    if (await hasCancelledAncestorTx(tx, root, task)) throw kernelError(KernelErrorCode.CONFLICT, 'Task ancestor cancelled');
    const sequence = task.pendingEvents.length + task.version + 1, acceptedAt = Date.now();
    const receipt: SignalReceipt = {requestId: options?.requestId, sequence, acceptedAt, replayed: false};
    const next: TaskRecord = wakeFromPendingEvents({...task,
        pendingEvents: [...task.pendingEvents, {type: 'signal', sequence, signal}], version: task.version + 1, updatedAt: acceptedAt});
    if (next.status === 'ready') await unregisterTaskWaitTx(tx, root, task);
    await writeTaskTx(tx, root, next); await indexTask(tx, root, next);
    if (key) await tx.setEntry(path, key, encode({fingerprint, receipt}));
    await appendEventTx(tx, root, task.sessionId, id, 'task.signal', signal);
    return receipt;
}
