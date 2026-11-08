import type {ISeqFileTransaction} from '@nestos/vfs-core';
import type {TaskRecord, TaskSpec} from '../../domain/types';
import {KernelErrorCode, kernelError} from '../../domain/errors';

export function decodeRecoveryEntry<T>(value: string): T {
    try {return JSON.parse(value) as T;}
    catch {throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Invalid recovery index encoding');}
}

export function recoveryPrefix(scope: string): string {
    if (typeof scope !== 'string' || !/^[a-zA-Z0-9_.-]{1,128}$/.test(scope))
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid recovery scope');
    return `recovery/${scope}/`;
}
export function validateTaskRecovery(recovery: TaskSpec['recovery']): void {
    if (recovery === undefined) return;
    if (!recovery || typeof recovery !== 'object' || Array.isArray(recovery)
        || Object.keys(recovery).some(key => !['scope', 'key', 'until'].includes(key)))
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid recovery responsibility');
    recoveryPrefix(recovery.scope);
    if (!['terminal', 'acknowledged'].includes(recovery.until) || recovery.key !== undefined
        && (typeof recovery.key !== 'string' || !recovery.key.length || recovery.key.length > 256))
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid recovery responsibility');
}

export async function indexSharedRecovery(tx: ISeqFileTransaction, path: string, sessionId: string, key: string,
    scope: string, pending: boolean): Promise<void> {
    const indexKey = 'shared-' + recoveryPrefix(scope) + encodeURIComponent(sessionId) + '/' + encodeURIComponent(key);
    if (pending) await tx.setEntry(path, indexKey, JSON.stringify({sessionId, key}));
    else await tx.deleteEntry(path, indexKey);
}
/** Responsibility and authoritative Task state commit in the same transaction. */
export async function indexTaskRecovery(tx: ISeqFileTransaction, task: TaskRecord): Promise<void> {
    if (!task.recovery) return;
    validateTaskRecovery(task.recovery);
    if (!task.recoveryIndexPath) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Recovery requires the catalog transaction domain');
    const key = recoveryPrefix(task.recovery.scope) + task.id;
    const terminal = ['succeeded', 'failed', 'cancelled'].includes(task.status);
    if (task.recoveryAcknowledged || task.recovery.until === 'terminal' && terminal) {
        await tx.deleteEntry(task.recoveryIndexPath, key); return;
    }
    const value = JSON.stringify({sessionId: task.sessionId, taskId: task.id, ...(task.recovery.key ? {key: task.recovery.key} : {})});
    if (await tx.getEntry(task.recoveryIndexPath, key) !== value) await tx.setEntry(task.recoveryIndexPath, key, value);
}
