import type {ISeqFileTransaction} from '@nestos/vfs-core';
import type {CrossSessionMessage, ResolvedStorageBinding, TaskRecord} from '../../domain/types';

export const RUNTIME_RECOVERY_PREFIX = 'runtime-recovery/';
interface ResourceRecoveryRow {
    sessionId?: string;
    released?: boolean;
    closed?: boolean;
    status?: string;
    actor?: {sessionId?: string; taskId?: string};
    notified?: boolean;
    state?: string;
    physical?: unknown;
}
export async function runtimeResponsibility(tx: ISeqFileTransaction, path: string | undefined, sessionId: string,
    kind: string, id: string, pending: boolean): Promise<void> {
    if (!path) return;
    const key = RUNTIME_RECOVERY_PREFIX + encodeURIComponent(sessionId) + '/' + kind + '/' + encodeURIComponent(id);
    const current = await tx.getEntry(path, key);
    if (pending && current !== sessionId) await tx.setEntry(path, key, sessionId);
    else if (!pending && current !== null) await tx.deleteEntry(path, key);
}
export function indexRuntimeTask(tx: ISeqFileTransaction, task: TaskRecord): Promise<void> {
    const terminal = ['succeeded', 'failed', 'cancelled'].includes(task.status);
    const effects = Object.values(task.effects).some(effect => effect.cleanupPending || ['pending', 'leased', 'indeterminate'].includes(effect.status));
    if (task.status === 'created' && !effects) return Promise.resolve();
    return runtimeResponsibility(tx, task.recoveryIndexPath, task.sessionId, 'task', task.id,
        !terminal && task.status !== 'created' || effects);
}
export function indexRuntimeMessage(tx: ISeqFileTransaction, path: string | undefined, message: CrossSessionMessage): Promise<void> {
    return runtimeResponsibility(tx, path, message.sourceSessionId, 'outbox', message.id,
        message.status === 'pending' || message.sourceSessionId !== message.targetSessionId && message.settlementAcknowledgedAt === undefined);
}
/** Coalesce resource mutations; settled requests and immutable history never remain candidates. */
export async function indexRuntimeResources(tx: ISeqFileTransaction, binding: ResolvedStorageBinding,
    keys: ReadonlySet<string>): Promise<void> {
    const context = binding.runtimeRecovery; if (!context) return;
    const path = binding.rootPath.replace(/\/$/, '') + '/resources.seq';
    for (const key of keys) {
        const raw = await tx.getEntry(path, key);
        const row: ResourceRecoveryRow | undefined = raw ? JSON.parse(raw) : undefined;
        const sessionId = context.sessionId ?? row?.sessionId ?? row?.actor?.sessionId;
        if (sessionId) await runtimeResponsibility(tx, context.catalogPath, sessionId, 'resource', path + '/' + key, resourcePending(key, row));
    }
}
function resourcePending(key: string, row: ResourceRecoveryRow | undefined): boolean {
    if (!row) return false;
    if (key.startsWith('managed/claim/')) return !row.released;
    if (key.startsWith('managed/handle/')) return !row.closed;
    if (key.startsWith('managed/cleanup/')) return row.status !== 'succeeded';
    if (key.startsWith('managed/request/')) return pendingRequest(row);
    return key.startsWith('managed/resource/') && pendingResource(row);
}
function pendingRequest(row: ResourceRecoveryRow): boolean {
    return row.status === 'pending' || !!row.actor?.taskId && !row.notified;
}
function pendingResource(row: ResourceRecoveryRow): boolean {
    return row.state === 'closing' || !!row.physical && row.state !== 'tombstoned';
}
