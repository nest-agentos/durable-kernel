import {indexRuntimeMessage, RUNTIME_RECOVERY_PREFIX} from './runtime-recovery-index';
import {taskStat} from '../../domain/status';
import {decodeRecoveryEntry, indexSharedRecovery, recoveryPrefix, validateTaskRecovery} from './task-recovery-index';
import {readTaskSubmissionReceiptTx} from './task-submission';
import {cancelRemainingTaskTreeTx, readTreeCancellationReceiptTx, validateTreeCancellation} from './task-tree-cancellation';
import {finishTaskWithoutClaimTx} from './task-terminalization';
import {withInteractionResponse, assertInteractionResponseIdentity, interactionResponseReceipt, validateInteractionResponseId} from './interaction-response';
import {prepareCancellationTx, readCancellationReceiptTx, validateCancellation} from './task-cancellation-store';
import { controlTaskTx, readControlReceiptTx } from './task-control-store';
import { signalTaskTx, validateSignal } from './signal-store';
import { ACTIVITY_REVISION, activityKey, indexTaskActivity } from './activity-index';
import { refreshRecoveryRecordsTx } from './recovery-records';
import type { LeaseGuardOptions } from '../../domain/types';
import type { ISeqFileTransaction } from '@nestos/vfs-core';
import { assertSharedLeaseTx } from './shared-lease';
import { enqueueMessageTx, deliverMessageTx, consumeMessageTx, pruneMessagesTx, acknowledgeMessageSettlementTx, enqueueSessionMessageTx, inboxPageTx, acknowledgeSessionMessageTx } from './mailbox-store';
import { executeResourceTx, type PreparedResourceCommand } from './managed-resources';
import { assertSessionLayout, currentSessionLayout } from './session-layout';
import { cleanupSessionCachesTx, cleanupTaskCachesTx, createCacheTx, readCacheTx, publishCacheTx, invalidateCacheTx, renewCacheTx, manageCacheTx, listCachesTx } from './cache-store';
import { refreshWaiters, recoverWaitGraphTx, hasCancelledAncestorTx, validateRetrySourceTx } from './store-helpers';
import { assertDurableValue } from '../../application/durability';
import {canonical} from '../../application/canonical';
import { assertCustomEventType } from '../../public/events';
import { taskRecoveryIssues } from '../../application/recovery-issues';
import { taskTreeIds } from '../../domain/task-tree';
import { prunedSequenceKey, retentionBoundary } from './event-store';
import { snapshotKey, ensureTaskEventIndexTx, taskEventCountKey, taskEventFirstKey, taskEventKey } from './seqfile-core';

import type {
    BudgetAccount,
    ContextBranch,
    ContextCommit,
    ContextCommitOptions,
    CrossSessionMessage,
    EventEnvelope,
    InteractionResponse,
    PersistedEffect,
    RecoveryReport,
    ResourceHandle,
    ResourceRecord,
    ResourceRight,
    ResolvedStorageBinding,
    SessionId,
    SessionRecord,
    SharedStateEntry,
    SharedStateRevision,
    SharedStateWriteOptions,
    StorageBindingRef,
    TaskAttempt,
    TaskId,
    TaskRecord,
    TaskSignal,
    TaskSpec,
    WorkspaceDiff,
    WorkspaceSnapshot,
    BudgetUsage,
} from '../../domain/types';
import { advanceDependants,allHandlesTx,appendEventTx,applySharedMutations,applySpawnsTx,assertBudgetCapacity,assertBudgetVersion,assertClaim,assertContextHead,assertRightsSubset,assertSharedVersion,attemptKey,authorizeHandleTx,budgetAccount,budgetKey,budgetUsageKey,cancelActiveEffects,catalogPath,claimMatches,claimTask,clearSeqRecords,collectContextHistory,contextBranchKey,contextCommitKey,contextPath,createId,decode,deletedRevision,dependencySatisfied,descendantHandleIds,effectAttempt,effectClaimMatches,encode,ensureSeqFile,ensureSessionLayout,ensureTaskLayout,ensureTree,eventsPath,finishAttemptTx,finishEffect,graphPath,handleKey,indexPath,indexTask,isTerminal,join,messagesPath,nextSharedVersion,outboxKey,readBudgetTx,readBudgetUsageTx,readContextBranchTx,readMessages,readSharedTx,readTaskTx,readyCandidates,recoverEffect,registerTaskWaitTx,replaceEffectAttempt,requireContextParents,requireHandleTx,requireResourceTx,requireSessionTx,requireTaskTx,requireTransactionalSeq,resourceBudgetsTx,resourceKey,resourcesPath,seq,sessionPath,sessionRecordPaths,sharedEntry,sharedHistoryPrefix,sharedKey,sharedPath,taskFromSpec,taskPath,terminalDependency,transaction,uniqueRights,unregisterTaskWaitTx,validateSharedKey,wakeFromPendingEvents,wakeTaskWaiters,workspaceDiffKey,workspaceSnapshotKey,writeContextBranchTx,writeSharedHistory,writeSharedRevision,writeTaskTx } from './store-helpers';
import { KernelErrorCode, kernelError } from '../../domain/errors';

const SESSION_KEY = 'record';
const TASK_KEY = 'record';

export interface TaskClaim { task: TaskRecord; attempt: TaskAttempt; }
export interface EffectClaim { taskId: TaskId; effectId: string; effect: PersistedEffect; }
export interface EffectCompletion {
    result?: unknown;
    error?: import('../../domain/types').SerializableError;
    indeterminate?: boolean;
    retryable?: boolean;
    cleanupPending?: boolean;
}
export interface PreparedSpawn { id: TaskId; spawnKey: string; spec: TaskSpec; }
export interface TaskCommitSideEffects {
    resources?: PreparedResourceCommand[];
    messages?: import('../../domain/types').TaskMessageRequest[];
    cache?: Array<Extract<import('../../domain/types').KernelAction, { type: 'cache-read' | 'cache-publish' | 'cache-create' | 'cache-invalidate' | 'cache-renew' }>>;
    shared?: Array<
        { type: 'set'; key: string; value: import('../../domain/types').JsonValue; expectedVersion?: number | null }
        | { type: 'delete'; key: string; expectedVersion?: number | null }
    >;
    events?: Array<{ type: string; payload?: unknown }>;
    spawns?: PreparedSpawn[];
    attemptOutcome?: TaskAttempt['outcome'];
}

export class SeqFileKernelStore {
    constructor(
        private readonly catalog: ResolvedStorageBinding,
        private readonly resolveStorage: (reference: StorageBindingRef) => Promise<ResolvedStorageBinding>,
    ) {}

    async listRuntimeRecoverySessions(): Promise<string[]> {
        const rows: Array<{key: string; value: string}> = [];
        await seq(this.catalog.fs).walkEntries(catalogPath(this.catalog.rootPath), row => {
            rows.push({key: row.key, value: row.value}); return true;
        }, {keyPrefix: RUNTIME_RECOVERY_PREFIX});
        const ids = new Set<string>();
        for (const row of rows) {
            if (!row.value || !row.key.startsWith(RUNTIME_RECOVERY_PREFIX + encodeURIComponent(row.value) + '/'))
                throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Invalid runtime recovery responsibility');
            ids.add(row.value);
        }
        return [...ids];
    }

    async listRecoveryTasks(scope: string): Promise<Array<{sessionId: string; taskId: string; key?: string}>> {
        const rows: Array<{key: string; value: string}> = [], prefix = recoveryPrefix(scope);
        await seq(this.catalog.fs).walkEntries(catalogPath(this.catalog.rootPath), row => {
            rows.push({key: row.key, value: row.value}); return true;
        }, {keyPrefix: prefix});
        return rows.map(entry => {
            const value = decodeRecoveryEntry<{sessionId: string; taskId: string; key?: string}>(entry.value);
            if (typeof value?.sessionId !== 'string' || !value.sessionId || typeof value.taskId !== 'string' || !value.taskId
                || value.key !== undefined && typeof value.key !== 'string' || entry.key !== prefix + value.taskId)
                throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Invalid recovery index entry');
            return value;
        });
    }

    async listRecoveryShared(scope: string): Promise<Array<{sessionId: string; key: string}>> {
        const rows: Array<{key: string; value: string}> = [], prefix = 'shared-' + recoveryPrefix(scope);
        await seq(this.catalog.fs).walkEntries(catalogPath(this.catalog.rootPath), row => {
            rows.push({key: row.key, value: row.value}); return true;
        }, {keyPrefix: prefix});
        return rows.map(entry => {
            const value = decodeRecoveryEntry<{sessionId: string; key: string}>(entry.value);
            if (typeof value?.sessionId !== 'string' || !value.sessionId || typeof value.key !== 'string' || !value.key
                || entry.key !== prefix + encodeURIComponent(value.sessionId) + '/' + encodeURIComponent(value.key))
                throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Invalid shared recovery index entry');
            return value;
        });
    }

    async acknowledgeTaskRecovery(binding: ResolvedStorageBinding, taskId: string, scope: string): Promise<void> {
        await transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, taskId);
            if (task.recovery?.scope !== scope || task.recovery.until !== 'acknowledged' || !taskStat(task).settled)
                throw kernelError(KernelErrorCode.CONFLICT, 'Recovery acknowledgement requires a terminal matching Task');
            if (task.recoveryAcknowledged) return;
            await writeTaskTx(tx, binding.rootPath, {...task, recoveryAcknowledged: true, version: task.version + 1});
        });
    }

    async createCache(binding: ResolvedStorageBinding, taskId: string, spec: import('../../domain/cache').CacheSpec) {
        return transaction(binding.fs, tx => createCacheTx(tx, binding.rootPath, taskId, spec));
    }
    async readCache(binding: ResolvedStorageBinding, taskId: string, request: import('../../domain/cache').CacheRead) {
        return transaction(binding.fs, tx => readCacheTx(tx, binding.rootPath, taskId, request));
    }
    async publishCache(binding: ResolvedStorageBinding, taskId: string, request: import('../../domain/cache').CachePublish) {
        return transaction(binding.fs, tx => publishCacheTx(tx, binding.rootPath, taskId, request));
    }
    async invalidateCache(binding: ResolvedStorageBinding, taskId: string, handleId: string, expectedGeneration: number) {
        return transaction(binding.fs, tx => invalidateCacheTx(tx, binding.rootPath, taskId, handleId, expectedGeneration));
    }
    async renewCache(binding: ResolvedStorageBinding, taskId: string, handleId: string, expectedGeneration: number, ttlMs: number) {
        return transaction(binding.fs, tx => renewCacheTx(tx, binding.rootPath, taskId, handleId, expectedGeneration, ttlMs));
    }
    async listCaches(binding: ResolvedStorageBinding, taskId: string) {
        return transaction(binding.fs, tx => listCachesTx(tx, binding.rootPath, taskId));
    }

    async initialize(): Promise<void> {
        await ensureTree(this.catalog.fs, this.catalog.rootPath);
        await this.catalog.fs.driver.updateMetadata(this.catalog.rootPath, { vfsFixedLayout: true });
        await ensureSeqFile(this.catalog.fs, catalogPath(this.catalog.rootPath));
        requireTransactionalSeq(this.catalog.fs);
    }

    async createSession(id: SessionId, storage: StorageBindingRef): Promise<SessionRecord> {
        const binding = await this.resolveStorage(storage);
        await ensureSessionLayout(binding);
        const now = Date.now();
        const intent: SessionRecord = { id, instanceId: createId('session-instance'), status: 'open', storage, nextEventSeq: 1, version: 0,
            createdAt: now, updatedAt: now, registrationPending: true };
        await transaction(this.catalog.fs, async tx => {
            const key = `session/${id}`;
            const raw = await tx.getEntry(catalogPath(this.catalog.rootPath), key);
            if (raw) {
                if (encode(decode<SessionRecord>(raw).storage) !== encode(storage)) throw new Error('Session storage binding conflict');
            } else await tx.setEntry(catalogPath(this.catalog.rootPath), key, encode(intent));
        });
        const record = await transaction(binding.fs, async tx => {
            const raw = await tx.getEntry(sessionPath(binding.rootPath), SESSION_KEY);
            if (raw) {
                const current = decode<SessionRecord>(raw);
                if (current.id !== id || encode(current.storage) !== encode(storage)) throw new Error('Session storage already belongs to another session');
                return current;
            }
            const created = { ...intent, registrationPending: false, layout: currentSessionLayout() };
            await tx.setEntry(sessionPath(binding.rootPath), SESSION_KEY, encode(created));
            await tx.setEntry(indexPath(binding.rootPath), 'task-order-version', '1');
            await appendEventTx(tx, binding.rootPath, id, undefined, 'session.created', created);
            return created;
        });
        await transaction(this.catalog.fs, async tx => {
            const path = catalogPath(this.catalog.rootPath), key = `session/${id}`, value = encode(record);
            // Rebinding an unchanged Session must not wake every catalog subscriber.
            if (await tx.getEntry(path, key) !== value) await tx.setEntry(path, key, value);
        });
        return record;
    }

    async ensureSessionInstance(binding: ResolvedStorageBinding): Promise<SessionRecord> {
        return transaction(binding.fs, async tx => {
            const record = await requireSessionTx(tx, binding.rootPath);
            if (record.instanceId) return record;
            const next = {...record, instanceId: createId('session-instance'), version: record.version + 1, updatedAt: Date.now()};
            await tx.setEntry(sessionPath(binding.rootPath), SESSION_KEY, encode(next));
            await appendEventTx(tx, binding.rootPath, next.id, undefined, 'session.identity.assigned', next);
            return next;
        });
    }

    /** Resolve existing storage without opening/scheduling the Session or changing metadata. */
    async inspectSessionBinding(id: SessionId): Promise<ResolvedStorageBinding> {
        return (await this.inspectSessionStorage(id)).binding;
    }

    /**
     * Read the catalog record and resolve its storage in one pass, so callers needing both the
     * binding reference and the resolved binding do not read the catalog twice.
     */
    async inspectSessionStorage(id: SessionId): Promise<{ record: SessionRecord; binding: ResolvedStorageBinding }> {
        const record = await this.readCatalog(id);
        if (!record) throw kernelError(KernelErrorCode.SESSION_NOT_FOUND, `Session not found: ${id}`);
        return { record, binding: await this.runtimeBinding(record) };
    }

    private async runtimeBinding(record: SessionRecord): Promise<ResolvedStorageBinding> {
        const binding = await this.resolveStorage(record.storage);
        return {...binding, runtimeRecovery: binding.fs === this.catalog.fs
            ? {catalogPath: catalogPath(this.catalog.rootPath), sessionId: record.id} : undefined};
    }

    async openSession(id: SessionId, options: {
        expectedStorage?: StorageBindingRef; previousBinding?: ResolvedStorageBinding;
    } = {}): Promise<{ record: SessionRecord; binding: ResolvedStorageBinding }> {
        const catalog = await this.readCatalog(id);
        if (!catalog) throw kernelError(KernelErrorCode.SESSION_NOT_FOUND, `Session not found: ${id}`);
        if (options.expectedStorage && encode(catalog.storage) !== encode(options.expectedStorage)) throw new Error('Session storage binding conflict');
        const binding = await this.runtimeBinding(catalog);
        if (catalog.registrationPending) await this.createSession(id, catalog.storage);
        const record = await this.readSession(binding);
        if (record.id !== id || encode(record.storage) !== encode(catalog.storage)) throw new Error('Session storage already belongs to another session');
        // Opening is the entry point: refuse a newer layout, pending migration or
        // capability this host cannot provide before any caller touches the records.
        assertSessionLayout(record);
        if (options.previousBinding?.fs !== binding.fs || options.previousBinding.rootPath !== binding.rootPath) {
            await binding.fs.driver.updateMetadata(binding.rootPath, { vfsFixedLayout: true });
        }
        return { record, binding };
    }

    async listSessions(): Promise<SessionRecord[]> {
        const entries: SessionRecord[] = [];
        await seq(this.catalog.fs).walkEntries(catalogPath(this.catalog.rootPath), entry => {
            if (entry.key.startsWith('session/')) entries.push(decode(entry.value));
            return true;
        }, { keyPrefix: 'session/' });
        return entries.sort((a, b) => b.updatedAt - a.updatedAt);
    }

    /**
     * Remove Kernel-owned Session storage and every catalog reference to it.
     *
     * Order matters for retry safety: release the fixed-layout pin, delete the
     * storage tree, clear the SeqFile records (they live outside the files), and
     * only then drop the catalog entry. An interrupted removal is therefore
     * repeatable — the catalog entry still resolves the binding until the end.
     */
    async removeSession(id: SessionId, binding: ResolvedStorageBinding): Promise<void> {
        const taskIds = await this.listTaskIds(binding);
        if (await binding.fs.driver.exists(binding.rootPath)) {
            await binding.fs.driver.updateMetadata(binding.rootPath, { vfsFixedLayout: false });
            await binding.fs.driver.delete([binding.rootPath], { recursive: true });
        }
        await clearSeqRecords(binding.fs, await sessionRecordPaths(binding, taskIds));
        await transaction(this.catalog.fs, async tx => {
            await tx.deleteEntry(catalogPath(this.catalog.rootPath), `session/${id}`);
            const stale: string[] = [];
            await tx.walkEntries(catalogPath(this.catalog.rootPath), entry => {
                if (entry.value === id) stale.push(entry.key);
                return true;
            }, { keyPrefix: 'task/' });
            for (const prefix of ['recovery/', 'shared-recovery/']) await tx.walkEntries(catalogPath(this.catalog.rootPath), entry => {
                if (decode<{sessionId: string}>(entry.value).sessionId === id) stale.push(entry.key);
                return true;
            }, {keyPrefix: prefix});
            await tx.walkEntries(catalogPath(this.catalog.rootPath), row => {
                stale.push(row.key); return true;
            }, {keyPrefix: RUNTIME_RECOVERY_PREFIX + encodeURIComponent(id) + '/'});
            for (const key of stale) await tx.deleteEntry(catalogPath(this.catalog.rootPath), key);
        });
    }

    async setSessionStatus(
        binding: ResolvedStorageBinding,
        status: SessionRecord['status'],
        closeMode?: 'drain' | 'cancel',
    ): Promise<SessionRecord> {
        const record = await transaction(binding.fs, async tx => {
            const value = await tx.getEntry(sessionPath(binding.rootPath), SESSION_KEY);
            if (!value) throw new Error(`Session record missing at ${binding.rootPath}`);
            const current = decode<SessionRecord>(value);
            // A repeated close request cannot reopen a terminal Session. Keep the check
            // in this transaction so a concurrent closer cannot race it.
            if (status === 'closing' && closeMode && ['closed', 'archived'].includes(current.status)) return current;
            const allowed: Record<SessionRecord['status'], SessionRecord['status'][]> = {
                open: ['open', 'suspended', 'suspending', 'closing'], suspending: ['suspending', 'suspended', 'closing'], suspended: ['suspended', 'open', 'closing'],
                closing: ['closing', 'closed'], closed: ['closed', 'archived'], archived: ['archived'],
            };
            if (!allowed[current.status].includes(status)) throw new Error(`Invalid session transition: ${current.status} -> ${status}`);
            if (status === 'closed') {
                let active = false;
                await tx.walkEntries(indexPath(binding.rootPath), row => { if (!isTerminal(decode<{ status: TaskRecord['status'] }>(row.value).status)) active = true; return true; }, { keyPrefix: 'task/' });
                if (active) throw new Error('Session still has unfinished tasks');
            }
            let nextStatus = status;
            if (status === 'open' || status === 'suspended' || (status === 'closing' && closeMode === 'drain')) {
                const ids: string[] = [];
                await tx.walkEntries(indexPath(binding.rootPath), row => { ids.push(row.key.slice(5)); return true; }, { keyPrefix: 'task/' });
                for (const id of ids) {
                    const task = await requireTaskTx(tx, binding.rootPath, id);
                    if (status === 'suspended' && Object.values(task.effects).some(e => e.status === 'leased' || e.status === 'indeterminate' || e.cleanupPending)) nextStatus = 'suspending';
                    if (isTerminal(task.status)) continue;
                    const held = status === 'suspended';
                    if (held && task.currentAttempt) await finishAttemptTx(tx, binding.rootPath, task, 'ready');
                    const nextTask: TaskRecord = { ...task, sessionPaused: held,
                        status: held && task.status === 'running' ? 'ready' : task.status,
                        currentAttempt: held ? undefined : task.currentAttempt,
                        stepAttemptCount: held && task.status === 'running' ? Math.max(0, (task.stepAttemptCount ?? 1) - 1) : task.stepAttemptCount,
                        version: task.version + 1, updatedAt: Date.now() };
                    await writeTaskTx(tx, binding.rootPath, nextTask); await indexTask(tx, binding.rootPath, nextTask);
                }
            }
            const next = { ...current, status: nextStatus, closeMode: closeMode ?? current.closeMode, version: current.version + 1, updatedAt: Date.now() };
            await tx.setEntry(sessionPath(binding.rootPath), SESSION_KEY, encode(next));
            await appendEventTx(tx, binding.rootPath, current.id, undefined, `session.${nextStatus}`, next);
            // `session` scope retention ends with the Session, including its physical reclaim.
            if (nextStatus === 'closed') await cleanupSessionCachesTx(tx, binding.rootPath, current.id);
            return next;
        });
        await transaction(this.catalog.fs, tx => tx.setEntry(
            catalogPath(this.catalog.rootPath), `session/${record.id}`, encode(record),
        ));
        return record;
    }

    /** Explicit new execution admission; terminal Tasks and reclaimed caches stay unchanged. */
    async reopenSession(binding: ResolvedStorageBinding): Promise<SessionRecord> {
        const record = await transaction(binding.fs, async tx => {
            const current = await requireSessionTx(tx, binding.rootPath);
            if (current.status === 'open') return current;
            if (current.status !== 'closed') throw new Error(`Cannot reopen session: ${current.status}`);
            const ids: string[] = [];
            await tx.walkEntries(indexPath(binding.rootPath), row => { ids.push(row.key.slice(5)); return true; }, { keyPrefix: 'task/' });
            for (const id of ids) {
                const task = await requireTaskTx(tx, binding.rootPath, id);
                if (!isTerminal(task.status) || Object.values(task.effects).some(effect => effect.cleanupPending)) {
                    throw new Error('Session still has unfinished tasks or pending cleanup');
                }
            }
            const next: SessionRecord = { ...current, status: 'open', version: current.version + 1, updatedAt: Date.now() };
            delete next.closeMode;
            await tx.setEntry(sessionPath(binding.rootPath), SESSION_KEY, encode(next));
            await appendEventTx(tx, binding.rootPath, current.id, undefined, 'session.reopened', next);
            return next;
        });
        await transaction(this.catalog.fs, tx => tx.setEntry(catalogPath(this.catalog.rootPath), `session/${record.id}`, encode(record)));
        return record;
    }

    async sessionRecord(binding: ResolvedStorageBinding): Promise<SessionRecord> {
        return this.readSession(binding);
    }

    async getShared<T extends import('../../domain/types').JsonValue>(
        binding: ResolvedStorageBinding,
        key: string,
    ): Promise<SharedStateEntry<T> | undefined> {
        const value = await seq(binding.fs).getEntry(sharedPath(binding.rootPath), sharedKey(key));
        return value ? decode<SharedStateEntry<T>>(value) : undefined;
    }

    /** Read several shared keys from one snapshot; the editor bind needs more than one. */
    async getSharedMany(
        binding: ResolvedStorageBinding,
        keys: string[],
    ): Promise<Record<string, SharedStateEntry<import('../../domain/types').JsonValue> | undefined>> {
        for (const key of keys) validateSharedKey(key);
        const unique = [...new Set(keys)];
        const result: Record<string, SharedStateEntry<import('../../domain/types').JsonValue> | undefined> = {};
        for (const key of unique) result[key] = undefined;
        if (!unique.length) return result;
        const fields = unique.map(sharedKey);
        const rows = await transaction(binding.fs, tx => tx.getEntries(sharedPath(binding.rootPath), fields));
        unique.forEach((key, index) => {
            const value = rows[fields[index]];
            if (value !== undefined) result[key] = decode<SharedStateEntry<import('../../domain/types').JsonValue>>(value);
        });
        return result;
    }

    async setShared<T extends import('../../domain/types').JsonValue>(binding: ResolvedStorageBinding, key: string, value: T,
        options?: SharedStateWriteOptions, effectClaim?: EffectClaim): Promise<SharedStateEntry<T>>;
    async setShared<T extends import('../../domain/types').JsonValue>(binding: ResolvedStorageBinding, key: string, value: T,
        options: SharedStateWriteOptions, effectClaim: EffectClaim, requireRunning: true): Promise<SharedStateEntry<T> | undefined>;
    async setShared<T extends import('../../domain/types').JsonValue>(
        binding: ResolvedStorageBinding,
        key: string,
        value: T,
        options: SharedStateWriteOptions = {},
        effectClaim?: EffectClaim,
        requireRunning = false,
    ): Promise<SharedStateEntry<T> | undefined> {
        validateSharedKey(key);
        assertDurableValue(value, 'Shared state');
        return transaction(binding.fs, async tx => {
            if (effectClaim) await this.assertEffectClaimTx(tx, binding, effectClaim);
            await assertSharedLeaseTx(tx, binding.rootPath, options.lease);
            if (requireRunning && effectClaim && !await this.effectCanContinue(tx, binding, effectClaim)) return;
            const current = await readSharedTx<T>(tx, binding.rootPath, key);
            assertSharedVersion(key, current?.version, options.expectedVersion);
            const version = await nextSharedVersion(tx, binding.rootPath, key);
            const entry = sharedEntry(key, value, version, options.taskId);
            const recovery = options.recovery ?? (current?.recoveryScope ? {scope: current.recoveryScope, pending: true} : undefined);
            if (recovery) {
                if (binding.fs !== this.catalog.fs || typeof recovery.pending !== 'boolean')
                    throw kernelError(KernelErrorCode.INVALID_SPEC, 'Recovery requires the catalog transaction domain');
                if (current?.recoveryScope && current.recoveryScope !== recovery.scope)
                    throw kernelError(KernelErrorCode.CONFLICT, 'Shared recovery scope cannot change');
                if (recovery.pending) entry.recoveryScope = recovery.scope;
                const session = await requireSessionTx(tx, binding.rootPath);
                await indexSharedRecovery(tx, catalogPath(this.catalog.rootPath), session.id, key, recovery.scope, recovery.pending);
            }
            await writeSharedRevision(tx, binding.rootPath, entry, options.history);
            await appendEventTx(tx, binding.rootPath, (await requireSessionTx(tx, binding.rootPath)).id,
                options.taskId, 'session.shared.set', { key, version: entry.version });
            return entry;
        });
    }

    private async effectCanContinue(tx: ISeqFileTransaction, binding: ResolvedStorageBinding, claim: EffectClaim): Promise<boolean> {
        const session = await requireSessionTx(tx, binding.rootPath);
        if (session.status === 'suspended' || session.status === 'suspending') return false;
        const task = await requireTaskTx(tx, binding.rootPath, claim.taskId);
        return !task.sessionPaused && !task.controlHolds?.length && (!task.control || task.control.mode === 'run');
    }

    async deleteShared(
        binding: ResolvedStorageBinding,
        key: string,
        options: SharedStateWriteOptions = {},
    ): Promise<boolean> {
        validateSharedKey(key);
        return transaction(binding.fs, async tx => {
            await assertSharedLeaseTx(tx, binding.rootPath, options.lease);
            const current = await readSharedTx(tx, binding.rootPath, key);
            assertSharedVersion(key, current?.version, options.expectedVersion);
            if (!current) return false;
            if (current.recoveryScope) await indexSharedRecovery(tx, catalogPath(this.catalog.rootPath),
                (await requireSessionTx(tx, binding.rootPath)).id, key, current.recoveryScope, false);
            const revision = deletedRevision(key, current.version + 1, options.taskId);
            await tx.deleteEntry(sharedPath(binding.rootPath), sharedKey(key));
            await writeSharedHistory(tx, binding.rootPath, revision, options.history);
            await appendEventTx(tx, binding.rootPath, (await requireSessionTx(tx, binding.rootPath)).id,
                options.taskId, 'session.shared.deleted', { key, version: current.version });
            return true;
        });
    }

    async listShared(binding: ResolvedStorageBinding, prefix = ''): Promise<SharedStateEntry[]> {
        // Listing is still a Session read: refuse a layout this host cannot interpret
        // instead of presenting shared state it does not understand.
        return transaction(binding.fs, async tx => {
            await requireSessionTx(tx, binding.rootPath);
            const entries: SharedStateEntry[] = [];
            await tx.walkEntries(sharedPath(binding.rootPath), entry => {
                entries.push(decode(entry.value));
                return true;
            }, { keyPrefix: sharedKey(prefix) });
            return entries.sort((a, b) => a.key.localeCompare(b.key));
        });
    }

    async sharedHistory<T extends import('../../domain/types').JsonValue>(
        binding: ResolvedStorageBinding,
        key: string,
    ): Promise<SharedStateRevision<T>[]> {
        validateSharedKey(key);
        const revisions: SharedStateRevision<T>[] = [];
        await seq(binding.fs).walkEntries(sharedPath(binding.rootPath), entry => {
            revisions.push(decode(entry.value));
            return true;
        }, { keyPrefix: sharedHistoryPrefix(key) });
        return revisions.sort((a, b) => a.version - b.version);
    }

    async createOutboxMessage<T extends import('../../domain/types').JsonValue>(
        binding: ResolvedStorageBinding,
        message: CrossSessionMessage<T>,
    ): Promise<void> {
        await transaction(binding.fs, async tx => {
            await indexRuntimeMessage(tx, binding.fs === this.catalog.fs ? catalogPath(this.catalog.rootPath) : undefined, message);
            await tx.setEntry(messagesPath(binding.rootPath), outboxKey(message.id), encode(message));
            await appendEventTx(tx, binding.rootPath, message.sourceSessionId, undefined,
                'session.message.queued', message);
        });
    }

    /** Retention/GC for settled messages; see `pruneMessagesTx` for what is kept. */
    async pruneMessages(
        binding: ResolvedStorageBinding,
        before: number,
        limit?: number,
    ): Promise<{ outbox: number; inbox: number }> {
        return transaction(binding.fs, tx => pruneMessagesTx(tx, binding.rootPath, before, limit));
    }

    async pendingOutbox(binding: ResolvedStorageBinding, dueAt?: number): Promise<CrossSessionMessage[]> {
        const messages = await this.outbox(binding);
        return messages.filter(message => (message.status === 'pending' || (message.sourceSessionId !== message.targetSessionId && message.settlementAcknowledgedAt === undefined)) && (dueAt === undefined || (message.nextAttemptAt ?? 0) <= dueAt || (message.expiresAt ?? Infinity) <= dueAt));
    }

    async outbox(binding: ResolvedStorageBinding): Promise<CrossSessionMessage[]> {
        return readMessages(binding, 'outbox/');
    }

    async inbox(binding: ResolvedStorageBinding, after = 0): Promise<CrossSessionMessage[]> {
        const messages: CrossSessionMessage[] = [];
        let query: import('../../domain/types').InboxQuery = { afterSequence: after, limit: 500 };
        do {
            const page = await this.inboxPage(binding, query);
            messages.push(...page.items);
            if (page.nextAfterSequence === undefined) break;
            query = { ...query, afterSequence: page.nextAfterSequence, throughSequence: page.throughSequence };
        } while (true);
        return messages;
    }

    async sendSessionMessage(binding: ResolvedStorageBinding, sessionId: string, request: import('../../domain/types').SessionMessageRequest) {
        return transaction(binding.fs, tx => enqueueSessionMessageTx(tx, binding.rootPath, sessionId, request, binding.fs === this.catalog.fs ? catalogPath(this.catalog.rootPath) : undefined));
    }

    async inboxPage(binding: ResolvedStorageBinding, query?: import('../../domain/types').InboxQuery) {
        return transaction(binding.fs, tx => inboxPageTx(tx, binding.rootPath, query));
    }

    async acknowledgeSessionMessage(binding: ResolvedStorageBinding, id: string, options?: LeaseGuardOptions) {
        return transaction(binding.fs, tx => acknowledgeSessionMessageTx(tx, binding.rootPath, id, options));
    }

    async deliverMessage(binding: ResolvedStorageBinding, message: CrossSessionMessage): Promise<boolean> {
        return transaction(binding.fs, tx => deliverMessageTx(tx, binding.rootPath, message));
    }

    async sendTaskMessage(binding: ResolvedStorageBinding, taskId: string, request: import('../../domain/types').TaskMessageRequest) {
        return transaction(binding.fs, async tx => {
            const sender = await requireTaskTx(tx, binding.rootPath, taskId);
            if (isTerminal(sender.status) && !await tx.getEntry(messagesPath(binding.rootPath), outboxKey(`${sender.id}:${encodeURIComponent(request.idempotencyKey)}`))) {
                throw kernelError(KernelErrorCode.CONFLICT, 'Task is terminal');
            }
            return enqueueMessageTx(tx, binding.rootPath, sender, request);
        });
    }

    async messageReceipt(binding: ResolvedStorageBinding, messageId: string): Promise<CrossSessionMessage> {
        const value = await seq(binding.fs).getEntry(messagesPath(binding.rootPath), `inbox/${messageId}`);
        if (!value) throw new Error('Message delivery receipt missing');
        return decode(value);
    }

    async markMessageDelivered(binding: ResolvedStorageBinding, messageId: string, receipt?: CrossSessionMessage): Promise<CrossSessionMessage> {
        return transaction(binding.fs, async tx => {
            const key = outboxKey(messageId);
            const value = await tx.getEntry(messagesPath(binding.rootPath), key);
            if (!value) throw new Error(`Outbox message not found: ${messageId}`);
            const current = decode<CrossSessionMessage>(value);
            if (current.status !== 'pending') return current;
            const next: CrossSessionMessage = { ...current, status: receipt?.status ?? 'delivered',
                deliveredAt: receipt?.status === 'rejected' ? undefined : receipt?.deliveredAt ?? Date.now(),
                rejectedAt: receipt?.rejectedAt, rejection: receipt?.rejection,
                settlementAcknowledgedAt: receipt?.settlementAcknowledgedAt, nextAttemptAt: undefined };
            await indexRuntimeMessage(tx, binding.fs === this.catalog.fs ? catalogPath(this.catalog.rootPath) : undefined, next);
            await tx.setEntry(messagesPath(binding.rootPath), key, encode(next));
            await appendEventTx(tx, binding.rootPath, current.sourceSessionId, current.sourceTaskId,
                `session.message.${next.status}`, next);
            return next;
        });
    }

    async acknowledgeMessageSettlement(binding: ResolvedStorageBinding, message: CrossSessionMessage,
        side: 'inbox' | 'outbox'): Promise<CrossSessionMessage | undefined> {
        return transaction(binding.fs, tx => acknowledgeMessageSettlementTx(tx, binding.rootPath, message, side, binding.fs === this.catalog.fs ? catalogPath(this.catalog.rootPath) : undefined));
    }

    async recordMessageRetry(binding: ResolvedStorageBinding, messageId: string, error: unknown): Promise<void> {
        await transaction(binding.fs, async tx => {
            const key = outboxKey(messageId), value = await tx.getEntry(messagesPath(binding.rootPath), key);
            if (!value) return;
            const message = decode<CrossSessionMessage>(value);
            if (message.settlementAcknowledgedAt !== undefined) return;
            const deliveryAttempts = (message.deliveryAttempts ?? 0) + 1;
            await tx.setEntry(messagesPath(binding.rootPath), key, encode({ ...message, deliveryAttempts,
                nextAttemptAt: Date.now() + Math.min(30_000, 250 * 2 ** Math.min(deliveryAttempts - 1, 7)),
                lastDeliveryError: error instanceof Error ? error.message : String(error) }));
        });
    }

    async commitContext<T extends import('../../domain/types').JsonValue>(
        binding: ResolvedStorageBinding,
        commit: ContextCommit<T>,
        options: ContextCommitOptions = {},
    ): Promise<ContextCommit<T>> {
        return transaction(binding.fs, async tx => {
            const branch = options.branch ?? 'main';
            const current = await readContextBranchTx(tx, binding.rootPath, branch);
            assertContextHead(branch, current.head, options.expectedHead);
            const parentIds = options.parents ?? (current.head ? [current.head] : []);
            await requireContextParents(tx, binding.rootPath, parentIds);
            const persisted = { ...commit, parentIds };
            await tx.setEntry(contextPath(binding.rootPath), contextCommitKey(commit.id), encode(persisted));
            await writeContextBranchTx(tx, binding.rootPath, branch, commit.id, current.version);
            await appendEventTx(tx, binding.rootPath, commit.sessionId, options.taskId,
                'session.context.committed', { commitId: commit.id, branch, parentIds });
            return persisted;
        });
    }

    async getContextCommit<T extends import('../../domain/types').JsonValue>(
        binding: ResolvedStorageBinding,
        id: string,
    ): Promise<ContextCommit<T> | undefined> {
        const value = await seq(binding.fs).getEntry(contextPath(binding.rootPath), contextCommitKey(id));
        return value ? decode(value) : undefined;
    }

    async getContextBranch(binding: ResolvedStorageBinding, name = 'main'): Promise<ContextBranch> {
        const value = await seq(binding.fs).getEntry(contextPath(binding.rootPath), contextBranchKey(name));
        return value ? decode(value) : { name, version: 0, updatedAt: 0 };
    }

    async contextHistory(binding: ResolvedStorageBinding, head?: string): Promise<ContextCommit[]> {
        const start = head ?? (await this.getContextBranch(binding)).head;
        if (!start) return [];
        return collectContextHistory(binding, start);
    }

    async createResource(
        binding: ResolvedStorageBinding,
        resource: ResourceRecord,
        handle: ResourceHandle,
        parentHandleId?: string,
        request?: { id: string; fingerprint: string },
        options?: LeaseGuardOptions,
    ): Promise<{ resource: ResourceRecord; handle: ResourceHandle }> {
        return transaction(binding.fs, async tx => {
            await assertSharedLeaseTx(tx, binding.rootPath, options?.lease);
            await requireTaskTx(tx, binding.rootPath, handle.holderTaskId);
            const key = request ? `create/${encodeURIComponent(handle.holderTaskId)}/${encodeURIComponent(request.id)}` : undefined;
            const saved = key ? await tx.getEntry(resourcesPath(binding.rootPath), key) : undefined;
            if (saved) {
                const previous = decode<{ fingerprint: string; resourceId: string; handleId: string }>(saved);
                if (previous.fingerprint !== request!.fingerprint) throw new Error('Resource creation conflict');
                return { resource: await requireResourceTx(tx, binding.rootPath, previous.resourceId),
                    handle: await requireHandleTx(tx, binding.rootPath, previous.handleId) };
            }
            if (resource.parentResourceId) {
                if (!parentHandleId) throw new Error('Child resource requires parent handle');
                const parent = await requireHandleTx(tx, binding.rootPath, parentHandleId);
                const parentResource = await authorizeHandleTx(tx, binding.rootPath, parent, 'grant');
                if (parentResource.id !== resource.parentResourceId) throw new Error('Parent resource handle mismatch');
            }
            await tx.setEntry(resourcesPath(binding.rootPath), resourceKey(resource.id), encode(resource));
            await tx.setEntry(resourcesPath(binding.rootPath), handleKey(handle.id), encode(handle));
            await appendEventTx(tx, binding.rootPath, resource.sessionId, handle.holderTaskId,
                'resource.created', { resourceId: resource.id, handleId: handle.id, kind: resource.kind });
            if (key) await tx.setEntry(resourcesPath(binding.rootPath), key, encode({
                fingerprint: request!.fingerprint, resourceId: resource.id, handleId: handle.id,
            }));
            return { resource, handle };
        });
    }

    async grantResource(
        binding: ResolvedStorageBinding,
        id: string,
        parentHandleId: string,
        holderTaskId: string,
        rights: ResourceRight[],
    ): Promise<ResourceHandle> {
        return transaction(binding.fs, async tx => {
            const parent = await requireHandleTx(tx, binding.rootPath, parentHandleId);
            const resource = await authorizeHandleTx(tx, binding.rootPath, parent, 'grant');
            await requireTaskTx(tx, binding.rootPath, holderTaskId);
            assertRightsSubset(parent, rights);
            const handle: ResourceHandle = {
                id, resourceId: resource.id, holderTaskId, rights: uniqueRights(rights),
                generation: resource.generation, parentHandleId,
            };
            await tx.setEntry(resourcesPath(binding.rootPath), handleKey(id), encode(handle));
            await appendEventTx(tx, binding.rootPath, resource.sessionId, holderTaskId,
                'resource.granted', { handleId: id, parentHandleId, rights });
            return handle;
        });
    }

    async revokeResource(binding: ResolvedStorageBinding, handleId: string): Promise<number> {
        return transaction(binding.fs, async tx => {
            const rootHandle = await requireHandleTx(tx, binding.rootPath, handleId);
            await authorizeHandleTx(tx, binding.rootPath, rootHandle, 'admin');
            const handles = await allHandlesTx(tx, binding.rootPath);
            const revoked = descendantHandleIds(handles, handleId);
            const revokedAt = Date.now();
            for (const id of revoked) {
                const handle = handles.get(id)!;
                await tx.setEntry(resourcesPath(binding.rootPath), handleKey(id), encode({ ...handle, revokedAt }));
            }
            const resource = await requireResourceTx(tx, binding.rootPath, rootHandle.resourceId);
            await appendEventTx(tx, binding.rootPath, resource.sessionId, rootHandle.holderTaskId,
                'resource.revoked', { handleId, revokedHandleIds: [...revoked], revokedAt });
            return revoked.size;
        });
    }

    async authorizeResource(
        binding: ResolvedStorageBinding,
        handleId: string,
        right: ResourceRight,
        holderTaskId?: string,
    ): Promise<ResourceRecord> {
        return transaction(binding.fs, async tx => {
            const handle = await requireHandleTx(tx, binding.rootPath, handleId);
            if (holderTaskId && handle.holderTaskId !== holderTaskId) throw new Error(`Handle holder mismatch: ${handleId}`);
            return authorizeHandleTx(tx, binding.rootPath, handle, right);
        });
    }

    async setBudget(
        binding: ResolvedStorageBinding,
        handleId: string,
        dimension: string,
        hardLimit: number,
        expectedVersion?: number | null,
        options?: LeaseGuardOptions,
    ): Promise<BudgetAccount> {
        return transaction(binding.fs, async tx => {
            await assertSharedLeaseTx(tx, binding.rootPath, options?.lease);
            const handle = await requireHandleTx(tx, binding.rootPath, handleId);
            const resource = await authorizeHandleTx(tx, binding.rootPath, handle, 'admin');
            const current = await readBudgetTx(tx, binding.rootPath, resource.id, dimension);
            assertBudgetVersion(resource.id, dimension, current?.version, expectedVersion);
            const budget = budgetAccount(resource.id, dimension, hardLimit, current);
            await tx.setEntry(resourcesPath(binding.rootPath), budgetKey(resource.id, dimension), encode(budget));
            await appendEventTx(tx, binding.rootPath, resource.sessionId, handle.holderTaskId,
                'budget.configured', budget);
            return budget;
        });
    }

    async chargeBudget(
        binding: ResolvedStorageBinding,
        handleId: string,
        dimension: string,
        amount: number,
        effectClaim?: EffectClaim,
        options: { usageId?: string } = {},
    ): Promise<BudgetAccount[]> {
        if (!Number.isFinite(amount) || amount <= 0) throw new Error('Budget charge must be positive');
        const usageId = options.usageId?.trim();
        if (options.usageId !== undefined && !usageId) throw new Error('Budget usage id must be a non-empty string');
        return transaction(binding.fs, async tx => {
            if (effectClaim) await this.assertEffectClaimTx(tx, binding, effectClaim);
            const handle = await requireHandleTx(tx, binding.rootPath, handleId);
            const resource = await authorizeHandleTx(tx, binding.rootPath, handle, 'write');
            // The settlement receipt and the charge share one transaction: a replay after a
            // crash sees the receipt (no second charge), and a lost receipt never happened.
            if (usageId) {
                const settled = await readBudgetUsageTx(tx, binding.rootPath, usageId);
                if (settled) {
                    if (settled.resourceId !== resource.id || settled.dimension !== dimension || settled.amount !== amount) {
                        throw new Error(`Budget usage ${usageId} was already settled for another charge`);
                    }
                    return settled.accounts;
                }
            }
            const budgets = await resourceBudgetsTx(tx, binding.rootPath, resource, dimension);
            for (const budget of budgets) assertBudgetCapacity(budget, amount);
            const charged = budgets.map(budget => ({
                ...budget, used: budget.used + amount, version: budget.version + 1, updatedAt: Date.now(),
            }));
            for (const budget of charged) {
                await tx.setEntry(resourcesPath(binding.rootPath), budgetKey(budget.resourceId, dimension), encode(budget));
            }
            if (usageId) {
                const usage: BudgetUsage = { usageId, resourceId: resource.id, dimension, amount, accounts: charged, settledAt: Date.now() };
                await tx.setEntry(resourcesPath(binding.rootPath), budgetUsageKey(usageId), encode(usage));
            }
            await appendEventTx(tx, binding.rootPath, resource.sessionId, handle.holderTaskId,
                'budget.consumed', { handleId, dimension, amount, accounts: charged, ...(usageId ? { usageId } : {}) });
            return charged;
        });
    }

    async saveWorkspaceSnapshot(
        binding: ResolvedStorageBinding,
        snapshot: WorkspaceSnapshot,
    ): Promise<WorkspaceSnapshot> {
        return transaction(binding.fs, async tx => {
            await requireResourceTx(tx, binding.rootPath, snapshot.resourceId);
            await tx.setEntry(resourcesPath(binding.rootPath), workspaceSnapshotKey(snapshot.id), encode(snapshot));
            await appendEventTx(tx, binding.rootPath, snapshot.sessionId, undefined,
                'workspace.snapshot.created', { snapshotId: snapshot.id, resourceId: snapshot.resourceId });
            return snapshot;
        });
    }

    async getWorkspaceSnapshot(
        binding: ResolvedStorageBinding,
        id: string,
    ): Promise<WorkspaceSnapshot> {
        const value = await seq(binding.fs).getEntry(resourcesPath(binding.rootPath), workspaceSnapshotKey(id));
        if (!value) throw new Error(`Workspace snapshot not found: ${id}`);
        return decode(value);
    }

    async saveWorkspaceDiff(binding: ResolvedStorageBinding, diff: WorkspaceDiff): Promise<WorkspaceDiff> {
        return transaction(binding.fs, async tx => {
            await tx.setEntry(resourcesPath(binding.rootPath), workspaceDiffKey(diff.id), encode(diff));
            await appendEventTx(tx, binding.rootPath, diff.sessionId, undefined,
                'workspace.diff.created', { diffId: diff.id, resourceId: diff.resourceId });
            return diff;
        });
    }

    async listTasks(binding: ResolvedStorageBinding): Promise<TaskRecord[]> {
        // Decode from the entries listTaskIds already read: reading each Task a
        // second time (readTask) doubles Task record reads during each full scan, and
        // the Kernel polls Sessions with hundreds of Tasks.
        return (await this.taskEntries(binding)).map(entry => decode(entry.value));
    }

    /** Existing status indexes bound recovery reads to non-terminal Task records. */
    async listPendingInteractionTasks(binding: ResolvedStorageBinding): Promise<TaskRecord[]> {
        return transaction(binding.fs, async tx => {
            const session = await requireSessionTx(tx, binding.rootPath);
            const candidates: string[] = [];
            await tx.walkEntries(indexPath(binding.rootPath), row => {
                if (!isTerminal(decode<{ status: TaskRecord['status'] }>(row.value).status)) {
                    candidates.push(row.key.slice('task/'.length));
                }
                return true;
            }, { keyPrefix: 'task/' });
            const result: TaskRecord[] = [];
            for (const id of candidates) {
                const task = await requireTaskTx(tx, binding.rootPath, id);
                if (task.sessionId !== session.id) throw new Error('Task interaction index scope mismatch');
                if (!isTerminal(task.status) && Object.values(task.interactions ?? {}).some(item => item.status === 'pending')) result.push(task);
            }
            return result;
        });
    }

    async listTaskPage(binding: ResolvedStorageBinding, query: import('../../domain/types').TaskListQuery = {}): Promise<import('../../domain/types').TaskListPage> {
        const after = query.afterIndex ?? 0, limit = query.limit ?? 100;
        if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 500) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task list page');
        return transaction(binding.fs, async tx => {
            const session = await requireSessionTx(tx, binding.rootPath), path = indexPath(binding.rootPath);
            const version = await tx.getEntry(path, 'task-order-version');
            if (version !== '1') {
                if (version !== null) throw new Error('Unsupported Task list index version');
                const ids: string[] = [];
                await tx.walkEntries(path, row => { ids.push(row.key.slice('task/'.length)); return true; }, { keyPrefix: 'task/' });
                for (const id of ids.sort()) await indexTask(tx, binding.rootPath, await requireTaskTx(tx, binding.rootPath, id));
                await tx.setEntry(path, 'task-order-version', '1');
            }
            const count = Number(await tx.getEntry(path, 'task-count') ?? 0), through = query.throughIndex ?? count;
            if (!Number.isSafeInteger(count) || count < 0 || !Number.isSafeInteger(through) || through < 0 || through > count) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task list upper bound');
            const end = Math.min(through, after + limit);
            const items = await readTaskPageTx(tx, binding.rootPath, session.id, after, end);
            return { items, throughIndex: through, ...(end < through ? { nextAfterIndex: end } : {}) };
        });
    }

    async prepareTaskDirectory(binding: ResolvedStorageBinding, taskId: TaskId): Promise<void> {
        await ensureTaskLayout(binding, taskId);
    }

    async readTaskSubmissionReceipt(binding: ResolvedStorageBinding, sessionId: SessionId, spec: TaskSpec): Promise<import('../../domain/types').TaskSubmissionReceipt | undefined> {
        return transaction(binding.fs, tx => readTaskSubmissionReceiptTx(tx, binding.rootPath, sessionId, spec));
    }

    async createTask(binding: ResolvedStorageBinding, sessionId: SessionId, spec: TaskSpec, options?: import('../../domain/types').LeaseGuardOptions,
        owned?: {taskId: string; instanceId: string; owner: import('../../domain/task-call').TaskCallOwner}): Promise<TaskRecord> {
        validateTaskRecovery(spec.recovery);
        if (spec.recovery && binding.fs !== this.catalog.fs) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Recovery requires the catalog transaction domain');
        if (spec.input !== undefined) assertDurableValue(spec.input, 'Task input');
        const id = owned?.taskId ?? createId('task');
        await ensureTaskLayout(binding, id);
        const now = Date.now();
        let task = taskFromSpec(id, sessionId, spec, now);
        if (binding.fs === this.catalog.fs) task.recoveryIndexPath = catalogPath(this.catalog.rootPath);
        if (owned) task.ownedBy = owned.owner;
        await transaction(binding.fs, async tx => {
            await assertSharedLeaseTx(tx, binding.rootPath, options?.lease);
            const session = await requireSessionTx(tx, binding.rootPath);
            if (owned && session.instanceId !== owned.instanceId) throw kernelError(KernelErrorCode.SESSION_INSTANCE_CHANGED, 'Owned Task Session incarnation changed');
            const fingerprint = owned ? canonical({spec, owner: owned.owner}) : encode(spec);
            if (spec.requestId) {
                const key = `submission/${encodeURIComponent(spec.requestId)}`;
                const raw = await tx.getEntry(sessionPath(binding.rootPath), key);
                if (raw) {
                    const previous = decode<{ id: string; fingerprint: string }>(raw);
                    if (previous.fingerprint !== fingerprint) throw kernelError(KernelErrorCode.CONFLICT, 'Task submission conflict');
                    task = await requireTaskTx(tx, binding.rootPath, previous.id);
                    return;
                }
                await tx.setEntry(sessionPath(binding.rootPath), key, encode({ id, fingerprint }));
            }
            if (session.status !== 'open' && session.status !== 'suspended' && session.status !== 'suspending') throw new Error(`Session is ${session.status}`);
            await validateRetrySourceTx(tx, binding.rootPath, spec.retryOfTaskId);
            if (spec.parent) {
                const parent = await requireTaskTx(tx, binding.rootPath, spec.parent);
                if (isTerminal(parent.status) || parent.controlHolds?.length || (parent.control && parent.control.mode !== 'run')) {
                    throw kernelError(KernelErrorCode.CONFLICT, 'Parent is not accepting children');
                }
                if (await hasCancelledAncestorTx(tx, binding.rootPath, parent)) throw kernelError(KernelErrorCode.CONFLICT, 'Task ancestor cancelled');
                task.rootTaskId = parent.rootTaskId;
            }
            let unresolvedDeps = task.unresolvedDeps;
            let failedDependency: { id: string; status: 'failed' | 'cancelled' } | undefined;
            const pendingEvents = [...task.pendingEvents];
            for (const dependency of spec.dependsOn ?? []) {
                const source = await readTaskTx(tx, binding.rootPath, dependency.task);
                if (!source) throw new Error(`Dependency not found: ${dependency.task}`);
                if (!isTerminal(source.status)) continue;
                if (dependencySatisfied(source, dependency.condition) || dependency.onFailure === 'continue') {
                    unresolvedDeps--;
                    if (source.exit) pendingEvents.push({ type: 'task-exited', taskId: source.id, exit: source.exit });
                    continue;
                }
                failedDependency = {
                    id: source.id,
                    status: dependency.onFailure === 'skip' ? 'cancelled' : 'failed',
                };
            }
            task = {
                ...task,
                sessionPaused: (session.status === 'suspended' || session.status === 'suspending') || undefined,
                pendingEvents,
                unresolvedDeps,
                status: spec.deferStart ? 'created' : unresolvedDeps === 0 ? 'ready' : task.status,
            };
            if (failedDependency) task = terminalDependency(task, failedDependency.status, failedDependency.id);
            await writeTaskTx(tx, binding.rootPath, task);
            for (const dependency of spec.dependsOn ?? []) {
                await tx.setEntry(
                    graphPath(binding.rootPath),
                    `edge/${dependency.task}/${id}`,
                    encode(dependency),
                );
            }
            await indexTask(tx, binding.rootPath, task);
            await appendEventTx(tx, binding.rootPath, sessionId, id, 'task.created', task);
        });
        await transaction(this.catalog.fs, tx => tx.setEntry(
            catalogPath(this.catalog.rootPath), `task/${task.id}`, sessionId,
        ));
        return task;
    }

    async locateTask(taskId: TaskId): Promise<SessionId> {
        const value = await seq(this.catalog.fs).getEntry(catalogPath(this.catalog.rootPath), `task/${taskId}`);
        if (!value) throw kernelError(KernelErrorCode.TASK_NOT_FOUND, `Task not found: ${taskId}`);
        return value;
    }

    async readTask(binding: ResolvedStorageBinding, taskId: TaskId): Promise<TaskRecord> {
        const value = await seq(binding.fs).getEntry(taskPath(binding.rootPath, taskId), TASK_KEY);
        if (!value) throw kernelError(KernelErrorCode.TASK_NOT_FOUND, `Task not found: ${taskId}`);
        return decode(value);
    }

    /** The incarnation and exact Task are inspected together; this never ensures or creates either. */
    async readTaskInSessionInstance(binding: ResolvedStorageBinding, sessionId: string, instanceId: string, taskId: string): Promise<TaskRecord | undefined> {
        return transaction(binding.fs, async tx => {
            const session = await requireSessionTx(tx, binding.rootPath);
            if (session.id !== sessionId || session.instanceId !== instanceId)
                throw kernelError(KernelErrorCode.SESSION_INSTANCE_CHANGED, 'Task observation Session incarnation changed');
            return await readTaskTx(tx, binding.rootPath, taskId) ?? undefined;
        });
    }

    async taskHistoryPage(binding: ResolvedStorageBinding, taskId: TaskId, query: import('../../domain/types').TaskHistoryQuery = {}): Promise<import('../../domain/types').TaskHistoryPage> {
        const after = query.afterVersion ?? -1, limit = query.limit ?? 100;
        if (!Number.isSafeInteger(after) || after < -1 || !Number.isSafeInteger(limit) || limit < 1 || limit > 500) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task history page');
        const current = await this.readTask(binding, taskId);
        const through = query.throughVersion ?? current.version;
        if (!Number.isSafeInteger(through) || through < 0 || through > current.version) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task history upper bound');
        const end = Math.min(through, after + limit);
        const keys: string[] = [];
        for (let version = after + 1; version <= end; version++) keys.push(snapshotKey(version));
        const rows = keys.length ? await seq(binding.fs).getEntries(taskPath(binding.rootPath, taskId), keys) : {};
        return { items: keys.flatMap(key => rows[key] ? [decode<TaskRecord>(rows[key])] : []),
            throughVersion: through, ...(end < through ? { nextAfterVersion: end } : {}) };
    }

    async taskHistory(binding: ResolvedStorageBinding, taskId: TaskId, afterVersion = -1): Promise<TaskRecord[]> {
        const snapshots: TaskRecord[] = [];
        await seq(binding.fs).walkEntries(taskPath(binding.rootPath, taskId), entry => {
            const snapshot = decode<TaskRecord>(entry.value);
            if (snapshot.version > afterVersion) snapshots.push(snapshot);
            return true;
        }, { keyPrefix: 'snapshot/' });
        return snapshots.sort((a, b) => a.version - b.version);
    }

    /**
     * Retention/GC for Task version history. Snapshots exist for audit and pinned reads;
     * the Task's main record is authoritative, so compaction never touches it, its
     * attempts, effects, interactions or receipts. The newest `keepVersions` snapshots
     * (and everything at/after `beforeVersion`) stay readable, and a pinned read of a
     * pruned version returns no items instead of corrupting state — the host owns the
     * retention window.
     */
    async compactTaskHistory(
        binding: ResolvedStorageBinding,
        taskId: TaskId,
        options: { keepVersions?: number; beforeVersion?: number } = {},
    ): Promise<{ removed: number; keptFrom: number }> {
        const keepVersions = options.keepVersions ?? 20;
        const beforeVersion = options.beforeVersion;
        if (!Number.isSafeInteger(keepVersions) || keepVersions < 1) throw new Error('keepVersions must be a positive safe integer');
        if (beforeVersion !== undefined && (!Number.isSafeInteger(beforeVersion) || beforeVersion < 0))
            throw new Error('beforeVersion must be a non-negative safe integer');
        return transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, taskId);
            const keptFrom = Math.max(0, Math.min(task.version - keepVersions + 1, beforeVersion ?? Infinity));
            const prefix = taskPath(binding.rootPath, taskId);
            const keys: string[] = [];
            await tx.walkEntries(prefix, entry => {
                if (entry.key.startsWith('snapshot/')
                    && Number(entry.key.slice('snapshot/'.length)) < keptFrom) keys.push(entry.key);
                return true;
            });
            for (const key of keys) await tx.deleteEntry(prefix, key);
            if (keys.length > 0) {
                await appendEventTx(tx, binding.rootPath, task.sessionId, taskId,
                    'task.history.compacted', { removed: keys.length, keptFrom });
            }
            return { removed: keys.length, keptFrom };
        });
    }

    async taskAttempts(binding: ResolvedStorageBinding, taskId: TaskId): Promise<TaskAttempt[]> {
        const attempts: TaskAttempt[] = [];
        await seq(binding.fs).walkEntries(taskPath(binding.rootPath, taskId), entry => {
            attempts.push(decode(entry.value));
            return true;
        }, { keyPrefix: 'attempt/' });
        return attempts.sort((a, b) => a.startedAt - b.startedAt);
    }

    async claimReady(
        binding: ResolvedStorageBinding,
        workerId: string,
        leaseMs: number,
    ): Promise<TaskClaim | undefined> {
        return transaction(binding.fs, async tx => {
            const session = await requireSessionTx(tx, binding.rootPath);
            if (session.status !== 'open' && !(session.status === 'closing' && session.closeMode === 'drain')) return undefined;
            const candidates = await readyCandidates(tx, binding.rootPath);
            for (const taskId of candidates) {
                const task = await readTaskTx(tx, binding.rootPath, taskId);
                if (!task || task.status !== 'ready' || task.sessionPaused || task.controlHolds?.length || (task.control && task.control.mode !== 'run') || (task.readyAt ?? 0) > Date.now()) continue;
                if (await hasCancelledAncestorTx(tx, binding.rootPath, task)) continue;
                return claimTask(tx, binding.rootPath, task, workerId, leaseMs);
            }
            return undefined;
        });
    }

    async blockUnavailableProgram(binding: ResolvedStorageBinding, claim: TaskClaim): Promise<void> {
        await transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, claim.task.id);
            assertClaim(task, claim);
            const next: TaskRecord = { ...task, status: 'waiting', blockedReason: 'program-unavailable', currentAttempt: undefined,
                stepAttemptCount: Math.max(0, (task.stepAttemptCount ?? 1) - 1), version: task.version + 1, updatedAt: Date.now() };
            await finishAttemptTx(tx, binding.rootPath, task, 'waiting');
            await writeTaskTx(tx, binding.rootPath, next); await indexTask(tx, binding.rootPath, next);
            await appendEventTx(tx, binding.rootPath, task.sessionId, task.id, 'task.program.unavailable', task.program);
        });
    }

    async unblockProgram(binding: ResolvedStorageBinding, taskId: string): Promise<void> {
        await transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, taskId);
            if (task.status !== 'waiting' || task.blockedReason !== 'program-unavailable') return;
            const next: TaskRecord = { ...task, status: 'ready', blockedReason: undefined, version: task.version + 1, updatedAt: Date.now() };
            await writeTaskTx(tx, binding.rootPath, next); await indexTask(tx, binding.rootPath, next);
        });
    }

    async renewLease(
        binding: ResolvedStorageBinding,
        claim: TaskClaim,
        leaseMs: number,
    ): Promise<boolean> {
        return transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, claim.task.id);
            if (!claimMatches(task, claim) || task.currentAttempt!.leaseUntil <= Date.now() || await hasCancelledAncestorTx(tx, binding.rootPath, task)) return false;
            const attempt = { ...task.currentAttempt!, leaseUntil: Date.now() + leaseMs };
            const next = { ...task, currentAttempt: attempt, updatedAt: Date.now() };
            await tx.setEntry(taskPath(binding.rootPath, task.id), TASK_KEY, encode(next));
            await tx.setEntry(taskPath(binding.rootPath, task.id), attemptKey(attempt.id), encode(attempt));
            return true;
        });
    }

    async commitTask(
        binding: ResolvedStorageBinding,
        claim: TaskClaim,
        next: TaskRecord,
        eventType: string,
        payload?: unknown,
        sideEffects: TaskCommitSideEffects = {},
    ): Promise<TaskRecord> {
        let committed!: TaskRecord;
        let spawned: TaskRecord[] = [];
        await transaction(binding.fs, async tx => {
            const current = await requireTaskTx(tx, binding.rootPath, next.id);
            assertClaim(current, claim);
            if (await hasCancelledAncestorTx(tx, binding.rootPath, current)) throw kernelError(KernelErrorCode.CONFLICT, 'Task ancestor cancelled');
            // Mailbox/effect arrivals append while a reducer owns its business state.
            // Preserve the authoritative tail and existing effect/interaction revisions.
            const retrying = sideEffects.attemptOutcome === 'failed' && next.status === 'ready';
            committed = { ...next,
                pendingEvents: [...next.pendingEvents, ...current.pendingEvents.slice(claim.task.pendingEvents.length)],
                effects: { ...next.effects, ...current.effects },
                interactions: { ...next.interactions, ...current.interactions },
                stepNumber: (current.stepNumber ?? 0) + (retrying ? 0 : 1),
                stepAttemptCount: retrying ? current.stepAttemptCount : 0,
                stateRevision: (current.stateRevision ?? 0) + (retrying ? 0 : 1),
                version: current.version + 1, updatedAt: Date.now() };
            if (committed.status === 'ready' && !retrying && committed.pendingEvents.length === 0) {
                committed.pendingEvents = [{ type: 'step' }];
            }
            for (const action of sideEffects.cache ?? []) {
                if (action.type === 'cache-publish') await publishCacheTx(tx, binding.rootPath, committed.id, action.request);
                else if (action.type === 'cache-read') {
                    const receipt = await readCacheTx(tx, binding.rootPath, committed.id, action.request);
                    committed.pendingEvents.push({ type: 'cache-result', receipt });
                } else {
                    const receipt = await manageCacheTx(tx, binding.rootPath, committed.id, action);
                    committed.pendingEvents.push({ type: 'cache-managed', receipt });
                }
            }
            if (isTerminal(committed.status)) {
                committed.effects = cancelActiveEffects(committed.effects, Date.now());
                committed.interactions = Object.fromEntries(Object.entries(committed.interactions).map(([id, interaction]) => [id, interaction.status === 'pending' ? { ...interaction, status: 'cancelled' as const } : interaction]));
            }
            spawned = await applySpawnsTx(tx, binding.rootPath, committed, sideEffects.spawns ?? []);
            committed = await registerTaskWaitTx(tx, binding.rootPath, committed);
            await finishAttemptTx(
                tx, binding.rootPath, current, committed.status, sideEffects.attemptOutcome,
            );
            await writeTaskTx(tx, binding.rootPath, committed);
            await indexTask(tx, binding.rootPath, committed);
            await appendEventTx(tx, binding.rootPath, next.sessionId, next.id,
                eventType === 'task.retry.scheduled' ? eventType : `task.${committed.status}`, isTerminal(committed.status) ? committed.exit : payload);
            await applySharedMutations(tx, binding.rootPath, next, sideEffects.shared ?? []);
            for (const event of sideEffects.events ?? []) {
                await appendEventTx(tx, binding.rootPath, next.sessionId, next.id, event.type, event.payload);
            }
            const selected = claim.task.pendingEvents[0];
            if (!retrying && (claim.task.initialized ?? claim.task.state !== undefined) && selected?.type === 'message') {
                await consumeMessageTx(tx, binding.rootPath, committed, selected.message.id);
            }
            for (const message of sideEffects.messages ?? []) await enqueueMessageTx(tx, binding.rootPath, committed, message);
            for (const resource of sideEffects.resources ?? []) {
                if (resource.authority.fs !== binding.fs || resource.actorRoot !== binding.rootPath || resource.actor.taskId !== committed.id || resource.actor.sessionId !== committed.sessionId) throw new Error('Resource Decision transaction mismatch');
                await executeResourceTx(tx, resource);
            }
            if (isTerminal(committed.status)) {
                await cleanupTaskCachesTx(tx, binding.rootPath, committed.sessionId, committed.id);
                await advanceDependants(tx, binding.rootPath, committed);
                await wakeTaskWaiters(tx, binding.rootPath, committed);
            }
        });
        if (spawned.length > 0) await this.routeSpawnedTasks(spawned);
        return this.readTask(binding, committed.id);
    }

    private async routeSpawnedTasks(tasks: TaskRecord[]): Promise<void> {
        await transaction(this.catalog.fs, async tx => {
            for (const task of tasks) {
                await tx.setEntry(catalogPath(this.catalog.rootPath), `task/${task.id}`, task.sessionId);
            }
        });
    }

    async readControlReceipt(binding: ResolvedStorageBinding, taskId: string, requestId: string) {
        if (typeof requestId !== 'string' || !requestId.trim())
            throw kernelError(KernelErrorCode.INVALID_SPEC, 'Control requestId is required');
        return transaction(binding.fs, tx => readControlReceiptTx(tx, binding.rootPath, taskId, requestId));
    }

    async controlTask(
        binding: ResolvedStorageBinding, taskId: string, action: import('./task-control-store').TaskControlAction,
        options: import('../../domain/types').TaskControlOptions & { signal?: TaskSignal },
    ): Promise<import('../../domain/types').TaskControl> {
        if (!options.requestId) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Control requestId is required');
        if (options.signal?.payload !== undefined) assertDurableValue(options.signal.payload, 'Signal payload');
        return transaction(binding.fs, tx => controlTaskTx(tx, binding.rootPath, taskId, action, options));
    }

    private async acknowledgeControl(binding: ResolvedStorageBinding, id: string): Promise<boolean> {
        return transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, id);
            if (!task.control || task.control.acknowledged) return false;
            const descendants: TaskRecord[] = [];
            await tx.walkEntries(indexPath(binding.rootPath), async row => {
                const t = await requireTaskTx(tx, binding.rootPath, row.key.slice(5));
                descendants.push(t);
                return true;
            }, { keyPrefix: 'task/' });
            const selected = taskTreeIds(descendants, id);
            if (descendants.some(t => selected.has(t.id) && Object.values(t.effects).some(e => e.status === 'leased' || e.status === 'indeterminate' || e.cleanupPending))) return false;
            const next = { ...task, control: { ...task.control, acknowledged: true }, version: task.version + 1 };
            await writeTaskTx(tx, binding.rootPath, next);
            await appendEventTx(tx, binding.rootPath, task.sessionId, id, 'task.control.acknowledged', next.control);
            return true;
        });
    }

    async signalTask(binding: ResolvedStorageBinding, taskId: TaskId, signal: TaskSignal,
        options?: import('../../domain/types').SignalOptions): Promise<import('../../domain/types').SignalReceipt> {
        validateSignal(signal, options);
        return transaction(binding.fs, tx => signalTaskTx(tx, binding.rootPath, taskId, signal, options));
    }

    async startTask(binding: ResolvedStorageBinding, taskId: TaskId, options: import('../../domain/types').TaskStartOptions = {}): Promise<TaskRecord> {
        if (options.signal) assertDurableValue(options.signal, 'Start signal');
        if (options.deadlineAt !== undefined && (!Number.isFinite(options.deadlineAt) || options.deadlineAt < 0))
            throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task start deadline');
        return transaction(binding.fs, async tx => {
            await assertSharedLeaseTx(tx, binding.rootPath, options?.lease);
            const task = await requireTaskTx(tx, binding.rootPath, taskId);
            const key = 'start-signal';
            const saved = await tx.getEntry(taskPath(binding.rootPath, taskId), key);
            if (options.signal && saved && saved !== encode(options.signal)) throw new Error('Task start signal conflict');
            if (task.status !== 'created') {
                if (options.signal && !saved) throw new Error('Task was not started with this signal');
                return task;
            }
            if (options.deadlineAt !== undefined && Date.now() >= options.deadlineAt)
                throw kernelError(KernelErrorCode.TASK_START_DEADLINE_EXCEEDED, 'Task start deadline exceeded');
            if (await hasCancelledAncestorTx(tx, binding.rootPath, task)) throw kernelError(KernelErrorCode.CONFLICT, 'Task ancestor cancelled');
            const next: TaskRecord = {
                ...task,
                pendingEvents: options.signal ? [...task.pendingEvents, { type: 'signal',
                    sequence: task.pendingEvents.length + task.version + 1, signal: options.signal }] : task.pendingEvents,
                status: task.unresolvedDeps > 0 ? 'blocked' : 'ready',
                version: task.version + 1,
                updatedAt: Date.now(),
            };
            if (options.signal) {
                await tx.setEntry(taskPath(binding.rootPath, taskId), key, encode(options.signal));
                await appendEventTx(tx, binding.rootPath, task.sessionId, taskId, 'task.signal', options.signal);
            }
            await writeTaskTx(tx, binding.rootPath, next);
            await indexTask(tx, binding.rootPath, next);
            await appendEventTx(tx, binding.rootPath, task.sessionId, taskId, 'task.started');
            return next;
        });
    }

    async readInteractionResponseReceipt(binding: ResolvedStorageBinding, taskId: TaskId, requestId: string) {
        validateInteractionResponseId(requestId);
        return interactionResponseReceipt(await this.readTask(binding, taskId), requestId);
    }

    async resolveInteraction(
        binding: ResolvedStorageBinding,
        taskId: TaskId,
        response: InteractionResponse<import('../../domain/types').JsonValue>,
        options?: import('../../domain/types').InteractionResponseOptions,
    ): Promise<TaskRecord> {
        return transaction(binding.fs, async tx => {
            await assertSharedLeaseTx(tx, binding.rootPath, options?.lease);
            const task = await requireTaskTx(tx, binding.rootPath, taskId);
            assertDurableValue(response.value, 'Interaction response');
            assertInteractionResponseIdentity(task, response.interactionId, options?.requestId);
            const interaction = task.interactions?.[response.interactionId];
            if (!interaction) throw new Error(`Interaction not found: ${response.interactionId}`);
            if (interaction.status !== 'pending') {
                if (interaction.status === 'resolved' && encode(interaction.response) === encode(response.value)) return task;
                throw kernelError(KernelErrorCode.CONFLICT, 'Interaction response conflict');
            }
            if (isTerminal(task.status)) throw kernelError(KernelErrorCode.CONFLICT, 'Task is terminal');
            if (await hasCancelledAncestorTx(tx, binding.rootPath, task)) throw kernelError(KernelErrorCode.CONFLICT, 'Task ancestor cancelled');
            const next = wakeFromPendingEvents(withInteractionResponse(task, response, options?.requestId, Date.now()));
            if (next.status === 'ready') await unregisterTaskWaitTx(tx, binding.rootPath, task);
            await writeTaskTx(tx, binding.rootPath, next);
            await indexTask(tx, binding.rootPath, next);
            await appendEventTx(tx, binding.rootPath, task.sessionId, task.id,
                'task.interaction.resolved', { interactionId: interaction.id });
            return next;
        });
    }

    async claimEffect(
        binding: ResolvedStorageBinding,
        taskId: TaskId,
        effectId: string,
        workerId: string,
        leaseMs: number,
    ): Promise<EffectClaim | undefined> {
        return transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, taskId);
            const current = task.effects[effectId];
            const session = await requireSessionTx(tx, binding.rootPath);
            if (!current || current.cleanupPending || current.status !== 'pending' || (current.readyAt ?? 0) > Date.now() || isTerminal(task.status) || task.sessionPaused || task.controlHolds?.length || (task.control && task.control.mode !== 'run')
                || (session.status !== 'open' && !(session.status === 'closing' && session.closeMode === 'drain'))) return undefined;
            if (await hasCancelledAncestorTx(tx, binding.rootPath, task)) return undefined;
            const attempt = effectAttempt(workerId, leaseMs);
            const effect: PersistedEffect = { ...current, status: 'leased',
                attemptCount: (current.attemptCount ?? 0) + 1,
                attempts: [...(current.attempts ?? []), attempt], currentAttempt: attempt };
            const next = { ...task, effects: { ...task.effects, [effectId]: effect },
                version: task.version + 1, updatedAt: Date.now() };
            await writeTaskTx(tx, binding.rootPath, next);
            await appendEventTx(tx, binding.rootPath, task.sessionId, taskId, 'effect.leased', attempt, { effectId, attemptId: attempt.id });
            return { taskId, effectId, effect };
        });
    }

    async renewEffectLease(
        binding: ResolvedStorageBinding,
        claim: EffectClaim,
        leaseMs: number,
    ): Promise<boolean> {
        return transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, claim.taskId);
            const effect = task.effects[claim.effectId];
            if (!effectClaimMatches(effect, claim) || effect.currentAttempt!.leaseUntil <= Date.now() || await hasCancelledAncestorTx(tx, binding.rootPath, task)) return false;
            const attempt = { ...effect.currentAttempt!, leaseUntil: Date.now() + leaseMs };
            const nextEffect = replaceEffectAttempt(effect, attempt);
            const next = { ...task, effects: { ...task.effects, [claim.effectId]: nextEffect } };
            await tx.setEntry(taskPath(binding.rootPath, task.id), TASK_KEY, encode(next));
            return true;
        });
    }

    /** Retire only this exact worker attempt after its local execution has stopped. */
    async retireEffectLease(binding: ResolvedStorageBinding, claim: EffectClaim): Promise<void> {
        await transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, claim.taskId), effect = task.effects[claim.effectId];
            if (!effectClaimMatches(effect, claim)) return;
            const retired = replaceEffectAttempt(effect, {...effect.currentAttempt!, leaseUntil: 0});
            await writeTaskTx(tx, binding.rootPath, {...task, effects: {...task.effects, [claim.effectId]: retired}, version: task.version + 1, updatedAt: Date.now()});
        });
    }

    async completeEffect(
        binding: ResolvedStorageBinding,
        taskId: TaskId,
        effectId: string,
        leaseToken: string,
        outcome: EffectCompletion,
    ): Promise<TaskRecord> {
        return transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, taskId);
            const effect = task.effects[effectId];
            if (!effect) throw new Error(`Effect not found: ${effectId}`);
            if (effect.status === 'succeeded' || effect.status === 'failed' || effect.status === 'indeterminate') {
                return task;
            }
            if (effect.status !== 'leased' || effect.currentAttempt?.leaseToken !== leaseToken
                || effect.currentAttempt.leaseUntil <= Date.now()
                || await hasCancelledAncestorTx(tx, binding.rootPath, task)) {
                throw kernelError(KernelErrorCode.STALE_EFFECT_CLAIM, `Stale effect claim: ${effectId}`);
            }
            if (outcome.error && outcome.retryable && effect.attemptCount < (effect.request.retry?.maxAttempts ?? 1)
                && (effect.deadlineAt ?? Infinity) > Date.now()) {
                const finished = finishEffect(effect, outcome);
                const pending = { ...finished, status: 'pending' as const, readyAt: Date.now() + (effect.request.retry?.backoffMs ?? 0) };
                const next = { ...task, effects: { ...task.effects, [effectId]: pending }, version: task.version + 1, updatedAt: Date.now() };
                await writeTaskTx(tx, binding.rootPath, next);
                await appendEventTx(tx, binding.rootPath, task.sessionId, taskId, 'effect.retry.scheduled', { effectId, readyAt: pending.readyAt, attempt: effect.attemptCount + 1,
                    maxAttempts: effect.request.retry?.maxAttempts ?? 1, error: outcome.error }, { effectId, attemptId: effect.currentAttempt!.id });
                return next;
            }
            const event = outcome.error
                ? { type: 'effect-failed' as const, effectId, error: outcome.error }
                : { type: 'effect-completed' as const, effectId, result: outcome.result };
            let next: TaskRecord = {
                ...task,
                pendingEvents: [...task.pendingEvents, event],
                effects: { ...task.effects, [effectId]: finishEffect(effect, outcome) },
                version: task.version + 1,
                updatedAt: Date.now(),
            };
            if (outcome.indeterminate) next.pendingEvents = task.pendingEvents;
            else next = wakeFromPendingEvents(next);
            if (next.status === 'ready') await unregisterTaskWaitTx(tx, binding.rootPath, task);
            await writeTaskTx(tx, binding.rootPath, next);
            await indexTask(tx, binding.rootPath, next);
            await appendEventTx(tx, binding.rootPath, task.sessionId, taskId, `effect.${next.effects[effectId].status}`, outcome,
                { effectId, attemptId: effect.currentAttempt!.id });
            return next;
        });
    }

    private async assertEffectClaimTx(tx: import('@nestos/vfs-core').ISeqFileTransaction, binding: ResolvedStorageBinding, claim: EffectClaim): Promise<void> {
        const task = await requireTaskTx(tx, binding.rootPath, claim.taskId);
        const effect = task.effects[claim.effectId];
        if (!effectClaimMatches(effect, claim) || (effect.currentAttempt?.leaseUntil ?? 0) <= Date.now() || await hasCancelledAncestorTx(tx, binding.rootPath, task)) throw new Error('Stale effect claim');
    }

    async resolveEffect(binding: ResolvedStorageBinding, taskId: string, request: import('../../domain/types').EffectResolution): Promise<void> {
        if (!request.requestId) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Resolution requestId is required');
        if (request.outcome.type === 'completed') assertDurableValue(request.outcome.result, 'Effect result');
        await transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, taskId);
            const key = `effect-resolution/${encodeURIComponent(request.requestId)}`, fingerprint = encode(request);
            const previous = await tx.getEntry(taskPath(binding.rootPath, taskId), key);
            if (previous) { if (previous !== fingerprint) throw kernelError(KernelErrorCode.CONFLICT, 'Effect resolution conflict'); return; }
            if (isTerminal(task.status)) throw kernelError(KernelErrorCode.CONFLICT, 'Task is terminal');
            if (await hasCancelledAncestorTx(tx, binding.rootPath, task)) throw kernelError(KernelErrorCode.CONFLICT, 'Task ancestor cancelled');
            const effect = task.effects[request.effectId];
            if (effect?.status !== 'indeterminate') throw kernelError(KernelErrorCode.CONFLICT, 'Effect is not indeterminate');
            const next: TaskRecord = { ...task, effects: { ...task.effects }, pendingEvents: task.pendingEvents.filter(event => !(event.type === 'effect-failed' && event.effectId === request.effectId)), version: task.version + 1, updatedAt: Date.now() };
            if (request.outcome.type === 'retry' || request.outcome.type === 'reconcile') {
                if (request.outcome.type==='retry'&&(effect.deadlineAt ?? Infinity) <= Date.now()) throw new Error('Effect deadline expired; create a new logical operation');
                next.effects[request.effectId] = { ...effect, status: 'pending', replayAuthorized: request.outcome.type==='retry', error: undefined };
            } else {
                const completed = request.outcome.type === 'completed';
                next.effects[request.effectId] = { ...effect, status: completed ? 'succeeded' : 'failed',
                    result: request.outcome.type === 'completed' ? request.outcome.result : undefined,
                    error: request.outcome.type === 'failed' ? request.outcome.error : undefined };
                next.pendingEvents.push(request.outcome.type === 'completed'
                    ? { type: 'effect-completed', effectId: request.effectId, result: request.outcome.result }
                    : { type: 'effect-failed', effectId: request.effectId, error: request.outcome.error });
            }
            const woken = wakeFromPendingEvents(next);
            if (woken.status === 'ready') await unregisterTaskWaitTx(tx, binding.rootPath, task);
            await writeTaskTx(tx, binding.rootPath, woken); await indexTask(tx, binding.rootPath, woken);
            await tx.setEntry(taskPath(binding.rootPath, taskId), key, fingerprint);
            await appendEventTx(tx, binding.rootPath, task.sessionId, taskId, 'effect.resolved', request,
                { effectId: request.effectId, attemptId: effect.attempts.at(-1)?.id });
        });
    }

    async confirmEffectCleanup(binding: ResolvedStorageBinding, taskId: string, effectId: string): Promise<void> {
        await transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, taskId), effect = task.effects[effectId];
            if (!effect?.cleanupPending) return;
            await writeTaskTx(tx, binding.rootPath, { ...task, effects: { ...task.effects, [effectId]: { ...effect, cleanupPending: false } }, version: task.version + 1 });
            await appendEventTx(tx, binding.rootPath, task.sessionId, taskId, 'effect.cleanup.confirmed', { effectId },
                { effectId, attemptId: effect.attempts.at(-1)?.id });
        });
    }

    async cancelTask(binding: ResolvedStorageBinding, taskId: TaskId, reason?: string, options?: import('../../domain/types').TaskCancellationOptions): Promise<TaskRecord> {
        validateCancellation(options);
        return this.finishWithoutClaim(binding, taskId, 'cancelled', undefined, { message: reason ?? 'Cancelled' }, false, options);
    }

    async cancelRemainingTaskTree(binding: ResolvedStorageBinding, taskId: string, options: import('../../domain/types').TaskTreeCancellationOptions): Promise<TaskRecord[]> {
        validateTreeCancellation(options);
        return transaction(binding.fs, tx => cancelRemainingTaskTreeTx(tx, binding.rootPath, taskId, options));
    }

    async readTreeCancellationReceipt(binding: ResolvedStorageBinding, taskId: string, requestId: string) {
        return transaction(binding.fs, tx => readTreeCancellationReceiptTx(tx, binding.rootPath, taskId, requestId));
    }

    async readCancellationReceipt(binding: ResolvedStorageBinding, taskId: string, requestId: string) {
        return transaction(binding.fs, tx => readCancellationReceiptTx(tx, binding.rootPath, taskId, requestId));
    }

    /** 放弃当前 claim，把 running task 恢复到 ready（dispose 时避免 task 卡在租约内无法重新调度）。 */
    async abandonClaim(binding: ResolvedStorageBinding, claim: TaskClaim): Promise<void> {
        await transaction(binding.fs, async tx => {
            const taskId = claim.task.id;
            const current = await requireTaskTx(tx, binding.rootPath, taskId);
            if (!claimMatches(current, claim)) return;
            const attempt = { ...current.currentAttempt!, outcome: 'lost' as const, finishedAt: Date.now() };
            const next: TaskRecord = {
                ...current, status: 'ready' as const, currentAttempt: undefined, readyAt: undefined,
                version: current.version + 1, updatedAt: Date.now(),
            };
            await writeTaskTx(tx, binding.rootPath, next);
            await tx.setEntry(taskPath(binding.rootPath, taskId), attemptKey(attempt.id), encode(attempt));
            await indexTask(tx, binding.rootPath, next);
        });
    }

    /**
     * Page a Task's indexed events. A stale cursor below the retention watermark is
     * clamped to the retained window and reported via `firstAvailableIndex`, so a
     * consumer that kept an old cursor resyncs instead of reading missing events.
     */
    async taskEventPage(binding: ResolvedStorageBinding, taskId: TaskId, query: import('../../domain/types').TaskEventQuery = {}): Promise<import('../../domain/types').TaskEventPage> {
        const after = query.afterIndex ?? 0, limit = query.limit ?? 100;
        if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 500) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task event page');
        return transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, taskId);
            await ensureTaskEventIndexTx(tx, binding.rootPath);
            const path = eventsPath(binding.rootPath);
            const count = Number(await tx.getEntry(path, taskEventCountKey(taskId)) ?? 0);
            if (!Number.isSafeInteger(count) || count < 0) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task event count');
            const first = Number(await tx.getEntry(path, taskEventFirstKey(taskId)) ?? 1);
            if (!Number.isSafeInteger(first) || first < 1 || first > count + 1) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task event retention watermark');
            const through = query.throughIndex ?? count;
            if (!Number.isSafeInteger(through) || through < 0 || through > count) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task event upper bound');
            const start = Math.max(after + 1, first);
            const end = Math.min(through, start - 1 + limit), keys: string[] = [];
            for (let index = start; index <= end; index++) keys.push(taskEventKey(taskId, index));
            const refs = await tx.getEntries(path, keys);
            const eventKeys = keys.map(key => {
                const sequence = Number(refs[key]);
                if (!Number.isSafeInteger(sequence) || sequence < 1) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task event index');
                return `event/${String(sequence).padStart(16, '0')}`;
            });
            const rows = await tx.getEntries(path, eventKeys);
            const items = eventKeys.map(key => {
                if (!rows[key]) throw new Error('Task event is missing');
                const event = decode<EventEnvelope>(rows[key]);
                if (event.taskId !== taskId || event.sessionId !== task.sessionId) throw new Error('Task event index scope mismatch');
                return event;
            });
            return { items, throughIndex: through, ...(end < through ? { nextAfterIndex: end } : {}),
                ...(first > 1 ? { firstAvailableIndex: first } : {}) };
        });
    }

    /**
     * Retention/GC for a Task's event log (Storage §5 `events.seq` retention). Keeps the
     * newest `keepEvents` indexed events (and the session-level events, which are not
     * indexed), removes older `event/<sequence>` rows together with their index entries,
     * and records the retention watermark that `taskEventPage` reports for resync.
     */
    async pruneTaskEvents(
        binding: ResolvedStorageBinding,
        taskId: TaskId,
        options: { keepEvents?: number } = {},
    ): Promise<{ removed: number; firstAvailableIndex: number }> {
        const keepEvents = options.keepEvents ?? 200;
        if (!Number.isSafeInteger(keepEvents) || keepEvents < 1) throw new Error('keepEvents must be a positive safe integer');
        return transaction(binding.fs, async tx => {
            const task = await requireTaskTx(tx, binding.rootPath, taskId);
            await ensureTaskEventIndexTx(tx, binding.rootPath);
            const path = eventsPath(binding.rootPath);
            const count = Number(await tx.getEntry(path, taskEventCountKey(taskId)) ?? 0);
            if (!Number.isSafeInteger(count) || count < 0) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task event count');
            const first = Number(await tx.getEntry(path, taskEventFirstKey(taskId)) ?? 1);
            if (!Number.isSafeInteger(first) || first < 1 || first > count + 1) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task event retention watermark');
            const nextFirst = Math.max(first, count - keepEvents + 1);
            if (nextFirst <= first) return { removed: 0, firstAvailableIndex: first };
            const sessionPruned = await retentionBoundary(tx, path);
            let removed = 0;
            let prunedThrough = Number(await tx.getEntry(path, prunedSequenceKey(taskId)) ?? 0);
            for (let index = first; index < nextFirst; index++) {
                const key = taskEventKey(taskId, index);
                const sequence = Number(await tx.getEntry(path, key));
                if (!Number.isSafeInteger(sequence) || sequence < 1) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid Task event index');
                const eventKey = `event/${String(sequence).padStart(16, '0')}`;
                const raw = await tx.getEntry(path, eventKey);
                if (!raw) throw new Error('Task event record missing');
                const event = decode<EventEnvelope>(raw);
                if (event.taskId !== taskId || event.sessionId !== task.sessionId || event.sequence !== sequence)
                    throw new Error('Task event index scope mismatch');
                await tx.deleteEntry(path, key);
                await tx.deleteEntry(path, eventKey);
                prunedThrough = Math.max(prunedThrough, sequence);
                removed++;
            }
            await tx.setEntry(path, taskEventFirstKey(taskId), String(nextFirst));
            await tx.setEntry(path, prunedSequenceKey(taskId), String(prunedThrough));
            await tx.setEntry(path, prunedSequenceKey(), String(Math.max(sessionPruned, prunedThrough)));
            await appendEventTx(tx, binding.rootPath, task.sessionId, taskId, 'task.events.pruned',
                { removed, firstAvailableIndex: nextFirst });
            return { removed, firstAvailableIndex: nextFirst };
        });
    }

    async events(binding: ResolvedStorageBinding, after = 0): Promise<EventEnvelope[]> {
        const values: EventEnvelope[] = [];
        await seq(binding.fs).walkEntries(eventsPath(binding.rootPath), entry => {
            const event = decode<EventEnvelope>(entry.value);
            if (event.sequence > after) values.push(event);
            return true;
        }, { keyPrefix: 'event/' });
        return values.sort((a, b) => a.sequence - b.sequence);
    }

    /**
     * Append a session event with a fresh sequence number.
     *
     * Used by effect execution to stream incremental events (e.g. LLM chunks)
     * to UI consumers polling via eventList. The event carries the owning
     * taskId so TaskHandle.events() can filter it.
     */
    async appendEvent(
        binding: ResolvedStorageBinding,
        sessionId: SessionId,
        taskId: TaskId | undefined,
        type: string,
        payload?: unknown,
        effectClaim?: EffectClaim,
    ): Promise<number> {
        assertCustomEventType(type);
        if (payload !== undefined) assertDurableValue(payload, 'Event payload');
        return transaction(binding.fs, async tx => {
            if (effectClaim) await this.assertEffectClaimTx(tx, binding, effectClaim);
            return appendEventTx(tx, binding.rootPath, sessionId, taskId, type, payload, effectClaim ? { effectId: effectClaim.effectId, attemptId: effectClaim.effect.currentAttempt!.id } : undefined);
        });
    }

    /** `tasks` lets a poll tick reuse the list it already read instead of rescanning storage. */
    async pendingEffects(binding: ResolvedStorageBinding, tasks?: TaskRecord[]): Promise<Array<{ task: TaskRecord; effectId: string }>> {
        const pending: Array<{ task: TaskRecord; effectId: string }> = [];
        for (const task of tasks ?? await this.listTasks(binding)) {
            for (const [effectId, effect] of Object.entries(task.effects)) {
                if (effect.status === 'pending') pending.push({ task, effectId });
            }
        }
        return pending;
    }

    async sweep(binding: ResolvedStorageBinding): Promise<TaskRecord[] | null> {
        await transaction(binding.fs, tx => refreshWaiters(tx, binding.rootPath));
        const tasks = await this.listTasks(binding);
        let changed = false;
        for (const task of tasks) {
            if (await this.cancelFromAncestor(binding, task)) { changed = true; continue; }
            if (await this.recoverExpiredEffects(binding, task)) changed = true;
            if (task.control && !task.control.acknowledged && await this.acknowledgeControl(binding, task.id)) changed = true;
            if (task.status === 'running' && task.currentAttempt && task.currentAttempt.leaseUntil <= Date.now()) {
                await this.requeueExpired(binding, task);
                changed = true;
            }
        }
        return changed ? null : tasks;
    }

    async recover(binding: ResolvedStorageBinding, options: import('../../domain/types').RecoveryOptions = {},
        knownSession?: SessionRecord): Promise<RecoveryReport> {
        let recoveredTasks = 0;
        let recoveredEffects = 0;
        let expiredAttempts = 0;
        // One directory scan yields every Task record for this Session. Recovery decisions,
        // index repair and the wait graph reuse these decoded records instead of re-reading
        // each Task up to five times through separate round trips.
        const records = new Map<string, TaskRecord>();
        for (const task of await this.listTasks(binding)) records.set(task.id, task);
        // `openSession` already read the Session record; only an online recovery needs a re-read.
        const session = knownSession ?? await this.readSession(binding);
        const tasks = await this.recoverProjections(binding, records);
        if (options.takeover && [...records.values()].some(needsRecoveryFence)) await transaction(binding.fs, async tx => {
            for (const id of tasks) {
                const task = await readTaskTx(tx, binding.rootPath, id);
                if (!task) continue;
                let changed = false;
                if (task.currentAttempt) {
                    task.currentAttempt = { ...task.currentAttempt, leaseUntil: 0, leaseToken: createId('fence') };
                    changed = true;
                }
                for (const [effectId, effect] of Object.entries(task.effects)) {
                    if (effect.status !== 'leased' || !effect.currentAttempt) continue;
                    task.effects[effectId] = { ...effect, currentAttempt: { ...effect.currentAttempt, leaseUntil: 0, leaseToken: createId('fence') } };
                    changed = true;
                }
                if (changed) {
                    const next = { ...task, version: task.version + 1, updatedAt: Date.now() };
                    await writeTaskTx(tx, binding.rootPath, next);
                    records.set(id, next);
                }
            }
        });
        await this.repairCatalog(session, tasks);
        for (const taskId of tasks) {
            const known = records.get(taskId);
            // Untouched Tasks cannot start, cancel or expire anything; skip their reads.
            if (!known || !needsRecoveryWork(known)) continue;
            const task = await this.readTask(binding, taskId);
            if (await this.cancelFromAncestor(binding, task)) continue;
            recoveredEffects += await this.recoverExpiredEffects(binding, task);
            if (task.status !== 'running' || !task.currentAttempt || task.currentAttempt.leaseUntil > Date.now()) continue;
            expiredAttempts++;
            await this.requeueExpired(binding, task, options.takeover);
            recoveredTasks++;
        }
        return { recoveredTasks, recoveredEffects, expiredAttempts, rebuiltIndexes: tasks.length,
            issues: [...records.values()].flatMap(task => taskRecoveryIssues(task).filter(issue => issue.reason !== 'effect-in-flight')) };
    }

    private async recoverExpiredEffects(binding: ResolvedStorageBinding, task: TaskRecord): Promise<number> {
        const expired = Object.entries(task.effects).filter(([, effect]) =>
            effect.status === 'leased' && (effect.currentAttempt?.leaseUntil ?? Infinity) <= Date.now());
        if (expired.length === 0) return 0;
        let recovered = 0;
        await transaction(binding.fs, async tx => {
            const current = await requireTaskTx(tx, binding.rootPath, task.id);
            let effects = current.effects;
            for (const [effectId] of expired) {
                const next = recoverEffect(effects, effectId);
                if (next !== effects) recovered++;
                effects = next;
            }
            if (recovered === 0) return;
            const next = { ...current, effects, version: current.version + 1, updatedAt: Date.now() };
            await writeTaskTx(tx, binding.rootPath, next);
            for (const [effectId] of expired) {
                if (effects[effectId]?.status !== 'pending') continue;
                await appendEventTx(tx, binding.rootPath, task.sessionId, task.id,
                    'effect.attempt.lost', { effectId }, { effectId, attemptId: current.effects[effectId].currentAttempt?.id });
            }
        });
        return recovered;
    }

    /**
     * Repair the Task projections instead of rebuilding them. An intact index (the normal case
     * after a clean shutdown) is only cleaned of projections left by removed Tasks, so a restart
     * writes nothing; a missing or inconsistent projection is rebuilt densely, which also
     * renumbers `task-order` slots the Task page reader requires to be contiguous.
     *
     * Index repair and the wait graph share **one** transaction: both walk the same index and
     * both were previously opened separately for every recovered Session.
     */
    private async recoverProjections(binding: ResolvedStorageBinding, records: Map<string, TaskRecord>): Promise<string[]> {
        return transaction(binding.fs, async tx => {
            const rows = new Map<string, string>();
            await tx.walkEntries(indexPath(binding.rootPath), row => {rows.set(row.key, row.value); return true;});
            await refreshRecoveryRecordsTx(tx, binding.rootPath, records, rows);
            const ids = await this.repairIndexesTx(tx, binding.rootPath, [...records.values()], rows);
            await recoverWaitGraphTx(tx, binding.rootPath, ids, records);
            return ids;
        });
    }

    private async repairIndexesTx(tx: ISeqFileTransaction, root: string, tasks: TaskRecord[], rows: Map<string, string>): Promise<string[]> {
        const path = indexPath(root);
        if (tasks.some(task => !indexTaskIntact(rows, task))) return this.rebuildAllIndexesTx(tx, root, tasks);
        const expected = new Set(tasks.map(task => `task/${task.id}`));
        for (const key of rows.keys()) {
            const id = projectionTaskId(key);
            if (id !== undefined && !expected.has(`task/${id}`)) await tx.deleteEntry(path, key);
        }
        for (const task of tasks) await indexTaskActivity(tx, root, task, rows.get(activityKey(task.id)) ?? null);
        return tasks.map(task => task.id);
    }

    private async rebuildAllIndexesTx(tx: ISeqFileTransaction, root: string, tasks: TaskRecord[]): Promise<string[]> {
        const path = indexPath(root), stale: string[] = [];
        await tx.walkEntries(path, row => { stale.push(row.key); return true; });
        const revision = await tx.getEntry(path, ACTIVITY_REVISION);
        for (const key of stale) await tx.deleteEntry(path, key);
        if (revision) await tx.setEntry(path, ACTIVITY_REVISION, revision);
        await tx.increment(path, ACTIVITY_REVISION);
        for (const task of tasks) { await indexTask(tx, root, task); await indexTaskActivity(tx, root, task, null); }
        return tasks.map(task => task.id);
    }

    /**
     * Repair the catalog in one transaction.
     *
     * Reads are bounded by this Session's Tasks: a keyed read per Task replaces a full scan of
     * the catalog's `task/` rows, which made recovery cost `sessions × (sessions + tasks)` and
     * dominated browser boot on IndexedDB. The transaction only writes what differs, which keeps
     * catalog subscribers asleep without adding round trips.
     */
    private async repairCatalog(session: SessionRecord, taskIds: string[]): Promise<void> {
        await transaction(this.catalog.fs, async tx => {
            const path = catalogPath(this.catalog.rootPath), key = `session/${session.id}`, value = encode(session);
            if (await tx.getEntry(path, key) !== value) await tx.setEntry(path, key, value);
            for (const taskId of taskIds) {
                const taskKey = `task/${taskId}`;
                if (await tx.getEntry(path, taskKey) !== session.id) await tx.setEntry(path, taskKey, session.id);
            }
        });
    }

    private async cancelFromAncestor(binding: ResolvedStorageBinding, task: TaskRecord): Promise<boolean> {
        if (!task.parentTaskId || isTerminal(task.status)) return false;
        const current = await this.finishWithoutClaim(binding, task.id, 'cancelled', undefined,
            { message: 'Ancestor cancelled' }, true);
        return current.status === 'cancelled';
    }

    private async finishWithoutClaim(
        binding: ResolvedStorageBinding,
        taskId: TaskId,
        status: 'cancelled' | 'failed',
        output?: unknown,
        error?: { message: string },
        requireCancelledAncestor = false,
        options?: import('../../domain/types').TaskCancellationOptions,
    ): Promise<TaskRecord> {
        return transaction(binding.fs, async tx => {
            await assertSharedLeaseTx(tx, binding.rootPath, options?.lease);
            const task = await requireTaskTx(tx, binding.rootPath, taskId);
            const cancellation = await prepareCancellationTx(tx, binding.rootPath, task, error?.message ?? 'Cancelled', options);
            if (cancellation?.replayed) return task;
            if (isTerminal(task.status)) {await cancellation?.commit(task); return task;}
            if (requireCancelledAncestor && !await hasCancelledAncestorTx(tx, binding.rootPath, task)) return task;
            const next = await finishTaskWithoutClaimTx(tx, binding.rootPath, task, status, output, error);
            await cancellation?.commit(next);
            return next;
        });
    }

    private async readCatalog(id: SessionId): Promise<SessionRecord | null> {
        const value = await seq(this.catalog.fs).getEntry(catalogPath(this.catalog.rootPath), `session/${id}`);
        return value ? decode(value) : null;
    }

    private async readSession(binding: ResolvedStorageBinding): Promise<SessionRecord> {
        const value = await seq(binding.fs).getEntry(sessionPath(binding.rootPath), SESSION_KEY);
        if (!value) throw new Error(`Session record missing at ${binding.rootPath}`);
        return decode(value);
    }

    private async listTaskIds(binding: ResolvedStorageBinding): Promise<string[]> {
        return (await this.taskEntries(binding)).map(entry => entry.id);
    }

    /**
     * Read every Task's id and stored value in one directory scan, sorted by id so
     * `listTasks` keeps the order callers relied on. Shared by listTaskIds/listTasks
     * so a full scan costs one read per Task instead of two.
     */
    private async taskEntries(binding: ResolvedStorageBinding): Promise<Array<{ id: string; value: string }>> {
        const root = join(binding.rootPath, 'tasks');
        // A removed storage tree has no Tasks; callers must stay usable for retries.
        if (!await binding.fs.driver.exists(root)) return [];
        const children = await binding.fs.driver.getChildren(root, { fields: 'entry' });
        // The directory listing already proved each Task directory exists, so no `exists` stat per
        // Task (a full metadata read on the desktop).
        const ids = children.flatMap(child => child.type === 'directory' ? [child.name] : []);
        if (!ids.length) return [];
        // One transaction and one batched record read for the whole scan: a per-Task read would cost
        // its own transaction plus a backend round-trip each (3 IPCs per Task on the desktop).
        const values = await transaction(binding.fs, tx => tx.getEntriesMany(
            ids.map(id => ({ fileIdOrPath: taskPath(binding.rootPath, id), key: TASK_KEY }))));
        // A crash can leave a Task directory without its seq file; the batch read reports that as null.
        return ids.flatMap((id, index) => values[index] ? [{ id, value: values[index]! }] : [])
            .sort((left, right) => left.id < right.id ? -1 : left.id > right.id ? 1 : 0);
    }

    private async requeueExpired(binding: ResolvedStorageBinding, task: TaskRecord, restart = false): Promise<void> {
        await transaction(binding.fs, async tx => {
            const current = await requireTaskTx(tx, binding.rootPath, task.id);
            if (current.status !== 'running' || current.currentAttempt?.leaseUntil !== task.currentAttempt?.leaseUntil) return;
            const active = current.currentAttempt;
            if (!active) return;
            const now = Date.now();
            const attempt = { ...active, outcome: 'lost' as const, finishedAt: now };
            const error = { message: `Task attempt lease expired: ${active.id}` };
            const exhausted = !restart && (current.stepAttemptCount ?? current.attemptCount) >= current.retry.maxAttempts;
            const next: TaskRecord = exhausted
                ? { ...current, status: 'failed', currentAttempt: undefined, lastError: error,
                    exit: { taskId: task.id, status: 'failed', error, completedAt: now },
                    version: current.version + 1, updatedAt: now }
                : { ...current, status: 'ready', currentAttempt: undefined, lastError: error,
                    readyAt: now + (current.retry.backoffMs ?? 0),
                    version: current.version + 1, updatedAt: now };
            await writeTaskTx(tx, binding.rootPath, next);
            await tx.setEntry(taskPath(binding.rootPath, task.id), attemptKey(attempt.id), encode(attempt));
            await indexTask(tx, binding.rootPath, next);
            await appendEventTx(tx, binding.rootPath, task.sessionId, task.id, 'task.attempt.lost', attempt);
            if (exhausted) {
                await appendEventTx(tx, binding.rootPath, task.sessionId, task.id, 'task.failed', next.exit);
                await advanceDependants(tx, binding.rootPath, next);
                await wakeTaskWaiters(tx, binding.rootPath, next);
            }
        });
    }
}

export { createId, ensureTree } from './store-helpers';

/** Index value `indexTask` writes for a Task, reused to detect an intact projection. */
function indexTaskValue(task: TaskRecord): string {
    return encode({ status: task.status, updatedAt: task.updatedAt });
}

/** Task id a Task projection key belongs to, or undefined for unrelated index keys. */
function projectionTaskId(key: string): string | undefined {
    if (key.startsWith('activity/task/')) return key.slice('activity/task/'.length);
    if (key.startsWith('task/')) return key.slice('task/'.length);
    if (key.startsWith('ready/')) return key.slice('ready/'.length);
    return undefined;
}

function indexTaskIntact(rows: ReadonlyMap<string, string>, task: TaskRecord): boolean {
    if (rows.get(`task/${task.id}`) !== indexTaskValue(task)) return false;
    if (!rows.has(`task-ordinal/${task.id}`)) return false;
    return task.status === 'ready' ? rows.has(`ready/${task.id}`) : !rows.has(`ready/${task.id}`);
}

/** A takeover fence is required only while a live attempt or leased Effect can still run. */
function needsRecoveryFence(task: TaskRecord): boolean {
    if (task.currentAttempt) return true;
    return Object.values(task.effects).some(effect => effect.status === 'leased' && effect.currentAttempt);
}

/**
 * Per-Task recovery work is limited to records that can still change: unfinished
 * ancestors, running attempts and leased Effects. Terminal Tasks without live work
 * are left untouched, so a clean restart reads no Task a second time.
 */
function needsRecoveryWork(task: TaskRecord): boolean {
    if (task.status === 'running' || task.currentAttempt) return true;
    if (task.parentTaskId && !isTerminal(task.status)) return true;
    return Object.values(task.effects).some(effect => effect.status === 'leased');
}

/** Read pointers and current Task records in two batches within the caller's snapshot. */
async function readTaskPageTx(tx: ISeqFileTransaction, root: string, sessionId: string,
    after: number, end: number): Promise<TaskRecord[]> {
    const keys = Array.from({ length: Math.max(0, end - after) }, (_, i) =>
        `task-order/${String(after + i + 1).padStart(16, '0')}`);
    if (!keys.length) return [];
    const pointers = await tx.getEntries(indexPath(root), keys);
    const ids = keys.map(key => {
        const id = pointers[key];
        if (!id) throw new Error('Task list index is missing');
        return id;
    });
    const values = await tx.getEntriesMany(ids.map(id => ({ fileIdOrPath: taskPath(root, id), key: TASK_KEY })));
    return values.map((value, index) => {
        if (!value) throw new Error(`Task not found: ${ids[index]}`);
        const task = decode<TaskRecord>(value);
        if (task.sessionId !== sessionId || task.id !== ids[index]) throw new Error('Task list index scope mismatch');
        return task;
    });
}
