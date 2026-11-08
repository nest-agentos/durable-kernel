import {SeqCommitSubscriptions} from '../infrastructure/seq-commit-subscriptions';
import { cancellationFrontier } from './cancellation-frontier';
import {canonical} from './canonical';
import {TaskCallEffect} from './task-call-effect';
import {taskCallProgram, taskCallProgramV2} from './task-call-program';
import {taskCallFingerprint, taskCallTargetId, taskCallTargetSpec, taskCallPlan, taskCallCapabilityUri, taskCallKey} from './task-call-identity';
import {inspectTaskCallTarget} from './task-call-inspection';
import type {TaskCallAuthority, TaskCallBinding, TaskCallRequest} from '../domain/task-call';
import {waitForChange} from '../public/wait';
import {bindCapabilities} from './capabilities';
import type { LeaseGuardOptions, InboxQuery, KernelDiagnostic, SessionMessageRequest, SessionSendOptions, ShutdownOptions, ShutdownReport } from '../domain/types';
import { taskRecoveryIssues } from './recovery-issues';
import { shutdown } from '../runtime/shutdown';
import { quiesceWorker } from '../runtime/worker-drain';
import { scanInterval } from '../runtime/scheduling';
import { scheduleProgram, scheduleTaskSpec } from './schedule-program';
import { CommitRescanner, type CommitScope } from '../runtime/commit-rescanner';
import { ACTIVITY_REVISION, readSessionActivity } from '../infrastructure/seqfile/activity-index';
import { createArchiveApi } from '../public/archive';
import { EventBus, FSError, pathUtils, type IFileSystem } from '@nestos/vfs-core';
import { EffectRegistry, ProgramRegistry, StorageResolverRegistry, WorkspaceRegistry } from '../ports/registry';
import type { KernelPlugin, KernelRegistration } from '../ports/plugin';
import { DurablePoller } from '../runtime/durable-poller';
import { LeaseHeartbeat } from '../runtime/lease-heartbeat';
import { EffectCleanupRunner } from '../runtime/effect-cleanup';
import { EffectWorkSlots, type EffectWorkPermit } from '../runtime/effect-work-slots';
import { DefaultSessionHandle } from '../public/session-handle';
import { DefaultTaskHandle } from '../public/task-handle';
import { resourceApi } from '../public/resources';
import { assertCustomEventType } from '../public/events';
import { closedSessionStat, sessionStat } from '../domain/status';
import { ManagedResourceStore } from '../infrastructure/seqfile/managed-resources';
import { catalogPath, eventsPath, indexPath, sessionPath, resourcesPath } from '../infrastructure/seqfile/seqfile-core';
import { eventPage, taskEventSnapshot, sessionEventSnapshot } from '../infrastructure/seqfile/event-store';
import {
    createId,
    SeqFileKernelStore,
    type EffectClaim,
    type TaskClaim,
} from '../infrastructure/seqfile/store';
import type {
    BudgetAccount,
    ContextBranch,
    ContextCommit,
    ContextCommitOptions,
    CrossSessionMessage,
    Decision,
    DurableTaskProgram,
    EffectAdapter,
    EffectRequest,
    EventEnvelope,
    InteractionResponse,
    RecoveryReport,
    ResolvedStorageBinding,
    ResourceGrant,
    ResourceHandle,
    ResourceRecord,
    ResourceRight,
    ResourceSpec,
    SessionHandle,
    SessionId,
    SessionRecord,
    SharedStateEntry,
    SharedStateRevision,
    SharedStateWriteOptions,
    SessionStorageResolver,
    StorageBindingRef,
    TaskHandle,
    TaskId,
    TaskRecord,
    TaskSignal,
    WorkspaceAdapter,
    WorkspaceDiff,
    WorkspaceMergeResult,
    WorkspaceSnapshot,
} from '../domain/types';
import { assertDurableValue } from './durability';
import { KernelError, KernelErrorCode, kernelError } from '../domain/errors';
import { failureDecision, isTerminalStatus, mergeReport, nextDecision, shouldRetry, transition, validateDecision } from './decision';
import { activeEffectIds, addEffect, addInteraction, effectControllerKey, effectFailure, executeEffectWithDeadline, isMissingPath, normalizeEffect, type RequiredEffect } from './effect-utils';
import { assertWorkspace, assertWorkspaceSnapshots, readWorkspaceSnapshots, workspaceContext, workspaceSnapshot } from './workspace-utils';
import { decisionSideEffects, prepareSpawns } from './actions';

/**
 * Why a `changed` notification fired.
 *
 * - `structure`: a Task/Session was created, removed, or changed status — views
 *   that list them (sidebars, task trees) must re-read.
 * - `content`: a Task's content moved (stream deltas, logs, shared state, context
 *   commits) without changing what those listings show. Fires many times per
 *   second while a run streams, so consumers must not re-render on it.
 */
export type { KernelChangeReason } from '../ports/kernel-view';
import type { KernelChangeReason, KernelInspectionPort, KernelViewPort } from '../ports/kernel-view';

interface KernelEvents { changed: { sessionId: string; taskId?: string; reason: KernelChangeReason }; }

export interface KernelOptions {
    /** Optional host single-writer/authorization gate; exact plan grants remain mandatory. */
    authorizeTaskCall?: TaskCallAuthority;
    /** Observations contain identities and classifications, never request bodies. */
    onDiagnostic?: (event: KernelDiagnostic) => void;
    catalog: { fs: IFileSystem; rootPath?: string };
    workerId?: string;
    maxConcurrent?: number;
    maxConcurrentEffects?: number;
    leaseMs?: number;
    /** Maximum wait for Effect cleanup confirmation; default 30 seconds. */
    effectCleanupTimeoutMs?: number;
    /** Optional compatibility polling; default 0 uses commits and deadline timers. */
    pollMs?: number;
    /** Safety scans for lost cross-process notifications; default 1000ms. */
    rescanMs?: number;
    /** Reliable host notification bridge; required to disable safety scans. */
    wakeSource?: import('../domain/types').KernelWakeSource;
}

export class Kernel implements KernelRegistration, KernelViewPort, KernelInspectionPort {
    private detachObservations=false;
    private readonly drainingObservations = new Set<AbortController>();
    private readonly managedObservations = new Set<AbortController>();
    private readonly observationDrainReason=Object.assign(new Error('Worker observation drained'),{code:'WORKER_OBSERVATION_DRAINED'});
    private readonly authorizeTaskCall?: TaskCallAuthority;
    readonly archives: import('../domain/archive').SessionArchiveApi;
    private readonly resourcePoller: DurablePoller<string>;
    private readonly managedResources: ManagedResourceStore;
    get resources() { return resourceApi(this.managedResources, {}); }
    resourceApi(sessionId: string, taskId?: string) { return resourceApi(this.managedResources, { sessionId, taskId }); }
    async sessionStat(sessionId: string) {
        try {
            const binding = await this.binding(sessionId);
            const session = await this.store.sessionRecord(binding);
            // An interrupted removal leaves records without storage; the Session
            // cannot be open, so report it closed instead of blocking cleanup.
            if (session.status !== 'closed' && !await binding.fs.driver.exists(binding.rootPath)) return closedSessionStat(sessionId);
            return sessionStat(session, session.status === 'closing' && !await this.managedResources.canClose(sessionId));
        } catch (error) {
            if (isMissingStorage(error)) return closedSessionStat(sessionId);
            throw error;
        }
    }
    readonly programs = new ProgramRegistry();
    readonly effects = new EffectRegistry();
    readonly storageResolvers = new StorageResolverRegistry();
    readonly workspaces = new WorkspaceRegistry();
    private readonly eventsBus = new EventBus<KernelEvents>();
    private readonly sessions = new Map<SessionId, ResolvedStorageBinding>();
    private readonly catalogFs: IFileSystem;
    private readonly catalogRoot: string;
    private catalogListener?: () => void;
    private readonly commitSubscriptions = new SeqCommitSubscriptions();
    private readonly storageListeners = new Map<SessionId, () => void>();
    private readonly requestedDrains = new Set<SessionId>();
    private readonly draining = new Set<SessionId>();
    private readonly waitingDrains = new Set<SessionId>();
    private readonly store: SeqFileKernelStore;
    /** Task list read during the current poll tick, shared until nextWakeDelay consumes it. */
    private readonly tickTasks = new Map<SessionId, TaskRecord[]>();
    /** Resource deadlines already computed by this tick's sweeps, reused instead of re-reading. */
    private readonly tickManaged = new Map<SessionId, { kernel: number | undefined; session: number | undefined }>();
    private readonly workerId: string;
    private readonly maxConcurrent: number;
    private readonly maxConcurrentEffects: number;
    private readonly leaseMs: number;
    private readonly effectCleanup: EffectCleanupRunner;
    private readonly effectWorkSlots: EffectWorkSlots;
    private readonly poller: DurablePoller<SessionId>;
    private readonly heartbeats = new Set<LeaseHeartbeat>();
    private readonly effectControllers = new Map<string, AbortController>();
    private readonly plugins = new Map<string, KernelPlugin>();
    private readonly onDiagnostic?: KernelOptions['onDiagnostic'];
    private shutdownWork?: Promise<ShutdownReport>;
    private drainWork?: Promise<import('../domain/types').WorkerDrainReport>;
    private drainingWorker = false;
    private readonly wakeSource?: import('../domain/types').KernelWakeSource;
    private wakeListener?: () => void;
    private rescanner?: CommitRescanner;
    private readonly rescanMs: number;
    private disposed = false;
    private readonly lifetime = new AbortController();
    private initialized = false;
    private admittedOperations = 0;
    private active = 0;
    private activeEffects = 0;
    private readonly reducerControllers = new Map<string, { sessionId: string; taskId: string; controller: AbortController }>();

    constructor(options: KernelOptions) {
        this.authorizeTaskCall = options.authorizeTaskCall;
        this.programs.register(taskCallProgram); this.programs.register(taskCallProgramV2);
        this.effects.register(new TaskCallEffect({
            prepare: (request, context) => this.prepareTaskCall(request, context),
            binding: async context => (await this.inspectSession(context.sessionId)).getShared(taskCallKey(context)),
            target: (request, binding, phase) => this.taskCallTarget(request, binding, phase),
            snapshot: async id => (await this.inspectSession(id)).snapshot(),
            resourcesSettled: (id, tasks) => this.managedResources.canSettleTasks(id, tasks),
            cancel: async (id, taskId, remaining) => {
                const requestId = `owned-stop:${taskId}`;
                if (remaining) {
                    const receipt = await this.readTaskTreeCancellationReceipt(id, taskId, requestId), task = await this.task(id, taskId);
                    await this.cancelRemainingTaskTree(id, taskId, {requestId, expectedEpoch: receipt?.expectedEpoch ?? task.control?.epoch ?? 0});
                } else await this.cancel(id, taskId, 'Owning Task call cancelled', {requestId});
            },
            changed: (id, signal) => waitForChange(this, id, undefined, 250, signal),
            diagnose: event => this.diagnose(event),
        }));
        this.programs.register(scheduleProgram);
        this.onDiagnostic = options.onDiagnostic;
        this.workerId = options.workerId ?? createId('worker');
        this.maxConcurrent = options.maxConcurrent ?? 4;
        this.maxConcurrentEffects = options.maxConcurrentEffects ?? 4;
        this.leaseMs = options.leaseMs ?? 30_000;
        this.effectCleanup = new EffectCleanupRunner(options.effectCleanupTimeoutMs ?? 30_000);
        this.rescanMs = scanInterval(options);
        const pollMs = options.pollMs ?? 0;
        this.wakeSource = options.wakeSource;
        if (!Number.isSafeInteger(this.maxConcurrent) || this.maxConcurrent < 0) throw new Error('Kernel maxConcurrent must be a non-negative safe integer');
        if (!Number.isSafeInteger(this.maxConcurrentEffects) || this.maxConcurrentEffects < 0) throw new Error('Kernel maxConcurrentEffects must be a non-negative safe integer');
        if (!Number.isSafeInteger(this.leaseMs) || this.leaseMs <= 0 || this.leaseMs > Number.MAX_SAFE_INTEGER - Date.now()) throw new Error('Kernel leaseMs must be a positive safe duration');
        if (!Number.isSafeInteger(pollMs) || pollMs < 0) throw new Error('Kernel pollMs must be a non-negative safe integer');
        this.effectWorkSlots = new EffectWorkSlots(this.maxConcurrentEffects, () => {
            for (const id of this.sessions.keys()) this.schedulePoll(id);
        });
        const catalog = { fs: options.catalog.fs, rootPath: options.catalog.rootPath ?? '/.config/kernel' };
        this.catalogFs = catalog.fs;
        this.catalogRoot = catalog.rootPath;
        this.store = new SeqFileKernelStore(catalog, reference => this.resolveStorage(reference));
        this.archives = createArchiveApi({catalog, inspect: id => this.store.inspectSessionBinding(id),
            resolve: ref => this.resolveStorage(ref), hasProgram: ref => this.programs.has(ref.kind, ref.version),
            hasEffect: ref => this.effects.has(ref.kind, ref.version), assertRunning: () => this.assertRunning(),
            diagnose: event => this.diagnose(event)});
        this.managedResources = new ManagedResourceStore(catalog, id => this.binding(id),
            async () => (await this.store.listSessions()).map(session => session.id),
            id => this.store.inspectSessionBinding(id));
        this.resourcePoller = new DurablePoller({
            intervalMs: 0,
            poll: async scope => {
                const at = await this.managedResources.sweep(scope);
                return { nextDelay: at === undefined ? undefined : Math.max(0, at - Date.now()) };
            },
            onError: (_scope, error) => this.handlePollError(error),
        });
        this.poller = new DurablePoller({
            intervalMs: pollMs,
            poll: sessionId => this.poll(sessionId),
            nextDelay: sessionId => this.nextWakeDelay(sessionId),
            onError: (_sessionId, error) => this.handlePollError(error),
        });
    }
    private async prepareTaskCall(request: TaskCallRequest, context: import('../domain/types').EffectExecutionContext): Promise<TaskCallBinding> {
        await this.authorizeTaskCall?.(taskCallPlan(request),
            {sessionId: context.sessionId, taskId: context.taskId, effectId: context.effectId}, 'execute');
        const owner = await this.taskCallOwner(context, request.maxDepth);
        const session = await this.store.ensureSessionInstance(await this.binding(request.targetSessionId));
        return {version: 1, targetSessionId: request.targetSessionId, instanceId: session.instanceId!, fingerprint: taskCallFingerprint(request),
            owner};
    }
    private async taskCallTarget(request: TaskCallRequest, owner: TaskCallBinding, phase: 'execute' | 'cleanup'): Promise<TaskHandle> {
        await this.authorizeTaskCall?.(taskCallPlan(request),
            {sessionId: owner.owner.sessionId, taskId: owner.owner.taskId, effectId: owner.owner.effectId}, phase);
        const expected = await this.taskCallOwner(owner.owner, request.maxDepth);
        if (canonical(expected) !== canonical(owner.owner))
            throw kernelError(KernelErrorCode.TASK_CALL_BINDING_INVALID, 'Owned Task scope changed');
        const binding = await this.binding(request.targetSessionId);
        const task = await this.store.createTask(binding, request.targetSessionId, taskCallTargetSpec(request, owner.owner), undefined,
            {taskId: taskCallTargetId(owner.owner), instanceId: owner.instanceId, owner: owner.owner});
        if (task.id !== taskCallTargetId(owner.owner) || task.rootTaskId !== task.id || task.sessionId !== request.targetSessionId
            || canonical(task.ownedBy) !== canonical(owner.owner))
            throw kernelError(KernelErrorCode.TASK_CALL_TARGET_INVALID, 'Owned Task target identity changed');
        this.notify(request.targetSessionId, task.id); this.queueDrain(request.targetSessionId);
        return new DefaultTaskHandle(this, request.targetSessionId, task.id);
    }
    private async taskCallOwner(context: Pick<import('../domain/types').EffectExecutionContext, 'sessionId' | 'taskId' | 'effectId'>, requested: number) {
        const caller = await this.task(context.sessionId, context.taskId), root = caller.id === caller.rootTaskId ? caller : await this.task(context.sessionId, caller.rootTaskId);
        const previous = root.ownedBy;
        if (previous && (!Number.isSafeInteger(previous.depth) || !Number.isSafeInteger(previous.maxDepth)
            || previous.depth < 1 || previous.maxDepth < previous.depth))
            throw kernelError(KernelErrorCode.TASK_CALL_BINDING_INVALID, 'Invalid inherited owned Task scope');
        const depth = (previous?.depth ?? 0) + 1, maxDepth = Math.min(requested, previous?.maxDepth ?? requested);
        if (!Number.isSafeInteger(depth) || depth > maxDepth) throw kernelError(KernelErrorCode.BUDGET_EXCEEDED, 'Owned Task call depth exceeded');
        return {sessionId: context.sessionId, taskId: context.taskId, effectId: context.effectId, depth, maxDepth};
    }

    async resolveEffect(sessionId: string, taskId: string, request: import('../domain/types').EffectResolution): Promise<void> {
        this.assertRunning();
        await this.store.resolveEffect(await this.binding(sessionId), taskId, request);
        this.notify(sessionId, taskId); this.queueDrain(sessionId);
    }

    async sendTaskMessage(sessionId: string, taskId: string, request: import('../domain/types').TaskMessageRequest) {
        this.assertRunning();
        const message = await this.store.sendTaskMessage(await this.binding(sessionId), taskId, request);
        this.notify(sessionId, undefined, 'content'); this.queueDrain(sessionId);
        return message;
    }

    async createCache(sessionId: string, taskId: string, spec: import('../domain/cache').CacheSpec) { return this.store.createCache(await this.binding(sessionId), taskId, spec); }
    async readCache(sessionId: string, taskId: string, request: import('../domain/cache').CacheRead) { return this.store.readCache(await this.binding(sessionId), taskId, request); }
    async publishCache(sessionId: string, taskId: string, request: import('../domain/cache').CachePublish) { return this.store.publishCache(await this.binding(sessionId), taskId, request); }
    async invalidateCache(sessionId: string, taskId: string, handleId: string, expectedGeneration: number) { return this.store.invalidateCache(await this.binding(sessionId), taskId, handleId, expectedGeneration); }
    async renewCache(sessionId: string, taskId: string, handleId: string, expectedGeneration: number, ttlMs: number) { return this.store.renewCache(await this.binding(sessionId), taskId, handleId, expectedGeneration, ttlMs); }
    async listCaches(sessionId: string, taskId: string) { return this.store.listCaches(await this.binding(sessionId), taskId); }

    async initialize(): Promise<void> {
        this.assertRunning();
        await this.store.initialize(); await this.managedResources.initialize();
        this.assertRunning();
        this.initialized = true;
        this.catalogListener?.();
        this.catalogListener = this.catalogFs.on('seq:committed', event => {
            const paths = event.payload.paths.map(path => pathUtils.normalize(path));
            const catalogChanged = paths.includes(pathUtils.normalize(catalogPath(this.catalogRoot)));
            const resourcesChanged = paths.includes(pathUtils.normalize(resourcesPath(this.catalogRoot)));
            // Resource-only writes wake cleanup without rescanning every Session.
            if (catalogChanged || resourcesChanged) this.resourcePoller.start('kernel');
            if (!catalogChanged) return;
            for (const id of this.sessions.keys()) this.schedulePoll(id);
        });
        this.resourcePoller.start('kernel');
        this.wakeListener?.();
        this.wakeListener = this.wakeSource?.subscribe(id => {
            if (this.disposed || this.drainingWorker || this.shutdownWork) return;
            for (const session of id ? [id] : this.sessions.keys()) if (this.sessions.has(session)) this.schedulePoll(session);
            this.resourcePoller.start('kernel');
        });
        this.rescanner?.dispose();
        this.rescanner = new CommitRescanner(this.rescanMs, () => this.commitScopes(), (id, reason) => {
            if (id === '$kernel') this.resourcePoller.start('kernel');
            else { this.notify(id, undefined, reason); this.schedulePoll(id); this.resourcePoller.start(`session:${id}`); }
        }, error => this.handlePollError(error));
    }
    get isDisposed(): boolean { return this.disposed; }
    get abortSignal(): AbortSignal { return this.lifetime.signal; }

    dispose(): void {
        this.disposed = true;
        this.lifetime.abort();
        this.tickTasks.clear();
        this.tickManaged.clear();
        this.waitingDrains.clear();
        this.poller.dispose();
        this.resourcePoller.dispose();
        this.managedResources.dispose();
        this.catalogListener?.();
        this.wakeListener?.();
        this.rescanner?.dispose();
        for (const off of this.storageListeners.values()) off();
        this.storageListeners.clear();
        for (const heartbeat of this.heartbeats) heartbeat.stop();
        this.heartbeats.clear();
        for (const { controller } of this.reducerControllers.values()) controller.abort(new Error('Worker disposed'));
        for (const controller of this.effectControllers.values()) controller.abort();
        this.effectControllers.clear();
    }

    /** 等待所有 in-flight 的 drain/execute 完成（dispose 后调用，避免上层过早关闭存储后端）。 */
    async waitIdle(timeoutMs = 5000): Promise<void> {
        if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0) throw new Error('Invalid idle timeout');
        const startedAt = Date.now();
        while (this.admittedOperations > 0 || this.active > 0 || this.activeEffects > 0 || this.managedObservations.size > 0 || this.draining.size > 0 || !this.poller.isIdle || !this.resourcePoller.isIdle || !this.managedResources.isIdle || this.rescanner?.isIdle === false) {
            if (Date.now() - startedAt >= timeoutMs) throw kernelError(KernelErrorCode.IDLE_TIMEOUT, 'Kernel still has in-flight operations');
            await new Promise(resolve => setTimeout(resolve, 10));
        }
    }
    /** Persist Session pauses, drain external work, then release this worker. Resume explicitly after recovery. */
    shutdown(options: ShutdownOptions = {}): Promise<ShutdownReport> {
        if (this.drainingWorker) return Promise.reject(kernelError(KernelErrorCode.CONFLICT, 'Worker drain already started'));
        if (this.shutdownWork) return this.shutdownWork;
        const timeoutMs = options.timeoutMs ?? 5000;
        if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0) return Promise.reject(new Error('Invalid shutdown timeout'));
        const started = Date.now(), operationId = createId('shutdown');
        const work = shutdown({ ids: [...this.sessions.keys()], stat: id => this.sessionStat(id),
            suspend: id => this.changeSessionStatus(id, 'suspended'), issues: id => this.recoveryIssues(id),
            dispose: () => this.dispose(), waitIdle: timeout => this.waitIdle(timeout) }, timeoutMs);
        this.shutdownWork = work.then(result => {
            this.diagnose({ operation: 'shutdown', stage: 'complete', outcome: result.status === 'stopped' ? 'success' : 'blocked',
                operationId, durationMs: Date.now() - started, errorCode: result.status === 'blocked' ? 'STOP_UNCONFIRMED' : undefined });
            return result;
        }, error => {
            this.diagnose({ operation: 'shutdown', stage: 'storage', outcome: 'failure', operationId,
                durationMs: Date.now() - started, errorCode: diagnosticCode(error) });
            throw error;
        });
        return this.shutdownWork;
    }

    /** Begin irreversible host retirement while accepted producers finish their metadata checkpoints. */
    beginObservationDrain(): void {
        this.detachObservations = true;
        for (const observer of this.drainingObservations) observer.abort(this.observationDrainReason);
    }

    /** Stop admission and polling; retain storage and leases until owned work reaches a boundary. */
    quiesce(options: ShutdownOptions & {detachObservations?:boolean} = {}): Promise<import('../domain/types').WorkerDrainReport> {
        if (this.drainWork) return this.drainWork;
        if (this.shutdownWork) return Promise.reject(kernelError(KernelErrorCode.CONFLICT, 'Session shutdown already started'));
        const timeoutMs = options.timeoutMs ?? 5000;
        if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0) return Promise.reject(kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid drain timeout'));
        const started = Date.now(), operationId = createId('drain');
        this.drainingWorker = true; this.poller.dispose(); this.resourcePoller.dispose(); this.rescanner?.dispose();
        if (options.detachObservations) this.beginObservationDrain();
        const work = quiesceWorker(this, timeoutMs).then(result => {
            this.diagnose({operation: 'worker.drain', stage: 'complete', outcome: result.localIdle ? 'success' : 'blocked',
                operationId, durationMs: Date.now() - started, errorCode: result.localIdle ? undefined : 'IDLE_TIMEOUT'});
            if (!result.localIdle) this.drainWork = undefined;
            return result;
        });
        this.drainWork = work;
        return work;
    }

    /** Retire a quiescent worker without changing shared Session controls or cancelling accepted work. */
    async drain(options: ShutdownOptions = {}): Promise<import('../domain/types').WorkerDrainReport> {
        const result = await this.quiesce(options);
        if (result.localIdle) this.dispose();
        return result;
    }

    async recoveryIssues(sessionId: string, taskId?: string) {
        const tasks = taskId ? [await this.task(sessionId, taskId)] : await this.store.listTasks(await this.store.inspectSessionBinding(sessionId));
        return tasks.flatMap(task => taskRecoveryIssues(task, { program: (kind, version) => this.programs.has(kind, version),
            effect: (kind, version) => this.effects.has(kind, version) }));
    }

    private async admit<T>(operation: () => Promise<T>): Promise<T> {
        this.assertRunning();
        this.admittedOperations++;
        try { return await operation(); }
        finally { this.admittedOperations--; }
    }

    private assertRunning(): void {
        if (this.disposed || this.shutdownWork || this.drainingWorker) throw kernelError(KernelErrorCode.DISPOSED, 'Kernel worker is stopped or shutting down');
    }

    private diagnose(event: KernelDiagnostic): void {
        try {
            if (this.onDiagnostic) this.onDiagnostic(event);
            else if (event.outcome !== 'success') console.error('Kernel operation requires attention', event);
        } catch { console.error('Kernel diagnostic observer failed', { operationId: event.operationId }); }
    }

    registerProgram(program: DurableTaskProgram): void { this.programs.register(program); for (const id of this.sessions.keys()) this.schedulePoll(id); }
    registerResourceAdapter(adapter: import('../domain/resource-api').ManagedResourceAdapter): void {
        this.managedResources.registerAdapter(adapter);
        if (!this.initialized) return;
        this.resourcePoller.start('kernel');
        for (const id of this.sessions.keys()) this.resourcePoller.start(`session:${id}`);
    }
    registerEffect(adapter: EffectAdapter): void { this.effects.register(adapter); for (const id of this.sessions.keys()) this.schedulePoll(id); }
    registerStorageResolver(resolver: SessionStorageResolver): void { this.storageResolvers.register(resolver); }
    registerWorkspace(adapter: WorkspaceAdapter): void { this.workspaces.register(adapter); }

    async use(plugin: KernelPlugin): Promise<void> {
        const key = `${plugin.id}@${plugin.version}`;
        if (this.plugins.has(key)) throw new Error(`Kernel plugin already installed: ${key}`);
        await plugin.install(this);
        this.plugins.set(key, plugin);
    }

    async createSession(spec: { id?: string; storage: StorageBindingRef }): Promise<SessionHandle> {
        return this.admit(async () => {
            const id = spec.id ?? createId('session');
            await this.store.createSession(id, spec.storage);
            const binding = await this.resolveStorage(spec.storage);
            this.assertRunning();
            this.rememberBinding(id, binding);
            this.schedulePoll(id);
            return new DefaultSessionHandle(this, id);
        });
    }

    async openSession(id: SessionId, expectedStorage?: StorageBindingRef): Promise<SessionHandle> {
        return this.admit(async () => {
            const opened = await this.store.openSession(id, { expectedStorage, previousBinding: this.sessions.get(id) });
            this.assertRunning();
            this.rememberBinding(id, opened.binding);
            this.queueDrain(id);
            this.schedulePoll(id);
            return new DefaultSessionHandle(this, id);
        });
    }

    private readonly sessionReopens = new Map<string, Promise<SessionHandle>>();

    /** Admit new Tasks after an explicit user rerun, without reviving any previous Task. */
    async reopenSession(id: SessionId): Promise<SessionHandle> {
        this.assertRunning();
        const pending = this.sessionReopens.get(id);
        if (pending) return pending;
        const work = this.reopenClosedSession(id);
        this.sessionReopens.set(id, work);
        try { return await work; }
        finally { if (this.sessionReopens.get(id) === work) this.sessionReopens.delete(id); }
    }

    private async reopenClosedSession(id: SessionId): Promise<SessionHandle> {
        const binding = await this.binding(id);
        const session = await this.store.sessionRecord(binding);
        if (session.status === 'open') return new DefaultSessionHandle(this, id);
        if (session.status !== 'closed') throw new Error(`Cannot reopen session: ${session.status}`);
        await this.sessionClosures.get(id);
        await Promise.all([...this.plugins.values()].map(plugin => plugin.onSessionClosed?.(id)));
        await this.store.reopenSession(binding);
        this.rememberBinding(id, binding);
        this.notify(id); this.queueDrain(id); this.schedulePoll(id);
        return new DefaultSessionHandle(this, id);
    }

    async *listSessions(): AsyncIterable<SessionRecord> {
        for (const session of await this.store.listSessions()) yield session;
    }

    /** Cold lookup of explicitly indexed recovery responsibilities, never historical Sessions. */
    listRecoveryTasks(scope: string) { return this.traceRecovery('list-tasks', () => this.store.listRecoveryTasks(scope)); }
    listRecoveryShared(scope: string) { return this.traceRecovery('list-shared', () => this.store.listRecoveryShared(scope)); }

    async acknowledgeTaskRecovery(sessionId: string, taskId: string, scope: string): Promise<void> {
        await this.traceRecovery('acknowledge-task', async () =>
            this.store.acknowledgeTaskRecovery(await this.store.inspectSessionBinding(sessionId), taskId, scope));
    }

    private async traceRecovery<T>(stage: string, run: () => Promise<T>): Promise<T> {
        const started = Date.now(), operationId = createId('recovery');
        try {
            const result = await run();
            this.diagnose({operation: 'recovery.index', operationId, stage, outcome: 'success', durationMs: Date.now() - started});
            return result;
        } catch (error) {
            this.diagnose({operation: 'recovery.index', operationId, stage, outcome: 'failure', errorCode: diagnosticCode(error), durationMs: Date.now() - started});
            throw error;
        }
    }

    /** Inspect persisted records without registering listeners or starting execution. */
    async sessionActivity(id: SessionId) {
        const started = Date.now(), operationId = createId('activity');
        try {
            const activity = await readSessionActivity(await this.store.inspectSessionBinding(id));
            this.diagnose({operation: 'session.activity', operationId, sessionId: id, stage: 'read', outcome: 'success', durationMs: Date.now() - started});
            return activity;
        } catch (error) {
            if (error instanceof KernelError && error.code === KernelErrorCode.SESSION_NOT_FOUND && !this.onDiagnostic) throw error;
            this.diagnose({operation: 'session.activity', operationId, sessionId: id, stage: 'read', outcome: 'failure',
                durationMs: Date.now() - started, errorCode: diagnosticCode(error)});
            throw error;
        }
    }

    async inspectSession(id: SessionId) {
        const { record, binding } = await this.store.inspectSessionStorage(id);
        // The catalog binding reference is returned alongside the resolved binding so a caller can
        // observe one Session without enumerating every other Session record.
        return { id, storage: record.storage, listTasks: () => this.store.listTasks(binding),
            snapshot: () => sessionEventSnapshot(binding),
            readEvents: (query?: import('../domain/types').EventQuery) => eventPage(binding, query),
            listTaskPage: (query?: import('../domain/types').TaskListQuery) => this.store.listTaskPage(binding, query),
            listPendingInteractionTasks: () => this.store.listPendingInteractionTasks(binding),
            getShared: <T extends import('../domain/types').JsonValue>(key: string) => this.store.getShared<T>(binding, key),
            attachTask: async <O = unknown>(taskId: string): Promise<TaskHandle<O>> => {
                await this.store.readTask(binding, taskId);
                return new DefaultTaskHandle<O>(this, id, taskId);
            } };
    }

    async openTask<O = unknown>(id: TaskId): Promise<TaskHandle<O>> {
        this.assertRunning();
        const sessionId = await this.store.locateTask(id);
        if (!this.sessions.has(sessionId)) await this.openSession(sessionId);
        return new DefaultTaskHandle<O>(this, sessionId, id);
    }

    async inspectTask(id: TaskId): Promise<import('../domain/types').TaskEventSnapshot> {
        const sessionId = await this.store.locateTask(id);
        return this.taskSnapshot(sessionId, id);
    }

    async attachTask<O = unknown>(sessionId: SessionId, taskId: TaskId): Promise<TaskHandle<O>> {
        const task = await this.store.readTask(await this.store.inspectSessionBinding(sessionId), taskId);
        if (task.sessionId !== sessionId) throw new Error(`Task ${taskId} does not belong to session ${sessionId}`);
        return new DefaultTaskHandle<O>(this, sessionId, taskId);
    }

    async listSessionTasks(sessionId: SessionId): Promise<TaskRecord[]> {
        return this.store.listTasks(this.sessions.get(sessionId) ?? await this.store.inspectSessionBinding(sessionId));
    }

    /** Read pending interactions without enumerating completed Task directories. */
    async listSessionPendingInteractionTasks(sessionId: SessionId): Promise<TaskRecord[]> {
        return this.store.listPendingInteractionTasks(await this.binding(sessionId));
    }

    /** Restore durable state without clearing explicit Task/Session pause controls. */
    async recoverSession(sessionId: SessionId, options: import('../domain/types').RecoveryOptions = {}): Promise<RecoveryReport> {
        return this.admit(async () => {
            if (options.takeover && (this.active || this.activeEffects || this.draining.size || !this.poller.isIdle))
                throw new Error('Takeover recovery requires an idle Kernel before opening sessions');
            const opened = await this.store.openSession(sessionId);
            // `openSession` already read the record; pass it so recovery does not read it again.
            const report = await this.store.recover(opened.binding, options, opened.record);
            await this.managedResources.recover('kernel', options.takeover);
            await this.managedResources.recover(`session:${sessionId}`, options.takeover);
            this.assertRunning();
            this.rememberBinding(sessionId, opened.binding);
            await this.poll(sessionId);
            this.schedulePoll(sessionId);
            return report;
        });
    }

    async recover(options: import('../domain/types').RecoveryOptions = {}): Promise<RecoveryReport> {
        const report = await this.recoverSessions((await this.store.listSessions()).map(session => session.id), options);
        await this.relayPendingMessages();
        return report;
    }

    /** Restore all leased Sessions before starting any of their workers. */
    async recoverSessions(sessionIds: SessionId[], options: import('../domain/types').RecoveryOptions = {}): Promise<RecoveryReport> {
        return this.admit(async () => {
            const total: RecoveryReport = {
                recoveredTasks: 0, recoveredEffects: 0, expiredAttempts: 0, rebuiltIndexes: 0,
            };
            if (options.takeover && (this.active || this.activeEffects || this.draining.size || !this.poller.isIdle))
                throw new Error('Takeover recovery requires an idle Kernel before opening sessions');
            const restored: Array<{ id: string; binding: ResolvedStorageBinding; stateMs: number }> = [];
            for (const id of new Set(sessionIds)) {
                const started = performance.now();
                const opened = await this.store.openSession(id);
                mergeReport(total, await this.store.recover(opened.binding, options, opened.record));
                restored.push({ id, binding: opened.binding, stateMs: performance.now() - started });
            }
            await this.managedResources.recover('kernel', options.takeover);
            for (const { id, stateMs } of restored) {
                const started = performance.now();
                await this.managedResources.recover(`session:${id}`, options.takeover);
                options.onSessionRecovered?.(id, { state: stateMs, resources: performance.now() - started });
            }
            for (const { id, binding } of restored) {
                this.assertRunning();
                this.rememberBinding(id, binding);
                this.schedulePoll(id);
            }
            return total;
        });
    }

    async submit<I, O>(sessionId: string, spec: import('../domain/types').TaskSpec<I>, options?: import('../domain/types').LeaseGuardOptions): Promise<TaskHandle<O>> {
        return this.admit(async () => {
            const binding = await this.binding(sessionId);
            this.assertRunning();
            const task = await this.store.createTask(binding, sessionId, spec, options);
            this.notify(sessionId, task.id);
            this.queueDrain(sessionId);
            return new DefaultTaskHandle<O>(this, sessionId, task.id);
        });
    }

    /** Read the original submission admission without opening a worker or retrying execution. */
    async readTaskSubmissionReceipt(sessionId: string, spec: import('../domain/types').TaskSpec): Promise<import('../domain/types').TaskSubmissionReceipt | undefined> {
        const started = Date.now(), supplied = spec?.requestId;
        const operationId = typeof supplied === 'string' && /^[\w:-]{1,256}$/.test(supplied) ? supplied : createId('submission-inspect');
        let stage = 'validate';
        try {
            const expected = structuredClone(spec); stage = 'lookup';
            const binding = await this.store.inspectSessionBinding(sessionId);
            const receipt = await this.store.readTaskSubmissionReceipt(binding, sessionId, expected);
            this.diagnose({operation: 'task.submission.inspect', stage, outcome: 'success', operationId, sessionId,
                taskId: receipt?.task.taskId, durationMs: Date.now() - started}); return receipt;
        } catch (error) {
            this.diagnose({operation: 'task.submission.inspect', stage, outcome: 'failure', operationId, sessionId,
                durationMs: Date.now() - started, errorCode: diagnosticCode(error)}); throw error;
        }
    }

    /** Cold ownership lookup only; never creates the target or advances execution. */
    async readTaskCallTarget(ref: Pick<import('../domain/task-call').TaskCallOwner, 'sessionId' | 'taskId' | 'effectId'>): Promise<import('../domain/task-call').TaskRef | undefined> {
        const started = Date.now(), progress = {stage: 'validate'}, operationId = createId('task-call-inspect');
        const safe = (value: unknown) => typeof value === 'string' && /^[\w:-]{1,256}$/.test(value) ? value : undefined;
        const identity = {sessionId: safe(ref?.sessionId), taskId: safe(ref?.taskId), effectId: safe(ref?.effectId)};
        let errorCode: string | undefined;
        try {
            const owner = structuredClone(ref);
            return await inspectTaskCallTarget({task: (id, task) => this.task(id, task),
                target: async (id, instance, task) => this.store.readTaskInSessionInstance(await this.store.inspectSessionBinding(id), id, instance, task),
                binding: async scope => (await this.inspectSession(scope.sessionId)).getShared(taskCallKey(scope)),
                owner: (scope, depth) => this.taskCallOwner(scope, depth)}, owner, progress);
        } catch (error) {errorCode = diagnosticCode(error); throw error;}
        finally {this.diagnose({operation: 'task.call.inspect', stage: progress.stage, outcome: errorCode ? 'failure' : 'success',
            operationId, ...identity, durationMs: Date.now() - started, errorCode});}
    }

    /** Host authorization grants this exact immutable plan, then starts its deferred caller. */
    async bindTaskCall(ref: import('../domain/task-call').TaskRef, plan: import('../domain/task-call').TaskCallPlan): Promise<void> {
        this.assertRunning();
        const task = await this.attachTask(ref.sessionId, ref.taskId);
        await bindCapabilities(task, [{kind: 'task', uri: taskCallCapabilityUri(plan), rights: ['execute'], signalKey: 'taskHandleId'}]);
    }

    async schedule(sessionId: string, spec: import('../domain/schedule').ScheduleSpec): Promise<TaskHandle> {
        const started = Date.now(), operationId = typeof spec?.requestId === 'string' ? spec.requestId : createId('schedule');
        let stage = 'validate';
        try {
            const taskSpec = scheduleTaskSpec(spec); stage = 'persist';
            const task = await this.submit(sessionId, taskSpec);
            this.diagnose({operation: 'schedule.submit', stage: 'accepted', outcome: 'success', operationId, sessionId,
                taskId: task.id, durationMs: Date.now() - started});
            return task;
        } catch (error) {
            this.diagnose({operation: 'schedule.submit', stage, outcome: 'failure', operationId, sessionId,
                durationMs: Date.now() - started, errorCode: diagnosticCode(error)});
            throw error;
        }
    }

    async retryTask<O = unknown>(sessionId: string, taskId: string, options: LeaseGuardOptions & { requestId: string }): Promise<TaskHandle<O>> {
        if (typeof options.requestId !== 'string' || !options.requestId.trim()) throw new Error('Manual retry requires requestId');
        const original = await this.task(sessionId, taskId);
        return this.submit(sessionId, {
            requestId: `retry:${JSON.stringify([taskId, options.requestId])}`,
            retryOfTaskId: taskId, program: original.program, input: original.input,
            dependsOn: original.dependencies, retry: original.retry, priority: original.priority,
            labels: original.labels, deferStart: true,
        }, options);
    }

    async task(sessionId: string, taskId: string): Promise<TaskRecord> {
        return this.store.readTask(await this.store.inspectSessionBinding(sessionId), taskId);
    }

    /** Retention/GC for Task version history; see `SeqFileKernelStore.compactTaskHistory`. */
    async compactTaskHistory(
        sessionId: string,
        taskId: string,
        options: { keepVersions?: number; beforeVersion?: number } = {},
    ): Promise<{ removed: number; keptFrom: number }> {
        return this.store.compactTaskHistory(await this.binding(sessionId), taskId, options);
    }

    async taskHistory(sessionId: string, taskId: string, afterVersion = -1): Promise<TaskRecord[]> {
        return this.store.taskHistory(await this.binding(sessionId), taskId, afterVersion);
    }

    async taskHistoryPage(sessionId: string, taskId: string, query: import('../domain/types').TaskHistoryQuery = {}): Promise<import('../domain/types').TaskHistoryPage> {
        return this.store.taskHistoryPage(await this.store.inspectSessionBinding(sessionId), taskId, query);
    }

    async taskAttempts(sessionId: string, taskId: string): Promise<import('../domain/types').TaskAttempt[]> {
        return this.store.taskAttempts(await this.binding(sessionId), taskId);
    }

    async signal(sessionId: string, taskId: string, signal: TaskSignal, options?: import('../domain/types').SignalOptions): Promise<import('../domain/types').SignalReceipt> {
        this.assertRunning();
        const started = Date.now(), operationId = options?.requestId ?? createId('signal');
        try {
            const receipt = await this.store.signalTask(await this.binding(sessionId), taskId, signal, options);
            this.notify(sessionId, taskId); this.queueDrain(sessionId);
            this.diagnose({operation: 'task.signal', stage: receipt.replayed ? 'replayed' : 'accepted', outcome: 'success',
                operationId, sessionId, taskId, durationMs: Date.now() - started});
            return receipt;
        } catch (error) {
            this.diagnose({operation: 'task.signal', stage: 'persist', outcome: 'failure', operationId, sessionId, taskId,
                durationMs: Date.now() - started, errorCode: diagnosticCode(error)});
            throw error;
        }
    }

    /** Cold inspection: no Session activation, scheduler wakeup or control replay. */
    async readTaskControlReceipt(sessionId: string, taskId: string, requestId: string): Promise<import('../domain/types').TaskControlReceipt | undefined> {
        return this.store.readControlReceipt(await this.store.inspectSessionBinding(sessionId), taskId, requestId);
    }

    async controlTask(sessionId: string, taskId: string, mode: import('../domain/types').TaskControl['mode'],
        options: import('../domain/types').TaskControlOptions & { signal?: TaskSignal }): Promise<import('../domain/types').TaskControl> {
        return this.changeTaskControl(sessionId, taskId, mode, options);
    }

    withdrawTaskPause(sessionId: string, taskId: string, options: import('../domain/types').TaskPauseWithdrawalOptions) {
        return this.changeTaskControl(sessionId, taskId, 'withdraw-pause', options);
    }

    private async changeTaskControl(sessionId: string, taskId: string,
        mode: import('../infrastructure/seqfile/task-control-store').TaskControlAction,
        options: import('../domain/types').TaskControlOptions & { signal?: TaskSignal }): Promise<import('../domain/types').TaskControl> {
        this.assertRunning();
        const started = Date.now(), operationId = options.requestId || createId('control');
        try {
            const control = await this.store.controlTask(await this.binding(sessionId), taskId, mode, options);
            await this.abortFencedReducers(sessionId);
            if (mode === 'interrupt') await this.abortInterruptedEffects(sessionId);
            this.notify(sessionId, taskId); this.queueDrain(sessionId); this.schedulePoll(sessionId);
            this.diagnose({ operation: `task.${mode}`, stage: 'persisted', outcome: 'success', operationId,
                sessionId, taskId, durationMs: Date.now() - started });
            return control;
        } catch (error) {
            this.diagnose({ operation: `task.${mode}`, stage: 'admission', outcome: 'failure', operationId,
                sessionId, taskId, durationMs: Date.now() - started, errorCode: diagnosticCode(error) });
            throw error;
        }
    }

    private async abortInterruptedEffects(sessionId: string): Promise<void> {
        for (const task of await this.store.listTasks(await this.binding(sessionId))) {
            for (const [id, effect] of Object.entries(task.effects)) if (effect.cleanupPending) {
                this.effectControllers.get(effectControllerKey(sessionId, task.id, id))?.abort();
            }
        }
    }

    async startTask(sessionId: string, taskId: string, options?: import('../domain/types').TaskStartOptions): Promise<void> {
        const started = Date.now(), operationId = createId('start');
        try {
            this.assertRunning();
            await this.store.startTask(await this.binding(sessionId), taskId, options);
            this.notify(sessionId, taskId); this.queueDrain(sessionId);
            this.diagnose({operation: 'task.start', operationId, sessionId, taskId, stage: 'admission', outcome: 'success', durationMs: Date.now() - started});
        } catch (error) {
            this.diagnose({operation: 'task.start', operationId, sessionId, taskId, stage: 'admission', outcome: 'failure',
                durationMs: Date.now() - started, errorCode: diagnosticCode(error)}); throw error;
        }
    }

    async respondInteraction<T extends import('../domain/types').JsonValue>(
        sessionId: string, taskId: string, response: InteractionResponse<T>,
        options?: import('../domain/types').InteractionResponseOptions,
    ): Promise<void> {
        const started = Date.now(), supplied = options?.requestId;
        const operationId = typeof supplied === 'string' && /^[\w:-]{1,256}$/.test(supplied) ? supplied : createId('response');
        try {
            this.assertRunning();
            await this.store.resolveInteraction(await this.binding(sessionId), taskId, response, options);
            this.notify(sessionId, taskId); this.queueDrain(sessionId);
            this.diagnose({operation: 'task.interaction.respond', operationId, sessionId, taskId, stage: 'persist', outcome: 'success', durationMs: Date.now() - started});
        } catch (error) {
            this.diagnose({operation: 'task.interaction.respond', operationId, sessionId, taskId, stage: 'persist', outcome: 'failure',
                durationMs: Date.now() - started, errorCode: diagnosticCode(error)}); throw error;
        }
    }

    /** Cold receipt query; never registers a worker or resubmits an answer. */
    async readTaskInteractionResponseReceipt(sessionId: string, taskId: string, requestId: string): Promise<import('../domain/types').InteractionResponseReceipt | undefined> {
        return this.store.readInteractionResponseReceipt(await this.store.inspectSessionBinding(sessionId), taskId, requestId);
    }

    /** Immutable admission only; inspect taskStat/taskStats for cleanup acknowledgement. */
    async readTaskCancellationReceipt(sessionId: string, taskId: string, requestId: string): Promise<import('../domain/types').TaskCancellationReceipt | undefined> {
        return this.store.readCancellationReceipt(await this.store.inspectSessionBinding(sessionId), taskId, requestId);
    }

    /** Stop remaining descendants and Effects while preserving the terminal root result. */
    async cancelRemainingTaskTree(sessionId: string, taskId: string, options: import('../domain/types').TaskTreeCancellationOptions): Promise<void> {
        const started = Date.now(), operationId = options?.requestId ?? createId('tree-cancel');
        let stage = 'admission';
        try {
            this.assertRunning();
            const tasks = await this.store.cancelRemainingTaskTree(await this.binding(sessionId), taskId, options);
            stage = 'cleanup';
            await this.abortFencedReducers(sessionId);
            for (const task of tasks) this.notify(sessionId, task.id);
            this.queueDrain(sessionId);
            await settleCancellations(tasks.map(task => this.cancelTaskEffects(task, activeEffectIds(task))),
                `Failed to clean up remaining task tree rooted at ${taskId}`);
            this.diagnose({operation: 'task.cancel-remaining-tree', stage, outcome: 'success', operationId,
                sessionId, taskId, durationMs: Date.now() - started});
        } catch (error) {
            this.diagnose({operation: 'task.cancel-remaining-tree', stage, outcome: 'failure', operationId,
                sessionId, taskId, durationMs: Date.now() - started, errorCode: diagnosticCode(error)});
            throw error;
        }
    }

    /** Immutable admission only; does not register a worker or repeat cancellation. */
    async readTaskTreeCancellationReceipt(sessionId: string, taskId: string, requestId: string): Promise<import('../domain/types').TaskTreeCancellationReceipt | undefined> {
        return this.store.readTreeCancellationReceipt(await this.store.inspectSessionBinding(sessionId), taskId, requestId);
    }

    async cancel(sessionId: string, taskId: string, reason?: string, options?: import('../domain/types').TaskCancellationOptions): Promise<void> {
        const started = Date.now(), operationId = options?.requestId ?? createId('cancel');
        let stage = 'admission';
        try {
            await this.cancelTaskAndEffects(sessionId, taskId, reason, options, value => {stage = value;});
            this.diagnose({operation: 'task.cancel', stage, outcome: 'success', operationId,
                sessionId, taskId, durationMs: Date.now() - started});
        } catch (error) {
            this.diagnose({operation: 'task.cancel', stage, outcome: 'failure', operationId,
                sessionId, taskId, durationMs: Date.now() - started, errorCode: diagnosticCode(error)});
            throw error;
        }
    }

    private async cancelTaskAndEffects(sessionId: string, taskId: string, reason: string | undefined, options: import('../domain/types').TaskCancellationOptions | undefined, stage: (value: string) => void): Promise<void> {
        this.assertRunning();
        const binding = await this.binding(sessionId);
        const task = await this.store.cancelTask(binding, taskId, reason, options);
        stage('cleanup');
        if (options?.requestId && task.status !== 'cancelled') {stage('terminal-observed'); return;}
        await this.abortFencedReducers(sessionId);
        this.notify(sessionId, taskId);
        this.queueDrain(sessionId);
        await settleCancellations([
            this.cancelTaskEffects(task, activeEffectIds(task)),
            this.cancelDescendants(binding, task, reason, options),
        ], `Failed to cancel task tree rooted at ${taskId}`);
    }

    private async cancelDescendants(binding: ResolvedStorageBinding, task: TaskRecord, reason: string | undefined, options: import('../domain/types').TaskCancellationOptions | undefined): Promise<void> {
        const children = cancellationFrontier(await this.store.listTasks(binding), task.id);
        await settleCancellations(children.map(child => this.cancel(task.sessionId, child.id,
            reason ?? `Parent task ${task.id} cancelled`, options?.lease ? {lease: options.lease} : undefined)),
            `Failed to cancel descendants of task ${task.id}`);
    }

    async eventList(sessionId: string, after: number): Promise<EventEnvelope[]> {
        return this.store.events(await this.binding(sessionId), after);
    }

    async readEvents(sessionId: string, query: import('../domain/types').EventQuery = {}, taskId?: string) {
        const started = Date.now();
        try { return await eventPage(await this.store.inspectSessionBinding(sessionId), query, taskId); }
        catch (error) {
            this.diagnose({ operation: 'events.read', stage: 'journal', outcome: 'failure', operationId: createId('read'),
                sessionId, taskId, durationMs: Date.now() - started, errorCode: diagnosticCode(error) });
            throw error;
        }
    }

    async taskSnapshot(sessionId: string, taskId: string) {
        return taskEventSnapshot(await this.store.inspectSessionBinding(sessionId), taskId);
    }

    async sessionSnapshot(sessionId: string) {
        return sessionEventSnapshot(await this.store.inspectSessionBinding(sessionId));
    }

    /** Retention/GC for a Task's event log; see `SeqFileKernelStore.pruneTaskEvents`. */
    async pruneTaskEvents(
        sessionId: string,
        taskId: string,
        options: { keepEvents?: number } = {},
    ): Promise<{ removed: number; firstAvailableIndex: number }> {
        return this.store.pruneTaskEvents(await this.binding(sessionId), taskId, options);
    }

    async taskEventPage(sessionId: string, taskId: string, query: import('../domain/types').TaskEventQuery = {}): Promise<import('../domain/types').TaskEventPage> {
        return this.store.taskEventPage(await this.store.inspectSessionBinding(sessionId), taskId, query);
    }

    async listSessionTaskPage(sessionId: string, query: import('../domain/types').TaskListQuery = {}): Promise<import('../domain/types').TaskListPage> {
        return this.store.listTaskPage(await this.binding(sessionId), query);
    }

    async getShared<T extends import('../domain/types').JsonValue>(sessionId: string, key: string): Promise<SharedStateEntry<T> | undefined> {
        return this.store.getShared<T>(await this.binding(sessionId), key);
    }

    async getSharedMany(
        sessionId: string, keys: string[],
    ): Promise<Record<string, SharedStateEntry<import('../domain/types').JsonValue> | undefined>> {
        return this.store.getSharedMany(await this.binding(sessionId), keys);
    }

    async setShared<T extends import('../domain/types').JsonValue>(
        sessionId: string, key: string, value: T, options?: SharedStateWriteOptions,
    ): Promise<SharedStateEntry<T>> {
        const entry = await this.store.setShared(await this.binding(sessionId), key, value, options);
        this.notify(sessionId, options?.taskId, 'content');
        return entry;
    }

    async deleteShared(sessionId: string, key: string, options?: SharedStateWriteOptions): Promise<boolean> {
        const deleted = await this.store.deleteShared(await this.binding(sessionId), key, options);
        if (deleted) this.notify(sessionId, options?.taskId, 'content');
        return deleted;
    }

    async listShared(sessionId: string, prefix?: string): Promise<SharedStateEntry[]> {
        return this.store.listShared(this.sessions.get(sessionId) ?? await this.store.inspectSessionBinding(sessionId), prefix);
    }

    async sharedHistory<T extends import('../domain/types').JsonValue>(
        sessionId: string, key: string,
    ): Promise<SharedStateRevision<T>[]> {
        return this.store.sharedHistory<T>(await this.binding(sessionId), key);
    }

    async sendCrossSession<T extends import('../domain/types').JsonValue>(
        sourceSessionId: string,
        targetSessionId: string,
        topic: string,
        payload: T,
        options?: SessionSendOptions,
    ): Promise<CrossSessionMessage<T>> {
        return this.sendSessionMessage(sourceSessionId, { targetSessionId, topic, payload,
            idempotencyKey: options?.idempotencyKey ?? createId('message'), expiresAt: options?.expiresAt,
            targetTaskId: options?.targetTaskId, correlationId: options?.correlationId }) as Promise<CrossSessionMessage<T>>;
    }

    async sendSessionMessage(sessionId: string, request: SessionMessageRequest): Promise<CrossSessionMessage> {
        this.assertRunning();
        const started = Date.now(), operationId = createId('message-send');
        try {
            const source = await this.binding(sessionId);
            const message = await this.store.sendSessionMessage(source, sessionId, request);
            this.diagnose({ operation: 'message.send', stage: 'persisted', outcome: 'success', operationId,
                sessionId, durationMs: Date.now() - started });
            try { return await this.relayMessage(source, message); }
            catch { return (await this.store.outbox(source)).find(item => item.id === message.id) ?? message; }
        } catch (error) {
            this.diagnose({ operation: 'message.send', stage: 'enqueue', outcome: 'failure', operationId,
                sessionId, durationMs: Date.now() - started, errorCode: diagnosticCode(error) });
            throw error;
        }
    }

    async inboxPage(sessionId: string, query?: InboxQuery) {
        return this.store.inboxPage(await this.binding(sessionId), query);
    }
    async acknowledgeMessage(sessionId: string, id: string, options?: LeaseGuardOptions) {
        this.assertRunning();
        return this.store.acknowledgeSessionMessage(await this.binding(sessionId), id, options);
    }

    async outbox(sessionId: string): Promise<CrossSessionMessage[]> {
        return this.store.outbox(await this.binding(sessionId));
    }

    async inbox(sessionId: string, after = 0): Promise<CrossSessionMessage[]> {
        return this.store.inbox(await this.binding(sessionId), after);
    }

    async commitContext<T extends import('../domain/types').JsonValue>(
        sessionId: string, delta: T, options: ContextCommitOptions = {},
    ): Promise<ContextCommit<T>> {
        const commit: ContextCommit<T> = {
            id: createId('context'), sessionId, parentIds: [], delta,
            authorTaskId: options.taskId, createdAt: Date.now(),
        };
        const result = await this.store.commitContext(await this.binding(sessionId), commit, options);
        this.notify(sessionId, options.taskId, 'content');
        return result;
    }

    async getContextCommit<T extends import('../domain/types').JsonValue>(sessionId: string, id: string): Promise<ContextCommit<T> | undefined> {
        return this.store.getContextCommit<T>(await this.binding(sessionId), id);
    }

    async getContextBranch(sessionId: string, name = 'main'): Promise<ContextBranch> {
        return this.store.getContextBranch(await this.binding(sessionId), name);
    }

    async contextHistory(sessionId: string, head?: string): Promise<ContextCommit[]> {
        return this.store.contextHistory(await this.binding(sessionId), head);
    }

    async createResource(sessionId: string, spec: ResourceSpec, options?: LeaseGuardOptions): Promise<ResourceGrant> {
        if (spec.requestId !== undefined && (typeof spec.requestId !== 'string' || !spec.requestId.trim())) throw new Error('Invalid resource requestId');
        const resource: ResourceRecord = {
            id: createId('resource'), sessionId, kind: spec.kind, uri: spec.uri,
            generation: 1, parentResourceId: spec.parentResourceId,
            metadata: spec.metadata, createdAt: Date.now(),
        };
        const handle: ResourceHandle = {
            id: createId('handle'), resourceId: resource.id, holderTaskId: spec.ownerTaskId,
            rights: spec.rights ?? ['read', 'write', 'execute', 'grant', 'admin'], generation: 1,
        };
        return this.store.createResource(await this.binding(sessionId), resource, handle, spec.parentHandleId,
            spec.requestId === undefined ? undefined : { id: spec.requestId, fingerprint: JSON.stringify({
                kind: spec.kind, uri: spec.uri, rights: handle.rights, parentResourceId: spec.parentResourceId,
                parentHandleId: spec.parentHandleId, metadata: spec.metadata,
            }) }, options);
    }

    async grantResource(
        sessionId: string, parentHandleId: string, holderTaskId: string, rights: ResourceRight[],
    ): Promise<ResourceHandle> {
        return this.store.grantResource(
            await this.binding(sessionId), createId('handle'), parentHandleId, holderTaskId, rights,
        );
    }

    async revokeResource(sessionId: string, handleId: string): Promise<number> {
        return this.store.revokeResource(await this.binding(sessionId), handleId);
    }

    async authorizeResource(
        sessionId: string, handleId: string, right: ResourceRight, holderTaskId?: string,
    ): Promise<ResourceRecord> {
        return this.store.authorizeResource(await this.binding(sessionId), handleId, right, holderTaskId);
    }

    async setBudget(
        sessionId: string, handleId: string, dimension: string, hardLimit: number,
        expectedVersion?: number | null, options?: LeaseGuardOptions,
    ): Promise<BudgetAccount> {
        return this.store.setBudget(
            await this.binding(sessionId), handleId, dimension, hardLimit, expectedVersion, options,
        );
    }

    async chargeBudget(
        sessionId: string, handleId: string, dimension: string, amount: number,
        options: { usageId?: string } = {},
    ): Promise<BudgetAccount[]> {
        return this.store.chargeBudget(await this.binding(sessionId), handleId, dimension, amount, undefined, options);
    }

    async snapshotWorkspace(
        sessionId: string,
        handleId: string,
        adapterRef: import('../domain/types').ProgramRef,
    ): Promise<WorkspaceSnapshot> {
        const binding = await this.binding(sessionId);
        const resource = await this.store.authorizeResource(binding, handleId, 'read');
        assertWorkspace(resource);
        const adapter = this.workspaces.resolve(adapterRef.kind, adapterRef.version);
        const payload = await adapter.snapshot(resource.uri, workspaceContext(sessionId, resource));
        const snapshot = workspaceSnapshot(sessionId, resource.id, adapterRef, payload);
        return this.store.saveWorkspaceSnapshot(binding, snapshot);
    }

    async diffWorkspace(
        sessionId: string,
        handleId: string,
        baseId: string,
        targetId: string,
    ): Promise<WorkspaceDiff> {
        const binding = await this.binding(sessionId);
        const resource = await this.store.authorizeResource(binding, handleId, 'read');
        const [base, target] = await readWorkspaceSnapshots(this.store, binding, [baseId, targetId]);
        assertWorkspaceSnapshots(resource, base, target);
        const adapter = this.workspaces.resolve(base.adapter.kind, base.adapter.version);
        const payload = await adapter.diff(base.payload, target.payload, workspaceContext(sessionId, resource));
        const diff: WorkspaceDiff = {
            id: createId('workspace-diff'), sessionId, resourceId: resource.id,
            adapter: base.adapter, baseSnapshotId: base.id, targetSnapshotId: target.id,
            payload, createdAt: Date.now(),
        };
        return this.store.saveWorkspaceDiff(binding, diff);
    }

    async mergeWorkspace(
        sessionId: string,
        handleId: string,
        baseId: string,
        leftId: string,
        rightId: string,
    ): Promise<WorkspaceMergeResult> {
        const binding = await this.binding(sessionId);
        const resource = await this.store.authorizeResource(binding, handleId, 'write');
        const [base, left, right] = await readWorkspaceSnapshots(this.store, binding, [baseId, leftId, rightId]);
        assertWorkspaceSnapshots(resource, base, left, right);
        const adapter = this.workspaces.resolve(base.adapter.kind, base.adapter.version);
        const result = await adapter.merge(base.payload, left.payload, right.payload,
            workspaceContext(sessionId, resource));
        const snapshot = workspaceSnapshot(sessionId, resource.id, base.adapter, result.payload, [left.id, right.id]);
        return { snapshot: await this.store.saveWorkspaceSnapshot(binding, snapshot), conflicts: result.conflicts ?? [] };
    }

    /**
     * Retention/GC entry point for settled Session messages. `before` is a wall-clock
     * watermark owned by the host; it must stay older than the deployment's replay window.
     */
    async pruneSessionMessages(sessionId: string, before: number, limit?: number): Promise<{ outbox: number; inbox: number }> {
        return this.store.pruneMessages(await this.binding(sessionId), before, limit);
    }

    async relayPendingMessages(): Promise<number> {
        let delivered = 0;
        for (const session of await this.store.listSessions()) {
            const source = await this.binding(session.id);
            for (const message of await this.store.pendingOutbox(source)) {
                try { if ((await this.relayMessage(source, message)).status === 'delivered') delivered++; } catch { /* Retry on recovery. */ }
            }
        }
        return delivered;
    }

    /** Retry settlement acknowledgement without ever re-delivering a terminal outbox record. */
    private async acknowledgeSettlement(source: ResolvedStorageBinding, message: CrossSessionMessage): Promise<CrossSessionMessage> {
        if (message.settlementAcknowledgedAt !== undefined || message.sourceSessionId === message.targetSessionId) return message;
        try {
            const target = await this.binding(message.targetSessionId);
            await this.store.acknowledgeMessageSettlement(target, message, 'inbox');
            return (await this.store.acknowledgeMessageSettlement(source, message, 'outbox'))!;
        } catch (error) {
            await this.store.recordMessageRetry(source, message.id, error);
            throw error;
        }
    }

    private async relayMessage(
        source: ResolvedStorageBinding,
        message: CrossSessionMessage,
    ): Promise<CrossSessionMessage> {
        if (message.status !== 'pending') return this.acknowledgeSettlement(source, message);
        const started = Date.now();
        let receipt: CrossSessionMessage;
        try {
            const target = await this.binding(message.targetSessionId);
            await this.store.deliverMessage(target, message);
            receipt = await this.store.messageReceipt(target, message.id);
        } catch (error) {
            // A missing target may still be created later. The caller can bound retries.
            if (message.expiresAt !== undefined && message.expiresAt <= Date.now()
                && error instanceof Error && 'code' in error && error.code === 'SESSION_NOT_FOUND') {
                receipt = { ...message, status: 'rejected', rejectedAt: Date.now(), settlementAcknowledgedAt: Date.now(),
                    rejection: { code: 'expired', message: 'Message delivery deadline expired' } };
            } else {
                await this.store.recordMessageRetry(source, message.id, error);
                this.diagnose({ operation: 'message.relay', stage: 'target-delivery', outcome: 'failure', operationId: message.id,
                    sessionId: message.sourceSessionId, durationMs: Date.now() - started, errorCode: diagnosticCode(error) });
                throw error;
            }
        }
        const delivered = await this.store.markMessageDelivered(source, message.id, receipt);
        this.notify(message.targetSessionId);
        this.notify(message.sourceSessionId);
        return this.acknowledgeSettlement(source, delivered);
    }

    async setSessionStatus(sessionId: string, status: SessionRecord['status']): Promise<void> {
        this.assertRunning();
        return this.changeSessionStatus(sessionId, status);
    }

    private async changeSessionStatus(sessionId: string, status: SessionRecord['status']): Promise<void> {
        await this.store.setSessionStatus(await this.binding(sessionId), status);
        await this.abortFencedReducers(sessionId);
        this.notify(sessionId);
        if (status === 'open') { this.queueDrain(sessionId); this.schedulePoll(sessionId); }
    }

    async closeSession(sessionId: string, cancelRunning: boolean): Promise<void> {
        let binding: ResolvedStorageBinding;
        try {
            binding = await this.binding(sessionId);
        } catch (error) {
            if (isMissingStorage(error)) return;
            throw error;
        }
        // Closing is idempotent: a Session whose storage an interrupted removal
        // already deleted has nothing to close, and its stale record must not
        // block the cleanup that follows.
        if (!await binding.fs.driver.exists(binding.rootPath)) return;
        const closing = await this.store.setSessionStatus(binding, 'closing', cancelRunning ? 'cancel' : 'drain');
        if (closing.status === 'closed' || closing.status === 'archived') return;
        if (cancelRunning) {
            const tasks = await this.store.listTasks(binding);
            await settleCancellations(cancellationFrontier(tasks)
                .map(task => this.cancel(sessionId, task.id, 'Session closed')), `Failed to cancel tasks in session ${sessionId}`);
        }
        await this.finishSessionClose(sessionId, binding);
        this.queueDrain(sessionId); this.schedulePoll(sessionId);
    }

    /**
     * Delete a Session's Kernel storage and catalog record. Refuses while Tasks are
     * live or resource cleanup is pending, so a failed close never destroys state;
     * the host must close the Session first and keep its own records until this
     * resolves.
     */
    async removeSession(sessionId: SessionId, options: { force?: boolean } = {}): Promise<boolean> {
        let binding: ResolvedStorageBinding;
        try {
            // Resolve straight from the catalog: openSession() requires the main
            // record file, which an interrupted removal may already have deleted.
            binding = await this.store.inspectSessionBinding(sessionId);
        } catch (error) {
            if (isMissingStorage(error)) return false;
            throw error;
        }
        const tasks = await this.store.listTasks(binding);
        const busy = tasks.some(task => !isTerminalStatus(task.status)
            || Object.values(task.effects).some(effect => effect.cleanupPending));
        if (busy && !options.force) {
            throw kernelError(KernelErrorCode.CONFLICT, `Session ${sessionId} still has running Tasks or pending resource cleanup`);
        }
        this.stopPoll(sessionId);
        this.resourcePoller.stop(`session:${sessionId}`);
        await this.abortFencedReducers(sessionId);
        await Promise.all([...this.plugins.values()].map(plugin => plugin.onSessionClosed?.(sessionId)));
        await this.store.removeSession(sessionId, binding);
        this.storageListeners.get(sessionId)?.();
        this.storageListeners.delete(sessionId);
        this.sessions.delete(sessionId);
        this.notify(sessionId);
        return true;
    }

    private readonly sessionClosures = new Map<string, Promise<void>>();

    private async finishSessionClose(sessionId: string, binding: ResolvedStorageBinding): Promise<void> {
        const pending = this.sessionClosures.get(sessionId);
        if (pending) return pending;
        const work = this.completeSessionClose(sessionId, binding);
        this.sessionClosures.set(sessionId, work);
        try { await work; }
        finally { if (this.sessionClosures.get(sessionId) === work) this.sessionClosures.delete(sessionId); }
    }

    private async completeSessionClose(sessionId: string, binding: ResolvedStorageBinding): Promise<void> {
        if ((await this.store.sessionRecord(binding)).status !== 'closing') return;
        const tasks = await this.store.listTasks(binding);
        if (tasks.some(t => !isTerminalStatus(t.status) || Object.values(t.effects).some(e => e.cleanupPending))) return;
        if (!await this.managedResources.canClose(sessionId)) return;
        if ((await this.store.pendingOutbox(binding)).length) return;
        await this.store.setSessionStatus(binding, 'closed');
        this.stopPoll(sessionId);
        await Promise.all([...this.plugins.values()].map(plugin => plugin.onSessionClosed?.(sessionId)));
        this.notify(sessionId);
    }

    onChanged(listener: (event: KernelEvents['changed']) => void): () => void {
        return this.eventsBus.on('changed', listener);
    }

    private async resolveStorage(reference: StorageBindingRef): Promise<ResolvedStorageBinding> {
        return this.storageResolvers.resolve(reference.kind).resolve(reference);
    }

    private rememberBinding(sessionId: string, binding: ResolvedStorageBinding): void {
        if (this.disposed || this.shutdownWork || this.drainingWorker) return;
        const previous = this.sessions.get(sessionId);
        if (previous?.fs === binding.fs && previous.rootPath === binding.rootPath) return;
        this.storageListeners.get(sessionId)?.();
        this.sessions.set(sessionId, binding);
        this.resourcePoller.start(`session:${sessionId}`);
        const root = pathUtils.normalize(binding.rootPath);
        // Session-scope managed resources live in one seq file. Restarting the resource sweep on
        // *any* Session commit made every Task/Effect record write re-read that file (and open a
        // transaction) although nothing about resources had changed.
        const resources = pathUtils.normalize(resourcesPath(root));
        const log = pathUtils.normalize(eventsPath(root));
        this.storageListeners.set(sessionId, this.commitSubscriptions.watch(binding.fs, root, changed => {
            if (this.disposed || !changed.length) return;
            // Event appends wake observers without invalidating task and Session listings.
            const reason = changed.every(path => path === log) ? 'content' : 'structure';
            this.notify(sessionId, undefined, reason);
            this.queueDrain(sessionId);
            if (changed.includes(resources)) this.resourcePoller.start(`session:${sessionId}`);
            // Observers receive notify above; dependent Sessions wake through their own durable inputs.
            this.schedulePoll(sessionId);
        }));
    }

    private async binding(sessionId: string): Promise<ResolvedStorageBinding> {
        const cached = this.sessions.get(sessionId);
        if (cached) return cached;
        const opened = await this.store.openSession(sessionId);
        this.rememberBinding(sessionId, opened.binding);
        return opened.binding;
    }

    private queueDrain(sessionId: string): void {
        if (this.disposed || this.shutdownWork || this.drainingWorker) return;
        if (this.draining.has(sessionId)) { this.requestedDrains.add(sessionId); return; }
        this.draining.add(sessionId);
        queueMicrotask(() => void this.drainSession(sessionId).catch(error => { if (!this.disposed) this.handlePollError(error); }));
    }

    private schedulePoll(sessionId: string): void {
        if (this.drainingWorker || this.disposed) return;
        this.tickTasks.delete(sessionId);
        this.poller.start(sessionId);
    }

    private stopPoll(sessionId: string): void { this.tickTasks.delete(sessionId); this.poller.stop(sessionId); }

    private commitScopes(): CommitScope[] {
        const entries = (root: string) => [{fileIdOrPath: resourcesPath(root), key: 'managed/sequence'},
            {fileIdOrPath: resourcesPath(root), key: 'managed/revision'}];
        return [{id: '$kernel', fs: this.catalogFs, entries: entries(this.catalogRoot)},
            ...[...this.sessions].map(([id, binding]) => ({id, fs: binding.fs, structureEntries: [3, 4], entries: [
                {fileIdOrPath: eventsPath(binding.rootPath), key: 'next-sequence'}, ...entries(binding.rootPath),
                {fileIdOrPath: indexPath(binding.rootPath), key: ACTIVITY_REVISION},
                {fileIdOrPath: sessionPath(binding.rootPath), key: 'record'}]}))];
    }

    private async poll(sessionId: string): Promise<boolean> {
        // After recovery, share one task scan with effect dispatch and wake calculation.
        // Recovery retains its own fresh transactional reads and ownership checks.
        this.tickTasks.delete(sessionId);
        // Both sweeps already compute their next deadline; keep them for nextWakeDelay so an
        // idle tick does not open two more transactions to read the same rows again.
        this.tickManaged.set(sessionId, {
            kernel: await this.managedResources.sweep('kernel'),
            session: await this.managedResources.sweep(`session:${sessionId}`),
        });
        const binding = await this.binding(sessionId);
        const session = await this.store.sessionRecord(binding);
        const status = session.status;
        const sweepMarker: TaskRecord[] = [];
        this.tickTasks.set(sessionId, sweepMarker);
        const sweptTasks = await this.store.sweep(binding);
        await this.abortFencedReducers(sessionId);
        const dueMessages = await this.store.pendingOutbox(binding, Date.now());
        for (const message of dueMessages) {
            try { await this.relayMessage(binding, message); } catch { /* Persisted outbox is retried by polling. */ }
        }
        const reuseSweep = sweptTasks && !dueMessages.length && this.tickTasks.get(sessionId) === sweepMarker;
        // Install before a fallback read so concurrent commits invalidate that snapshot too.
        const snapshot = reuseSweep ? sweepMarker : [];
        if (!reuseSweep) this.tickTasks.set(sessionId, snapshot);
        const tasks = reuseSweep ? sweptTasks : await this.store.listTasks(binding);
        if (this.tickTasks.get(sessionId) === snapshot) this.tickTasks.set(sessionId, tasks);
        // Any write below makes the snapshot stale for nextWakeDelay(); the common idle
        // tick mutates nothing and reuses it, a mutating tick re-reads instead.
        let mutated = await this.maintainPolledTasks(binding, session, tasks);
        if (status === 'open' || (status === 'closing' && session.closeMode === 'drain')) {
            this.queueDrain(sessionId);
            if (await this.dispatchPendingEffects(binding, tasks)) mutated = true;
        }
        if (status === 'suspending' && !tasks.some(task => Object.values(task.effects).some(e => e.status === 'leased' || e.status === 'indeterminate' || e.cleanupPending))) {
            await this.store.setSessionStatus(binding, 'suspended'); this.notify(sessionId);
        }
        if (status === 'closing') await this.finishSessionClose(sessionId, binding);
        const latest = (await this.store.sessionRecord(binding)).status;
        const again = latest !== 'closed' && latest !== 'archived';
        // The poller only asks for a delay while it will keep polling; do not keep a
        // snapshot alive for a Session that just stopped, and drop a stale one.
        if (!again || mutated) this.tickTasks.delete(sessionId);
        return again;
    }

    private async maintainPolledTasks(binding: ResolvedStorageBinding, session: SessionRecord, tasks: TaskRecord[]): Promise<boolean> {
        let mutated = false;
        const cleanup: TaskRecord[] = [];
        for (let task of tasks) {
            if (task.blockedReason === 'program-unavailable' && this.programs.has(task.program.kind, task.program.version)) {
                await this.store.unblockProgram(binding, task.id); mutated = true;
            }
            if (session.status === 'closing' && session.closeMode === 'cancel' && !isTerminalStatus(task.status)) {
                task = await this.store.cancelTask(binding, task.id, 'Session closed'); mutated = true;
            }
            if (Object.values(task.effects).some(effect => effect.cleanupPending)) cleanup.push(task);
        }
        // Attach handlers before waiting: one failed or hanging adapter must not suppress another cleanup.
        try {
            await settleCancellations(cleanup.map(task => this.cancelTaskEffects(task, activeEffectIds(task))),
                `Failed to recover cleanup for session ${session.id}`);
        } catch { /* Per-Effect diagnostics and durable cleanup duties remain authoritative. */ }
        return mutated || cleanup.length > 0;
    }

    private async nextWakeDelay(sessionId: string): Promise<number | undefined> {
        const binding = await this.binding(sessionId), now = Date.now();
        // Reuse the list poll() just read for this Session; it is the same tick.
        const cached = this.tickTasks.get(sessionId);
        this.tickTasks.delete(sessionId);
        let at = Infinity;
        const future = (value?: number) => { if (value !== undefined && value > now) at = Math.min(at, value); };
        const visit = (wait: import('../domain/types').WaitSpec, task: TaskRecord) => {
            if (wait.type === 'timer') {
                if (!task.pendingEvents.some(e => e.type === 'timer-fired' && e.id === wait.id)) at = Math.min(at, wait.at);
            } else if ('waits' in wait) for (const child of wait.waits) visit(child, task);
        };
        for (const task of cached ?? await this.store.listTasks(binding)) {
            if (task.status === 'waiting' && task.wait) visit(task.wait, task);
            if (task.status === 'ready') future(task.readyAt);
            if (task.status === 'running' && task.currentAttempt) at = Math.min(at, task.currentAttempt.leaseUntil);
            for (const effect of Object.values(task.effects)) {
                if (effect.status === 'leased' && effect.currentAttempt) at = Math.min(at, effect.currentAttempt.leaseUntil);
                if (effect.status === 'pending') future(effect.readyAt);
                if (effect.cleanupPending) at = Math.min(at, now + 1000);
            }
        }
        for (const message of await this.store.pendingOutbox(binding)) {
            at = Math.min(at, message.nextAttemptAt ?? now);
            if (message.expiresAt !== undefined) at = Math.min(at, message.expiresAt);
        }
        const swept = this.tickManaged.get(sessionId);
        this.tickManaged.delete(sessionId);
        if (swept) {
            if (swept.kernel !== undefined) at = Math.min(at, swept.kernel);
            if (swept.session !== undefined) at = Math.min(at, swept.session);
        } else for (const scope of ['kernel', `session:${sessionId}`]) {
            const deadline = await this.managedResources.nextDeadline(scope);
            if (deadline !== undefined) at = Math.min(at, deadline);
        }
        return Number.isFinite(at) ? Math.max(0, at - now) : undefined;
    }

    private handlePollError(error: unknown): boolean {
        // A removed Session can still have an in-flight poll tick; that is not an error.
        if (isMissingPath(error) || isMissingStorage(error)) return false;
        console.error('Kernel poll failed', error);
        return true;
    }

    private async drainSession(sessionId: string): Promise<void> {
        try {
            const binding = await this.binding(sessionId);
            const session = await this.store.sessionRecord(binding);
            if (session.status !== 'open' && !(session.status === 'closing' && session.closeMode === 'drain')) return;
            while (true) {
                const claim = await this.claimNext(binding, sessionId);
                if (!claim) break;
                void this.execute(binding, claim).finally(() => {
                    this.releaseSlot();
                    for (const id of this.sessions.keys()) this.queueDrain(id);
                });
            }
        } finally {
            this.draining.delete(sessionId);
            if (this.requestedDrains.delete(sessionId)) this.queueDrain(sessionId);
        }
    }

    private async claimNext(binding: ResolvedStorageBinding, sessionId: string): Promise<TaskClaim | undefined> {
        if (this.disposed || this.shutdownWork || this.drainingWorker) return undefined;
        if (this.active >= this.maxConcurrent) { this.waitingDrains.add(sessionId); return undefined; }
        // Reserve before yielding: other Session drains share this counter.
        this.active++;
        try {
            const claim = await this.store.claimReady(binding, this.workerId, this.leaseMs);
            if (claim && !this.disposed && !this.shutdownWork && !this.drainingWorker) return claim;
            if (claim && this.drainingWorker) await this.store.abandonClaim(binding, claim);
            this.releaseSlot();
            return undefined;
        } catch (error) { this.releaseSlot(); throw error; }
    }

    private releaseSlot(): void {
        this.active--;
        const waiting = [...this.waitingDrains];
        this.waitingDrains.clear();
        for (const id of waiting) this.queueDrain(id);
    }

    private async abortFencedReducers(sessionId: string): Promise<void> {
        for (const [token, active] of this.reducerControllers) {
            if (active.sessionId !== sessionId) continue;
            const task = await this.store.readTask(await this.binding(sessionId), active.taskId);
            if (task.currentAttempt?.leaseToken !== token) active.controller.abort(new Error('Reducer claim invalidated'));
        }
    }

    private async execute(binding: ResolvedStorageBinding, claim: TaskClaim): Promise<void> {
        const controller = new AbortController();
        this.reducerControllers.set(claim.attempt.leaseToken, { sessionId: claim.task.sessionId, taskId: claim.task.id, controller });
        const stopHeartbeat = this.startLeaseHeartbeat(binding, claim, controller);
        try {
            if (!this.programs.has(claim.task.program.kind, claim.task.program.version)) {
                await this.store.blockUnavailableProgram(binding, claim); return;
            }
            const program = this.programs.resolve(claim.task.program.kind, claim.task.program.version);
            const interrupted = new Promise<never>((_, reject) => {
                controller.signal.addEventListener('abort', () => reject(controller.signal.reason), { once: true });
            });
            const decision = await Promise.race([nextDecision(program, claim.task), interrupted]);
            if (this.disposed) return;
            const next = await this.applyDecision(binding, claim, decision);
            this.notify(next.sessionId, next.id);
            if (next.status === 'ready') this.queueDrain(next.sessionId);
        } catch (error) {
            if (this.disposed || controller.signal.aborted) return;
            const failed = failureDecision(claim.task.state, error);
            try {
                const next = await this.applyDecision(binding, claim, failed);
                if (next.status === 'ready') this.queueDrain(next.sessionId);
            } catch { /* A newer lease owns the task. */ }
            this.notify(claim.task.sessionId, claim.task.id);
        } finally {
            stopHeartbeat();
            this.reducerControllers.delete(claim.attempt.leaseToken);
        }
    }

    private startLeaseHeartbeat(binding: ResolvedStorageBinding, claim: TaskClaim, controller: AbortController): () => void {
        const heartbeat = new LeaseHeartbeat({
            intervalMs: Math.min(2147483647, Math.max(1, Math.floor(this.leaseMs / 3))),
            renew: async () => {
                const valid = !this.disposed && await this.store.renewLease(binding, claim, this.leaseMs);
                if (!valid) controller.abort(new Error('Reducer lease lost'));
                return valid;
            },
            onError: error => console.error('Kernel lease heartbeat failed', error),
        });
        const stop = (): void => {
            heartbeat.stop();
            this.heartbeats.delete(heartbeat);
        };
        this.heartbeats.add(heartbeat);
        heartbeat.start();
        return stop;
    }

    private async applyDecision(
        binding: ResolvedStorageBinding,
        claim: TaskClaim,
        decision: Decision,
    ): Promise<TaskRecord> {
        validateDecision(decision);
        const retrying = shouldRetry(claim.task, decision);
        const pendingEvents = !(claim.task.initialized ?? claim.task.state !== undefined) || retrying
            ? claim.task.pendingEvents
            : claim.task.pendingEvents.slice(1);
        const state = retrying ? claim.task.state : decision.state;
        let next: TaskRecord = { ...claim.task, state, pendingEvents,
            initialized: retrying ? claim.task.initialized : true };
        const actions = decision.next.type === 'fail' ? [] : decision.actions ?? [];
        for (const action of actions) {
            if (action.type === 'request-interaction') next = addInteraction(next, action.interaction);
        }
        const spawns = prepareSpawns(actions);
        await Promise.all(spawns.map(spawn => this.store.prepareTaskDirectory(binding, spawn.id)));
        const effects = actions.filter(action => action.type === 'effect')
            .map(action => normalizeEffect(action.effect));
        for (const effect of effects) next = addEffect(next, effect);
        next = transition(next, decision);
        const sideEffects = decisionSideEffects(actions, spawns);
        sideEffects.resources = await Promise.all(actions.filter(action => action.type === 'resource')
            .map(action => this.managedResources.prepare({ sessionId: claim.task.sessionId, taskId: claim.task.id }, action.command)));
        if (retrying) sideEffects.attemptOutcome = 'failed';
        const eventType = retrying ? 'task.retry.scheduled' : `task.${next.status}`;
        const payload = retrying ? { error: next.lastError, readyAt: next.readyAt } : undefined;
        const committed = await this.store.commitTask(binding, claim, next, eventType, payload, sideEffects);
        for (const effect of effects) void this.dispatchEffect(binding, committed, effect.id).catch(error => this.handlePollError(error));
        if (decision.next.type === 'continue' || spawns.length > 0) this.queueDrain(next.sessionId);
        return committed;
    }

    /** Candidates may start asynchronously; invalidate the snapshot whenever dispatch is attempted. */
    private async dispatchPendingEffects(binding: ResolvedStorageBinding, tasks?: TaskRecord[]): Promise<boolean> {
        const pending = await this.store.pendingEffects(binding, tasks);
        for (const item of pending) {
            void this.dispatchEffect(binding, item.task, item.effectId).catch(error => this.handlePollError(error));
        }
        return pending.length > 0;
    }

    private async dispatchEffect(
        binding: ResolvedStorageBinding,
        task: TaskRecord,
        effectId: string,
    ): Promise<void> {
        const requested = task.effects[effectId]?.request;
        if (!requested || !this.effects.has(requested.kind, requested.version)) return;
        if (this.disposed || this.shutdownWork || this.drainingWorker || this.detachObservations) return;
        const permit = this.effectWorkSlots.tryAcquire();
        if (!permit) return;
        this.activeEffects++;
        try {
            const claim = await this.store.claimEffect(binding, task.id, effectId, this.workerId, this.leaseMs);
            if (claim) await this.executeEffect(binding, task, claim, permit);
        } finally { this.activeEffects--; permit.close(); }
    }

    private async executeEffect(
        binding: ResolvedStorageBinding,
        task: TaskRecord,
        claim: EffectClaim,
        permit: EffectWorkPermit,
    ): Promise<void> {
        const startedAt = Date.now(), effect = claim.effect.request as RequiredEffect;
        const controller = new AbortController();
        const controllerKey = effectControllerKey(task.sessionId, task.id, effect.id);
        this.effectControllers.set(controllerKey, controller);
        const stopHeartbeat = this.startEffectHeartbeat(binding, claim, controller);
        try {
            const grants: import('../domain/types').AuthorizedEffectGrant[] = [];
            for (const grant of effect.grants ?? []) {
                const resource = await this.store.authorizeResource(
                    binding, grant.handleId, grant.right, task.id,
                );
                grants.push({ ...grant, resource });
            }
            const adapter = this.effects.resolve(effect.kind, effect.version);
            if (!await this.store.renewEffectLease(binding, claim, this.leaseMs)) return;
            const context = {
                sessionId: task.sessionId, taskId: task.id, effectId: effect.id, idempotencyKey: effect.idempotencyKey,
                attemptId: claim.effect.currentAttempt!.id, attemptNumber: claim.effect.attemptCount,
                abortSignal: controller.signal, deadlineAt: claim.effect.deadlineAt, grants,
                waitForObservation: <T>(observe: (signal: AbortSignal) => Promise<T>, options?: {confirmLocalStop?: boolean}) => this.observeEffect(permit, controller.signal,
                    {sessionId: task.sessionId, taskId: task.id, effectId: effect.id}, observe, options?.confirmLocalStop),
                sessionState: {
                    get: <T extends import('../domain/types').JsonValue>(key: string) =>
                        this.store.getShared<T>(binding, key),
                    setIfRunning: <T extends import('../domain/types').JsonValue>(key: string, value: T, expectedVersion?: number | null) =>
                        this.store.setShared(binding, key, value, { taskId: task.id, expectedVersion }, claim, true),
                    set: <T extends import('../domain/types').JsonValue>(
                        key: string, value: T, expectedVersion?: number | null,
                    ) => this.store.setShared(binding, key, value, {
                        taskId: task.id, expectedVersion,
                    }, claim),
                },
                emit: async (event: { type: string; payload?: unknown }): Promise<void> => {
                    if (this.disposed) return;
                    assertCustomEventType(event.type);
                    await this.store.appendEvent(binding, task.sessionId, task.id, event.type, event.payload, claim);
                    this.notify(task.sessionId, task.id, 'content');
                },
                // Effect-driven charges default to one settlement per logical Effect, so a
                // retried attempt after a crash cannot charge the same tokens twice.
                chargeBudget: (handleId: string, dimension: string, amount: number, options?: { usageId?: string }) =>
                    this.store.chargeBudget(binding, handleId, dimension, amount, claim,
                        { usageId: options?.usageId ?? `effect:${[task.id, effect.id, handleId, dimension].map(encodeURIComponent).join(':')}` }),
            };
            const result = await executeEffectWithDeadline(adapter, effect, claim, context, controller);
            if ('result' in result) assertDurableValue(result.result, 'Effect result');
            await this.store.completeEffect(
                binding, task.id, effect.id, claim.effect.currentAttempt!.leaseToken, result,
            );
        } catch (error) {
            if (this.disposed) return;
            if (this.detachObservations && error === this.observationDrainReason) {await this.store.retireEffectLease(binding, claim); return;}
            try {
                const failedAdapter = this.effects.resolve(effect.kind, effect.version);
                const indeterminate = !controller.signal.aborted && failedAdapter.failureOutcome?.(error) === 'indeterminate';
                await this.store.completeEffect(binding, task.id, effect.id,
                    claim.effect.currentAttempt!.leaseToken, { ...effectFailure(error), indeterminate: indeterminate || undefined,
                        cleanupPending: controller.signal.aborted || undefined,
                        retryable: !controller.signal.aborted && !indeterminate && (failedAdapter.shouldRetry?.(error,
                            { sessionId: task.sessionId, taskId: task.id, effectId: effect.id, idempotencyKey: effect.idempotencyKey, abortSignal: controller.signal, grants: [] })
                            ?? failedAdapter.recoveryPolicy === 'idempotent-retry') });
            } catch (error) {
                this.diagnose({ operation: 'effect.complete', stage: 'commit', outcome: 'failure', operationId: claim.effect.currentAttempt!.id,
                    sessionId: task.sessionId, taskId: task.id, effectId: effect.id, durationMs: Date.now() - startedAt, errorCode: diagnosticCode(error) });
            }
        } finally {
            stopHeartbeat();
            if (this.effectControllers.get(controllerKey) === controller) this.effectControllers.delete(controllerKey);
            this.notify(task.sessionId, task.id);
            this.queueDrain(task.sessionId);
        }
    }

    private async observeEffect<T>(permit: EffectWorkPermit, signal: AbortSignal,
        identity: {sessionId: string; taskId: string; effectId: string}, observe: (signal: AbortSignal) => Promise<T>, confirmLocalStop = false): Promise<T> {
        const started = Date.now(); let outcome: 'success' | 'failure' = 'failure', errorCode: string | undefined;
        this.diagnose({operation: 'effect.observe', stage: 'yield', outcome: 'blocked', ...identity,
            operationId: effectControllerKey(identity.sessionId, identity.taskId, identity.effectId), durationMs: 0});
        const observer=new AbortController();this.drainingObservations.add(observer);
        if (confirmLocalStop) this.managedObservations.add(observer);
        if(this.detachObservations)observer.abort(this.observationDrainReason);
        try {const value = await permit.observe(observe, AbortSignal.any([signal,observer.signal]), confirmLocalStop); outcome = 'success'; return value;}
        catch (error) {errorCode = diagnosticCode(error); throw error;}
        finally {this.drainingObservations.delete(observer);this.managedObservations.delete(observer);this.diagnose({operation: 'effect.observe', stage: observer.signal.aborted ? 'drained' : signal.aborted ? 'aborted' : 'resume', outcome,
            operationId: effectControllerKey(identity.sessionId, identity.taskId, identity.effectId), ...identity,
            durationMs: Date.now() - started, ...(outcome === 'failure' ? {errorCode: signal.aborted ? 'EFFECT_OBSERVATION_ABORTED' : errorCode} : {})});}
    }

    private async cancelTaskEffects(task: TaskRecord, active: Set<string>): Promise<void> {
        const effects = Object.entries(task.effects).filter(([id, effect]) => active.has(id) && effect.cleanupPending);
        await settleCancellations(effects.map(([, effect]) => this.cleanupEffect(task, effect.request)),
            `Failed to cancel effects for task ${task.id}`);
    }

    private async cleanupEffect(task: TaskRecord, effect: EffectRequest): Promise<void> {
        const started = Date.now(), operationId = effectControllerKey(task.sessionId, task.id, effect.id!);
        let stage = 'adapter-confirmation';
        const identity = { sessionId: task.sessionId, taskId: task.id, effectId: effect.id };
        try {
            await this.effectCleanup.run(operationId, () => this.cancelEffect(task, effect));
            stage = 'persist-confirmation';
            await this.store.confirmEffectCleanup(await this.binding(task.sessionId), task.id, effect.id!);
            this.diagnose({ operation: 'effect.cleanup', stage: 'confirmed', outcome: 'success', operationId,
                ...identity, durationMs: Date.now() - started });
        } catch (error) {
            this.diagnose({ operation: 'effect.cleanup', stage, outcome: 'failure', operationId,
                ...identity, durationMs: Date.now() - started, errorCode: diagnosticCode(error) });
            throw error;
        }
    }

    private async cancelEffect(task: TaskRecord, effect: EffectRequest): Promise<void> {
        if (!effect.id) return;
        const key = effectControllerKey(task.sessionId, task.id, effect.id);
        const controller = this.effectControllers.get(key);
        controller?.abort();
        const adapter = this.effects.resolve(effect.kind, effect.version);
        if (!adapter.cancel) {
            if ((task.effects[effect.id]?.attemptCount ?? 0) > 0) throw new Error(`Effect cleanup requires adapter confirmation: ${effect.id}`);
            return;
        }
        await adapter.cancel(effect.request, {
            sessionId: task.sessionId, taskId: task.id, effectId: effect.id, idempotencyKey: effect.idempotencyKey,
            attemptId: task.effects[effect.id]?.attempts.at(-1)?.id, attemptNumber: task.effects[effect.id]?.attemptCount,
            abortSignal: controller?.signal ?? AbortSignal.abort(),
            grants: [],
            // Cancel path has no storage binding; effect adapters must not emit.
            emit: async () => {},
        });
    }

    private startEffectHeartbeat(binding: ResolvedStorageBinding, claim: EffectClaim, controller: AbortController): () => void {
        const heartbeat = new LeaseHeartbeat({
            intervalMs: Math.min(2147483647, Math.max(1, Math.floor(this.leaseMs / 3))),
            renew: async () => {
                const valid = !this.disposed && await this.store.renewEffectLease(binding, claim, this.leaseMs);
                if (!valid) controller.abort(new Error('Effect lease lost'));
                return valid;
            },
            onError: error => console.error('Kernel effect heartbeat failed', error),
        });
        const stop = (): void => { heartbeat.stop(); this.heartbeats.delete(heartbeat); };
        this.heartbeats.add(heartbeat);
        heartbeat.start();
        return stop;
    }

    private notify(sessionId: string, taskId?: string, reason: KernelChangeReason = 'structure'): void {
        this.tickTasks.delete(sessionId);
        this.eventsBus.emit('changed', { sessionId, taskId, reason });
    }
}

/**
 * True when a Session has no Kernel storage left: the catalog entry is gone or
 * the storage root was deleted by an interrupted removal. Callers must stay
 * usable in that state so cleanup can be resumed after a restart.
 */
function isMissingStorage(error: unknown): boolean {
    return (error instanceof KernelError && error.code === KernelErrorCode.SESSION_NOT_FOUND)
        || (error instanceof FSError && error.code === 'ENOENT');
}

function diagnosticCode(error: unknown): string {
    if (error instanceof Error && 'code' in error && typeof error.code === 'string' && /^[A-Z0-9_]{1,80}$/.test(error.code)) return error.code;
    if (error instanceof Error && error.message.startsWith('Effect cleanup timed out:')) return 'EFFECT_CLEANUP_TIMEOUT';
    if (error instanceof Error && error.message.startsWith('Effect cleanup requires adapter confirmation:')) return 'EFFECT_CLEANUP_UNSUPPORTED';
    if (error instanceof Error && error.message.includes('not registered')) return 'ADAPTER_UNAVAILABLE';
    if (error instanceof Error && error.message === 'Message identity conflict') return 'MESSAGE_IDENTITY_CONFLICT';
    if (error instanceof Error && error.message.includes('Message')) return 'MESSAGE_INVALID';
    return 'ADAPTER_OR_STORAGE_ERROR';
}

/** Join every independent branch; one cleanup failure never suppresses another stop request. */
async function settleCancellations(operations: Promise<void>[], message: string): Promise<void> {
    const results = await Promise.allSettled(operations);
    const errors = results.filter(result => result.status === 'rejected').map(result => result.reason);
    if (errors.length) throw new AggregateError(errors, message);
}
