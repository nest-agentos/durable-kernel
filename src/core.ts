/** Small public entry point. Legacy APIs and diagnostic records remain in the root export. */
import { Kernel, type KernelOptions } from './application/kernel';
export {taskCallCapabilityUri, snapshotTaskCallPlan} from './application/task-call-identity';
export type {TaskRef, TaskCallPlan, TaskCallRequest, TaskCallResult, TaskCallOwner, TaskCallAuthority} from './domain/task-call';
import type { SessionHandle, TaskHandle, TaskSpec, LeaseGuardOptions } from './domain/types';

export interface Task<O = unknown> extends Pick<TaskHandle<O>,
    'id' | 'resources' | 'cache' | 'send' | 'stat' | 'stats' | 'watch' | 'wait' | 'poll' | 'start'
    | 'pause' | 'interrupt' | 'resume' | 'withdrawPause' | 'cancel' | 'signal' | 'respond' | 'snapshot' | 'readEvents' | 'waitStopped' | 'recoveryIssues' | 'resolveEffect'> {
    retry(options: LeaseGuardOptions & { requestId: string }): Promise<Task<O>>;
}
export interface Session extends Pick<SessionHandle, 'id' | 'resources' | 'stat' | 'watch' | 'suspend' | 'resume' | 'close' | 'recover'
    | 'listTaskPage' | 'send' | 'inboxPage' | 'acknowledgeMessage' | 'snapshot' | 'readEvents'> {
    submit<I, O = unknown>(spec: TaskSpec<I>, options?: import('./domain/types').LeaseGuardOptions): Promise<Task<O>>;
    attachTask<O = unknown>(id: string): Promise<Task<O>>;
    schedule(spec: import('./domain/schedule').ScheduleSpec): Promise<Task>;
}
export type SessionReader = Pick<Awaited<ReturnType<Kernel['inspectSession']>>, 'id' | 'snapshot' | 'readEvents' | 'listTaskPage'>;
export interface Harness extends Pick<Kernel, 'initialize' | 'dispose' | 'shutdown' | 'drain' | 'archives' | 'waitIdle' | 'recover' | 'recoverSession' | 'recoverSessions' | 'recoveryIssues' | 'listSessions' | 'resources' | 'sessionActivity'
    | 'readTaskCallTarget' | 'readTaskSubmissionReceipt' | 'inspectTask' | 'onChanged' | 'registerProgram' | 'registerResourceAdapter' | 'registerEffect' | 'registerStorageResolver' | 'registerWorkspace' | 'use' | 'bindTaskCall'> {
    inspectSession(id: string): Promise<SessionReader>;
    createSession(spec: Parameters<Kernel['createSession']>[0]): Promise<Session>;
    openSession(id: string): Promise<Session>;
    openTask<O = unknown>(id: string): Promise<Task<O>>;
}
export function createHarness(options: KernelOptions): Harness { return new Kernel(options); }
export { resourceResult } from './public/resources';
export { defineTask, type TaskStepEvent } from './public/program';
export type { ManagedResource as Resource, ManagedResourceRef as ResourceRef } from './domain/resource-api';
export type { KernelOptions } from './application/kernel';
export type { SessionActivity, TaskActivityPhase } from './domain/activity';
export type { SignalOptions, SignalReceipt, WorkerDrainReport, KernelWakeSource } from './domain/types';
export type { ScheduleSpec } from './domain/schedule';
export type { SessionArchiveApi, ArchiveLimits, ArchiveManifest, ArchiveExportOptions, ArchiveValidation,
    ArchiveImportOptions, ArchiveImportReport } from './domain/archive';
export type { ManagedResourceAdapter, ResourceCleanupContext, ResourceCleanupReceipt, PhysicalResourceBinding, ResourceCleanup, ManagedGrant, ResourceQuery, ResourcePage, ResourceRequestInfo, ResourceApi, ManagedResource, ManagedResourceRef, ManagedHandle, ResourceClaim, ResourceCommand, DurableResourceRequest,
    ResourceResult, ResourceRequestSnapshot, ResourceAuthority, ManagedAuthority, AuthorityClaim, ManagedResourceStat } from './domain/resource-api';
export type { TaskStat, TaskStats, SessionStat } from './domain/status';
export type { CacheApi, CacheSpec, CacheRead, CachePublish, CacheNamespace, CacheEntry, CacheReceipt, CacheManagementReceipt } from './domain/cache';
export type { RecoveryOptions, RecoveryReport, DurableTaskProgram, Decision, TaskInputEvent, KernelAction, EffectAdapter, SessionStorageResolver } from './domain/types';
export type { TaskSpec, TaskSubmissionReceipt, TaskControl, TaskControlOptions, TaskCancellationOptions, TaskCancellationReceipt, TaskTreeCancellationOptions, TaskTreeCancellationReceipt, TaskPauseWithdrawalOptions, TaskMessageRequest, SessionMessageRequest, SessionSendOptions,
    InboxQuery, InboxPage, CrossSessionMessage, ShutdownOptions, ShutdownReport, RecoveryIssue, KernelDiagnostic,
    EffectResolution, TaskListQuery, TaskListPage, LeaseGuardOptions, StorageBindingRef, JsonValue,
    EventEnvelope, EventQuery, EventPage, WatchOptions, TaskWaitOptions, TaskEventSnapshot, SessionEventSnapshot,
    ExitRecord, WaitSpec, TaskSignal, TaskStartOptions, EffectRequest, EffectExecutionContext, EffectReconcileResult,
    ProgramRef, TaskDependency, RetryPolicy, SerializableError, InteractionResponse, TaskRecord, SessionRecord,
    EffectAttempt, TaskStatus, SessionStatus, TaskProgramManifest, RecoveryTiming, ResolvedStorageBinding,
    ResourceRight, AuthorizedEffectGrant } from './domain/types';
export type { KernelPlugin } from './ports/plugin';
export { parseEventEnvelope, parseKernelEvent } from './public/events';
export type { KernelEventMap, KernelEvent } from './domain/events';
export { KernelError, KernelErrorCode } from './domain/errors';
