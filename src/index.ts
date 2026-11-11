export * from './domain/types';
export type {TaskRef, TaskCallPlan, TaskCallRequest, TaskCallResult, TaskCallOwner, TaskCallAuthority} from './domain/task-call';
export {taskCallCapabilityUri, snapshotTaskCallPlan} from './application/task-call-identity';
export type { KernelEventMap, KernelEvent } from './domain/events';
export { parseEventEnvelope, parseKernelEvent } from './public/events';
export * from './domain/resource-api';
export type { CacheApi } from './domain/cache';
export type { KernelInspectionPort } from './ports/kernel-view';
export type { TaskStat, TaskStats, SessionStat, TaskExecutionStat } from './domain/status';
// Observation projections are public so hosts can render "requested vs confirmed" without
// re-deriving control state; see doc/kernel-api.md (domain/status.ts).
export { taskStat, taskStats, sessionStat, taskExecutionStat } from './domain/status';
export { createHarness, type Harness, type Session, type Task, type SessionReader } from './core';
export { resourceResult } from './public/resources';
export { defineTask, type TaskStepEvent } from './public/program';
export * from './domain/errors';
export * from './ports/registry';
export * from './ports/plugin';
export { Kernel, type KernelOptions } from './application/kernel';
export { bindCapabilities, type CapabilityBinding } from './application/capabilities';
export { assertEffectGrant, interactionApproved } from './application/effect-utils';
// Adapters use the same validated Task layout as the store without duplicating its paths.
export { taskPath as taskStoragePath } from './infrastructure/seqfile/seqfile-core';
export {
    SeqFileKernelStore,
    createId,
    ensureTree,
    type TaskClaim,
} from './infrastructure/seqfile/store';

export type { AttachedTask, TaskControlPlane, PendingInteractionTaskSource, TaskAttachmentSession, EditorTaskControlPlane, TaskAttachmentRecovery, ReadTaskAttachment } from './ports/task-control';
