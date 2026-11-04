import type {CapabilityBinding, EffectExecutionContext, ExitRecord, TaskSpec} from './types';

export interface TaskRef {sessionId: string; taskId: string}
export interface TaskCallOwner extends TaskRef {effectId: string; depth: number; maxDepth: number}
/** The host authorizes this exact plan; the Kernel owns execution and settlement. */
export interface TaskCallPlan {
    targetSessionId: string;
    task: Omit<TaskSpec, 'requestId' | 'parent' | 'spawnKey' | 'deferStart'>;
    bindings?: CapabilityBinding[];
    maxDepth: number;
    /** null opts into kernel.call@2 without a total execution deadline. */
    timeoutMs?: number | null;
}
export interface TaskCallRequest extends TaskCallPlan {resourceHandleId: string}
export interface TaskCallResult {task: TaskRef; exit: ExitRecord}
export interface TaskCallBinding {
    version: 1; owner: TaskCallOwner; targetSessionId: string; instanceId: string; fingerprint: string;
}
export type TaskCallAuthority = (request: TaskCallPlan, owner: Pick<EffectExecutionContext, 'sessionId' | 'taskId' | 'effectId'>,
    phase: 'execute' | 'cleanup') => Promise<void>;
