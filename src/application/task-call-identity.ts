import type {EffectExecutionContext, TaskSpec} from '../domain/types';
import type {TaskCallPlan, TaskCallRequest, TaskCallBinding, TaskCallOwner} from '../domain/task-call';
import {ArchiveHash} from './archive-hash';
import {canonical} from './canonical';
import {assertDurableValue} from './durability';
import {KernelErrorCode, kernelError} from '../domain/errors';

type Owner = Pick<EffectExecutionContext, 'sessionId' | 'taskId' | 'effectId'>;
export function taskCallFingerprint(value: TaskCallPlan): string {
    const hash = new ArchiveHash(); hash.update(canonical(plan(value))); return hash.finish();
}
/** Validate before admission without rewriting opaque input representations or their integrity seals. */
export function snapshotTaskCallPlan(value: TaskCallPlan): TaskCallPlan {
    validateTaskCall({...value, resourceHandleId: 'unbound'});
    if (Object.hasOwn(value, 'resourceHandleId')) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Task call plan cannot supply a resource handle');
    return JSON.parse(JSON.stringify(plan(value)));
}
export function taskCallCapabilityUri(value: TaskCallPlan): string {return `task-call:${taskCallFingerprint(value)}`;}
export function taskCallKey(owner: Owner): string {return `kernel.task-call.${ownerHash(owner)}`;}
export function taskCallTargetId(owner: Owner): string {return `task_owned_${ownerHash(owner)}`;}
function ownerHash(owner: Owner): string {
    const hash = new ArchiveHash(); hash.update(canonical([owner.sessionId, owner.taskId, owner.effectId])); return hash.finish();
}
export function taskCallTargetSpec(request: TaskCallPlan, owner: Owner): TaskSpec {
    return structuredClone({...request.task, requestId: `task-call:${ownerHash(owner)}`, deferStart: true});
}
export function taskCallPlan(request: TaskCallRequest): TaskCallPlan {return structuredClone(plan(request));}
function plan(request: TaskCallPlan): TaskCallPlan {
    return {targetSessionId: request.targetSessionId, task: request.task, maxDepth: request.maxDepth,
        ...(request.timeoutMs !== undefined ? {timeoutMs: request.timeoutMs} : {}),
        ...(request.bindings ? {bindings: request.bindings} : {})};
}
export function validateTaskCall(request: TaskCallRequest): void {
    if (!request || !validEnvelope(request) || !validTask(request.task) || !validBindings(request.bindings))
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid owned Task call plan');
    assertDurableValue(plan(request), 'Task call plan');
}
function validEnvelope(request: TaskCallRequest): boolean {
    return text(request.targetSessionId) && text(request.resourceHandleId) && depth(request.maxDepth) && timeout(request.timeoutMs)
        && Object.keys(request).every(key => ['targetSessionId', 'task', 'bindings', 'maxDepth', 'timeoutMs', 'resourceHandleId'].includes(key));
}
function validTask(task: TaskCallPlan['task']): boolean {
    return !!task && typeof task === 'object' && !Array.isArray(task) && text(task.program?.kind) && text(task.program?.version)
        && Object.keys(task).every(key => ['program', 'input', 'dependsOn', 'retry', 'priority', 'labels', 'retryOfTaskId', 'recovery'].includes(key));
}
function validBindings(bindings: TaskCallPlan['bindings']): boolean {
    if (bindings === undefined) return true;
    if (!Array.isArray(bindings)) return false;
    const keys = new Set<string>();
    return bindings.every(binding => {
        if (!binding || ![binding.kind, binding.uri, binding.signalKey].every(text) || keys.has(binding.signalKey)
            || !Array.isArray(binding.rights) || !binding.rights.every(right => ['read', 'write', 'execute', 'grant', 'admin'].includes(right))) return false;
        keys.add(binding.signalKey);
        return Object.keys(binding).every(key => ['kind', 'uri', 'signalKey', 'rights'].includes(key));
    });
}
function depth(value: number): boolean {return Number.isSafeInteger(value) && value >= 1;}
function timeout(value?: number | null): boolean {return value === undefined || value === null || Number.isFinite(value) && value > 0 && value <= 2147483647;}
function text(value: unknown): value is string {return typeof value === 'string' && !!value.trim();}

export function validateTaskCallBinding(value: unknown, request: TaskCallRequest, context: Pick<EffectExecutionContext, 'sessionId' | 'taskId' | 'effectId'>): TaskCallBinding {
    const binding = value as TaskCallBinding;
    if (!validHeader(binding, request) || !validOwner(binding.owner, context, request.maxDepth)) throw kernelError(KernelErrorCode.TASK_CALL_BINDING_INVALID, 'TASK_CALL_BINDING_INVALID');
    return binding;
}
function validHeader(binding: TaskCallBinding, request: TaskCallRequest): boolean {
    return !!binding && binding.version === 1 && binding.targetSessionId === request.targetSessionId && typeof binding.instanceId === 'string'
        && !!binding.instanceId && binding.fingerprint === taskCallFingerprint(request);
}
function validOwner(owner: TaskCallOwner, context: Pick<EffectExecutionContext, 'sessionId' | 'taskId' | 'effectId'>, maximum: number): boolean {
    return !!owner && owner.sessionId === context.sessionId && owner.taskId === context.taskId && owner.effectId === context.effectId
        && Number.isSafeInteger(owner.depth) && Number.isSafeInteger(owner.maxDepth) && owner.depth >= 1 && owner.depth <= owner.maxDepth && owner.maxDepth <= maximum;
}
