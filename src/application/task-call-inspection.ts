import type {SharedStateEntry, TaskRecord} from '../domain/types';
import type {TaskCallOwner, TaskRef} from '../domain/task-call';
import {KernelErrorCode, kernelError} from '../domain/errors';
import {canonical} from './canonical';
import {taskCallTargetId, validateTaskCall, validateTaskCallBinding} from './task-call-identity';

type OwnerRef = Pick<TaskCallOwner, 'sessionId' | 'taskId' | 'effectId'>;
interface Ports {
    task(sessionId: string, taskId: string): Promise<TaskRecord>;
    target(sessionId: string, instanceId: string, taskId: string): Promise<TaskRecord | undefined>;
    binding(owner: OwnerRef): Promise<SharedStateEntry | undefined>;
    owner(owner: OwnerRef, maxDepth: number): Promise<TaskCallOwner>;
}
/** Read immutable ownership facts only; absence never authorizes a new target or turn. */
export async function inspectTaskCallTarget(ports: Ports, owner: OwnerRef, progress: {stage: string}): Promise<TaskRef | undefined> {
    assertOwner(owner);
    progress.stage = 'caller'; const effect = await callEffect(ports, owner); if (!effect) return;
    const request = effect.request.request as import('../domain/task-call').TaskCallRequest;
    progress.stage = 'binding'; const stored = await ports.binding(owner); if (!stored) return;
    const binding = validateTaskCallBinding(stored.value, request, owner), expected = await ports.owner(owner, request.maxDepth);
    if (canonical(binding.owner) !== canonical(expected)) throw invalid('TASK_CALL_BINDING_INVALID');
    progress.stage = 'target';
    const id = taskCallTargetId(owner), target = await ports.target(binding.targetSessionId, binding.instanceId, id);
    if (!target) {if (effect.status === 'succeeded') throw invalid('TASK_CALL_TARGET_INVALID'); return;}
    assertTarget(target, id, binding);
    return {sessionId: binding.targetSessionId, taskId: id};
}
function assertOwner(owner: OwnerRef): void {
    if (![owner?.sessionId, owner?.taskId, owner?.effectId].every(value => typeof value === 'string' && !!value.trim()))
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Task call inspection requires the original owner identity');
}
async function callEffect(ports: Ports, owner: OwnerRef) {
    const caller = await ports.task(owner.sessionId, owner.taskId);
    if (caller.id !== owner.taskId || caller.sessionId !== owner.sessionId) throw invalid('TASK_CALL_BINDING_INVALID');
    const effect = caller.effects[owner.effectId]; if (!effect) return;
    if (effect.request.kind !== 'task.call' || effect.request.version !== '1') throw kernelError(KernelErrorCode.INVALID_SPEC, 'The selected Effect is not a Task call');
    const request = effect.request.request as import('../domain/task-call').TaskCallRequest; validateTaskCall(request);
    return effect;
}
function assertTarget(target: TaskRecord, id: string, binding: import('../domain/task-call').TaskCallBinding): void {
    if (target.id !== id || target.rootTaskId !== id || target.parentTaskId || target.sessionId !== binding.targetSessionId
        || canonical(target.ownedBy) !== canonical(binding.owner)) throw invalid('TASK_CALL_TARGET_INVALID');
}
function invalid(code: 'TASK_CALL_BINDING_INVALID' | 'TASK_CALL_TARGET_INVALID'): Error {return kernelError(KernelErrorCode[code], 'Task call ownership evidence is invalid');}
