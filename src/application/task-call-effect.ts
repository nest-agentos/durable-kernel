import type {EffectAdapter, EffectExecutionContext, EffectReconcileResult, SessionEventSnapshot, TaskHandle, KernelDiagnostic, SharedStateEntry} from '../domain/types';
import type {TaskCallBinding, TaskCallRequest, TaskCallResult} from '../domain/task-call';
import {KernelErrorCode, kernelError} from '../domain/errors';
import {taskStat} from '../domain/status';
import {canonical} from './canonical';
import {taskCallCapabilityUri, taskCallKey, validateTaskCall, validateTaskCallBinding} from './task-call-identity';
import {assertEffectGrant} from './effect-utils';
import {bindCapabilities} from './capabilities';
import {taskTreeIds} from '../domain/task-tree';

export interface TaskCallPorts {
    prepare(request: TaskCallRequest, context: EffectExecutionContext): Promise<TaskCallBinding>;
    binding(context: EffectExecutionContext): Promise<SharedStateEntry | undefined>;
    target(request: TaskCallRequest, binding: TaskCallBinding, phase: 'execute' | 'cleanup'): Promise<TaskHandle>;
    snapshot(sessionId: string): Promise<SessionEventSnapshot>;
    resourcesSettled(sessionId: string, taskIds: string[]): Promise<boolean>;
    cancel(sessionId: string, taskId: string, remaining: boolean): Promise<void>;
    changed(sessionId: string, signal: AbortSignal): Promise<void>;
    diagnose(event: KernelDiagnostic): void;
}
class AdmissionFailure extends Error {
    readonly code: string;
    constructor(cause: unknown) {super(cause instanceof Error ? cause.message : 'Task call admission failed'); this.code = (cause as {code?: string})?.code ?? 'TASK_CALL_ADMISSION_FAILED';}
}
/** The original Effect is the durable intent; the target's idempotent deferred Task is the stop fence. */
export class TaskCallEffect implements EffectAdapter<TaskCallRequest, TaskCallResult> {
    readonly kind = 'task.call';
    readonly version = '1';
    constructor(private readonly ports: TaskCallPorts) {}
    shouldRetry(): boolean {return false;}
    failureOutcome(error: unknown): 'failed' | 'indeterminate' {return error instanceof AdmissionFailure ? 'failed' : 'indeterminate';}
    async execute(request: TaskCallRequest, context: EffectExecutionContext): Promise<TaskCallResult> {
        return this.diagnose('execute', context, async progress => {
            const binding = await this.admit(request, context); progress.stage = 'target';
            const target = await this.ports.target(request, binding, 'execute');
            context.abortSignal.throwIfAborted(); progress.stage = 'start';
            if ((await target.status()).task.status === 'created') {
                if (request.bindings?.length) await bindCapabilities(target, request.bindings, undefined, {deadlineAt: context.deadlineAt});
                else await target.start({deadlineAt: context.deadlineAt});
            }
            progress.stage = 'observe';
            return context.waitForObservation!(signal => this.observe(target, request.targetSessionId, signal));
        });
    }
    private async admit(request: TaskCallRequest, context: EffectExecutionContext): Promise<TaskCallBinding> {
        let absent = false;
        try {
            validateTaskCall(request); authorize(request, context);
            if (!context.sessionState?.setIfRunning || !context.waitForObservation) throw failure('TASK_CALL_CAPABILITY_UNAVAILABLE');
            const key = taskCallKey(context), existing = await context.sessionState.get(key); absent = !existing;
            if (existing) return validateTaskCallBinding(existing.value, request, context);
            const binding = await this.ports.prepare(request, context);
            if (!await context.sessionState.setIfRunning(key, JSON.parse(canonical(binding)), null)) throw failure('TASK_CALL_ADMISSION_HELD');
            return binding;
        } catch (error) {
            if (absent || context.attemptNumber === 1) throw new AdmissionFailure(error);
            throw error;
        }
    }
    async reconcile(request: TaskCallRequest, context: EffectExecutionContext): Promise<EffectReconcileResult<TaskCallResult>> {
        // Re-creating the same deferred Task is idempotent and cannot dispatch it twice.
        return {status: 'completed', result: await this.execute(request, context)};
    }
    async cancel(request: TaskCallRequest, context: EffectExecutionContext): Promise<void> {
        return this.diagnose('cancel', context, async progress => {
            validateTaskCall(request);
            const saved = await this.ports.binding(context);
            if (!saved) return;
            const binding = validateTaskCallBinding(saved.value, request, context); progress.stage = 'target';
            const target = await this.ports.target(request, binding, 'cleanup'), snapshot = await target.status();
            progress.stage = 'stop'; await this.ports.cancel(request.targetSessionId, target.id, !!snapshot.task.exit);
            progress.stage = 'settlement'; const observed = await this.tree(request.targetSessionId, target.id);
            if (!observed.settled) throw failure('TASK_CALL_CLEANUP_UNCONFIRMED');
        });
    }
    private async observe(target: TaskHandle, sessionId: string, signal: AbortSignal): Promise<TaskCallResult> {
        while (true) {
            signal.throwIfAborted(); const {root, settled} = await this.tree(sessionId, target.id);
            if (settled) return {task: {sessionId, taskId: target.id}, exit: root.exit!};
            await this.ports.changed(sessionId, signal);
        }
    }
    private async tree(sessionId: string, taskId: string) {
        const snapshot = await this.ports.snapshot(sessionId), root = snapshot.tasks.find(task => task.id === taskId);
        if (!root || root.rootTaskId !== root.id || root.parentTaskId || root.sessionId !== sessionId) throw failure('TASK_CALL_TARGET_INVALID');
        const ids = taskTreeIds(snapshot.tasks, root.id), tasks = snapshot.tasks.filter(task => ids.has(task.id));
        if (tasks.some(task => task.rootTaskId !== root.id || task.sessionId !== sessionId)
            || snapshot.tasks.some(task => task.rootTaskId === root.id && !ids.has(task.id))) throw failure('TASK_CALL_TARGET_INVALID');
        return {root, settled: tasks.every(task => taskStat(task).settled) && await this.ports.resourcesSettled(sessionId, [...ids])};
    }
    private async diagnose<T>(operation: string, context: EffectExecutionContext, work: (progress: {stage: string}) => Promise<T>): Promise<T> {
        const started = Date.now(), progress = {stage: 'admission'}; let errorCode: string | undefined, success = false;
        try {const result = await work(progress); success = true; return result;}
        catch (error) {const code = (error as {code?: unknown})?.code;
            errorCode = typeof code === 'string' && /^[A-Z0-9_]{1,80}$/.test(code) ? code : 'TASK_CALL_OPERATION_FAILED'; throw error;}
        finally {try {this.ports.diagnose({operation: `task.call.${operation}`, stage: progress.stage, outcome: success ? 'success' : 'failure',
            operationId: taskCallKey(context), sessionId: context.sessionId, taskId: context.taskId, effectId: context.effectId,
            durationMs: Date.now() - started, errorCode});} catch { /* Diagnostics cannot change durable execution facts. */ }}
    }
}
function authorize(request: TaskCallRequest, context: EffectExecutionContext): void {
    assertEffectGrant(context, request.resourceHandleId, 'task');
    const grant = context.grants.find(item => item.handleId === request.resourceHandleId && item.right === 'execute')!;
    if (grant.resource.uri !== taskCallCapabilityUri(request)) throw failure('TASK_CALL_PLAN_NOT_AUTHORIZED');
    context.abortSignal.throwIfAborted();
}
function failure(code: keyof typeof KernelErrorCode): Error {return kernelError(KernelErrorCode[code], code);}
