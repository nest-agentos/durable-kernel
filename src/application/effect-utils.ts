// @file: durable-kernel/src/application/effect-utils.ts
// Effect 执行的纯辅助：请求规范化、结果回填、超时/取消、失败序列化。

import type {
    EffectAdapter,
    EffectExecutionContext,
    EffectRequest,
    InteractionRequest,
    JsonValue,
    ResourceRight,
    SerializableError,
    TaskRecord,
} from '../domain/types';
import { type EffectClaim, type EffectCompletion } from '../infrastructure/seqfile/store';
import { assertDurableValue } from './durability';
import { KernelErrorCode, kernelError } from '../domain/errors';

export type RequiredEffect = EffectRequest & { id: string; timeoutMs: number | null };

export function normalizeEffect(effect: EffectRequest): RequiredEffect {
    const normalized = { ...effect, id: effect.id ?? `effect_${encodeURIComponent(effect.idempotencyKey)}`, timeoutMs: effect.timeoutMs === undefined ? 30_000 : effect.timeoutMs };
    if (!normalized.kind || !normalized.version || !normalized.idempotencyKey) {
        throw new Error('Effect kind, version, and idempotencyKey are required');
    }
    if (normalized.timeoutMs !== null && (!Number.isFinite(normalized.timeoutMs) || normalized.timeoutMs <= 0)) {
        throw new Error('Effect timeoutMs must be null or a positive finite number');
    }
    if (effect.retry && (!Number.isInteger(effect.retry.maxAttempts) || effect.retry.maxAttempts < 1 || !Number.isFinite(effect.retry.backoffMs ?? 0) || (effect.retry.backoffMs ?? 0) < 0)) throw new Error('Invalid Effect retry policy');
    assertDurableValue(normalized.request, 'Effect request');
    return normalized;
}

export function addEffect(task: TaskRecord, effect: RequiredEffect): TaskRecord {
    const existing = task.effects[effect.id] ?? Object.values(task.effects)
        .find(item => item.request.idempotencyKey === effect.idempotencyKey);
    if (existing) {
        if (canonical(existing.request) !== canonical(effect)) throw new Error(`Effect identity conflict: ${effect.id}`);
        return task;
    }
    const persisted = { request: effect, status: 'pending' as const, ...(effect.timeoutMs === null ? {} : {deadlineAt: Date.now() + effect.timeoutMs}), attemptCount: 0, attempts: [] };
    return { ...task, effects: { ...task.effects, [effect.id]: persisted } };
}

export function addInteraction(
    task: TaskRecord,
    request: InteractionRequest<JsonValue>,
): TaskRecord {
    if (!request.id || !request.prompt) throw new Error('Interaction id and prompt are required');
    if (task.interactions?.[request.id]) return task;
    const interaction = { ...request, status: 'pending' as const, requestedAt: Date.now() };
    return { ...task, interactions: { ...(task.interactions ?? {}), [request.id]: interaction } };
}

export async function executeEffectAdapter(
    adapter: EffectAdapter,
    effect: RequiredEffect,
    claim: EffectClaim,
    context: EffectExecutionContext,
): Promise<EffectCompletion> {
    if (context.abortSignal.aborted) throw abortError(context.abortSignal.reason);
    const indeterminate=claim.effect.attempts.some(attempt=>attempt.outcome==='indeterminate');
    const wasRecovered = indeterminate||claim.effect.attempts.some(attempt => attempt.outcome === 'lost');
    if (wasRecovered && !claim.effect.replayAuthorized && adapter.reconcile) {
        const reconciled = await adapter.reconcile(effect.request, context);
        if (reconciled.status === 'completed') return { result: reconciled.result };
        if (reconciled.status === 'indeterminate') {
            return { error: reconciled.error, indeterminate: true };
        }
    } else if (wasRecovered && !claim.effect.replayAuthorized && (indeterminate||adapter.recoveryPolicy !== 'idempotent-retry')) {
        return { error: { message: 'Effect outcome is unknown; reconciliation is required' }, indeterminate: true };
    }
    if (context.abortSignal.aborted) throw abortError(context.abortSignal.reason);
    if ((claim.effect.deadlineAt ?? Infinity) <= Date.now()) return { error: { message: 'Effect deadline expired' }, indeterminate: wasRecovered };
    return { result: await adapter.execute(effect.request, context) };
}

function canonical(value: unknown): string {
    return JSON.stringify(value, (_key, item) => item && typeof item === 'object' && !Array.isArray(item)
        ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b))) : item);
}

export async function executeEffectWithDeadline(
    adapter: EffectAdapter,
    effect: RequiredEffect,
    claim: EffectClaim,
    context: EffectExecutionContext,
    controller: AbortController,
): Promise<EffectCompletion> {
    if (context.abortSignal.aborted) throw abortError(context.abortSignal.reason);
    let onAbort = () => {};
    const interrupted = new Promise<never>((_, reject) => {
        onAbort = () => reject(abortError(context.abortSignal.reason));
        context.abortSignal.addEventListener('abort', onAbort, { once: true });
    });
    const stopDeadline = armDeadline(effect, claim.effect.deadlineAt, controller);
    try {
        return await Promise.race([executeEffectAdapter(adapter, effect, claim, context), interrupted]);
    } catch (error) {
        if ((claim.effect.deadlineAt ?? Infinity) <= Date.now()) controller.abort(timeoutError(effect));
        throw error;
    } finally {
        stopDeadline(); context.abortSignal.removeEventListener('abort', onAbort);
    }
}
function armDeadline(effect: RequiredEffect, persisted: number | undefined, controller: AbortController): () => void {
    const deadlineAt = persisted ?? (effect.timeoutMs === null ? undefined : Date.now() + effect.timeoutMs);
    if (deadlineAt === undefined) return () => {};
    let timer: ReturnType<typeof setTimeout>;
    const schedule = () => {timer = setTimeout(() => {
        if (deadlineAt > Date.now()) schedule(); else controller.abort(timeoutError(effect));
    }, Math.max(1, Math.min(2_147_483_647, deadlineAt - Date.now())));};
    schedule(); return () => clearTimeout(timer);
}
function timeoutError(effect: RequiredEffect): Error {
    const error = kernelError(KernelErrorCode.EFFECT_TIMEOUT, `Effect timed out after ${effect.timeoutMs}ms`);
    error.name = 'EffectTimeoutError'; return error;
}

export function abortError(reason: unknown): Error {
    if (reason instanceof Error) return reason;
    const error = kernelError(KernelErrorCode.EFFECT_CANCELLED, 'Effect execution was cancelled');
    error.name = 'AbortError';
    return error;
}

export function effectFailure(error: unknown): EffectCompletion {
    return { error: serializeError(error) };
}

export function effectControllerKey(sessionId: string, taskId: string, effectId: string): string {
    return `${sessionId}/${taskId}/${effectId}`;
}

export function activeEffectIds(task: TaskRecord): Set<string> {
    return new Set(Object.entries(task.effects)
        .filter(([, effect]) => effect.status === 'pending' || effect.status === 'leased' || effect.status === 'indeterminate' || effect.cleanupPending)
        .map(([id]) => id));
}

export function serializeError(error: unknown): SerializableError {
    return error instanceof Error ? { message: error.message, stack: error.stack,
        code: 'code' in error && typeof error.code === 'string' ? error.code : undefined } : { message: String(error) };
}

export function isMissingPath(error: unknown): boolean {
    return typeof error === 'object' && error !== null && 'code' in error
        && (error as { code?: unknown }).code === 'ENOENT';
}

/** 断言 Effect 上下文已授予某 handle 的某类 resource 的某 right。 */
export function assertEffectGrant(
    context: EffectExecutionContext,
    handleId: string,
    resourceKind: string,
    right: ResourceRight = 'execute',
): void {
    const grant = context.grants.find(item =>
        item.handleId === handleId
        && item.right === right
        && item.resource.kind === resourceKind);
    if (!grant) throw new Error(`${resourceKind.toUpperCase()} ${right} grant is required: ${handleId}`);
}

/** 统一判定交互审批结果：true / {approved:true} / 常见同意字符串。 */
export function interactionApproved(value: JsonValue): boolean {
    if (value === true) return true;
    if (typeof value === 'string') {
        return ['yes', 'approved', 'allow', 'true', 'y', 'ok'].includes(value.trim().toLowerCase());
    }
    return typeof value === 'object' && value !== null && !Array.isArray(value) && value.approved === true;
}
