import type { EventEnvelope } from '../domain/types';
import type { KernelEvent, KernelEventMap } from '../domain/events';
import { KernelErrorCode, kernelError } from '../domain/errors';
import { assertDurableValue } from '../application/durability';

type Row = Record<string, unknown>;
const row = (value: unknown): value is Row => !!value && typeof value === 'object' && !Array.isArray(value);
const text = (value: unknown): value is string => typeof value === 'string' && value.trim().length > 0;
const number = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value);
const error = (value: unknown) => row(value) && typeof value.message === 'string'
    && (value.code === undefined || typeof value.code === 'string') && (value.stack === undefined || typeof value.stack === 'string');
const attempt = (p: Row) => text(p.id) && text(p.workerId) && text(p.leaseToken) && number(p.leaseUntil) && number(p.startedAt);
const control = (p: Row) => Number.isSafeInteger(p.epoch) && text(p.requestId) && ['run', 'pause', 'interrupt'].includes(String(p.mode)) && typeof p.acknowledged === 'boolean';
const session = (p: Row) => text(p.id) && ['open', 'suspending', 'suspended', 'closing', 'closed', 'archived'].includes(String(p.status));
const message = (p: Row) => text(p.id) && text(p.sourceSessionId) && text(p.targetSessionId) && text(p.topic)
    && ['pending', 'delivered', 'rejected'].includes(String(p.status)) && p.payload !== undefined;
const exit = (p: Row) => text(p.taskId) && number(p.completedAt) && ['succeeded', 'failed', 'cancelled'].includes(String(p.status));

const validators: Record<string, (p: Row) => boolean> = {
    'session.identity.assigned': p => session(p) && text(p.instanceId),
    'session.archive.imported': p => ['restore', 'fork'].includes(String(p.mode)) && text(p.archiveId),
    ...Object.fromEntries(['created', 'open', 'suspending', 'suspended', 'closing', 'closed', 'archived', 'reopened'].map(name => [`session.${name}`, session])),
    ...Object.fromEntries(['run', 'pause', 'interrupt', 'withdraw-pause', 'acknowledged'].map(name => [`task.control.${name}`, control])),
    ...Object.fromEntries(['succeeded', 'failed', 'cancelled'].map(name => [`task.${name}`, exit])),
    ...Object.fromEntries(['queued', 'received', 'delivered', 'rejected'].map(name => [`session.message.${name}`, message])),
    ...Object.fromEntries(['interrupt.requested', 'cleanup.confirmed', 'attempt.lost'].map(name => [`effect.${name}`, (p: Row) => text(p.effectId)])),
    'task.tree-cancellation.requested': p => text(p.taskId) && text(p.requestId)
        && Number.isSafeInteger(p.expectedEpoch) && Number(p.expectedEpoch) >= 0
        && Number.isSafeInteger(p.controlEpoch) && Number(p.controlEpoch) >= 0
        && ['succeeded', 'failed', 'cancelled'].includes(String(p.rootStatus)) && number(p.acceptedAt)
        && Number.isSafeInteger(p.taskCount) && Number(p.taskCount) >= 1,
    'task.created': p => text(p.id) && text(p.sessionId) && text(p.status),
    'task.leased': attempt,
    'effect.leased': attempt,
    'effect.succeeded': p => 'result' in p,
    'effect.failed': p => error(p.error),
    'effect.indeterminate': p => error(p.error),
    'effect.retry.scheduled': p => text(p.effectId) && number(p.readyAt) && number(p.attempt) && number(p.maxAttempts) && error(p.error),
    'effect.resolved': p => text(p.requestId) && text(p.effectId) && row(p.outcome)
        && (p.outcome.type === 'retry' || p.outcome.type === 'reconcile' || (p.outcome.type === 'completed' && 'result' in p.outcome) || (p.outcome.type === 'failed' && error(p.outcome.error))),
    'message.consumed': p => text(p.messageId),
    'session.message.settlement-acknowledged': p => text(p.messageId) && ['inbox', 'outbox'].includes(String(p.side)),
};

export function assertCustomEventType(type: string): void {
    if (!text(type) || /^(task|effect|session|message|resource|cache|budget|workspace)\./.test(type)) {
        throw kernelError(KernelErrorCode.EVENT_INVALID, 'Custom events must use an application-owned event namespace');
    }
}

export function parseEventEnvelope(value: unknown): EventEnvelope {
    if (!row(value) || !text(value.type) || !text(value.sessionId) || !Number.isSafeInteger(value.sequence)
        || Number(value.sequence) < 1 || !number(value.occurredAt)) throw invalidEvent();
    if (value.schemaVersion !== undefined && value.schemaVersion !== 1 && value.schemaVersion !== 2) throw invalidEvent();
    for (const key of ['taskId', 'effectId', 'attemptId']) if (value[key] !== undefined && !text(value[key])) throw invalidEvent();
    try { assertDurableValue(value, 'Event'); } catch { throw invalidEvent(); }
    return value as unknown as EventEnvelope;
}

/** Legacy v1 and unknown event types remain raw; never guess missing legacy identities. */
export function parseKernelEvent(value: unknown): KernelEvent | undefined {
    const event = parseEventEnvelope(value), validate = validators[event.type];
    if ((event.schemaVersion ?? 1) < 2 || !Object.hasOwn(validators, event.type)) return undefined;
    if (!row(event.payload) || !validate(event.payload)) throw invalidEvent();
    if (event.type.startsWith('effect.')) assertEffectIdentity(event);
    if (event.type.startsWith('task.') && !text(event.taskId)) throw invalidEvent();
    if (['task.succeeded', 'task.failed', 'task.cancelled'].includes(event.type)
        && (event.payload.taskId !== event.taskId || event.payload.status !== event.type.slice(5))) throw invalidEvent();
    if (event.type === 'task.tree-cancellation.requested' && event.payload.taskId !== event.taskId) throw invalidEvent();
    if (event.payload.effectId !== undefined && event.type.startsWith('effect.') && event.payload.effectId !== event.effectId) throw invalidEvent();
    return event as KernelEvent;
}

function assertEffectIdentity(event: EventEnvelope): void {
    if (!text(event.effectId) || !text(event.taskId)) throw invalidEvent();
    if (['effect.leased', 'effect.succeeded', 'effect.failed', 'effect.indeterminate', 'effect.retry.scheduled'].includes(event.type)
        && !text(event.attemptId)) throw invalidEvent();
}
function invalidEvent() { return kernelError(KernelErrorCode.EVENT_INVALID, 'Invalid journal event envelope or payload'); }

/** The runtime catalog covers built-in names; augmented application names retain their own validators. */
export type { KernelEventMap };
