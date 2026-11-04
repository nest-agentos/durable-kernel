import type { CrossSessionMessage, EffectAttempt, EffectResolution, EventEnvelope, ExitRecord, SerializableError, SessionRecord, TaskAttempt, TaskControl, TaskRecord, TaskTreeCancellationReceipt } from './types';

/** Extend with application-owned names and validate those payloads in their owning adapter. */
export interface KernelEventMap {
    'session.created': SessionRecord;
    'session.identity.assigned': SessionRecord & {instanceId: string};
    'session.open': SessionRecord;
    'session.suspending': SessionRecord;
    'session.suspended': SessionRecord;
    'session.closing': SessionRecord;
    'session.closed': SessionRecord;
    'session.archived': SessionRecord;
    'session.reopened': SessionRecord;
    'session.archive.imported': {mode: 'restore' | 'fork'; archiveId: string};
    'task.created': TaskRecord;
    'task.tree-cancellation.requested': TaskTreeCancellationReceipt;
    'task.leased': TaskAttempt;
    'task.succeeded': ExitRecord & { status: 'succeeded' };
    'task.failed': ExitRecord & { status: 'failed' };
    'task.cancelled': ExitRecord & { status: 'cancelled' };
    'task.control.run': TaskControl;
    'task.control.pause': TaskControl;
    'task.control.interrupt': TaskControl;
    'task.control.withdraw-pause': TaskControl;
    'task.control.acknowledged': TaskControl;
    'effect.leased': EffectAttempt;
    'effect.succeeded': { result: unknown };
    'effect.failed': { error: SerializableError; cleanupPending?: boolean };
    'effect.indeterminate': { error: SerializableError };
    'effect.retry.scheduled': { effectId: string; readyAt: number; attempt: number; maxAttempts: number; error: SerializableError };
    'effect.resolved': EffectResolution;
    'effect.interrupt.requested': { effectId: string };
    'effect.cleanup.confirmed': { effectId: string };
    'effect.attempt.lost': { effectId: string };
    'session.message.queued': CrossSessionMessage;
    'session.message.received': CrossSessionMessage;
    'session.message.delivered': CrossSessionMessage;
    'session.message.rejected': CrossSessionMessage;
    'message.consumed': { messageId: string };
    'session.message.settlement-acknowledged': { messageId: string; side: 'inbox' | 'outbox' };
}

export type KernelEvent = { [K in keyof KernelEventMap]: Omit<EventEnvelope, 'type' | 'payload' | 'schemaVersion'> & {
    schemaVersion: 2; type: K; payload: KernelEventMap[K];
} & (K extends `effect.${string}` ? { taskId: string; effectId: string } : {})
    & (K extends `task.${string}` ? { taskId: string } : {})
    & (K extends 'effect.leased' | 'effect.succeeded' | 'effect.failed' | 'effect.indeterminate' | 'effect.retry.scheduled'
        ? { attemptId: string } : {}) }[keyof KernelEventMap];
