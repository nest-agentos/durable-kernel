import {indexRuntimeMessage} from './runtime-recovery-index';
import type { ISeqFileTransaction } from '@nestos/vfs-core';
import type { CrossSessionMessage, InboxPage, InboxQuery, LeaseGuardOptions, SessionMessageRequest, TaskMessageRequest, TaskRecord } from '../../domain/types';
import { assertDurableValue } from '../../application/durability';
import { appendEventTx, decode, encode, inboxKey, messagesPath, outboxKey } from './seqfile-core';
import { hasCancelledAncestorTx, indexTask, isTerminal, requireSessionTx, requireTaskTx, unregisterTaskWaitTx, wakeFromPendingEvents, writeTaskTx } from './store-helpers';
import { assertSharedLeaseTx } from './shared-lease';
import { KernelErrorCode, kernelError } from '../../domain/errors';

export async function enqueueSessionMessageTx(tx: ISeqFileTransaction, root: string, sessionId: string,
    request: SessionMessageRequest, recoveryPath?: string): Promise<CrossSessionMessage> {
    validateMessage(request);
    const id = `session-message:${encodeURIComponent(sessionId)}:${encodeURIComponent(request.idempotencyKey)}`;
    const path = messagesPath(root), fingerprint = canonicalMessage(request);
    const raw = await tx.getEntry(path, outboxKey(id));
    if (raw) {
        const previous = decode<CrossSessionMessage>(raw);
        if (previous.requestFingerprint !== fingerprint) throw kernelError(KernelErrorCode.MESSAGE_IDENTITY_CONFLICT, 'Message identity conflict');
        return previous;
    }
    const session = await requireSessionTx(tx, root);
    if (session.id !== sessionId) throw new Error('Message source session identity mismatch');
    if (!['open', 'suspended', 'suspending'].includes(session.status)) throw new Error('Message source session is closed');
    const message: CrossSessionMessage = { id, sourceSessionId: sessionId, targetSessionId: request.targetSessionId,
        targetTaskId: request.targetTaskId, topic: request.topic, payload: request.payload, correlationId: request.correlationId,
        expiresAt: request.expiresAt, requestFingerprint: fingerprint, status: 'pending', createdAt: Date.now() };
    await indexRuntimeMessage(tx, recoveryPath, message);
    await tx.setEntry(path, outboxKey(id), encode(message));
    await appendEventTx(tx, root, sessionId, undefined, 'session.message.queued', message);
    if (request.targetSessionId === sessionId) {
        await deliverMessageTx(tx, root, message);
        Object.assign(message, decode<CrossSessionMessage>((await tx.getEntry(path, inboxKey(id)))!));
        await indexRuntimeMessage(tx, recoveryPath, message);
        await tx.setEntry(path, outboxKey(id), encode(message));
    }
    return message;
}

function validateMessage(request: SessionMessageRequest): void {
    if (!request || !nonempty(request.idempotencyKey) || !nonempty(request.topic) || !nonempty(request.targetSessionId)) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Message identity, topic and target are required');
    if (request.targetTaskId !== undefined && !nonempty(request.targetTaskId)) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Message target task is empty');
    if (request.correlationId !== undefined && typeof request.correlationId !== 'string') throw kernelError(KernelErrorCode.INVALID_SPEC, 'Message correlationId must be a string');
    if (request.expiresAt !== undefined && (!Number.isFinite(request.expiresAt) || request.expiresAt < 0)) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid message deadline');
    try { assertDurableValue(request.payload, 'Message payload'); }
    catch (error) { throw kernelError(KernelErrorCode.INVALID_SPEC, error instanceof Error ? error.message : 'Message payload is not durable'); }
}

function nonempty(value: unknown): value is string { return typeof value === 'string' && !!value.trim(); }

function canonicalMessage(request: SessionMessageRequest): string {
    const identity = [request.targetSessionId, request.targetTaskId, request.topic, request.correlationId, request.payload, request.expiresAt];
    return JSON.stringify(identity, (_key, value) => value && typeof value === 'object' && !Array.isArray(value)
        ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b))) : value);
}

export async function inboxPageTx(tx: ISeqFileTransaction, root: string, query: InboxQuery = {}): Promise<InboxPage> {
    const after = query.afterSequence ?? 0, limit = query.limit ?? 100;
    if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 500) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid inbox page');
    await requireSessionTx(tx, root);
    const path = messagesPath(root), messages: CrossSessionMessage[] = [];
    await tx.walkEntries(path, row => { messages.push(decode(row.value)); return true; }, { keyPrefix: 'inbox/' });
    await repairDeliverySequences(tx, path, messages);
    const current = Number(await tx.getEntry(path, 'delivery-sequence') ?? 0);
    const throughSequence = query.throughSequence ?? current;
    if (!Number.isSafeInteger(throughSequence) || throughSequence < after || throughSequence > current) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid inbox cursor');
    const candidates = messages.filter(message => message.deliverySequence! > after && message.deliverySequence! <= throughSequence)
        .sort((a, b) => a.deliverySequence! - b.deliverySequence!);
    const items = candidates.slice(0, limit);
    return { items, throughSequence, nextAfterSequence: candidates.length > limit ? items.at(-1)!.deliverySequence : undefined };
}

async function repairDeliverySequences(tx: ISeqFileTransaction, path: string, messages: CrossSessionMessage[]): Promise<void> {
    for (const message of messages) if (message.deliverySequence === undefined) {
        message.deliverySequence = await tx.increment(path, 'delivery-sequence');
        await tx.setEntry(path, inboxKey(message.id), encode(message));
    }
}

export async function acknowledgeSessionMessageTx(tx: ISeqFileTransaction, root: string, id: string,
    options?: LeaseGuardOptions): Promise<CrossSessionMessage> {
    await assertSharedLeaseTx(tx, root, options?.lease);
    const session = await requireSessionTx(tx, root), path = messagesPath(root);
    const raw = await tx.getEntry(path, inboxKey(id));
    if (!raw) throw kernelError(KernelErrorCode.MESSAGE_NOT_FOUND, 'Message receipt is missing');
    const message = decode<CrossSessionMessage>(raw);
    if (message.targetSessionId !== session.id || message.targetTaskId) throw kernelError(KernelErrorCode.MESSAGE_CONSUMER_MISMATCH, 'Message is not owned by the Session consumer');
    if (message.status !== 'delivered') throw kernelError(KernelErrorCode.MESSAGE_NOT_DELIVERED, 'Message was not delivered');
    if (message.consumedAt !== undefined) return message;
    const consumed = { ...message, consumedAt: Date.now() };
    await tx.setEntry(path, inboxKey(id), encode(consumed));
    await appendEventTx(tx, root, session.id, undefined, 'message.consumed', { messageId: id });
    return consumed;
}

export async function enqueueMessageTx(tx: ISeqFileTransaction, root: string, sender: TaskRecord, request: TaskMessageRequest): Promise<CrossSessionMessage> {
    validateMessage(request);
    if (!request.targetTaskId) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Message target task is required');
    const id = `${sender.id}:${encodeURIComponent(request.idempotencyKey)}`;
    const raw = await tx.getEntry(messagesPath(root), outboxKey(id));
    const fingerprint = encode(request);
    if (raw) {
        const old = decode<CrossSessionMessage>(raw);
        if (old.requestFingerprint !== fingerprint && canonicalMessage(decode<SessionMessageRequest>(old.requestFingerprint!)) !== canonicalMessage(request)) throw kernelError(KernelErrorCode.MESSAGE_IDENTITY_CONFLICT, 'Message identity conflict');
        return old;
    }
    if (await hasCancelledAncestorTx(tx, root, sender)) throw new Error('Task ancestor cancelled');
    const message: CrossSessionMessage = {
        id, sourceSessionId: sender.sessionId, sourceTaskId: sender.id,
        targetSessionId: request.targetSessionId, targetTaskId: request.targetTaskId,
        topic: request.topic, payload: request.payload, correlationId: request.correlationId,
        status: 'pending', createdAt: Date.now(), requestFingerprint: fingerprint, expiresAt: request.expiresAt,
    };
    await indexRuntimeMessage(tx, sender.recoveryIndexPath, message);
    await tx.setEntry(messagesPath(root), outboxKey(id), encode(message));
    await appendEventTx(tx, root, sender.sessionId, sender.id, 'session.message.queued', message);
    if (message.targetSessionId === sender.sessionId) {
        await deliverMessageTx(tx, root, message);
        Object.assign(message, decode<CrossSessionMessage>((await tx.getEntry(messagesPath(root), inboxKey(message.id)))!));
        await indexRuntimeMessage(tx, sender.recoveryIndexPath, message);
        await tx.setEntry(messagesPath(root), outboxKey(id), encode(message));
    }
    return message;
}

export async function deliverMessageTx(tx: ISeqFileTransaction, root: string, message: CrossSessionMessage): Promise<boolean> {
    const existing = await tx.getEntry(messagesPath(root), inboxKey(message.id));
    if (existing) {
        const old = decode<CrossSessionMessage>(existing);
        if (messageIdentity(old) !== messageIdentity(message)) throw new Error('Message identity conflict');
        return false;
    }
    const session = await requireSessionTx(tx, root);
    if (session.id !== message.targetSessionId) throw new Error('Message target session identity mismatch');
    if (message.expiresAt !== undefined && message.expiresAt <= Date.now()) return rejectMessageTx(tx, root, message, 'expired', 'Message delivery deadline expired');
    if (session.status !== 'open' && session.status !== 'suspended' && session.status !== 'suspending') {
        return rejectMessageTx(tx, root, message, 'target-closed', 'Message target session is unavailable');
    }
    const sequence = await tx.increment(messagesPath(root), 'delivery-sequence');
    const delivered: CrossSessionMessage = { ...message, deliverySequence: sequence, status: 'delivered', deliveredAt: Date.now() };
    if (message.targetTaskId) {
        const task = await requireTaskTx(tx, root, message.targetTaskId);
        if (isTerminal(task.status)) return rejectMessageTx(tx, root, message, 'target-terminal', 'Message target task is terminal');
        if (await hasCancelledAncestorTx(tx, root, task)) return rejectMessageTx(tx, root, message, 'target-ancestor-cancelled', 'Message target task ancestor is cancelled');
        let next: TaskRecord = { ...task, pendingEvents: [...task.pendingEvents, { type: 'message', message: delivered }],
            version: task.version + 1, updatedAt: Date.now() };
        next = wakeFromPendingEvents(next);
        if (next.status === 'ready') await unregisterTaskWaitTx(tx, root, task);
        await writeTaskTx(tx, root, next); await indexTask(tx, root, next);
    }
    await tx.setEntry(messagesPath(root), inboxKey(message.id), encode(delivered));
    await appendEventTx(tx, root, message.targetSessionId, message.targetTaskId, 'session.message.received', delivered);
    return true;
}

export async function consumeMessageTx(tx: ISeqFileTransaction, root: string, task: TaskRecord, id: string): Promise<void> {
    const key = inboxKey(id), raw = await tx.getEntry(messagesPath(root), key);
    if (!raw) throw new Error('Message receipt is missing');
    const message = decode<CrossSessionMessage>(raw);
    if (message.targetTaskId !== task.id) throw new Error('Message consumer mismatch');
    if (message.consumedAt !== undefined) return;
    await tx.setEntry(messagesPath(root), key, encode({ ...message, consumedAt: Date.now() }));
    await appendEventTx(tx, root, task.sessionId, task.id, 'message.consumed', { messageId: id });
}

function messageIdentity(message: CrossSessionMessage): string {
    return encode([message.id, message.sourceSessionId, message.sourceTaskId, message.targetSessionId,
        message.targetTaskId, message.topic, message.correlationId, message.payload, message.requestFingerprint, message.expiresAt]);
}

async function rejectMessageTx(tx: ISeqFileTransaction, root: string, message: CrossSessionMessage,
    code: NonNullable<CrossSessionMessage['rejection']>['code'], reason: string): Promise<boolean> {
    const rejected: CrossSessionMessage = { ...message, deliverySequence: await tx.increment(messagesPath(root), 'delivery-sequence'),
        status: 'rejected', rejectedAt: Date.now(), rejection: { code, message: reason } };
    await tx.setEntry(messagesPath(root), inboxKey(message.id), encode(rejected));
    await appendEventTx(tx, root, message.targetSessionId, message.targetTaskId, 'session.message.rejected', rejected);
    return false;
}

/**
 * Retention/GC for settled messages (Storage §5 `messages.seq`).
 *
 * Only records that can no longer change are removed: an outbox entry that reached a
 * terminal status, and an inbox receipt the target already consumed (or that was
 * rejected). An undelivered outbox entry, an unconsumed delivery receipt and an inbox
 * entry whose sender has not recorded delivery yet are kept regardless of age — dropping
 * them would lose delivery responsibility or a receipt the sender still needs.
 * `before` bounds the replay window: a host must keep it older than any window in which
 * a repeated `idempotencyKey` still has to be rejected as a conflict.
 */
export async function pruneMessagesTx(
    tx: ISeqFileTransaction,
    root: string,
    before: number,
    limit = 1_000,
): Promise<{ outbox: number; inbox: number }> {
    if (!Number.isFinite(before) || before < 0) throw new Error('Invalid message retention watermark');
    if (!Number.isSafeInteger(limit) || limit < 0) throw new Error('Invalid message retention limit');
    const path = messagesPath(root);
    const selected = (await retentionCandidates(tx, path, before)).slice(0, Math.max(0, limit));
    for (const key of selected) await tx.deleteEntry(path, key);
    const inboxCount = selected.filter(key => key.startsWith('inbox/')).length;
    const counts = { outbox: selected.length - inboxCount, inbox: inboxCount };
    if (selected.length > 0) {
        await tx.setEntry(path, 'retention', String(before));
        const session = await requireSessionTx(tx, root);
        await appendEventTx(tx, root, session.id, undefined, 'session.messages.pruned', { before, ...counts });
    }
    return counts;
}

/** Settled records older than the watermark; undelivered/unconsumed records are never candidates. */
async function retentionCandidates(tx: ISeqFileTransaction, path: string, before: number): Promise<string[]> {
    const pending = new Set<string>();
    const outboxKeys: string[] = [], inboxKeys: string[] = [];
    await tx.walkEntries(path, row => {
        if (row.key.startsWith('outbox/')) {
            const message = decode<CrossSessionMessage>(row.value);
            if (message.status === 'pending') pending.add(inboxKey(message.id));
            else if ((message.status === 'delivered' || message.status === 'rejected') && canReclaimMessage(message) && settledAt(message) < before) outboxKeys.push(row.key);
        } else if (row.key.startsWith('inbox/')) {
            const message = decode<CrossSessionMessage>(row.value);
            if (canReclaimMessage(message) && (message.consumedAt !== undefined || message.status === 'rejected') && settledAt(message) < before) {
                inboxKeys.push(row.key);
            }
        }
        return true;
    });
    return [...outboxKeys, ...inboxKeys.filter(key => !pending.has(key))];
}

/** Terminal message timestamp; the latest recorded transition wins. */
function settledAt(message: CrossSessionMessage): number {
    return Math.max(message.deliveredAt ?? 0, message.rejectedAt ?? 0, message.consumedAt ?? 0, message.settlementAcknowledgedAt ?? 0, message.createdAt);
}

/** Cross-store consumption alone cannot prove that the sender stopped retrying delivery. */
function canReclaimMessage(message: CrossSessionMessage): boolean {
    return message.sourceSessionId === message.targetSessionId
        || (Number.isSafeInteger(message.settlementAcknowledgedAt) && message.settlementAcknowledgedAt! >= 0);
}

/** An acknowledged terminal source never delivers again, even if this receipt was already reclaimed. */
export async function acknowledgeMessageSettlementTx(tx: ISeqFileTransaction, root: string,
    message: CrossSessionMessage, side: 'inbox' | 'outbox', recoveryPath?: string): Promise<CrossSessionMessage | undefined> {
    if (message.status === 'pending') throw new Error('Cannot acknowledge an unsettled message');
    const session = await requireSessionTx(tx, root);
    if (session.id !== (side === 'inbox' ? message.targetSessionId : message.sourceSessionId)) throw new Error('Message settlement session mismatch');
    const key = side === 'inbox' ? inboxKey(message.id) : outboxKey(message.id);
    const raw = await tx.getEntry(messagesPath(root), key);
    if (!raw) {
        if (side === 'inbox') return undefined;
        throw new Error('Outbox settlement record missing');
    }
    const current = decode<CrossSessionMessage>(raw);
    if (messageIdentity(current) !== messageIdentity(message) || current.status !== message.status) {
        throw new Error('Message settlement identity conflict');
    }
    if (current.settlementAcknowledgedAt !== undefined) return current;
    const next = { ...current, settlementAcknowledgedAt: Date.now(), nextAttemptAt: undefined };
    if (side === 'outbox') await indexRuntimeMessage(tx, recoveryPath, next);
    await tx.setEntry(messagesPath(root), key, encode(next));
    await appendEventTx(tx, root, session.id, undefined, 'session.message.settlement-acknowledged', { messageId: message.id, side });
    return next;
}
