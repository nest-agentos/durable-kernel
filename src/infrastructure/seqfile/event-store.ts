import type { ISeqFileTransaction } from '@nestos/vfs-core';
import type { EventEnvelope, EventPage, EventQuery, ResolvedStorageBinding, SessionEventSnapshot, TaskEventSnapshot, TaskRecord } from '../../domain/types';
import { KernelError, KernelErrorCode, kernelError } from '../../domain/errors';
import { decode, eventsPath, ensureTaskEventIndexTx, taskEventCountKey, taskEventFirstKey, taskEventKey } from './seqfile-core';
import { requireSessionTx, requireTaskTx, transaction, indexPath } from './store-helpers';

export const prunedSequenceKey = (taskId?: string) => taskId ? `task-event-pruned/${encodeURIComponent(taskId)}` : 'pruned-through-sequence';
const eventKey = (sequence: number) => `event/${String(sequence).padStart(16, '0')}`;

async function scopedTask(tx: ISeqFileTransaction, root: string, id: string, sessionId: string): Promise<TaskRecord> {
    const task = await requireTaskTx(tx, root, id);
    if (task.id !== id || task.sessionId !== sessionId) throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Task snapshot scope mismatch');
    return task;
}

function integer(raw: string | null, fallback = 0): number {
    const value = raw === null ? fallback : Number(raw);
    if (!Number.isSafeInteger(value) || value < 0) throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Invalid journal index');
    return value;
}

export async function retentionBoundary(tx: ISeqFileTransaction, path: string, taskId?: string): Promise<number> {
    const stored = await tx.getEntry(path, prunedSequenceKey(taskId));
    if (stored !== null) return integer(stored);
    if (taskId) {
        const first = integer(await tx.getEntry(path, taskEventFirstKey(taskId)), 1);
        return first > 1 ? integer(await tx.getEntry(path, taskEventKey(taskId, first))) - 1 : 0;
    }
    let boundary = 0;
    await tx.walkEntries(path, async row => {
        if (integer(row.value) <= 1) return true;
        const id = decodeURIComponent(row.key.slice('task-event-first/'.length));
        boundary = Math.max(boundary, await retentionBoundary(tx, path, id));
        return true;
    }, { keyPrefix: 'task-event-first/' });
    return boundary;
}

function bounds(query: EventQuery, head: number) {
    const after = query.afterSequence ?? 0, through = query.throughSequence ?? head, limit = query.limit ?? 100;
    if (![after, through, limit].every(Number.isSafeInteger) || after < 0 || through < after || through > head || limit < 1 || limit > 500) {
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid event page bounds');
    }
    return { after, through, limit };
}

async function readEvents(tx: ISeqFileTransaction, path: string, sequences: number[], sessionId: string, taskId?: string): Promise<EventEnvelope[]> {
    const rows = await tx.getEntries(path, sequences.map(eventKey));
    return sequences.map(sequence => {
        const raw = rows[eventKey(sequence)];
        if (!raw) throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Journal event is missing');
        let event: EventEnvelope;
        try { event = decode<EventEnvelope>(raw); }
        catch { throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Invalid journal event encoding'); }
        if (!event || event.sequence !== sequence || event.sessionId !== sessionId || (taskId && event.taskId !== taskId)) {
            throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Journal event scope mismatch');
        }
        return event;
    });
}

async function upperIndex(tx: ISeqFileTransaction, path: string, taskId: string, sequence: number, first: number, count: number): Promise<number> {
    let lo = first, hi = count + 1;
    while (lo < hi) {
        const mid = lo + Math.floor((hi - lo) / 2);
        const raw = await tx.getEntry(path, taskEventKey(taskId, mid));
        if (raw === null) throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Task event index is missing');
        if (integer(raw) <= sequence) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

async function taskSequences(tx: ISeqFileTransaction, path: string, taskId: string, after: number, through: number, limit: number) {
    const count = integer(await tx.getEntry(path, taskEventCountKey(taskId)));
    const first = integer(await tx.getEntry(path, taskEventFirstKey(taskId)), 1);
    if (first < 1 || first > count + 1) throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Invalid Task retention index');
    const start = await upperIndex(tx, path, taskId, after, first, count);
    const stop = await upperIndex(tx, path, taskId, through, first, count) - 1;
    const end = Math.min(stop, start + limit - 1);
    const keys = Array.from({ length: Math.max(0, end - start + 1) }, (_, i) => taskEventKey(taskId, start + i));
    const refs = await tx.getEntries(path, keys);
    const sequences = keys.map(key => {
        if (refs[key] === undefined) throw kernelError(KernelErrorCode.STORAGE_CORRUPT, 'Task event index is missing');
        return integer(refs[key]);
    });
    return { sequences, more: end < stop };
}

/** One transaction pins membership, checks retention and reads only bounded indexed rows. */
export async function eventPage(binding: ResolvedStorageBinding, query: EventQuery = {}, taskId?: string): Promise<EventPage> {
    return transaction(binding.fs, async tx => {
        const session = await requireSessionTx(tx, binding.rootPath), path = eventsPath(binding.rootPath);
        if (taskId) { await scopedTask(tx, binding.rootPath, taskId, session.id); await ensureTaskEventIndexTx(tx, binding.rootPath); }
        const { after, through, limit } = bounds(query, integer(await tx.getEntry(path, 'next-sequence')));
        const pruned = await retentionBoundary(tx, path, taskId);
        if (after < pruned) throw new KernelError(KernelErrorCode.CURSOR_EXPIRED, 'Event cursor expired; reload a snapshot',
            { sessionId: session.id, taskId, afterSequence: after, prunedThroughSequence: pruned });
        const selection = taskId ? await taskSequences(tx, path, taskId, after, through, limit)
            : { sequences: Array.from({ length: Math.min(limit, through - after) }, (_, i) => after + i + 1), more: through - after > limit };
        const items = await readEvents(tx, path, selection.sequences, session.id, taskId);
        return { items, throughSequence: through, ...(selection.more ? { nextAfterSequence: items.at(-1)!.sequence } : {}) };
    });
}

export async function taskEventSnapshot(binding: ResolvedStorageBinding, taskId: string): Promise<TaskEventSnapshot> {
    return transaction(binding.fs, async tx => {
        const session = await requireSessionTx(tx, binding.rootPath);
        return { task: await scopedTask(tx, binding.rootPath, taskId, session.id),
            asOfSequence: integer(await tx.getEntry(eventsPath(binding.rootPath), 'next-sequence')) };
    });
}

export async function sessionEventSnapshot(binding: ResolvedStorageBinding): Promise<SessionEventSnapshot> {
    return transaction(binding.fs, async tx => {
        const session = await requireSessionTx(tx, binding.rootPath);
        const tasks: TaskRecord[] = [];
        await tx.walkEntries(indexPath(binding.rootPath), async row => {
            tasks.push(await scopedTask(tx, binding.rootPath, row.key.slice('task/'.length), session.id)); return true;
        }, { keyPrefix: 'task/' });
        return { session, tasks, asOfSequence: integer(await tx.getEntry(eventsPath(binding.rootPath), 'next-sequence')) };
    });
}
