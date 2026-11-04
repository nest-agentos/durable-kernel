import type { ISeqFileTransaction } from '@nestos/vfs-core';
import {kernelError, KernelErrorCode} from '../../domain/errors';
import type { ResolvedStorageBinding, SessionRecord, TaskRecord, TaskStatus } from '../../domain/types';
import { taskActivity, type SessionActivity, type TaskActivity, type TaskActivityPhase } from '../../domain/activity';
import { decode, encode, eventsPath, indexPath, sessionPath, taskPath, transaction } from './seqfile-core';

export const ACTIVITY_REVISION = 'activity/revision';
export const activityKey = (id: string) => `activity/task/${id}`;

/** Derived metadata is committed atomically with the authoritative Task, never per token. */
export async function indexTaskActivity(tx: ISeqFileTransaction, root: string, task: TaskRecord, previous?: string | null): Promise<void> {
    const path = indexPath(root), key = activityKey(task.id), row = taskActivity(task), value = encode(row);
    const saved = previous === undefined ? await tx.getEntry(path, key) : previous;
    if (saved === value) return;
    await tx.setEntry(path, key, value);
    if (!sameActivity(saved, row)) await tx.increment(path, ACTIVITY_REVISION);
}

function sameActivity(raw: string | null, row: TaskActivity): boolean {
    try {return !!raw && encode({...decode<TaskActivity>(raw), taskVersion: 0}) === encode({...row, taskVersion: 0});}
    catch {return false;}
}

/** Cold, atomic inspection; legacy records are derived without performing a migration. */
export function readSessionActivity(binding: ResolvedStorageBinding): Promise<SessionActivity> {
    return transaction(binding.fs, async tx => {
        const raw = await tx.getEntry(sessionPath(binding.rootPath), 'record');
        if (!raw) throw corrupt('Session activity storage is missing');
        const session = activityRecord<SessionRecord>(raw), rows = await activityRows(tx, binding.rootPath, session.id);
        const revision = Number(await tx.getEntry(indexPath(binding.rootPath), ACTIVITY_REVISION) ?? 0);
        const asOfSequence = Number(await tx.getEntry(eventsPath(binding.rootPath), 'next-sequence') ?? 0);
        const rawTime=await tx.getEntry(eventsPath(binding.rootPath),'last-execution-activity-at'),lastActivityAt=rawTime===null?undefined:Number(rawTime);
        if (!validCount(revision) || !validCount(asOfSequence)) throw corrupt('Invalid Session activity cursor');
        if(lastActivityAt!==undefined&&!validCount(lastActivityAt)) throw corrupt('Invalid Session activity time');
        return {...summarize(session, rows, revision, asOfSequence),...(lastActivityAt!==undefined?{lastActivityAt}:{})};
    });
}

async function activityRows(tx: ISeqFileTransaction, root: string, sessionId: string): Promise<TaskActivity[]> {
    const rows: Array<{id: string; status: TaskStatus; updatedAt: number}> = [];
    await tx.walkEntries(indexPath(root), row => {
        rows.push({ id: row.key.slice(5), ...activityRecord<{status: TaskStatus; updatedAt: number}>(row.value) }); return true;
    }, {keyPrefix: 'task/'});
    const saved = await tx.getEntries(indexPath(root), rows.map(row => activityKey(row.id)));
    const missing = rows.filter(row => !saved[activityKey(row.id)]);
    const bodies = await tx.getEntriesMany(missing.map(row => ({fileIdOrPath: taskPath(root, row.id), key: 'record'})));
    const legacy = new Map(missing.map((row, index) => [row.id, legacyActivity(bodies[index], sessionId, row.id)]));
    return rows.map(row => saved[activityKey(row.id)] ? checkedActivity(saved[activityKey(row.id)]!, row.status) : legacy.get(row.id)!);
}

function legacyActivity(raw: string | null | undefined, sessionId: string, taskId: string): TaskActivity {
    if (!raw) throw corrupt('Session activity Task is missing');
    const task = activityRecord<TaskRecord>(raw);
    if (task.id !== taskId || task.sessionId !== sessionId) throw corrupt('Session activity scope mismatch');
    try {return taskActivity(task);} catch {throw corrupt('Invalid legacy Task activity');}
}

function checkedActivity(raw: string, status: TaskStatus): TaskActivity {
    const row = activityRecord<TaskActivity>(raw);
    if (row.schemaVersion !== 1 || row.status !== status || !phases.includes(row.phase)
        || ![row.taskVersion, row.approvals, row.inputs, row.cleanup].every(validCount)) {
        throw corrupt('Invalid Task activity projection');
    }
    return row;
}
const phases: TaskActivityPhase[] = ['running', 'queued', 'created', 'waiting', 'paused', 'blocked', 'succeeded', 'failed', 'cancelled'];
const terminal = (status: string) => ['succeeded', 'failed', 'cancelled'].includes(status);
const validCount = (value: number) => Number.isSafeInteger(value) && value >= 0;
const corrupt = (message: string) => kernelError(KernelErrorCode.STORAGE_CORRUPT, message);
function activityRecord<T>(raw: string): T {
    try {
        const value = decode<T>(raw);
        if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid object');
        return value;
    } catch {throw corrupt('Invalid activity record encoding');}
}
function summarize(session: SessionRecord, rows: TaskActivity[], revision: number, asOfSequence: number): SessionActivity {
    const result: SessionActivity = {sessionId: session.id, sessionStatus: session.status, revision, asOfSequence,
        counts: Object.fromEntries(phases.map(phase => [phase, 0])) as SessionActivity['counts'], pending: {approval: 0, input: 0, cleanup: 0}};
    for (const row of rows) {
        const held = ['suspended', 'suspending'].includes(session.status) && !terminal(row.status) && row.phase !== 'running';
        result.counts[held ? 'paused' : row.phase]++;
        result.pending.approval += row.approvals; result.pending.input += row.inputs;
        result.pending.cleanup += row.cleanup;
        if (terminal(row.status) && row.exitAt !== undefined && row.exitAt >= (result.latestExit?.at ?? -1)) {
            result.latestExit = {status: row.status as NonNullable<SessionActivity['latestExit']>['status'], at: row.exitAt};
        }
    }
    return result;
}
