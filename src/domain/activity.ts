import type { SessionStatus, TaskRecord, TaskStatus } from './types';

export type TaskActivityPhase = 'running' | 'queued' | 'created' | 'waiting' | 'paused' | 'blocked' | 'succeeded' | 'failed' | 'cancelled';
export interface TaskActivity {
    schemaVersion: 1;
    taskVersion: number;
    status: TaskStatus;
    phase: TaskActivityPhase;
    approvals: number;
    inputs: number;
    cleanup: number;
    exitAt?: number;
}
export interface SessionActivity {
    /** Last persisted Task-scoped execution event; absent on older journals. */
    lastActivityAt?: number;
    sessionId: string;
    sessionStatus: SessionStatus;
    revision: number;
    asOfSequence: number;
    counts: Record<TaskActivityPhase, number>;
    pending: { approval: number; input: number; cleanup: number };
    latestExit?: { status: 'succeeded' | 'failed' | 'cancelled'; at: number };
}

export function taskActivity(task: TaskRecord): TaskActivity {
    const terminal = ['succeeded', 'failed', 'cancelled'].includes(task.status);
    const interactions = terminal ? [] : Object.values(task.interactions).filter(item => item.status === 'pending');
    return { schemaVersion: 1, taskVersion: task.version, status: task.status, phase: activityPhase(task),
        approvals: interactions.filter(item => item.kind === 'approval').length,
        inputs: interactions.filter(item => item.kind === 'input').length,
        cleanup: Object.values(task.effects).filter(effect => effect.cleanupPending).length,
        ...(task.exit ? { exitAt: task.exit.completedAt } : {}) };
}

function activityPhase(task: TaskRecord): TaskActivityPhase {
    if (['succeeded', 'failed', 'cancelled'].includes(task.status)) return task.status as TaskActivityPhase;
    const effects = Object.values(task.effects);
    if (task.blockedReason || effects.some(effect => effect.status === 'indeterminate')) return 'blocked';
    if (effects.some(effect => effect.status === 'leased') || task.status === 'running') return 'running';
    if (held(task)) return 'paused';
    if (effects.some(effect => effect.status === 'pending' && !effect.cleanupPending)) return 'running';
    if (task.status === 'ready') return 'queued';
    if (task.status === 'created') return 'created';
    return 'waiting';
}

function held(task: TaskRecord): boolean {
    return !!(task.sessionPaused || task.controlHolds?.length || task.control && task.control.mode !== 'run');
}
