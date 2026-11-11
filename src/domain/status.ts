import type { SessionRecord, TaskRecord } from './types';

export function taskStat(task: TaskRecord) {
    const terminal = !!task.exit;
    const blockedBy = task.sessionPaused ? 'session-control' : task.controlHolds?.length ? 'ancestor-control'
        : task.control && task.control.mode !== 'run' ? 'task-control'
        : task.blockedReason ?? (task.status === 'blocked' ? 'dependency' : task.wait?.type);
    const phase = terminal ? 'done' : task.status === 'running' ? 'running' : blockedBy ? 'waiting'
        : task.status === 'created' ? 'created' : task.status === 'ready' ? 'ready' : 'waiting';
    const activeOperations = Object.values(task.effects).filter(e => e.status === 'pending' || e.status === 'leased' || e.status === 'indeterminate' || e.cleanupPending).length;
    const externalOperations = Object.values(task.effects).filter(e => e.status === 'leased' || e.status === 'indeterminate' || e.cleanupPending).length;
    const settled = terminal && activeOperations === 0;
    const held = (!!task.sessionPaused || !!task.controlHolds?.length || (!!task.control && task.control.mode !== 'run'))
        && task.control?.acknowledged !== false;
    return {
        id: task.id, revision: task.version, phase,
        control: { requested: task.status === 'cancelled' ? 'cancel' : task.control?.mode ?? 'run', acknowledged: task.status === 'cancelled' ? activeOperations === 0 : task.control?.acknowledged ?? true },
        blockedBy, exit: task.exit,
        activeOperations,
        settled, stopped: settled || (held && !task.currentAttempt && externalOperations === 0),
    } as const;
}
export function taskStats(task: TaskRecord) {
    return { observedRevision: task.version, completedSteps: task.stepNumber ?? 0,
        attempts: task.attemptCount, pendingInputs: task.pendingEvents.length,
        operations: Object.keys(task.effects).length, createdAt: task.createdAt, updatedAt: task.updatedAt };
}
export function sessionStat(session: SessionRecord, resourceBlocked = false) {
    return { id: session.id, phase: session.status === 'suspending' || session.status === 'suspended' ? 'paused'
        : session.status === 'archived' ? 'closed' : session.status,
        acknowledged: session.status !== 'suspending', archived: session.status === 'archived',
        blockedBy: resourceBlocked ? 'resource-claims' : undefined } as const;
}
/** Stat for a Session whose storage is gone: an interrupted removal, or a stale catalog entry. */
export function closedSessionStat(sessionId: string) {
    return { id: sessionId, phase: 'closed', acknowledged: true, archived: false, blockedBy: undefined } as const;
}
export type TaskStat = ReturnType<typeof taskStat>;
export type TaskStats = ReturnType<typeof taskStats>;
export type SessionStat = ReturnType<typeof sessionStat>;

/** A read-only execution projection; indeterminate effects never become terminal by inference. */
export function taskExecutionStat(task:TaskRecord,tasks:readonly TaskRecord[]) {
    const tree=tasks.filter(row=>row.sessionId===task.sessionId&&row.rootTaskId===task.rootTaskId);
    const uncertain=tree.flatMap(row=>Object.values(row.effects)).find(effect=>effect.status==='indeterminate');
    const code=uncertain?.error?.code;
    return {settled:tree.some(row=>row.id===task.id)&&tree.every(row=>taskStat(row).settled),
        indeterminate:!!uncertain,code:typeof code==='string'&&/^[A-Z][A-Z0-9_]{0,79}$/.test(code)?code:undefined};
}
export type TaskExecutionStat=ReturnType<typeof taskExecutionStat>;
