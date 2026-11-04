import type { PersistedEffect, RecoveryIssue, TaskRecord } from '../domain/types';

interface Availability {
    program(kind: string, version: string): boolean;
    effect(kind: string, version: string): boolean;
}

/** Facts requiring attention; availability is supplied by the current host registry. */
export function taskRecoveryIssues(task: TaskRecord, available?: Availability): RecoveryIssue[] {
    const identity = { sessionId: task.sessionId, taskId: task.id };
    const issues: RecoveryIssue[] = [];
    if (!task.exit && programMissing(task, available)) {
        issues.push({ ...identity, reason: 'program-unavailable' });
    }
    for (const [effectId, effect] of Object.entries(task.effects)) {
        const reason = effectIssue(effect);
        if (reason) issues.push({ ...identity, effectId, reason });
        if ((reason || effect.status === 'pending') && available && !available.effect(effect.request.kind, effect.request.version)) {
            issues.push({ ...identity, effectId, reason: 'effect-unavailable' });
        }
    }
    return issues;
}

function programMissing(task: TaskRecord, available?: Availability): boolean {
    return task.blockedReason === 'program-unavailable' || !!(available && !available.program(task.program.kind, task.program.version));
}

function effectIssue(effect: PersistedEffect): RecoveryIssue['reason'] | undefined {
    if (effect.cleanupPending) return 'effect-cleanup';
    if (effect.status === 'indeterminate') return 'effect-indeterminate';
    if (effect.status === 'leased') return 'effect-in-flight';
    if (effect.status === 'pending' && effect.attempts.some(attempt => attempt.outcome === 'lost')) return 'effect-reconciliation';
    return undefined;
}
