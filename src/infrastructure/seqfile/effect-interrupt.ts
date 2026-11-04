import type { ISeqFileTransaction } from '@itookit/vfs-core';
import type { TaskRecord } from '../../domain/types';
import { appendEventTx } from './seqfile-core';

export function activeTaskTrees(tasks: TaskRecord[]): Set<string> {
    const byId = new Map(tasks.map(task => [task.id, task])), active = new Set<string>();
    for (const task of tasks) {
        if (!Object.values(task.effects).some(effect => effect.status === 'leased' || effect.status === 'indeterminate' || effect.cleanupPending)) continue;
        let current: TaskRecord | undefined = task;
        while (current && !active.has(current.id)) {
            active.add(current.id); current = current.parentTaskId ? byId.get(current.parentTaskId) : undefined;
        }
    }
    return active;
}

/** Fence before cancelling; a resumed attempt must reconcile the interrupted operation. */
export async function interruptEffectsTx(tx: ISeqFileTransaction, root: string, task: TaskRecord): Promise<TaskRecord['effects']> {
    const effects = { ...task.effects };
    for (const [effectId, effect] of Object.entries(effects)) {
        if (effect.cleanupPending || (effect.status !== 'leased' && effect.status !== 'indeterminate')) continue;
        const previous = effect.currentAttempt ?? effect.attempts.at(-1);
        const attempts = previous ? effect.attempts.map(attempt => attempt.id === previous.id
            ? { ...attempt, outcome: 'lost' as const, finishedAt: Date.now() } : attempt) : effect.attempts;
        effects[effectId] = { ...effect, status: 'pending', currentAttempt: undefined, attempts,
            cleanupPending: true, replayAuthorized: false, result: undefined, error: undefined };
        await appendEventTx(tx, root, task.sessionId, task.id, 'effect.interrupt.requested', { effectId },
            { effectId, attemptId: previous?.id });
    }
    return effects;
}
