import {expect, it} from 'vitest';
import type {TaskRecord} from '../domain/types';
import {cancellationFrontier} from './cancellation-frontier';
function task(id: string, parentTaskId?: string, settled = false): TaskRecord {
    return {id, parentTaskId, status: settled ? 'succeeded' : 'created', effects: {},
        ...(settled ? {exit: {taskId: id, status: 'succeeded'}} : {})} as TaskRecord;
}
it('selects a disjoint Session frontier and reaches live tasks below settled ancestors', () => {
    const tasks = [task('active'), task('nested', 'active'), task('done', undefined, true), task('remaining', 'done'), task('independent')];
    expect(cancellationFrontier(tasks).map(task => task.id)).toEqual(['active', 'independent', 'remaining']);
    expect(cancellationFrontier(tasks, 'active').map(task => task.id)).toEqual(['nested']);
});
it('keeps terminal tasks with cleanup duties on the frontier without duplicating their descendants', () => {
    const root = task('cleaning', undefined, true); root.effects = {e: {status: 'cancelled', cleanupPending: true}} as TaskRecord['effects'];
    expect(cancellationFrontier([root, task('child', root.id)]).map(task => task.id)).toEqual(['cleaning']);
});
