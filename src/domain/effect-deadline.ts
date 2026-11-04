import type {TaskRecord} from './types';

export function hasUnboundedEffect(task: Pick<TaskRecord, 'effects'>): boolean {
    return Object.values(task.effects).some(effect => effect.request.timeoutMs === null);
}
