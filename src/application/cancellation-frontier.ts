import type { TaskRecord } from '../domain/types';
import { taskStat } from '../domain/status';

/** Traverse settled ancestors without cancelling them or rewriting their results. */
export function cancellationFrontier(tasks: TaskRecord[], parentId?: string): TaskRecord[] {
    const children = new Map<string | undefined, TaskRecord[]>();
    for (const task of tasks) {
        const siblings = children.get(task.parentTaskId) ?? [];
        siblings.push(task); children.set(task.parentTaskId, siblings);
    }
    const pending = [parentId], visited = new Set<string | undefined>(), result: TaskRecord[] = [];
    while (pending.length) {
        const id = pending.pop()!;
        if (visited.has(id)) continue;
        visited.add(id);
        for (const child of children.get(id) ?? []) {
            if (taskStat(child).settled) pending.push(child.id);
            else result.push(child);
        }
    }
    return result;
}
