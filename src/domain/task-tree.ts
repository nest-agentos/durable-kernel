import type {TaskRecord} from './types';

export function taskTreeIds(tasks: TaskRecord[], root: string): Set<string> {
    const ids = new Set([root]);
    for (let changed = true; changed;) {
        changed = false;
        for (const task of tasks) if (task.parentTaskId && ids.has(task.parentTaskId) && !ids.has(task.id)) {
            ids.add(task.id); changed = true;
        }
    }
    return ids;
}
