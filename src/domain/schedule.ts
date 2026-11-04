import type { TaskSpec } from './types';

/** Fixed-rate UTC scheduling; each occurrence is a child Task with a stable spawn key. */
export interface ScheduleSpec {
    requestId: string;
    task: TaskSpec;
    at: number;
    everyMs?: number;
    missed?: 'skip' | 'catch-up';
    maxRuns?: number;
}
