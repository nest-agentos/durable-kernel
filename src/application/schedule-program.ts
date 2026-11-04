import type { Decision, DurableTaskProgram, TaskInputEvent, TaskSpec } from '../domain/types';
import type { ScheduleSpec } from '../domain/schedule';
import { KernelErrorCode, kernelError } from '../domain/errors';
import { assertDurableValue } from './durability';

export const scheduleProgramRef = {kind: 'kernel.schedule', version: '1'};
interface State { spec: ScheduleSpec; nextAt: number; runs: number; child?: string; }

export function scheduleTaskSpec(spec: ScheduleSpec): TaskSpec<ScheduleSpec> {
    if (!spec || !validSchedule(spec)) {
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid schedule identity, UTC deadline, interval or run limit');
    }
    assertDurableValue(spec, 'Schedule');
    if (!spec.task?.program || spec.task.parent || spec.task.spawnKey || spec.task.retryOfTaskId) {
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Schedule Task must define a program without an existing parent or retry origin');
    }
    return {program: scheduleProgramRef, input: spec, requestId: `schedule:${spec.requestId}`};
}

function waiting(state: State): Decision<State, unknown> {
    return {state, next: {type: 'wait', on: state.child ? {type: 'child', spawnKey: state.child}
        : {type: 'timer', id: 'schedule', at: state.nextAt}}};
}

function tick(state: State, event: Extract<TaskInputEvent, {type: 'timer-fired'}>): Decision<State, unknown> {
    const {spec} = state, interval = spec.everyMs;
    const at = occurrenceAt(state, event), spawnKey = `scheduled:${at}`, runs = state.runs + 1;
    const nextAt = at + (interval ?? 0);
    if (!Number.isSafeInteger(nextAt)) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Schedule deadline exceeds the safe integer range');
    const finished = !interval || spec.maxRuns !== undefined && runs >= spec.maxRuns;
    const next = finished ? {type: 'wait' as const, on: {type: 'child' as const, spawnKey}}
        : waiting({...state, nextAt, runs}).next;
    return {state: {...state, nextAt, runs, child: finished ? spawnKey : undefined},
        actions: [{type: 'spawn', spawnKey, spec: {...spec.task, requestId: undefined}}], next};
}

export const scheduleProgram: DurableTaskProgram<State, ScheduleSpec, unknown> = {
    manifest: scheduleProgramRef,
    init: spec => waiting({spec, nextAt: spec.at, runs: 0}),
    reduce: (state, event) => {
        if (event.type === 'timer-fired' && !state.child) return tick(state, event);
        if (event.type === 'task-exited' && state.child === event.spawnKey) {
            return {state, next: {type: 'complete', output: {runs: state.runs, lastExit: event.exit}}};
        }
        return waiting(state);
    },
};

function validSchedule(spec: ScheduleSpec): boolean {
    const positive = (value: number | undefined) => value === undefined || Number.isSafeInteger(value) && value > 0;
    return [typeof spec.requestId === 'string' && !!spec.requestId.trim(), Number.isSafeInteger(spec.at) && spec.at >= 0,
        positive(spec.everyMs), positive(spec.maxRuns), spec.missed === undefined || ['skip', 'catch-up'].includes(spec.missed)].every(Boolean);
}
function occurrenceAt(state: State, event: Extract<TaskInputEvent, {type: 'timer-fired'}>): number {
    const interval = state.spec.everyMs;
    if (!interval || state.spec.missed === 'catch-up') return state.nextAt;
    return state.nextAt + Math.max(0, Math.floor(((event.firedAt ?? event.at) - state.nextAt) / interval)) * interval;
}
