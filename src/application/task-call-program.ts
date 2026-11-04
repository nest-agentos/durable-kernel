import type {Decision, DurableTaskProgram, TaskInputEvent} from '../domain/types';
import type {TaskCallPlan, TaskCallRequest, TaskCallResult} from '../domain/task-call';

interface State {plan?: TaskCallPlan; started: boolean}
/** The caller waits durably; its Effect retains the owned target until physical settlement. */
export const taskCallProgram = createProgram('1');
export const taskCallProgramV2 = createProgram('2');
function createProgram(version: '1' | '2'): DurableTaskProgram<State, TaskCallPlan, TaskCallResult> {
    return {manifest: {kind: 'kernel.call', version},
        init: plan => ({state: {plan: structuredClone(plan), started: false}, next: {type: 'wait', on: {type: 'signal', id: 'capabilities'}}}),
        reduce: (state, event) => reduce(state, event, version)};
}
function reduce(state: Readonly<State>, event: TaskInputEvent, version: '1' | '2'): Decision<State, TaskCallResult> {
    if (event.type === 'effect-completed' && event.effectId === 'call') {
        return complete(state, event.result as TaskCallResult);
    }
    if (event.type === 'effect-failed' && event.effectId === 'call') return {state, next: {type: 'fail', error: event.error}};
    if (!state.started && event.type === 'signal' && event.signal.type === 'capabilities') {
        const resourceHandleId = (event.signal.payload as {taskHandleId?: string})?.taskHandleId;
        return start(state, resourceHandleId, version);
    }
    return {state, next: {type: 'wait', on: state.started ? {type: 'effect', id: 'call'} : {type: 'signal', id: 'capabilities'}}};
}
function complete(state: Readonly<State>, result: TaskCallResult): Decision<State, TaskCallResult> {
    return {state, next: result.exit.status === 'succeeded' ? {type: 'complete', output: result}
        : {type: 'fail', error: result.exit.error ?? {message: 'Owned Task did not succeed'}}};
}
function start(state: Readonly<State>, resourceHandleId: string | undefined, version: '1' | '2'): Decision<State, TaskCallResult> {
    if (!resourceHandleId || !state.plan) return {state, next: {type: 'fail', error: {message: 'Task call capability and plan are required'}}};
    if (version === '1' && state.plan.timeoutMs === null || version === '2' && state.plan.timeoutMs === undefined)
        return {state, next: {type: 'fail', error: {code: 'INVALID_SPEC', message: 'kernel.call@2 requires an explicit timeout policy; null is unsupported by @1'}}};
    const request: TaskCallRequest = {...state.plan, resourceHandleId};
    return {state: {started: true}, actions: [{type: 'effect', effect: {id: 'call', kind: 'task.call', version: '1',
        request, idempotencyKey: 'call', ...(state.plan.timeoutMs !== undefined ? {timeoutMs: state.plan.timeoutMs} : {}),
        grants: [{handleId: resourceHandleId, right: 'execute'}]}}], next: {type: 'wait', on: {type: 'effect', id: 'call'}}};
}
