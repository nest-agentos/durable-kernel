import {taskTreeIds} from '../../domain/task-tree';
import {readSessionTasksTx} from './task-tree';
import type { ISeqFileTransaction } from '@itookit/vfs-core';
import type { TaskControl, TaskControlOptions, TaskRecord, TaskSignal } from '../../domain/types';
import { KernelErrorCode, kernelError } from '../../domain/errors';
import { assertSharedLeaseTx } from './shared-lease';
import { activeTaskTrees, interruptEffectsTx } from './effect-interrupt';
import { appendEventTx, decode, encode, finishAttemptTx, hasCancelledAncestorTx, indexTask,
    isTerminal, requireTaskTx, taskPath, unregisterTaskWaitTx, writeTaskTx } from './store-helpers';

export type TaskControlAction = TaskControl['mode'] | 'withdraw-pause';
type Options = TaskControlOptions & { signal?: TaskSignal };
type Receipt = { fingerprint: string; control: TaskControl };

/** Admission and tree holds share the same Session transaction as pause and child creation. */
export async function controlTaskTx(tx: ISeqFileTransaction, root: string, taskId: string,
    action: TaskControlAction, options: Options): Promise<TaskControl> {
    await assertSharedLeaseTx(tx, root, options.lease);
    const task = await requireTaskTx(tx, root, taskId), key = `control/${encodeURIComponent(options.requestId)}`;
    const { lease: _lease, ...identity } = options;
    const fingerprint = encode({ mode: action, ...identity });
    const previous = await tx.getEntry(taskPath(root, taskId), key);
    if (previous) return replayControl(task, decode<Receipt>(previous), fingerprint, options.requestId);
    await validateTarget(tx, root, task, action, options);
    const all = await readSessionTasksTx(tx, root, taskId), selected = taskTreeIds(all, taskId);
    if (action === 'withdraw-pause') validateWithdrawal(task, all.filter(value => selected.has(value.id)), options);
    const unsettled = activeTaskTrees(all), mode = action === 'withdraw-pause' ? 'run' : action;
    let result!: TaskControl;
    for (const current of all) {
        if (!selected.has(current.id) || isTerminal(current.status)) continue;
        const control = await writeControl(tx, root, current, taskId, mode, action, options, unsettled);
        if (current.id === taskId) result = control;
    }
    await tx.setEntry(taskPath(root, taskId), key, encode({ fingerprint, control: result }));
    return result;
}

function replayControl(task: TaskRecord, receipt: Receipt, fingerprint: string, requestId: string): TaskControl {
    if (receipt.fingerprint !== fingerprint) throw kernelError(KernelErrorCode.CONFLICT, 'Control request conflict');
    return task.control?.requestId === requestId ? task.control : receipt.control;
}

async function validateTarget(tx: ISeqFileTransaction, root: string, task: TaskRecord,
    action: TaskControlAction, options: Options): Promise<void> {
    if (isTerminal(task.status)) throw kernelError(KernelErrorCode.CONFLICT, 'Task is terminal');
    if (options.expectedEpoch !== undefined && options.expectedEpoch !== (task.control?.epoch ?? 0))
        throw kernelError(KernelErrorCode.CONFLICT, 'Control epoch conflict');
    if (await hasCancelledAncestorTx(tx, root, task)) throw kernelError(KernelErrorCode.CONFLICT, 'Task ancestor cancelled');
    if (action === 'run' && task.control && !task.control.acknowledged)
        throw kernelError(KernelErrorCode.CONTROL_NOT_SETTLED, 'Control not settled');
}

function validateWithdrawal(task: TaskRecord, tree: TaskRecord[], options: Options): void {
    if (!Number.isSafeInteger(options.expectedEpoch) || options.expectedEpoch! < 0 || options.signal !== undefined)
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Pause withdrawal requires a control epoch and no signal');
    if (task.control?.mode !== 'pause' || task.control.acknowledged)
        throw kernelError(KernelErrorCode.CONFLICT, 'Pause is not awaiting confirmation');
    if (tree.some(member => Object.values(member.effects).some(effect => effect.cleanupPending)))
        throw kernelError(KernelErrorCode.CONTROL_NOT_SETTLED, 'Pause withdrawal cannot bypass cleanup');
}

async function writeControl(tx: ISeqFileTransaction, root: string, task: TaskRecord, taskId: string,
    mode: TaskControl['mode'], action: TaskControlAction, options: Options, unsettled: Set<string>): Promise<TaskControl> {
    const holds = nextHolds(task, taskId, mode);
    const control = nextControl(task, taskId, mode, options, unsettled, holds);
    const next = await nextControlledTask(tx, root, task, taskId, mode, control);
    if (mode === 'run' && task.id === taskId && options.signal) await addControlSignal(tx, root, next, options.signal);
    await writeTaskTx(tx, root, next); await indexTask(tx, root, next);
    await appendEventTx(tx, root, task.sessionId, task.id, `task.control.${action}`, control);
    return control;
}

function nextControl(task: TaskRecord, taskId: string, mode: TaskControl['mode'], options: Options, unsettled: Set<string>, holds: string[]): TaskControl {
    const control: TaskControl = { epoch: (task.control?.epoch ?? 0) + 1, mode: task.control?.mode ?? 'run',
        requestId: task.control?.requestId ?? options.requestId, reason: task.control?.reason, acknowledged: false };
    if (task.id === taskId) { control.mode = mode; control.requestId = options.requestId; control.reason = options.reason; }
    const held = task.sessionPaused || holds.length || control.mode !== 'run';
    control.acknowledged = !held || !unsettled.has(task.id);
    return control;
}

async function nextControlledTask(tx: ISeqFileTransaction, root: string, task: TaskRecord, taskId: string,
    mode: TaskControl['mode'], control: TaskControl): Promise<TaskRecord> {
    const next: TaskRecord = { ...task, control, controlHolds: nextHolds(task, taskId, mode),
        effects: mode === 'interrupt' ? await interruptEffectsTx(tx, root, task) : task.effects,
        stepAttemptCount: task.status === 'running' ? Math.max(0, (task.stepAttemptCount ?? 1) - 1) : task.stepAttemptCount,
        status: task.status === 'running' ? 'ready' : task.status, currentAttempt: undefined,
        version: task.version + 1, updatedAt: Date.now() };
    if (task.currentAttempt) await finishAttemptTx(tx, root, task, 'ready');
    return next;
}

function nextHolds(task: TaskRecord, taskId: string, mode: TaskControl['mode']): string[] {
    const holds = new Set(task.controlHolds ?? []);
    if (task.id !== taskId) { if (mode === 'run') holds.delete(taskId); else holds.add(taskId); }
    return [...holds];
}

async function addControlSignal(tx: ISeqFileTransaction, root: string, task: TaskRecord, signal: TaskSignal): Promise<void> {
    task.pendingEvents = [{ type: 'signal', sequence: task.version, signal }, ...task.pendingEvents];
    if (task.status !== 'waiting') return;
    await unregisterTaskWaitTx(tx, root, task); task.status = 'ready'; task.wait = undefined;
}


/** Read the original admission without replaying a control or exposing its signal payload. */
export async function readControlReceiptTx(tx: ISeqFileTransaction, root: string, taskId: string,
    requestId: string): Promise<import('../../domain/types').TaskControlReceipt | undefined> {
    const task = await requireTaskTx(tx, root, taskId);
    const raw = await tx.getEntry(taskPath(root, taskId), `control/${encodeURIComponent(requestId)}`);
    if (!raw) return;
    const receipt = decode<Receipt>(raw);
    const identity = decode<{mode: TaskControlAction; expectedEpoch?: number}>(receipt.fingerprint);
    return {requestId, action: identity.mode, ...(identity.expectedEpoch === undefined ? {} : {expectedEpoch: identity.expectedEpoch}),
        accepted: receipt.control, ...(task.control ? {current: task.control} : {})};
}
