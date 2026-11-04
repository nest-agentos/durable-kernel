import type {InteractionResponseReceipt, InteractionResponse, JsonValue, TaskRecord} from '../../domain/types';
import {KernelErrorCode, kernelError} from '../../domain/errors';

export function validateInteractionResponseId(id: unknown): asserts id is string {
    if (typeof id !== 'string' || !id.trim() || id.length > 256)
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Interaction response requestId must be a non-empty string of at most 256 characters');
}

/** The first response owns its identity; another request cannot adopt an existing answer. */
export function assertInteractionResponseIdentity(task: TaskRecord, interactionId: string, requestId?: string): void {
    if (requestId === undefined) return;
    validateInteractionResponseId(requestId);
    const previous = Object.values(task.interactions).find(item => item.responseRequestId === requestId);
    if (previous && previous.id !== interactionId)
        throw kernelError(KernelErrorCode.CONFLICT, 'Interaction response requestId was used for another interaction');
    const target = task.interactions[interactionId];
    if (target?.status === 'resolved' && target.responseRequestId !== requestId)
        throw kernelError(KernelErrorCode.CONFLICT, 'Interaction was resolved by another response request');
}

export function interactionResponseReceipt(task: TaskRecord, requestId: string): InteractionResponseReceipt | undefined {
    const interaction = Object.values(task.interactions).find(item => item.responseRequestId === requestId);
    if (!interaction || interaction.status !== 'resolved' || interaction.resolvedAt === undefined) return;
    return {requestId, taskId: task.id, interactionId: interaction.id, resolvedAt: interaction.resolvedAt};
}

export function withInteractionResponse(task: TaskRecord, response: InteractionResponse<JsonValue>, requestId: string | undefined, now: number): TaskRecord {
    const interaction = task.interactions[response.interactionId];
    const resolved = {...interaction, status: 'resolved' as const, response: response.value, resolvedAt: now,
        ...(requestId !== undefined ? {responseRequestId: requestId} : {})};
    const event = {type: 'interaction-resolved' as const, interactionId: response.interactionId, value: response.value};
    return {...task, interactions: {...task.interactions, [interaction.id]: resolved}, pendingEvents: [...task.pendingEvents, event],
        version: task.version + 1, updatedAt: now};
}
