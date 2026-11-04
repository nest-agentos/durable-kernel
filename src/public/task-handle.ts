import type {InteractionResponseOptions} from '../domain/types';
import type { LeaseGuardOptions } from '../domain/types';
import type { Kernel } from '../application/kernel';
import type {
    EventEnvelope,
    ExitRecord,
    InteractionResponse,
    JsonValue,
    ResourceGrant,
    TaskResourceSpec,
    TaskAttempt,
    TaskHandle,
    TaskRecord,
    TaskSignal,
    TaskSnapshot,
} from '../domain/types';
import { eventStream, waitForChange } from './event-stream';
import { taskStat, taskStats } from '../domain/status';
import { assertWait, assertObserving, observeRead } from './wait';
import { KernelErrorCode, kernelError } from '../domain/errors';
import type { EventQuery, TaskWaitOptions, WatchOptions } from '../domain/types';

export class DefaultTaskHandle<O> implements TaskHandle<O> {
    get resources() { return this.kernel.resourceApi(this.sessionId, this.id); }
    get cache(): import('../domain/cache').CacheApi {
        return { create: spec => this.createCache(spec), list: () => this.listCaches(), read: request => this.readCache(request),
            publish: request => this.publishCache(request), invalidate: (id, generation) => this.invalidateCache(id, generation),
            renew: (id, generation, ttl) => this.renewCache(id, generation, ttl) };
    }
    send(request: import('../domain/types').TaskMessageRequest) { return this.sendMessage(request); }
    async stat() { return taskStat(await this.kernel.task(this.sessionId, this.id)); }
    async stats() { return taskStats(await this.kernel.task(this.sessionId, this.id)); }
    recoveryIssues() { return this.kernel.recoveryIssues(this.sessionId, this.id); }
    watch(options?: WatchOptions) { return this.events(options); }
    readEvents(query?: EventQuery) { return this.kernel.readEvents(this.sessionId, query, this.id); }
    snapshot() { return this.kernel.taskSnapshot(this.sessionId, this.id); }
    constructor(
        private readonly kernel: Kernel,
        private readonly sessionId: string,
        readonly id: string,
    ) {}

    resolveEffect(request: import('../domain/types').EffectResolution) { return this.kernel.resolveEffect(this.sessionId, this.id, request); }
    sendMessage(request: import('../domain/types').TaskMessageRequest) { return this.kernel.sendTaskMessage(this.sessionId, this.id, request); }
    createCache(spec: import('../domain/cache').CacheSpec) { return this.kernel.createCache(this.sessionId, this.id, spec); }
    renewCache(handleId: string, expectedGeneration: number, ttlMs: number) { return this.kernel.renewCache(this.sessionId, this.id, handleId, expectedGeneration, ttlMs); }
    listCaches() { return this.kernel.listCaches(this.sessionId, this.id); }
    readCache(request: import('../domain/cache').CacheRead) { return this.kernel.readCache(this.sessionId, this.id, request); }
    publishCache(request: import('../domain/cache').CachePublish) { return this.kernel.publishCache(this.sessionId, this.id, request); }
    invalidateCache(handleId: string, expectedGeneration: number) { return this.kernel.invalidateCache(this.sessionId, this.id, handleId, expectedGeneration); }

    retry(options: LeaseGuardOptions & { requestId: string }): Promise<TaskHandle<O>> {
        return this.kernel.retryTask<O>(this.sessionId, this.id, options);
    }

    async status(): Promise<TaskSnapshot> {
        return { task: await this.kernel.task(this.sessionId, this.id) };
    }

    async wait(options: TaskWaitOptions = {}): Promise<ExitRecord<O>> {
        assertWait(options);
        const startedAt = Date.now();
        while (true) {
            assertObserving(this.kernel, options.signal);
            const stat = await observeRead(this.stat(), this.kernel, options.signal);
            if (!stat) { assertObserving(this.kernel, options.signal); continue; }
            if (stat.exit && (options.until !== 'settled' || stat.settled)) return stat.exit as ExitRecord<O>;
            if (timedOut(startedAt, options.timeoutMs)) throw kernelError(KernelErrorCode.WAIT_TIMEOUT, `Task wait timed out: ${this.id}`);
            await waitForChange(this.kernel, this.sessionId, this.id, waitDelay(startedAt, options.timeoutMs), options.signal);
        }
    }

    async waitStopped(options: Omit<TaskWaitOptions, 'until'> = {}) {
        assertWait(options);
        const startedAt = Date.now();
        while (true) {
            assertObserving(this.kernel, options.signal);
            const stat = await observeRead(this.stat(), this.kernel, options.signal);
            if (!stat) { assertObserving(this.kernel, options.signal); continue; }
            if (stat.stopped) return stat;
            if (timedOut(startedAt, options.timeoutMs)) throw kernelError(KernelErrorCode.WAIT_TIMEOUT, `Task stop wait timed out: ${this.id}`);
            await waitForChange(this.kernel, this.sessionId, this.id, waitDelay(startedAt, options.timeoutMs), options.signal);
        }
    }

    async poll(): Promise<ExitRecord<O> | undefined> {
        return (await this.kernel.task(this.sessionId, this.id)).exit as ExitRecord<O> | undefined;
    }

    signal(signal: TaskSignal, options?: import('../domain/types').SignalOptions): Promise<import('../domain/types').SignalReceipt> {
        return this.kernel.signal(this.sessionId, this.id, signal, options);
    }

    pause(options: import('../domain/types').TaskControlOptions) { return this.kernel.controlTask(this.sessionId, this.id, 'pause', options); }
    interrupt(options: import('../domain/types').TaskControlOptions) { return this.kernel.controlTask(this.sessionId, this.id, 'interrupt', options); }
    resume(options: import('../domain/types').TaskControlOptions & { signal?: TaskSignal }) { return this.kernel.controlTask(this.sessionId, this.id, 'run', options); }
    withdrawPause(options: import('../domain/types').TaskPauseWithdrawalOptions) { return this.kernel.withdrawTaskPause(this.sessionId, this.id, options); }

    start(options?: import('../domain/types').TaskStartOptions): Promise<void> {
        return this.kernel.startTask(this.sessionId, this.id, options);
    }

    respond<T extends JsonValue>(response: InteractionResponse<T>, options?: InteractionResponseOptions): Promise<void> {
        return this.kernel.respondInteraction(this.sessionId, this.id, response, options);
    }

    createResource(spec: TaskResourceSpec, options?: LeaseGuardOptions): Promise<ResourceGrant> {
        return this.kernel.createResource(this.sessionId, { ...spec, ownerTaskId: this.id }, options);
    }

    cancel(reason?: string, options?: import('../domain/types').TaskCancellationOptions): Promise<void> {
        return this.kernel.cancel(this.sessionId, this.id, reason, options);
    }

    events(options?: WatchOptions): AsyncIterable<EventEnvelope> {
        return eventStream(this.kernel, this.sessionId, this.id, options?.after ?? 0, options);
    }

    history(options?: { afterVersion?: number }): Promise<TaskRecord[]> {
        return this.kernel.taskHistory(this.sessionId, this.id, options?.afterVersion ?? -1);
    }

    attempts(): Promise<TaskAttempt[]> {
        return this.kernel.taskAttempts(this.sessionId, this.id);
    }
}

function timedOut(startedAt: number, timeoutMs: number | undefined): boolean {
    return timeoutMs !== undefined && Date.now() - startedAt >= timeoutMs;
}
function waitDelay(startedAt: number, timeoutMs: number | undefined): number {
    return timeoutMs === undefined ? 100 : Math.max(0, Math.min(100, timeoutMs - (Date.now() - startedAt)));
}
