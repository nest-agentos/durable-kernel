import type { Kernel } from '../application/kernel';
import type { EventEnvelope, WatchOptions } from '../domain/types';
import { KernelErrorCode, kernelError } from '../domain/errors';
import { taskStat } from '../domain/status';
import { assertWait, waitForChange, observeRead } from './wait';

export { waitForChange } from './wait';

/** Sequence pages provide the same retention checks to streaming and batch consumers. */
export async function* eventStream(kernel: Kernel, sessionId: string, taskId: string | undefined,
    after: number, options: WatchOptions = {}): AsyncGenerator<EventEnvelope> {
    assertWait(options);
    let cursor = after, endSeen = false;
    while (true) {
        if (options.signal?.aborted) throw kernelError(KernelErrorCode.ABORTED, 'Event observation aborted');
        if (kernel.isDisposed) return;
        const page = await observeRead(kernel.readEvents(sessionId, { afterSequence: cursor, limit: 100 }, taskId), kernel, options.signal);
        if (!page) {
            if (options.signal?.aborted) throw kernelError(KernelErrorCode.ABORTED, 'Event observation aborted');
            return;
        }
        for (const event of page.items) {
            if (options.signal?.aborted) throw kernelError(KernelErrorCode.ABORTED, 'Event observation aborted');
            if (kernel.isDisposed) return;
            cursor = event.sequence; yield event;
        }
        cursor = page.nextAfterSequence ?? page.throughSequence;
        if (page.nextAfterSequence !== undefined) continue;
        if (endSeen) return;
        endSeen = await observeRead(streamEnded(kernel, sessionId, taskId, options.until ?? 'settled'), kernel, options.signal) ?? false;
        if (!endSeen) await waitForChange(kernel, sessionId, taskId, 250, options.signal);
    }
}

async function streamEnded(kernel: Kernel, sessionId: string, taskId: string | undefined, until: 'exit' | 'settled'): Promise<boolean> {
    if (taskId) {
        const stat = taskStat(await kernel.task(sessionId, taskId));
        return until === 'exit' ? !!stat.exit : stat.settled;
    }
    return (await kernel.sessionStat(sessionId)).phase === 'closed';
}
