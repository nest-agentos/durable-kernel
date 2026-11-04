import { KernelErrorCode, kernelError } from '../domain/errors';
import type { Kernel } from '../application/kernel';
import type { TaskWaitOptions } from '../domain/types';

export function assertWait(options: TaskWaitOptions = {}): void {
    if (options.timeoutMs !== undefined && (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs < 0)) {
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid wait timeout');
    }
    if (options.until !== undefined && options.until !== 'exit' && options.until !== 'settled') {
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid completion boundary');
    }
}

export function assertObserving(kernel: Kernel, signal?: AbortSignal): void {
    if (signal?.aborted) throw kernelError(KernelErrorCode.ABORTED, 'Observation aborted');
    if (kernel.isDisposed) throw kernelError(KernelErrorCode.DISPOSED, 'Kernel disposed while observing');
}

/** Cancel only the observation; an already-started storage read remains side-effect free. */
export async function observeRead<T>(operation: Promise<T>, kernel: Kernel, signal?: AbortSignal): Promise<T | undefined> {
    let finish = () => {};
    const cancelled = new Promise<undefined>(resolve => { finish = () => resolve(undefined); });
    const lifetime = kernel.abortSignal;
    signal?.addEventListener('abort', finish, { once: true });
    lifetime?.addEventListener('abort', finish, { once: true });
    if (signal?.aborted || lifetime?.aborted) finish();
    try { return await Promise.race([operation, cancelled]); }
    finally { signal?.removeEventListener('abort', finish); lifetime?.removeEventListener('abort', finish); }
}

/** Release every listener on timeout, caller cancellation, or Kernel disposal. */
export function waitForChange(kernel: Kernel, sessionId: string, taskId: string | undefined, timeoutMs: number, signal?: AbortSignal): Promise<void> {
    return new Promise(resolve => {
        let off = () => {};
        const timer = setTimeout(done, timeoutMs);
        const lifetime = kernel.abortSignal;
        function done(): void {
            clearTimeout(timer); off(); signal?.removeEventListener('abort', done); lifetime?.removeEventListener('abort', done); resolve();
        }
        off = kernel.onChanged(event => {
            if (event.sessionId === sessionId && (!taskId || !event.taskId || event.taskId === taskId)) done();
        });
        signal?.addEventListener('abort', done, { once: true });
        lifetime?.addEventListener('abort', done, { once: true });
        if (signal?.aborted || lifetime?.aborted) done();
    });
}
