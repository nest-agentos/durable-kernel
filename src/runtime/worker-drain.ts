import type { WorkerDrainReport } from '../domain/types';
import { KernelErrorCode } from '../domain/errors';

/** A blocked drain retains heartbeats and storage; callers may wait again or force dispose. */
export async function drainWorker(host: {waitIdle(timeoutMs: number): Promise<void>; dispose(): void}, timeoutMs: number): Promise<WorkerDrainReport> {
    try { await host.waitIdle(timeoutMs); }
    catch (error) {
        if ((error as {code?: string})?.code !== KernelErrorCode.IDLE_TIMEOUT) throw error;
        return {status: 'blocked', localIdle: false};
    }
    host.dispose();
    return {status: 'drained', localIdle: true};
}
