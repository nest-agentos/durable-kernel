import type { KernelWakeSource } from '../domain/types';
import { KernelErrorCode, kernelError } from '../domain/errors';

/** Periodic reconciliation repairs lost or process-local commit notifications. */
export function scanInterval(options: {pollMs?: number; rescanMs?: number; wakeSource?: KernelWakeSource}): number {
    const rescan = options.rescanMs ?? 1000, poll = options.pollMs ?? 0;
    if (!Number.isSafeInteger(rescan) || rescan < 0 || !Number.isSafeInteger(poll) || poll < 0) {
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Kernel scan intervals must be non-negative safe integers');
    }
    if (rescan === 0 && !options.wakeSource) {
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Kernel rescanMs: 0 requires a reliable host wakeSource');
    }
    return rescan;
}
