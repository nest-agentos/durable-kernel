import type { RecoveryIssue, ShutdownReport } from '../domain/types';
import type { SessionStat } from '../domain/status';

export interface ShutdownHost {
    ids: string[];
    stat(id: string): Promise<SessionStat>;
    suspend(id: string): Promise<void>;
    issues(id: string): Promise<RecoveryIssue[]>;
    dispose(): void;
    waitIdle(timeoutMs: number): Promise<void>;
}

/** Persist pauses before releasing workers. A timeout never asserts an external stop. */
export async function shutdown(host: ShutdownHost, timeoutMs: number): Promise<ShutdownReport> {
    const deadline = Date.now() + timeoutMs;
    let pending = [...host.ids];
    try {
        for (const id of host.ids) if ((await host.stat(id)).phase === 'open') await host.suspend(id);
        do {
            pending = [];
            for (const id of host.ids) {
                const stat = await host.stat(id);
                if (!stat.acknowledged || stat.phase === 'closing') pending.push(id);
            }
            if (pending.length && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
        } while (pending.length && Date.now() < deadline);
    } finally { host.dispose(); }
    let localIdle = true;
    try { await host.waitIdle(Math.max(0, deadline - Date.now())); } catch { localIdle = false; }
    const issues = (await Promise.all(host.ids.map(id => host.issues(id)))).flat();
    const active = issues.some(issue => ['effect-in-flight', 'effect-indeterminate', 'effect-cleanup'].includes(issue.reason));
    return { status: localIdle && !pending.length && !active ? 'stopped' : 'blocked', localIdle, issues, pendingSessions: pending };
}
