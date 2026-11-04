import type { IFileSystem } from '@nestos/vfs-core';
import { DurablePoller } from './durable-poller';

export interface CommitScope { id: string; fs: IFileSystem; entries: Array<{fileIdOrPath: string; key: string}>; structureEntries?: number[]; }
type Changed = (id: string, reason: 'structure' | 'content') => void;
/** One batched marker read per backend; idle Sessions never enumerate Tasks or resource records. */
export class CommitRescanner {
    private readonly heads = new Map<string, {all: string; structure: string}>();
    private readonly poller: DurablePoller<string>;
    private disposed = false;
    constructor(private readonly intervalMs: number, scopes: () => CommitScope[], changed: Changed, onError: (error: unknown) => void) {
        this.poller = new DurablePoller({intervalMs, poll: async () => {
            const groups = new Map<IFileSystem, CommitScope[]>();
            const current = scopes(), active = new Set(current.map(scope => scope.id));
            for (const id of this.heads.keys()) if (!active.has(id)) this.heads.delete(id);
            for (const scope of current) { const group = groups.get(scope.fs) ?? []; group.push(scope); groups.set(scope.fs, group); }
            for (const [fs, group] of groups) {if (this.disposed) break; await this.read(fs, group, changed);}
            return {nextDelay: intervalMs || undefined};
        }, onError: (_key, error) => { onError(error); return true; }});
        this.wake();
    }
    /** Reconcile local structure before a later safety scan can repeat its notification. */
    wake(): void {if (this.intervalMs > 0) this.poller.start('commits');}
    get isIdle() { return this.poller.isIdle; }
    dispose() { this.disposed = true; this.poller.dispose(); this.heads.clear(); }
    private async read(fs: IFileSystem, scopes: CommitScope[], changed: Changed) {
        const requests = scopes.flatMap(scope => scope.entries);
        const values = await fs.meta.seq!.getEntriesMany(requests);
        if (this.disposed) return;
        let offset = 0;
        for (const scope of scopes) {
            const rows = values.slice(offset, offset + scope.entries.length); offset += scope.entries.length;
            const head = {all: JSON.stringify(rows), structure: JSON.stringify(scope.structureEntries?.map(index => rows[index]))};
            const previous = this.heads.get(scope.id); this.heads.set(scope.id, head);
            if (previous?.all !== head.all) changed(scope.id, previous?.structure !== head.structure ? 'structure' : 'content');
        }
    }
}
