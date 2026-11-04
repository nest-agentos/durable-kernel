import type {IFileSystem} from '@nestos/vfs-core';
import {pathUtils} from '@nestos/vfs-core';
type Watch = {active: boolean; callback(paths: string[]): void};
interface Source {roots: Map<string, Set<Watch>>; stop(): void}
/** Route committed paths through their ancestors, independent of the number of idle Sessions. */
export class SeqCommitSubscriptions {
    private readonly sources = new Map<IFileSystem, Source>();
    watch(fs: IFileSystem, root: string, callback: (paths: string[]) => void): () => void {
        root = pathUtils.normalize(root);
        let source = this.sources.get(fs);
        if (!source) {
            source = {roots: new Map(), stop: () => {}}; const current = source;
            source.stop = fs.on('seq:committed', event => this.dispatch(current, event.payload.paths));
            this.sources.set(fs, source);
        }
        const watches = source.roots.get(root) ?? new Set<Watch>(), watch = {active: true, callback};
        watches.add(watch); source.roots.set(root, watches);
        return () => {
            if (!watch.active) return; watch.active = false; watches.delete(watch);
            if (!watches.size) source.roots.delete(root);
            if (!source.roots.size) {source.stop(); this.sources.delete(fs);}
        };
    }
    private dispatch(source: Source, paths: string[]): void {
        const deliveries = new Map<Watch, Set<string>>(), errors: unknown[] = [];
        for (const changed of new Set(paths.map(pathUtils.normalize))) {
            let ancestor = changed;
            while (true) {
                for (const watch of source.roots.get(ancestor) ?? []) {
                    const matched = deliveries.get(watch) ?? new Set<string>(); matched.add(changed); deliveries.set(watch, matched);
                }
                if (ancestor === '/') break;
                ancestor = pathUtils.dirname(ancestor);
            }
        }
        for (const [watch, matched] of deliveries) if (watch.active) {
            try {watch.callback([...matched]);} catch (error) {errors.push(error);}
        }
        if (errors.length) throw new AggregateError(errors, 'Commit subscribers failed');
    }
}
