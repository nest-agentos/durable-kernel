import type { IFileSystem, ISeqFileTransaction } from '@nestos/vfs-core';
import type {ResolvedStorageBinding} from '../../domain/types';
import {indexRuntimeResources} from './runtime-recovery-index';
import { transaction } from './seqfile-core';

/** Track Decision and standalone resource writes in their original atomic transaction. */
export async function trackResourceTransaction<T>(tx: ISeqFileTransaction, bindings: ResolvedStorageBinding[],
    operation: (tx: ISeqFileTransaction) => Promise<T>): Promise<T> {
    const changed = new Map<string, Set<string>>();
    const tracked = new Proxy(tx, {get(target, key) {
        const method = Reflect.get(target, key, target);
        if (typeof method !== 'function') return method;
        if (!['setEntry', 'deleteEntry', 'increment', 'append', 'compareAndSet'].includes(String(key))) return method.bind(target);
        return async (path: string, ...args: unknown[]) => {
            const result = await method.call(target, path, ...args);
            if (path.endsWith('/resources.seq') && (key !== 'compareAndSet' || result)) {
                const keys = changed.get(path) ?? new Set<string>(); changed.set(path, keys);
                if (typeof args[0] === 'string' && /^managed\/(handle|claim|cleanup|request|resource)\//.test(args[0])) keys.add(args[0]);
            }
            return result;
        };
    }});
    const result = await operation(tracked);
    for (const [path, keys] of changed) {
        await tx.increment(path, 'managed/revision');
        const binding = bindings.find(candidate => candidate.rootPath.replace(/\/$/, '') + '/resources.seq' === path);
        if (binding) await indexRuntimeResources(tx, binding, keys);
    }
    return result;
}
/** Resource-only writes share the same responsibility and revision tracking as Decisions. */
export function resourceTransaction<T>(scope: IFileSystem | ResolvedStorageBinding, operation: (tx: ISeqFileTransaction) => Promise<T>): Promise<T> {
    const binding = 'fs' in scope ? scope as ResolvedStorageBinding : undefined;
    return transaction(binding?.fs ?? scope as IFileSystem, tx => trackResourceTransaction(tx, binding ? [binding] : [], operation));
}
