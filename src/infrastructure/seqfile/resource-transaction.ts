import type { IFileSystem, ISeqFileTransaction } from '@itookit/vfs-core';
import { transaction } from './seqfile-core';

/** Resource-only writes have a cheap durable marker for cross-process reconciliation. */
export function resourceTransaction<T>(fs: IFileSystem, operation: (tx: ISeqFileTransaction) => Promise<T>): Promise<T> {
    return transaction(fs, async tx => {
        const changed = new Set<string>();
        const tracked = new Proxy(tx, {get(target, key) {
            const method = Reflect.get(target, key, target);
            if (typeof method !== 'function') return method;
            if (!['setEntry', 'deleteEntry', 'increment', 'append', 'compareAndSet'].includes(String(key))) return method.bind(target);
            return async (path: string, ...args: unknown[]) => {
                const result = await method.call(target, path, ...args);
                if (path.endsWith('/resources.seq') && (key !== 'compareAndSet' || result)) changed.add(path);
                return result;
            };
        }});
        const result = await operation(tracked);
        for (const path of changed) await tx.increment(path, 'managed/revision');
        return result;
    });
}
