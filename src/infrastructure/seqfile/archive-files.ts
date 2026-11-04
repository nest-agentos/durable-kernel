import type { ArchiveExportOptions } from '../../domain/archive';
import type { ResolvedStorageBinding } from '../../domain/types';
import { archiveDefaults, archiveFailure } from '../../application/archive-codec';
import { join } from './seqfile-core';

/** Inventory only: content binding and cross-filesystem consistency belong to the host. */
export async function archiveExternalFiles(binding: ResolvedStorageBinding, options: ArchiveExportOptions): Promise<string[]> {
    const queue = [binding.rootPath], files: string[] = []; let visited = 0;
    const coreFile = /^(?:session|shared|context|messages|resources|events|graph|index)\.seq$|^tasks\/[^/]+\/task\.seq$/;
    for (let i = 0; i < queue.length; i++) {
        options.signal?.throwIfAborted();
        const children = await binding.fs.driver.getChildren(queue[i], {includeHidden: true, includeAssetDirs: true, includeInternalDirs: true, fields: 'entry'});
        for (const child of children) {
            if (++visited > (options.maxRecords ?? archiveDefaults.maxRecords)) archiveFailure('Archive file inventory limit exceeded');
            const relative = child.path.slice(join(binding.rootPath).replace(/\/$/, '').length + 1);
            if (coreFile.test(relative)) continue;
            if (child.type === 'directory') queue.push(child.path); else files.push(relative);
        }
    }
    return files.sort();
}
