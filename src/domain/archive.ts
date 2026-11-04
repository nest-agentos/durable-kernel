import type { JsonValue, ProgramRef, SessionRecord, StorageBindingRef, TaskRecord } from './types';

export interface ArchiveLimits { maxBytes?: number; maxRecords?: number; signal?: AbortSignal; }
export interface ArchiveExportOptions extends ArchiveLimits {
    purpose: 'inspect' | 'recover';
    /** Opaque external dependencies are declared and validated by the owning host. */
    dependencies?: JsonValue;
}
export interface ArchiveManifest {
    format: 'durable-kernel/session'; schemaVersion: 1; purpose: 'inspect' | 'recover';
    archiveId: string; session: SessionRecord; asOfSequence: number; paths: string[]; dependencies?: JsonValue;
    /** Non-record files are host-owned dependencies, never silently treated as restored content. */
    externalPaths?: string[];
}
export interface ArchiveValidation {
    manifest: ArchiveManifest; records: number; bytes: number; checksum: string;
    tasks: number; programs: ProgramRef[]; effects: ProgramRef[]; externalBindings: boolean;
}
export interface ArchiveImportOptions extends ArchiveLimits {
    mode: 'restore' | 'fork'; requestId: string; storage: StorageBindingRef; sessionId?: string;
    /** Must hold exclusive ownership and retire the source for the entire restore, including retries. */
    assertExclusive?: (manifest: ArchiveManifest) => Promise<void>;
    /** Required when resources, caches, cross-Session records or declared external data need binding. */
    validateBindings?: (validation: ArchiveValidation) => Promise<void>;
}
export interface ArchiveImportReport {
    sessionId: string; mode: 'restore' | 'fork'; replayed: boolean; paused: true;
    taskIds: Record<string, string>;
}
export interface SessionArchiveApi {
    exportSession(id: string, options: ArchiveExportOptions): AsyncIterable<string>;
    validateArchive(source: AsyncIterable<string>, options?: ArchiveLimits): Promise<ArchiveValidation>;
    importSession(source: AsyncIterable<string>, options: ArchiveImportOptions): Promise<ArchiveImportReport>;
}
export interface ArchiveRecord { path: string; key: string; value: string; }
export interface ParsedArchive { validation: ArchiveValidation; records: ArchiveRecord[]; tasks: TaskRecord[]; }
