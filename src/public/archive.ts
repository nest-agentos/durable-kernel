import type { ArchiveExportOptions, ArchiveImportOptions, ArchiveImportReport, ArchiveLimits, ParsedArchive, SessionArchiveApi } from '../domain/archive';
import type { KernelDiagnostic, ProgramRef, ResolvedStorageBinding, StorageBindingRef } from '../domain/types';
import { KernelErrorCode, kernelError } from '../domain/errors';
import { parseArchive, archiveLimits } from '../application/archive-codec';
import { archiveLines, assertQuiescent, readArchive, writeArchive } from '../infrastructure/seqfile/archive-store';
import { catalogPath, createId, decode, transaction } from '../infrastructure/seqfile/seqfile-core';
import { canonical } from '../application/canonical';
import { archiveExternalFiles } from '../infrastructure/seqfile/archive-files';

export interface ArchiveHost {
    catalog: ResolvedStorageBinding;
    inspect(id: string): Promise<ResolvedStorageBinding>;
    resolve(reference: StorageBindingRef): Promise<ResolvedStorageBinding>;
    hasProgram(ref: ProgramRef): boolean;
    hasEffect(ref: ProgramRef): boolean;
    assertRunning(): void;
    diagnose(event: KernelDiagnostic): void;
}

interface Stage {value: string}
async function checkedImport(host: ArchiveHost, archive: ParsedArchive, options: ArchiveImportOptions, stage: Stage) {
    if (!validImport(options)) {
        throw kernelError(KernelErrorCode.INVALID_SPEC, 'Archive import mode and stable requestId are required');
    }
    options.signal?.throwIfAborted();
    const programs = options.mode === 'fork' ? archive.tasks.filter(task => !task.parentTaskId).map(task => task.program) : archive.validation.programs;
    available(programs, ref => host.hasProgram(ref), 'Program');
    if (options.mode === 'restore') {
        if (archive.validation.manifest.purpose !== 'recover') throw kernelError(KernelErrorCode.ARCHIVE_UNSAFE, 'Inspection archives cannot restore execution');
        assertQuiescent(archive.validation.manifest.session, archive.tasks);
        if (!options.assertExclusive) throw kernelError(KernelErrorCode.ARCHIVE_UNSAFE, 'Restore requires source ownership and retirement validation');
        available(archive.validation.effects, ref => host.hasEffect(ref), 'Effect');
        stage.value = 'ownership'; await options.assertExclusive(archive.validation.manifest);
    }
    stage.value = 'external-bindings';
    if (archive.validation.externalBindings && !options.validateBindings) throw kernelError(KernelErrorCode.ARCHIVE_UNSAFE, 'External archive dependencies require host validation');
    await options.validateBindings?.(archive.validation);
    options.signal?.throwIfAborted(); host.assertRunning();
}

async function importArchive(host: ArchiveHost, archive: ParsedArchive, options: ArchiveImportOptions, stage: Stage): Promise<ArchiveImportReport> {
    stage.value = 'contract'; host.assertRunning(); await checkedImport(host, archive, options, stage);
    stage.value = 'receipt';
    const key = `archive-import/${encodeURIComponent(options.requestId)}`;
    const fingerprint = canonical([archive.validation.checksum, options.mode, options.storage, options.sessionId]);
    const saved = await transaction(host.catalog.fs, tx => tx.getEntry(catalogPath(host.catalog.rootPath), key));
    if (saved) {
        const previous = decode<{fingerprint: string; report: ArchiveImportReport}>(saved);
        if (previous.fingerprint !== fingerprint) throw kernelError(KernelErrorCode.CONFLICT, 'Archive import identity conflict');
        return {...previous.report, replayed: true};
    }
    const sourceId = archive.validation.manifest.session.id;
    const id = options.mode === 'restore' ? sourceId : options.sessionId ?? createId('session');
    if (options.mode === 'restore' && options.sessionId !== undefined && options.sessionId !== sourceId) {
        throw kernelError(KernelErrorCode.ARCHIVE_UNSAFE, 'Restore preserves the source Session identity');
    }
    stage.value = 'storage-binding'; const target = await host.resolve(options.storage);
    stage.value = 'transaction';
    return writeArchive(host.catalog, target, archive, options, id);
}

export function createArchiveApi(host: ArchiveHost): SessionArchiveApi {
    return {exportSession: (id, options) => exportSession(host, id, options),
        validateArchive: (source, options) => validateArchive(host, source, options),
        importSession: (source, options) => importSession(host, source, options)};
}
async function* exportSession(host: ArchiveHost, id: string, options: ArchiveExportOptions) {
    const started = Date.now(), operationId = createId('export'), stage = {value: 'contract'};
    try {
        archiveLimits(options);
        if (!['inspect','recover'].includes(options.purpose)) throw kernelError(KernelErrorCode.INVALID_SPEC, 'Invalid archive purpose');
        stage.value = 'binding'; const binding = await host.inspect(id);
        stage.value = 'file-inventory'; const externalPaths = await archiveExternalFiles(binding, options);
        stage.value = 'record-snapshot'; const snapshot = await readArchive(binding, options); snapshot.manifest.externalPaths = externalPaths;
        stage.value = 'encode'; yield* archiveLines(snapshot, options);
        host.diagnose({operation: 'archive.export', stage: 'complete', outcome: 'success', operationId, sessionId: id, durationMs: Date.now() - started});
    } catch (error) {failed(host, 'archive.export', operationId, started, error, stage.value); throw error;}
}
async function validateArchive(host: ArchiveHost, source: AsyncIterable<string>, options?: ArchiveLimits) {
    const started = Date.now(), operationId = createId('validate');
    try {
        const result = (await parseArchive(source, options)).validation;
        host.diagnose({operation: 'archive.validate', operationId, stage: 'complete', outcome: 'success', durationMs: Date.now() - started});
        return result;
    } catch (error) {failed(host, 'archive.validate', operationId, started, error, 'decode'); throw error;}
}
async function importSession(host: ArchiveHost, source: AsyncIterable<string>, options: ArchiveImportOptions) {
    const started = Date.now(), operationId = options?.requestId ?? createId('import'), stage = {value: 'decode'};
    try {
        const report = await importArchive(host, await parseArchive(source, options), options, stage);
        host.diagnose({operation: 'archive.import', stage: report.replayed ? 'replayed' : 'committed', outcome: 'success', operationId,
            sessionId: report.sessionId, durationMs: Date.now() - started}); return report;
    } catch (error) {failed(host, 'archive.import', operationId, started, error, stage.value); throw error;}
}
function failed(host: ArchiveHost, operation: string, operationId: string, started: number, error: unknown, stage: string) {
    host.diagnose({operation, operationId, stage, outcome: 'failure', durationMs: Date.now() - started,
        errorCode: (error as {code?: string})?.code ?? 'ARCHIVE_HOST_OR_STORAGE'});
}

function available(refs: ProgramRef[], has: (ref: ProgramRef) => boolean, kind: string) {
    for (const ref of refs) if (!has(ref)) throw kernelError(KernelErrorCode.ARCHIVE_UNSAFE, `Archive ${kind} version is unavailable`);
}
function validImport(options: ArchiveImportOptions): boolean {
    return !!options && ['restore','fork'].includes(options.mode) && typeof options.requestId === 'string' && !!options.requestId.trim();
}
