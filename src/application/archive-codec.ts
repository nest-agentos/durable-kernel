import {hasUnboundedEffect} from '../domain/effect-deadline';
import type { ArchiveLimits, ArchiveManifest, ArchiveRecord, ArchiveValidation, ParsedArchive } from '../domain/archive';
import type { ProgramRef, TaskRecord } from '../domain/types';
import { KernelErrorCode, kernelError } from '../domain/errors';
import { assertSessionLayout, UNBOUNDED_EFFECT_CAPABILITY } from '../infrastructure/seqfile/session-layout';
import { canonical } from './canonical';
import { ArchiveHash } from './archive-hash';

export const archiveDefaults = {maxBytes: 16 * 1024 * 1024, maxRecords: 100_000};
export function archiveFailure(message: string): never { throw kernelError(KernelErrorCode.ARCHIVE_INVALID, message); }
export function archivePath(path: unknown): path is string {
    return typeof path === 'string' && /^(?:session|shared|context|messages|resources|events|graph|index)\.seq$|^tasks\/[^/\\\u0000-\u001f]+\/task\.seq$/.test(path)
        && !path.split('/').some(part => part === '.' || part === '..');
}
export function archiveLimits(options: ArchiveLimits) {
    const limits = {...archiveDefaults, ...options};
    for (const value of [limits.maxBytes, limits.maxRecords]) if (!Number.isSafeInteger(value) || value < 1) archiveFailure('Invalid archive limits');
    return limits;
}
async function* lines(source: AsyncIterable<string>, options: ArchiveLimits) {
    const limits = archiveLimits(options); let buffer = '', bytes = 0;
    for await (const chunk of source) {
        options.signal?.throwIfAborted();
        if (typeof chunk !== 'string') archiveFailure('Archive chunks must be UTF-8 text');
        if (chunk.length > limits.maxBytes - bytes) archiveFailure('Archive byte limit exceeded');
        bytes += new TextEncoder().encode(chunk).length;
        if (bytes > limits.maxBytes) archiveFailure('Archive byte limit exceeded');
        buffer += chunk; let split: number;
        while ((split = buffer.indexOf('\n')) >= 0) {
            const line = buffer.slice(0, split).replace(/\r$/, ''); buffer = buffer.slice(split + 1);
            if (!line) archiveFailure('Empty archive record');
            yield {line, bytes};
        }
    }
    if (buffer) archiveFailure('Archive has an incomplete final record');
}
function manifestOf(value: any): ArchiveManifest {
    if (!value || !validManifest(value)) archiveFailure('Invalid archive manifest');
    for (const name of ['session','shared','context','messages','resources','events','graph','index']) {
        if (!value.paths.includes(`${name}.seq`)) archiveFailure('Archive omits a required record family');
    }
    try { assertSessionLayout(value.session); } catch { archiveFailure('Unsupported archive Session layout'); }
    return value;
}
function parseLine(line: string): any { try { return JSON.parse(line); } catch { return archiveFailure('Malformed archive JSON'); } }

export async function parseArchive(source: AsyncIterable<string>, options: ArchiveLimits = {}): Promise<ParsedArchive> {
    const records: ArchiveRecord[] = [], seen = new Set<string>(), hash = new ArchiveHash(), limits = archiveLimits(options);
    let manifest: ArchiveManifest | undefined, footer = false, bytes = 0, checksum = '';
    for await (const entry of lines(source, options)) {
        const item = parseLine(entry.line); bytes = entry.bytes;
        if (footer) archiveFailure('Records after archive footer');
        if (!manifest) { manifest = manifestOf(item); hash.update(canonical(item) + '\n'); continue; }
        if (item?.type === 'end') {
            checksum = hash.finish(); footer = true;
            checkFooter(item, records.length, checksum);
            continue;
        }
        checkRecord(item, manifest);
        const identity = canonical([item.path, item.key]);
        if (seen.has(identity)) archiveFailure('Duplicate archive record');
        seen.add(identity); records.push(item); hash.update(canonical(item) + '\n');
        if (records.length > limits.maxRecords) archiveFailure('Archive record limit exceeded');
    }
    if (!manifest || !footer) archiveFailure('Archive is incomplete');
    return inspectArchive(manifest, records, bytes, checksum);
}

function inspectArchive(manifest: ArchiveManifest, records: ArchiveRecord[], bytes: number, checksum: string): ParsedArchive {
    const session = records.find(row => row.path === 'session.seq' && row.key === 'record');
    if (!session || canonical(parseLine(session.value)) !== canonical(manifest.session)) archiveFailure('Session record differs from manifest');
    const tasks = records.filter(row => row.path.startsWith('tasks/') && row.key === 'record').map(row => readTask(row, manifest.session.id));
    checkRelations(tasks);
    if (tasks.some(hasUnboundedEffect) && (manifest.session.layout?.recordSchemas.effect !== 2
        || !manifest.session.layout.requiredCapabilities.includes(UNBOUNDED_EFFECT_CAPABILITY)))
        archiveFailure('Unbounded Effect deadline requires its declared storage contract');
    const taskPaths = new Set(tasks.map(task => `tasks/${task.id}/task.seq`));
    for (const path of manifest.paths) if (path.startsWith('tasks/') && !taskPaths.has(path)) archiveFailure('Task record is missing');
    const head = records.find(row => row.path === 'events.seq' && row.key === 'next-sequence');
    if (Number(head?.value ?? 0) !== manifest.asOfSequence) archiveFailure('Archive sequence differs from its consistency boundary');
    const programs = unique(tasks.map(task => task.program));
    const effects = unique(tasks.flatMap(task => Object.values(task.effects).map(effect => ({kind: effect.request.kind, version: effect.request.version}))));
    const externalBindings = tasks.some(task => !!task.ownedBy) || hasExternalBindings(manifest, records);
    const validation: ArchiveValidation = {manifest, records: records.length, bytes, checksum, tasks: tasks.length, programs, effects, externalBindings};
    return {validation, records, tasks};
}
function unique(values: ProgramRef[]): ProgramRef[] { return [...new Map(values.map(value => [canonical(value), value])).values()]; }

function validManifest(value: any): boolean {
    return [value.format === 'durable-kernel/session', value.schemaVersion === 1, ['inspect','recover'].includes(value.purpose),
        text(value.archiveId), text(value.session?.id), text(value.session?.storage?.kind),
        ['open','suspending','suspended','closing','closed','archived'].includes(value.session?.status),
        Number.isSafeInteger(value.asOfSequence) && value.asOfSequence >= 0,
        Array.isArray(value.paths) && value.paths.every(archivePath) && new Set(value.paths).size === value.paths.length,
        validExternalPaths(value.externalPaths)].every(Boolean);
}
function validExternalPaths(paths: unknown): boolean {
    if (paths === undefined) return true;
    return Array.isArray(paths) && paths.every(path => typeof path === 'string' && !!path && !path.startsWith('/')
        && !/[\\\u0000-\u001f]/.test(path) && !path.split('/').includes('..'));
}
function checkRecord(item: any, manifest: ArchiveManifest): void {
    if (!archivePath(item?.path) || !manifest.paths.includes(item.path) || typeof item.key !== 'string'
        || !item.key || typeof item.value !== 'string') archiveFailure('Invalid archive record');
}
function checkFooter(item: any, count: number, checksum: string) {
    if (item.records !== count || item.checksum !== checksum) archiveFailure('Archive checksum or count mismatch');
}
function readTask(row: ArchiveRecord, sessionId: string): TaskRecord {
    const task = parseLine(row.value);
    if (!task || !validTask(task)) archiveFailure('Invalid Task record');
    if (task.sessionId !== sessionId || row.path !== `tasks/${task.id}/task.seq`) archiveFailure('Invalid Task scope');
    return task;
}
function validTask(task: any): boolean {
    return [text(task.id), text(task.program?.kind), text(task.program?.version),
        Array.isArray(task.pendingEvents) && task.pendingEvents.every((event: any) => text(event?.type)),
        record(task.effects) && Object.values(task.effects).every(validEffect),
        Array.isArray(task.dependencies) && task.dependencies.every((dep: any) => text(dep?.task)),
        Number.isSafeInteger(task.version), Number.isSafeInteger(task.attemptCount),
        ['created','blocked','ready','running','waiting','succeeded','failed','cancelled'].includes(task.status)].every(Boolean);
}
function text(value: unknown): value is string {return typeof value === 'string' && !!value.trim();}
function record(value: unknown): value is Record<string, unknown> {return !!value && typeof value === 'object' && !Array.isArray(value);}
function validEffect(value: any): boolean {
    return !!value && [text(value.request?.kind), text(value.request?.version), text(value.request?.idempotencyKey),
        Array.isArray(value.attempts), ['pending','leased','succeeded','failed','indeterminate','cancelled'].includes(value.status)].every(Boolean);
}
function checkRelations(tasks: TaskRecord[]) {
    const ids = new Set(tasks.map(task => task.id));
    for (const task of tasks) if (!ids.has(task.rootTaskId) || task.parentTaskId && !ids.has(task.parentTaskId)
        || (task.dependencies ?? []).some(dep => !ids.has(dep.task))) archiveFailure('Archive omits a related Task');
}
function hasExternalBindings(manifest: ArchiveManifest, records: ArchiveRecord[]) {
    return !!manifest.externalPaths?.length || manifest.dependencies != null || records.some(row =>
        row.path === 'resources.seq' && !['managed/schema','managed/revision'].includes(row.key)
        || row.path === 'messages.seq' && (row.key.startsWith('inbox/') || row.key.startsWith('outbox/')));
}
