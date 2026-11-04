import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync, copyFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import assert from 'node:assert/strict';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const require = createRequire(import.meta.url), consumer = mkdtempSync(join(tmpdir(), 'durable-kernel-consumer-'));
function run(command, args, cwd = consumer) {
    try { return execFileSync(command, args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }); }
    catch (error) { throw new Error(`${command} failed: ${error.stdout ?? ''}${error.stderr ?? ''}`); }
}
try {
    const manifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
    const tarball = process.argv[2] ? resolve(process.argv[2]) : join(consumer, `itookit-durable-kernel-${manifest.version}.tgz`);
    if (!process.argv[2]) run('pnpm', ['pack', '--pack-destination', consumer], root);
    writeFileSync(join(consumer, 'package.json'), JSON.stringify({ private: true, type: 'module' }));
    const deps = [tarball, ...(process.env.DURABLE_KERNEL_VFS_CORE_TARBALL ? [resolve(process.env.DURABLE_KERNEL_VFS_CORE_TARBALL)] : [])];
    run('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', ...deps]);
    const installedRoot = join(consumer, 'node_modules/@nestos/durable-kernel');
    const installed = JSON.parse(readFileSync(join(installedRoot, 'package.json'), 'utf8'));
    assert.deepEqual(Object.keys(installed.dependencies), ['@nestos/vfs-core']);
    assert.ok(!Object.values(installed.dependencies).some(value => /^(workspace:|link:|file:)/.test(value)));
    for (const path of ['README.md', 'docs/api.md', 'docs/events.md', 'docs/storage.md', 'docs/scheduling.md', 'docs/archives.md', 'docs/harness-scenarios.md', 'docs/changelog.md']) {
        assert.ok(readFileSync(join(installedRoot, path), 'utf8').trim().length, `Missing package documentation: ${path}`);
    }
    copyFileSync(join(root, 'scripts/package-smoke.mjs'), join(consumer, 'smoke.mjs'));
    process.stdout.write(run('node', ['smoke.mjs']));
    const source = `import { Kernel } from '@nestos/durable-kernel';
import { createHarness, snapshotTaskCallPlan, type Harness, type Task, type EventEnvelope, type ExitRecord, type WaitSpec, type TaskCallPlan, type TaskSpec, type TaskSubmissionReceipt } from '@nestos/durable-kernel/core';
import type { EffectExecutionContext, SessionHandle, TaskCancellationOptions, TaskCancellationReceipt } from '@nestos/durable-kernel/contracts';
export type Consumer = [Kernel, Harness, SessionHandle, typeof createHarness, EventEnvelope, ExitRecord, WaitSpec];
export function admission(harness: Harness, sessionId: string, spec: TaskSpec): Promise<TaskSubmissionReceipt | undefined> {
    return harness.readTaskSubmissionReceipt(sessionId, spec);
}
export function ownedTarget(harness: Harness, sessionId: string, taskId: string) {
    return harness.readTaskCallTarget({sessionId, taskId, effectId: 'call'});
}
export async function cancellation(kernel: Kernel, task: Task, sessionId: string): Promise<TaskCancellationReceipt | undefined> {
    const options: TaskCancellationOptions = {requestId: 'cancel', expectedEpoch: 0};
    await task.cancel('stop', options);
    return kernel.readTaskCancellationReceipt(sessionId, task.id, 'cancel');
}
export async function retry(task: Task) {
    const next = await task.retry({ requestId: 'again' });
    await next.snapshot();
    // @ts-expect-error Legacy aliases must not leak through recursive core returns.
    next.sendMessage;
    return next;
}
export async function guardedStart(context: EffectExecutionContext, child: Task): Promise<void> {
    await child.start({deadlineAt: context.deadlineAt});
}
export async function observe(context: EffectExecutionContext, child: Task): Promise<ExitRecord> {
    if (!context.waitForObservation) throw new Error('Observation capacity unavailable');
    return context.waitForObservation(signal => child.wait({signal, until: 'settled'}));
}
export async function ownedCall(harness: Harness, provided: TaskCallPlan): Promise<ExitRecord> {
    const plan = snapshotTaskCallPlan({...provided, timeoutMs: null});
    const session = await harness.openSession('caller');
    const task = await session.submit({program: {kind: 'kernel.call', version: '2'}, input: plan, deferStart: true});
    await harness.bindTaskCall({sessionId: session.id, taskId: task.id}, plan);
    return task.wait({until: 'settled'});
}
`;
    for (const ext of ['mts', 'cts']) writeFileSync(join(consumer, `consumer.${ext}`), source);
    run('node', [require.resolve('typescript/bin/tsc'), '--noEmit', '--strict', '--target', 'ES2022', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', 'consumer.mts', 'consumer.cts']);
    console.log('Standalone TypeScript NodeNext ESM/CommonJS declarations passed.');
} finally { rmSync(consumer, { recursive: true, force: true }); }
