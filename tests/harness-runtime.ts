import type {IFileSystem} from '@nestos/vfs-core';
import {createHarness, defineTask} from '../src/core';
import type {DurableTaskProgram, JsonValue, TaskCallPlan} from '../src/contracts';

const collector = defineTask<JsonValue[], number>({kind: 'team.collect', version: '1', state: [], step(state, event) {
    if (event.type === 'initialize') return {state: [event.input], next: {type: 'wait', on: {type: 'message', topic: 'result'}}};
    if (event.type !== 'message') throw new Error('Expected result');
    const values = [...state, event.message.payload];
    return {state: values, next: values.length > Number(state[0]) ? {type: 'complete', output: values.slice(1)} : {type: 'wait', on: {type: 'message', topic: 'result'}}};
}});
type Reply = {session: string; task: string};
const member: DurableTaskProgram<Reply, Reply> = {manifest: {kind: 'team.member', version: '1'},
    init: state => ({state, next: {type: 'wait', on: {type: 'message', topic: 'job'}}}),
    reduce(state, event) {
        if (event.type !== 'message') throw new Error('Expected job');
        return {state, actions: [{type: 'send-message', message: {targetSessionId: state.session, targetTaskId: state.task,
            topic: 'result', idempotencyKey: event.message.id, payload: event.message.payload}}], next: {type: 'complete', output: 'sent'}};
    }};
const menu = defineTask({kind: 'team.menu', version: '1', state: null, step(state, event) {
    if (event.type === 'initialize') return {state, actions: [{type: 'request-interaction', interaction: {id: 'menu', kind: 'input', prompt: 'Choose', payload: {options: ['yes', 'later']}}}], next: {type: 'wait', on: {type: 'interaction', id: 'menu'}}};
    if (event.type !== 'interaction-resolved') throw new Error('Expected menu response');
    return {state, next: {type: 'complete', output: event.value}};
}});

function ownedCallSpec(args: {target: string; unbounded?: boolean; requestId?: string}) {
    const plan: TaskCallPlan = {targetSessionId: args.target, task: {program: {kind: 'owned.menu', version: '1'}, input: null}, maxDepth: 4,
        ...(args.unbounded === true ? {timeoutMs: null} : {})};
    return {program: {kind: 'kernel.call', version: args.unbounded === true ? '2' : '1'}, input: plan, deferStart: true,
        ...(args.requestId ? {requestId: args.requestId} : {})};
}

/** Test host speaks control RPC only; durable messages travel through Kernel outbox/inbox. */
export async function createHarnessScenarioHost(fs: IFileSystem) {
    const harness = createHarness({catalog: {fs, rootPath: '/harness-catalog'}, maxConcurrentEffects: 1});
    harness.registerStorageResolver({kind: 'local', resolve: async ref => ({fs, rootPath: String(ref.locator)})});
    for (const program of [collector, member, menu]) harness.registerProgram(program);
    await harness.initialize();
    const session = (args: {session: string}) => harness.openSession(args.session);
    const task = (args: {task: string}) => harness.openTask(args.task);
    const operations: Record<string, (args: any) => Promise<unknown>> = {
        'owned-start': async args => {
            const spec = ownedCallSpec(args), caller = await (await session(args)).submit(spec);
            await harness.bindTaskCall({sessionId: args.session, taskId: caller.id}, spec.input); return caller.id;
        },
        'owned-receipt': args => harness.readTaskSubmissionReceipt(args.session, ownedCallSpec(args)),
        'owned-target': args => harness.readTaskCallTarget({sessionId: args.session, taskId: args.task, effectId: 'call'}),
        'owned-worker': async args => {
            const ownedMenu: typeof menu = {...menu, manifest: {kind: 'owned.menu', version: '1'}};
            harness.registerProgram(ownedMenu);
            await harness.recoverSession(args.session); return true;
        },
        'owned-targets': async args => (await (await harness.inspectSession(args.session)).snapshot()).tasks.filter(task => task.ownedBy),
        recover: args => harness.recoverSession(args.session, {takeover: args.takeover === true}),
        cancel: async args => (await task(args)).cancel('Owned call cancelled'),
        create: args => harness.createSession({id: args.session, storage: {kind: 'local', locator: `/teams/${args.session}`}}).then(() => true),
        collect: async args => (await (await session(args)).submit({program: collector.manifest, input: args.count})).id,
        member: async args => (await (await session(args)).submit({program: member.manifest, input: args.reply})).id,
        menu: async args => {
            const t = await (await session(args)).submit({program: menu.manifest, input: null});
            while (!(await t.snapshot()).task.interactions.menu) await new Promise(resolve => setTimeout(resolve, 5)); return t.id;
        },
        send: async args => (await task(args)).send(args.request),
        respond: async args => {await (await session(args)).attachTask(args.task); await (await task(args)).respond({interactionId: 'menu', value: args.value});},
        wait: async args => (await task(args)).wait({until: 'settled', timeoutMs: 5000}),
        snapshot: async args => (await task(args)).snapshot(),
        events: async args => (await task(args)).readEvents(),
        summary: args => harness.sessionActivity(args.session),
        suspend: async args => {const s = await session(args); await s.suspend(); while (!(await s.stat()).acknowledged) await new Promise(resolve => setTimeout(resolve, 5));},
        resume: async args => (await session(args)).resume(),
        shutdown: () => harness.shutdown(),
        drain: () => harness.drain(),
    };
    return {call: (action: string, args: unknown) => {
        const operation = operations[action]; if (!operation) throw new Error('Unknown Harness test operation'); return operation(args);
    }};
}
