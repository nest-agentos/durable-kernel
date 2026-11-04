import {afterEach, expect, it} from 'vitest';
import {createVFS, MemoryBackend} from '@nestos/vfs-core';
import {createHarness, defineTask, type Harness} from './core';
import type {Decision, DurableTaskProgram, EffectAdapter, JsonValue} from './contracts';

const releases: Array<() => Promise<void>> = [];
afterEach(async () => {for (const close of releases.splice(0).reverse()) await close();});
const mailbox = defineTask<JsonValue[], number, JsonValue[]>({kind: 'collect', version: '1', state: [],
    step(state, event) {
        if (event.type === 'initialize') return {state: [event.input], next: {type: 'wait', on: {type: 'message', topic: 'result'}}};
        if (event.type !== 'message') throw new Error('Expected durable message');
        const next = [...state, event.message.payload];
        return {state: next, next: next.length === Number(state[0]) + 1
            ? {type: 'complete', output: next.slice(1)} : {type: 'wait', on: {type: 'message', topic: 'result'}}};
    }});
type WorkerInput = {replySession: string; replyTask: string};
const worker: DurableTaskProgram<WorkerInput, WorkerInput> = {manifest: {kind: 'worker', version: '1'},
    init: state => ({state, next: {type: 'wait', on: {type: 'message', topic: 'job'}}}),
    reduce(state, event) {
        if (event.type !== 'message') throw new Error('Expected job');
        return {state, actions: [{type: 'send-message', message: {targetSessionId: state.replySession, targetTaskId: state.replyTask,
            topic: 'result', correlationId: event.message.id, idempotencyKey: `reply:${event.message.id}`, payload: event.message.payload}}],
            next: {type: 'complete', output: event.message.payload}};
    }};
const waiting = defineTask({kind: 'wait', version: '1', state: null,
    step: (state, event) => ({state, next: event.type === 'initialize' ? {type: 'wait', on: {type: 'signal'}} : {type: 'complete', output: event.type}})});
async function setup(programs: DurableTaskProgram<any, any, any>[] = [], effects: EffectAdapter<any, any>[] = []) {
    const {manager} = await createVFS({rootBackend: new MemoryBackend()}), fs = await manager.openFileSystem('/');
    releases.push(() => manager.dispose());
    const make = async () => {
        const harness = createHarness({catalog: {fs, rootPath: '/catalog'}});
        harness.registerStorageResolver({kind: 'local', resolve: async ref => ({fs, rootPath: String(ref.locator)})});
        for (const program of [mailbox, worker, waiting, ...programs]) harness.registerProgram(program);
        for (const effect of effects) harness.registerEffect(effect);
        await harness.initialize(); releases.push(async () => {harness.dispose(); await harness.waitIdle();}); return harness;
    };
    const harness = await make();
    const session = (h: Harness, id: string) => h.createSession({id, storage: {kind: 'local', locator: `/sessions/${id}`}});
    return {fs, harness, make, session};
}

it('supports parallel peer Tasks and atomically consumes jobs while committing replies in one Session', async () => {
    const {harness, session} = await setup(), s = await session(harness, 'team');
    const coordinator = await s.submit({program: mailbox.manifest, input: 2});
    const members = await Promise.all([1, 2].map(() => s.submit({program: worker.manifest, input: {replySession: s.id, replyTask: coordinator.id}})));
    await Promise.all(members.map((task, index) => coordinator.send({targetSessionId: s.id, targetTaskId: task.id,
        topic: 'job', idempotencyKey: `job-${index}`, payload: `analysis-${index}`})));
    expect((await coordinator.wait({until: 'settled', timeoutMs: 3000})).output).toEqual(expect.arrayContaining(['analysis-0', 'analysis-1']));
    await Promise.all(members.map(task => task.wait({until: 'settled', timeoutMs: 3000})));
    await expect.poll(async () => (await s.inboxPage()).items.filter(row => row.consumedAt === undefined).length).toBe(0);
    expect((await s.inboxPage()).items.every(row => row.consumedAt !== undefined)).toBe(true);
    expect(await harness.sessionActivity(s.id)).toMatchObject({counts: {succeeded: 3}, pending: {approval: 0, input: 0}});
});

it('supports request/reply between independently owned Sessions and deduplicates retries after completion', async () => {
    const {harness: a, make, session} = await setup(), b = await make(), source = await session(a, 'source'), target = await session(b, 'target');
    const client = await source.submit({program: mailbox.manifest, input: 1});
    const server = await target.submit({program: worker.manifest, input: {replySession: source.id, replyTask: client.id}});
    const request = {targetSessionId: target.id, targetTaskId: server.id, topic: 'job', idempotencyKey: 'request', payload: {result: 'review'}};
    const sent = await client.send(request); expect((await client.wait({timeoutMs: 3000})).output).toEqual([{result: 'review'}]);
    expect((await server.wait({timeoutMs: 3000})).status).toBe('succeeded');
    expect((await client.send(request)).id).toBe(sent.id);
    expect((await target.inboxPage()).items).toHaveLength(1); expect((await source.inboxPage()).items).toHaveLength(1);
    await expect(client.send({...request, payload: 'different'})).rejects.toMatchObject({code: 'MESSAGE_IDENTITY_CONFLICT'});
});

it('keeps Session pause and individual Task pause independent while other Sessions continue', async () => {
    const {harness, session} = await setup(), team = await session(harness, 'team'), other = await session(harness, 'other');
    const one = await team.submit({program: waiting.manifest, input: null}), two = await team.submit({program: waiting.manifest, input: null});
    await expect.poll(async () => (await one.snapshot()).task.status).toBe('waiting');
    await one.pause({requestId: 'own-pause'}); await team.suspend();
    await expect.poll(async () => (await team.stat()).acknowledged).toBe(true);
    await one.signal({type: 'go'}, {requestId: 'one'}); await two.signal({type: 'go'}, {requestId: 'two'});
    const independent = await other.submit({program: waiting.manifest, input: null}); await independent.signal({type: 'go'});
    expect((await independent.wait({timeoutMs: 3000})).status).toBe('succeeded');
    expect((await one.snapshot()).task.exit).toBeUndefined(); expect((await two.snapshot()).task.exit).toBeUndefined();
    await team.resume(); expect((await two.wait({timeoutMs: 3000})).status).toBe('succeeded');
    expect((await one.snapshot()).task.exit).toBeUndefined(); await one.resume({requestId: 'own-resume'});
    expect((await one.wait({timeoutMs: 3000})).status).toBe('succeeded');
    await expect(one.resume({requestId: 'terminal'})).rejects.toMatchObject({code: 'CONFLICT'});
});

type AgentState = {phase: 'model' | 'approval' | 'tool' | 'final'};
const effect = (id: string, kind: string, request: JsonValue) => ({type: 'effect' as const, effect: {id, kind, version: '1', idempotencyKey: id, request}});
const agent: DurableTaskProgram<AgentState, null, unknown> = {
    manifest: {kind: 'agent-loop', version: '1'},
    init: () => ({state: {phase: 'model'}, actions: [effect('model-1', 'model', 'first')], next: {type: 'wait', on: {type: 'effect', id: 'model-1'}}}),
    reduce(state, event): Decision<AgentState> {
        if (state.phase === 'model') return {state: {phase: 'approval'}, actions: [{type: 'request-interaction', interaction: {id: 'approve-tool', kind: 'approval', prompt: 'Allow tool?'}}], next: {type: 'wait', on: {type: 'interaction', id: 'approve-tool'}}};
        if (state.phase === 'approval') {
            if (event.type !== 'interaction-resolved' || event.value !== 'approved') throw new Error('Approval required');
            return {state: {phase: 'tool'}, actions: [effect('tool-1', 'tool', null)], next: {type: 'wait', on: {type: 'effect', id: 'tool-1'}}};
        }
        if (state.phase === 'tool') return {state: {phase: 'final'}, actions: [effect('model-2', 'model', 'final')], next: {type: 'wait', on: {type: 'effect', id: 'model-2'}}};
        if (event.type !== 'effect-completed') throw new Error('Model result required');
        return {state, next: {type: 'complete', output: event.result}};
    },
};

it('runs an Agent model/tool loop with approval, shutdown/recovery and event resynchronization through public APIs', async () => {
    let models = 0, tools = 0;
    const {harness, session, make} = await setup([agent], [
        {kind: 'model', version: '1', execute: async (request, context) => {models++; await context.emit?.({type: 'agent.delta', payload: {text: 'sample'}}); return request === 'first' ? {tool: 'fixture'} : 'finished';}},
        {kind: 'tool', version: '1', execute: async () => {tools++; return {ok: true};}},
    ]);
    const s = await session(harness, 'agent'), task = await s.submit({program: agent.manifest, input: null});
    await expect.poll(async () => (await harness.sessionActivity(s.id)).pending.approval).toBe(1);
    const before = await task.snapshot(); expect(models).toBe(1); expect(tools).toBe(0);
    expect((await harness.shutdown()).status).toBe('stopped');
    const restarted = await make(), reopened = await restarted.openSession(s.id), attached = await restarted.openTask(task.id);
    expect((await restarted.sessionActivity(s.id)).pending.approval).toBe(1);
    await attached.respond({interactionId: 'approve-tool', value: 'approved'});
    expect(tools).toBe(0); await reopened.resume();
    expect((await attached.wait({until: 'settled', timeoutMs: 3000})).output).toBe('finished'); expect(models).toBe(2); expect(tools).toBe(1);
    const events = []; for await (const event of attached.watch({after: before.asOfSequence})) events.push(event);
    expect(events.every(event => event.sequence > before.asOfSequence)).toBe(true);
    expect(new Set(events.map(event => event.sequence)).size).toBe(events.length);
    expect(events.filter(event => event.type === 'task.interaction.requested')).toHaveLength(0);
    expect(events.filter(event => event.type === 'task.interaction.resolved')).toHaveLength(1);
    expect(events.filter(event => event.type === 'agent.delta').every(event => event.effectId && event.attemptId)).toBe(true);
});

it('closes a Session by cancelling all owned Tasks and settles pending interactions', async () => {
    const {harness, session} = await setup([agent]), s = await session(harness, 'closing');
    const tasks = await Promise.all([0, 1, 2].map(() => s.submit({program: waiting.manifest, input: null})));
    await s.close({cancelRunning: true});
    await Promise.all(tasks.map(async task => expect((await task.wait({until: 'settled', timeoutMs: 3000})).status).toBe('cancelled')));
    expect(await harness.sessionActivity(s.id)).toMatchObject({sessionStatus: 'closed', counts: {cancelled: 3}, pending: {approval: 0, input: 0}});
});

it('lets a host bridge file creation and modification callbacks into durable inputs during Session pause', async () => {
    const watcher = defineTask<string[]>({kind: 'watch-files', version: '1', state: [], step(state, event) {
        if (event.type === 'initialize') return {state, next: {type: 'wait', on: {type: 'signal'}}};
        if (event.type !== 'signal') throw new Error('Expected file change signal');
        const changes = [...state, event.signal.type];
        return {state: changes, next: changes.length === 2 ? {type: 'complete', output: changes} : {type: 'wait', on: {type: 'signal'}}};
    }});
    const {fs, harness, session} = await setup([watcher]), s = await session(harness, 'files');
    const task = await s.submit({program: watcher.manifest, input: null});
    await expect.poll(async () => (await task.snapshot()).task.status).toBe('waiting');
    await s.suspend(); await expect.poll(async () => (await s.stat()).acknowledged).toBe(true);
    const accepted: Promise<unknown>[] = [];
    const created = fs.on('node:created', event => {if (event.payload.nodes.some(node => node.path === '/watched.txt')) accepted.push(task.signal({type: 'file.created'}, {requestId: 'create-operation'}));});
    const updated = fs.on('node:updated', event => {if (event.payload.nodes.some(node => node.path === '/watched.txt')) accepted.push(task.signal({type: 'file.modified'}, {requestId: 'write-operation'}));});
    try {
        await fs.driver.createFile({parentPath: '/', name: 'watched.txt', content: ''});
        await fs.driver.writeContent('/watched.txt', 'new'); await Promise.all(accepted);
        expect(accepted).toHaveLength(2); expect((await task.snapshot()).task.exit).toBeUndefined();
        await s.resume(); expect((await task.wait({timeoutMs: 2000})).output).toEqual(['file.created', 'file.modified']);
    } finally {created(); updated();}
});
