# @itookit/durable-kernel

基于事务性 SeqFile 的持久执行内核。支持 Session 内多任务、任务依赖和父子关系、持久消息、共享状态、资源与预算，以及停止后的恢复。唯一运行依赖为 `@itookit/vfs-core`；不依赖 MindOS、LLM、UI 或具体工具。

文档：[接口参考](./docs/api.md) · [事件契约](./docs/events.md) · [版本记录](./docs/changelog.md)。文档随 npm 制品提供。另见[存储与通知](./docs/storage.md)、[持久定时运行](./docs/scheduling.md)和[文本归档](./docs/archives.md)。

安装：`npm install @itookit/durable-kernel`。

## 最小使用

```ts
import { createVFS, MemoryBackend } from '@itookit/vfs-core';
import { createHarness, defineTask } from '@itookit/durable-kernel/core';

const { manager } = await createVFS({ rootBackend: new MemoryBackend() });
const fs = await manager.openFileSystem('/example');
const harness = createHarness({ catalog: { fs, rootPath: '/catalog' } });
harness.registerStorageResolver({
    kind: 'example',
    async resolve(reference) {
        return { fs, rootPath: reference.locator as string };
    },
});
harness.registerProgram(defineTask<null, string, string>({
    kind: 'echo', version: '1', state: null,
    step(state, event) {
        if (event.type === 'initialize') return { state, next: { type: 'complete', output: event.input } };
        throw new Error('Unexpected input');
    },
}));
await harness.initialize();
const session = await harness.createSession({ id: 'example', storage: { kind: 'example', locator: '/sessions/example' } });
const task = await session.submit({ requestId: 'echo-once', program: { kind: 'echo', version: '1' }, input: 'hello' });
console.log(await task.wait());
const stop = await harness.shutdown({ timeoutMs: 5000 });
if (!stop.localIdle) throw new Error('Storage is still in use');
await manager.dispose();
```

MemoryBackend 仅用于示例，进程退出后不保留数据。需要跨重启恢复时，注入 IndexedDB 或 LocalFS/SQLite 等持久后端；必须提供 `meta.seq.transaction`、所需目录/元数据操作和提交通知。同一事务涉及的 SeqFile 必须位于同一事务后端。

## 入口和执行模型

- `/core`：`createHarness`、`defineTask`，以及 submit / send / inboxPage / stat / snapshot / readEvents / watch 等 canonical 接口、冷读取和关联类型。
- 根入口：以上接口，以及完整 `Kernel`、存储诊断与兼容 API。
- `/contracts`：仅类型导出，不加载执行实现。

Program 由 `{kind, version}` 精确固定，`init/reduce` 只计算 Decision。Task 状态、输入消费、本地 actions 和 wait 注册在同一 Session 事务中提交。外部副作用必须通过 EffectAdapter 执行；reducer 可以重放，不应直接执行网络、文件或进程操作。输入、状态和结果须可持久序列化。

`maxConcurrent` 与 `maxConcurrentEffects` 分别限制单个 Kernel 实例的 reducer 和 Effect 并发，并不构成多个进程共享的全局配额。

## 消息

```ts
const receipt = await session.send({
    idempotencyKey: 'operation-123',
    targetSessionId: 'receiver',
    targetTaskId: 'receiver-task',
    topic: 'work',
    correlationId: 'request-123',
    payload: { value: 1 },
});
```

Task 的 `send()` 必须指定目标 Task。Session 的 `send()` 可以指定目标 Task，也可以省略 `targetTaskId` 投递到宿主消费的 Session 邮箱。Task 使用 `Decision.actions` 的 `send-message` 与 `wait(message)` 完成事务性的消费和回复；Session 邮箱不会广播到所有 Task。

稳定的幂等键在源 Task/Session 内去重，同键异内容冲突。outbox 重试投递，目标 inbox 去重；`delivered` 表示持久接收，不表示业务处理完成。跨 Session 没有原子提交或全局顺序；外部业务副作用仍由适配器负责幂等与核对。

`inboxPage({afterSequence, throughSequence?, limit?})` 按接收方 `deliverySequence` 分页。首个页面的 `throughSequence` 可固定后续页面边界；读取完成后保存该边界作为下次增量游标。`inbox({after})` 的 `after` 同样表示接收序号，不能传发送时间戳。

宿主处理 Session 邮箱后调用 `acknowledgeMessage(id)`，重复确认返回原消费记录。该确认仅适用于未指定 Task 的消息；Task 消息只能随 Decision 原子消费。宿主自身的外部处理与确认不在同一事务，需自行保持幂等。

`sendToSession()` 保留兼容：传 `options.idempotencyKey` 才能安全重试；不传则每次创建新消息。retention 的水位必须早于部署允许的重放窗口；清除幂等记录后不再保证旧请求去重。分页 cursor 不替代消费确认。

本机多个进程可通过共享事务存储通信；`tests/ipc.test.ts` 使用真实 LocalFS/SQLite 和 SIGKILL 验证。默认每 1000ms 按存储批量核对提交水位，修复空闲进程漏通知；本地结构提交立即唤醒批量核对，避免延迟的结构通知打断正文流式展示；正文事件不额外触发结构核对，水位不变不扫描 Task。宿主可注入 wakeSource 降低延迟；关闭 rescanMs 必须提供可靠通知桥。跨主机和独立存储之间的传输、身份路由与所有权由宿主提供，本库不包含网络消息代理。

## 停止与恢复

- `Task.pause` 等待在途 Effect 自行排空；`interrupt` 持久登记取消并 fence 旧 attempt，适配器确认后才完成停止。`waitStopped()` 等待确认，`resume` 保留 Task 身份并先核对中止操作的结果。
- `Task.withdrawPause({requestId, expectedEpoch})` 只撤销尚未确认的自身暂停，不中止或重发原 Effect；存在清理责任时拒绝。Effect 内后续操作可用 `sessionState.setIfRunning` 将原 journal 的新准入写入与暂停屏障原子校验，结算仍使用普通 `set`。详见 [控制接口](./docs/api.md)。
- `Session.suspend` 持久关闭该 Session 的调度，默认等待在途 Effect 排空。返回表示请求已保存；通过 `stat().acknowledged` 判断停止确认。
- `cancel` 是不可恢复终态；`retry({requestId})` 创建新的 deferred Task，需要重新授权并 start。
- `drain({timeoutMs})` 只退休本实例，排空已接纳执行，不暂停共享 Session；超时 blocked 保留心跳和存储责任，可再次等待。
- `shutdown({timeoutMs})` 对本实例已登记的 open Session 持久请求暂停，等待排空，再释放本地 worker。它不关闭 Session，也不取消其 Task。重复调用复用同一次停机；`stopped` 表示已确认，`blocked` 附带待确认 Session/Effect。它不会替其他进程停止未登记的 Session。
- `dispose()` 仅释放本地 worker，不承诺外部操作已停止；适合模拟崩溃或明确放弃本地执行。`waitIdle()` 超时抛 `IDLE_TIMEOUT`，不会静默报告成功。

重建 Harness，注册相同版本的 Program、Effect 和 StorageResolver 后调用 `recover()` 或 `recoverSessions(ids)`。暂停意图保持不变；对于 shutdown 暂停的 Session，显式 `resume()` 才继续。`takeover: true` 要求调用方已持有写入所有权并停止旧执行者；否则等待旧 lease 自然到期。

恢复从最后提交的 checkpoint 继续，不恢复任意 JavaScript 调用栈。Effect 外部已执行但结果尚未落盘时，调用 `reconcile`、按明确的幂等策略重试，或进入 `indeterminate`。通过 `task.recoveryIssues()` / `harness.recoveryIssues(sessionId)` 检查，再用 `task.resolveEffect()` 裁决；禁止未经核对自动重复副作用。

超时、取消请求和物理停止是不同事实。超时后的 `cleanupPending` 会持久保留，直到 `EffectAdapter.cancel()` 确认外部停止；缺失 cancel 或取消失败会阻止停止确认。适配器的 cancel 必须幂等，并且只在该 attempt 已停止、不会再迟到启动时成功返回。execute / cancel context 提供 attemptId 和单调 attemptNumber，外部系统须据此实施 fencing。同步 CPU 死循环需要宿主 Worker/进程隔离才能强制中断。

`onDiagnostic(event)` 接收操作、阶段、结果、错误分类、耗时和关联身份，不携带请求正文或密钥。观察回调抛错不会改变业务提交结果。

## 观察与补读

Task 默认 `wait()` 等逻辑 ExitRecord；`wait({until: 'settled'})` 等清理完成。Task watch 默认等 settled 后排空结束，可用 `until: 'exit'` 保留逻辑完成边界；Session watch 在 closed 后排空结束。`watch({signal})` 取消观察，不取消执行；dispose 结束事件流，恢复后重新附加句柄。

Session / Task 的 `readEvents({afterSequence?, throughSequence?, limit?})` 提供有界补读；watch.after 同样使用 Session sequence。收到 CURSOR_EXPIRED 时读取 snapshot，重建视图，再从其原子绑定的 asOfSequence 继续。不要将事件 sequence 与消息 deliverySequence 混用。

Harness.inspectTask / inspectSession 均为冷读取，不启动 worker。/core 保留一套主名称；spawn、status、events、sendToSession 等兼容名称仍可从根句柄使用。v2 事件及 manifest 升级要求旧 writer 先退出，不支持混合旧版本写入。

## 开发和制品

本目录可单独复制、安装和构建；TypeScript 配置不引用父仓库。

```bash
pnpm install
pnpm typecheck
pnpm test:unit
pnpm test:integration
pnpm build
pnpm pack --pack-destination ./artifacts
```

源码工作区使用 `src` 入口。使用 `pnpm pack` 应用 `publishConfig`，制品入口指向 dist 的 ESM/CJS 和对应声明；不要直接把源码 manifest 交给 `npm publish`。运行依赖只有 vfs-core；LocalFS、SQLite 和测试工具仅用于开发与集成验收。

许可证沿用来源工程的 ISC。

## 幂等输入、文件通知和定时运行

```ts
const accepted = await task.signal({type: 'files.changed'}, {requestId: 'stable-operation-id'});
console.log(accepted.sequence, accepted.replayed);
const off = harness.onChanged(event => refresh(event.sessionId));
const schedule = await session.schedule({requestId: 'hourly', at: Date.now() + 60_000,
    everyMs: 3_600_000, missed: 'skip', task: {program: {kind: 'my-job', version: '1'}, input: null}});
```

signal 的 requestId 与输入同事务落盘，重试返回原回执，同键异内容冲突；终态拒绝新输入，已接纳回执仍可重放。省略 requestId 不去重。回执与 Task 同生命周期保留，取消保留窗口内的回执不得删掉。

VFS 提供 node:created / node:updated 等回调；外部文件直接写入须接宿主 watcher。watcher 可触发持久 signal，wakeSource 只唤醒执行器，不创造业务输入。schedule 返回普通 Task，支持 pause、resume、cancel、watch 和跨重启恢复；固定间隔与补跑策略见定时文档。

harness.archives 提供有容量上限、SHA-256 校验的 JSONL 导出校验及 restore / fork 导入。restore 保留身份并要求源端所有权与外部依赖核对；fork 按原根任务输入建立新的 deferred Task，由根 Program 重新创建后代。导入始终暂停，不自动执行外部副作用；详见归档文档。

## Harness 场景与状态总览

`await harness.sessionActivity(session.id)` 冷读取任务阶段、待确认/输入和等待清理数量，适合侧栏等总览；内容流不会推进活动修订。任务和 Session 的停止确认继续使用 stat / waitStopped。常用 Harness 组合、多进程请求/回复、菜单恢复与性能边界见 [Harness 场景验证](./docs/harness-scenarios.md)。
