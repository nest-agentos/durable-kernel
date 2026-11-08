# durable-kernel 接口参考

本文对应 0.1.0 当前实现，描述已提供的接口。[README](../README.md) 提供可运行的装配示例，[事件契约](./events.md) 描述顺序、补读和当前限制。发布包的 `dist/*.d.ts`、`dist/*.d.cts` 是完整类型签名；以下按使用顺序说明接口语义。

## 入口与职责

| 入口 | 用途 |
| --- | --- |
| `@nestos/durable-kernel/core` | `createHarness()`、`defineTask()`、`Harness` / `Session` / `Task`、资源和缓存等常用类型 |
| `@nestos/durable-kernel` | 上述入口及完整 `Kernel`、兼容句柄、注册表、状态投影、存储诊断接口 |
| `@nestos/durable-kernel/contracts` | 完整的领域、资源、缓存、交互类型；使用 `import type` |

`/core` 提供 canonical 操作及 `EventEnvelope`、`ExitRecord`、`WaitSpec` 等关联类型；完整领域类型还可从 `/contracts` 导入。托管资源句柄明确命名为 `ManagedHandle`；根入口的旧能力句柄 `ResourceHandle` 保留原语义。

```ts
import { createHarness, defineTask } from '@nestos/durable-kernel/core';
import type { EventEnvelope, ExitRecord, WaitSpec } from '@nestos/durable-kernel/core';
```

Session 是事务和调度控制边界，一个 Session 可以包含多个并发 Task。Task 运行固定版本的 Program；EffectAdapter 执行外部副作用。库不包含 LLM、聊天 turn、工具业务规则或网络 IPC transport。

## 装配与 Harness

```ts
interface KernelOptions {
    catalog: { fs: IFileSystem; rootPath?: string };
    workerId?: string;
    maxConcurrent?: number;
    maxConcurrentEffects?: number;
    leaseMs?: number;
    effectCleanupTimeoutMs?: number;
    pollMs?: number;
    rescanMs?: number;
    wakeSource?: KernelWakeSource;
    onDiagnostic?: (event: KernelDiagnostic) => void;
}
```

| 配置 | 默认 | 语义 |
| --- | --- | --- |
| `catalog.rootPath` | `/.config/kernel` | 持久 Session / Task 目录所在存储根 |
| `workerId` | 自动生成 | 当前执行者身份；租约还使用 token / epoch 防止旧执行者提交 |
| `maxConcurrent` | `4` | 本实例 reducer 并发；`0` 禁止领取 reducer |
| `maxConcurrentEffects` | `4` | 本实例 Effect 并发；`0` 禁止领取 Effect |
| `leaseMs` | `30000` | reducer / Effect 租约时长，单位毫秒 |
| `effectCleanupTimeoutMs` | `30000` | 单次等待取消确认的上限；超时保留清理责任 |
| `pollMs` | `0` | 兼容轮询；不关闭独立安全补扫 |
| `rescanMs` | `1000` | 批量核对提交水位；本地结构提交立即唤醒批量核对，正文事件沿用周期补扫；`0` 必须提供可靠 wakeSource |
| `wakeSource` | 未设置 | 宿主提交唤醒桥，返回订阅清理函数 |
| `onDiagnostic` | 未设置 | 观察操作、阶段、结果、错误分类、耗时及关联身份；不含业务正文 |

并发配置只约束当前 Kernel 实例，不是多进程全局配额。仅观察数据时不要以 `maxConcurrent: 0` 代替只读接口：Effect 和资源 worker 有自己的执行通道。

`createHarness(options): Harness` 创建对象。先注册存储解析器、Program、Effect 等，再 `await initialize()`。

| Harness 成员 | 返回 / 含义 |
| --- | --- |
| `initialize()` | `Promise<void>`；初始化目录、资源存储和宿主提交通知 |
| `registerStorageResolver(resolver)` | 注册 `{ kind, resolve(reference) }`，将持久 locator 解析为 `{ fs, rootPath }` |
| `registerProgram(program)` | 按 `{kind, version}` 注册状态机 |
| `registerEffect(adapter)` | 按 `{kind, version}` 注册外部操作适配器 |
| `registerResourceAdapter(adapter)` | 注册托管资源物理清理适配器 |
| `registerWorkspace(adapter)` | 注册工作区 snapshot / diff / merge 适配器 |
| `use(plugin)` | `Promise<void>`；执行插件注册及生命周期接线 |
| `createSession({ id?, storage })` | `Promise<Session>`；登记持久身份及存储绑定 |
| `openSession(id)` | `Promise<Session>`；打开并激活该 Session 的本地调度，不清除已持久暂停意图 |
| `openTask<O>(id)` | `Promise<Task<O>>`；定位 Task，必要时激活其 Session |
| `inspectTask(id)` | `Promise<TaskEventSnapshot>`；冷读取 `{task, asOfSequence}`，不登记 worker |
| `inspectSession(id)` | `Promise<SessionReader>`；只暴露 id、snapshot、readEvents、listTaskPage 的冷读取端口 |
| `sessionActivity(id)` | `Promise<SessionActivity>`；同事务冷读 Session 生命周期、各执行阶段 Task 数量、待确认/输入/清理数量、可选 lastActivityAt 及事件水位，不启动任务 |
| `listSessions()` | `AsyncIterable<SessionRecord>`；读取持久目录，不激活这些 Session |
| `listSessionTasks(sessionId)` / `listShared(sessionId, prefix?)` | 完整 Kernel 的冷查询；只读已有任务/共享记录，不打开 Session、不登记资源轮询或提交订阅；执行恢复仍须显式调用恢复/打开入口 |
| `listRecoveryTasks(scope)` / `listRecoveryShared(scope)` | 完整 Kernel 的冷查询；只读指定 scope 的精确待办引用，不遍历 Session、Task 正文或历史；空索引直接返回空数组 |
| `acknowledgeTaskRecovery(sessionId, taskId, scope)` | 对 until=acknowledged 的匹配 Task 显式确认；要求 Task 已结算，确认标志与待办删除在同一事务提交；重复确认幂等 |
| `recover(options?)` | `Promise<RecoveryReport>`；恢复全部已登记 Session，随后处理消息投递 |
| `recoverSession(id, options?)` | `Promise<RecoveryReport>`；恢复并激活指定 Session |
| `recoverSessions(ids, options?)` | `Promise<RecoveryReport>`；去重并先恢复所选集合，再登记 worker；不是跨 Session 原子事务 |
| `recoveryIssues(sessionId, taskId?)` | `Promise<RecoveryIssue[]>`；检查缺失 Program / Effect、不确定结果、清理及在途操作 |
| `resources` | Kernel 作用域的 `ResourceApi` |
| `drain({timeoutMs?}?)` | `Promise<WorkerDrainReport>`；只停止本实例领取并排空；drained / blocked；共享 Session 不暂停 |
| `archives` | `SessionArchiveApi`；有界 JSONL 导出、校验、原子导入；见[归档](./archives.md) |
| `onChanged(listener)` | 进程内变更回调；返回清理函数；见[通知契约](./storage.md) |
| `shutdown({ timeoutMs? }?)` | `Promise<ShutdownReport>`；持久请求本地已登记的 open Session 暂停，排空并释放 worker；默认 `5000ms` |
| `dispose()` | `void`；释放本地 worker，不表示外部物理停止 |
| `waitIdle(timeoutMs?)` | `Promise<void>`；默认 `5000ms`，本地登记的执行未排空则抛 `IDLE_TIMEOUT` |

`RecoveryOptions.takeover: true` 只适用于宿主已取得写入所有权并停止旧执行者的情况。普通恢复等待未过期租约，保留暂停意图；重启后必须注册原有精确版本。`RecoveryReport` 的计数不代表所有外部副作用已确定，须另查 `recoveryIssues()`。

TaskSpec 可声明 `recovery: {scope, key?, until: 'terminal' | 'acknowledged'}`。任务接纳与 catalog 待办写入共用一个事务；terminal 模式在终态转换时移除，acknowledged 模式等宿主核验其后续持久结果后确认。`SharedStateWriteOptions.recovery: {scope, pending}` 同事务登记/结束 shared 记录的接纳责任，普通覆盖保留已有责任，删除同步清除。索引只存身份引用，不存业务输入，也不授予执行权限。索引操作要求 Session 与 catalog 在同一 IFileSystem 事务域，跨域明确拒绝；归档导入重绑定目标索引路径，Session 删除移除其待办。没有旧数据扫描、回填或损坏回退；新增恢复索引不是历史事实的替代品。

```ts
interface ShutdownReport {
    status: 'stopped' | 'blocked';
    localIdle: boolean;
    issues: RecoveryIssue[];
    pendingSessions: string[];
}
```

`stopped` 表示此次范围内确认完成；`blocked` 保留待核对事实。`localIdle` 只描述本地登记的执行工作，不证明其他进程或外部服务已停止。重启后显式 `session.resume()` 继续暂停的 Session。

## Session

| Session 成员 | 返回 / 含义 |
| --- | --- |
| `id` | 持久 Session ID |
| `schedule(spec)` | `Promise<Task>`；单次 / 固定间隔持久调度；返回可暂停取消的普通 Task，见[定时运行](./scheduling.md) |
| `submit<I, O>(spec, options?)` | `Promise<Task<O>>`；持久提交；根 SessionHandle 仍保留 spawn 别名 |
| `attachTask<O>(id)` | `Promise<Task<O>>`；校验该 Task 属于此 Session 并附加句柄 |
| `listTaskPage(query?)` | `Promise<TaskListPage>`；包含终态 Task 的索引分页 |
| `stat()` | `Promise<SessionStat>`；`phase`、`acknowledged`、`archived`、`blockedBy` |
| `watch({ after?, signal? }?)` | `AsyncIterable<EventEnvelope>`；追尾 Session 日志，关闭后排空结束；dispose 结束，signal 中断仅观察 |
| `readEvents({afterSequence?, throughSequence?, limit?}?)` | `Promise<EventPage>`；有界事件分页；默认 100、最大 500 |
| `snapshot()` | `Promise<SessionEventSnapshot>`；同事务读取 `{session, tasks, asOfSequence}`，用于裁剪后重同步 |
| `send(request)` | `Promise<CrossSessionMessage>`；幂等登记 Session 发出的消息 |
| `inboxPage(query?)` | `Promise<InboxPage>`；接收方投递序号分页 |
| `acknowledgeMessage(messageId, options?)` | 确认宿主邮箱消费；重复确认返回原记录，拒绝确认 Task 所有的消息 |
| `suspend()` | `Promise<void>`；持久暂停请求；已开始的 Effect 默认排空，返回不等于停止确认 |
| `resume()` | `Promise<void>`；解除 Session 暂停；Task 自己的控制屏障仍保留 |
| `close({ cancelRunning? }?)` | `Promise<void>`；默认 drain，设 `true` 取消未完成 Task；须观察关闭最终状态 |
| `recover(options?)` | 此 Session 的恢复入口 |
| `resources` | Session 作用域的 `ResourceApi` |

`SessionStat.phase` 为 `open | paused | closing | closed`。暂停请求已持久而尚未排空时，`phase: paused` 且 `acknowledged: false`；确认后为 `true`。`close()` 与暂停不同，关闭流程还等待资源清理和待交付 outbox。

```ts
interface TaskSpec<I = unknown> {
    program: { kind: string; version: string };
    input: I;
    requestId?: string;
    parent?: string;
    spawnKey?: string;
    dependsOn?: TaskDependency[];
    retry?: RetryPolicy;
    priority?: number;
    labels?: Record<string, string>;
    deferStart?: boolean;
    retryOfTaskId?: string;
}
```

`requestId` 在 Session 内去重提交，同键异规格冲突；省略时每次调用都可能新建 Task。`deferStart: true` 保存为 created，宿主完成授权后 `task.start()`。`submit` 和根入口的 spawn 别名不隐式创建父子关系；需要树状生命周期时填写 `parent`，或在 Program 中使用带 `spawnKey` 的 spawn action。

`LeaseGuardOptions` 可携带 `lease: { key, ownerId, epoch }`，在同一存储事务中校验共享租约；它不是网络认证或跨存储所有权协议。

## Task

| Task 成员 | 返回 / 含义 |
| --- | --- |
| `id` | 持久 Task ID |
| `stat()` | `Promise<TaskStat>`；含 revision、phase、control、blockedBy、exit、activeOperations、stopped、settled |
| `stats()` | `Promise<TaskStats>`；步骤、attempt、待处理输入和操作数量等观察值 |
| `snapshot()` | `Promise<TaskEventSnapshot>`；同事务读取完整 Task 和 asOfSequence；根入口仍保留 status 兼容方法 |
| `readEvents(query?)` | `Promise<EventPage>`；仅此 Task 的有界日志页，cursor 仍是 Session sequence |
| `poll()` | `Promise<ExitRecord<O> \| undefined>`；检查逻辑终态 |
| `wait({ timeoutMs?, signal?, until? }?)` | `Promise<ExitRecord<O>>`；until 默认 exit，设 settled 则同时等待清理；超时 / 中断不取消 Task |
| `waitStopped({timeoutMs?, signal?}?)` | `Promise<TaskStat>`；等已确认的暂停 / 中止或已清理的终态；等待本身不发控制命令 |
| `watch({ after?, signal?, until? }?)` | Task 范围事件流；默认 settled 后排空结束；设 exit 可只观察逻辑终态 |
| `start({ signal?, lease?, deadlineAt? }?)` | `Promise<void>`；启动 deferred Task；相同初始信号可幂等重放 |
| `pause(options)` | `Promise<TaskControl>`；暂停此任务树的后续派发并使旧 reducer 提交失效 |
| `interrupt(options)` | `Promise<TaskControl>`；持久中止树内在途 Effect，fence 旧 attempt，调用幂等 cancel；重启后继续清理 |
| `resume({ requestId, expectedEpoch?, reason?, lease?, signal? })` | `Promise<TaskControl>`；设为 run，可原子附加信号；终态不可恢复 |
| `withdrawPause({ requestId, expectedEpoch, reason?, lease? })` | `Promise<TaskControl>`；按控制版本撤销尚未确认的自身暂停请求，不中止或重发在途 Effect |
| `cancel(reason?, options?)` | `Promise<void>`；不可恢复的 cancelled 终态并处理清理；清理失败可能在终态已保存后抛错 |
| `retry({ requestId, lease? })` | `Promise<Task<O>>`；新 deferred root Task，带 retryOfTaskId，需重新授权和 start；返回类型递归保持 core Task |
| `signal(signal, {requestId?, lease?}?)` | `Promise<SignalReceipt>`；保存输入并返回 sequence / acceptedAt / replayed；稳定 requestId 去重，终态拒绝新输入 |
| `respond(response, options?)` | `Promise<void>`；按 interactionId 解决交互；已保存的同值回应可重放 |
| `send(request)` | 幂等登记 Task 发往指定 Task 的消息；在 reducer 中改用 send-message action |
| `recoveryIssues()` | 此 Task 的恢复阻碍列表 |
| `resolveEffect(request)` | `Promise<void>`；对 indeterminate Effect 人工裁决；需稳定 requestId |
| `resources` / `cache` | Task 资源 / 缓存接口 |

```ts
interface TaskControlOptions {
    requestId: string;
    expectedEpoch?: number;
    reason?: string;
    lease?: { key: string; ownerId: string; epoch: number };
}
interface TaskControl {
    epoch: number;
    mode: 'run' | 'pause' | 'interrupt';
    requestId: string;
    reason?: string;
    acknowledged: boolean;
}
```

控制命令按稳定 requestId 去重，expectedEpoch 拒绝陈旧控制。返回值表示请求已保存；用 `waitStopped()` 或 `stat().control.acknowledged` 等待物理确认。`phase: done` 与默认 `wait()` 表示逻辑终态；`settled` 表示逻辑终态且 Effect 全部清理，默认 watch 等待这一边界。stopped 还可表示已确认的非终态控制暂停；待核对的 pending Effect 不等于仍在物理执行。

取消传播按实际父子关系跨过已收尾的终态中间任务，继续取消未收尾后代；中间任务的终态和输出保持不变。有清理责任的终态节点仍进入既有清理路径。带 requestId 的取消若发现指定根任务已成功或失败，只保存真实终态回执，不将该迟到请求解释为“停止剩余后代”。此类控制需要宿主另行明确目标和权限。取消回执仅证明接纳，不能代替任务树的 settled 核对。

取消清理的 Effect 集合取自取消事务返回的任务记录，包含事务提交前并发接纳的 Effect，不使用较早的观察快照。当前任务清理与后代取消分别推进，兄弟取消分支并发发起；一个适配器的清理失败或等待不会阻止其他分支获得取消请求。本次已发起分支分别结束后汇总错误，失败/超时仍保留原 cleanupPending 与取消回执，重试核对原身份。取消返回及接纳回执不能替代 taskStat/taskStats 的实际收尾确认。

恢复扫描先完成原 Task 的维护/关闭取消事务，再独立发起每个任务的既有清理；一个适配器等待或失败不会挡住其他任务。关闭取消使用事务返回记录确定本轮清理责任，不等下一轮才发现新增 cleanupPending。显式 `close({cancelRunning: true})` 同样按真实父链选择最外层未收尾任务，独立发起子树取消并汇合结果，避免父子重复发起整树遍历；清理错误保留持久 closing/cleanupPending，原关闭判定与后续核对继续有效，不因部分任务确认而提前宣称整个 Session 已关闭。



pause 等待在途 Effect 自行结束；interrupt 主动取消。中止先保存 pending + cleanupPending，并将旧 attempt 记为 lost，使旧结果、进度及带 claim 的共享写入失效。取消确认后才允许控制确认；resume 保留 Task 身份，先 reconcile，或按明确 idempotent-retry 策略处理，无法核对时进入 indeterminate。Effect 原绝对 deadline 不因暂停而重置。cancel 是不可恢复终态。

长活 Effect 等待外部输入时，暂停可能一直没有确认。此时 `resume` 仍返回 `CONTROL_NOT_SETTLED`；用户可显式 `withdrawPause` 撤销这次未确认的暂停。该操作必须提供精确 `expectedEpoch`，且目标自己的控制仍为未确认 pause；已确认、interrupt、终态或整棵子树存在 cleanupPending 时拒绝。它只释放该目标拥有的 hold，保留 Session、祖先和子节点自己的暂停，不触发物理 cancel、核对或 Effect 重派发。同一 requestId 的持久回执可重放，与普通 resume 共用 ID 会冲突；状态已变化时刷新后选择对应操作，不能自动改成恢复。

Effect 的 `context.sessionState.setIfRunning(key, value, expectedVersion?)` 与控制屏障在同一 Session 事务内校验 claim 并提交共享记录。任务、祖先或 Session 暂停时返回 undefined，不写记录；失去 claim 或取消后拒绝。适配器可用它把长活 Effect 内的新外部操作接纳记录写入原 journal；此前已接纳的操作仍可核对和结算，普通 `set` 不受暂停限制。准入顺序由 journal 事务定义，不保证远端发包恰好发生在暂停按钮之前；在途 Effect 未结束时仍不能报告整体停止。缺少该可选能力的宿主不得声称具有此保证。

## Program、Decision 与 Effect

```ts
interface DurableTaskProgram<S, I, O> {
    manifest: { kind: string; version: string };
    init(input: I): Decision<S, O> | Promise<Decision<S, O>>;
    reduce(state: Readonly<S>, event: TaskInputEvent): Decision<S, O> | Promise<Decision<S, O>>;
}
interface Decision<S, O> {
    state: S;
    actions?: KernelAction[];
    next: { type: 'continue' }
        | { type: 'wait'; on: WaitSpec }
        | { type: 'complete'; output: O }
        | { type: 'fail'; error: SerializableError; retryable?: boolean };
}
```

`defineTask({kind, version, state, step})` 是单个同步 step 函数的便捷封装，初始化输入为 `{type: 'initialize', input}`，后续为 `TaskInputEvent`。初始 state 会复制。需要旧异步签名时直接实现 DurableTaskProgram；异步不改变 reducer 必须可重放的要求。

| action / 等待 | 机制 |
| --- | --- |
| `effect` | 登记 `{id?, kind, version, request, idempotencyKey, timeoutMs?, retry?, grants?}` |
| `spawn` | 以稳定 spawnKey 登记子 Task |
| `send-message` | 与此步状态提交、输入消费同事务登记 outbox |
| `request-interaction` | 登记交互，使用 wait(interaction) 等回应 |
| `set-shared` / `delete-shared` | 带可选版本约束修改 Session 共享状态 |
| `resource`、`cache-*` | 登记资源命令、缓存读取 / 发布 / 管理收据 |
| `emit` | 写自定义 journal 事件；不得冒用内核保留事件名 |
| `wait` | resource、message、cache、cache-management、signal、effect、task、child、interaction、shared-version、timer |
| 组合 wait | `any`、`all`、`quorum`；等待注册持久化 |

自定义 emit 禁止使用 task、effect、session、message、resource、cache、budget、workspace 的保留前缀；冒用时抛 EVENT_INVALID。持久数据必须可序列化；不要将 Promise、句柄、函数、进程内对象或非确定性操作藏入 state / reducer。

EffectRequest.timeoutMs 省略时保持原默认 30_000ms；正有限数表示总期限，显式 null 表示不设置总执行期限。后者适用于持久人工交互或子调用观察，不改变取消、租约、预算、清理责任及 drain 的 blocked 判定。执行/核对 context.deadlineAt 为原 Effect 冻结的绝对期限，无期限时省略；清理上下文不带执行期限，仍受原独立清理超时控制。数字期限的计时器按平台上限分段，不因 JavaScript 定时器溢出提前触发。

Task.start 的可选 deadlineAt 在 created → ready 的同一目标事务检查；到期拒绝新启动并报告 TASK_START_DEADLINE_EXCEEDED。已经接纳的相同初始信号仍可核对，不因重放发生在期限后而抹去原事实。期限不是授权替代，也不是跨机器时钟同步保证。task.call 在恢复时将原执行期限传给该边界，防止微任务先启动新目标、超时回调随后才执行的竞态；超时仍保留原目标清理责任。task.start 诊断记录 admission 阶段、安全错误分类、关联身份与耗时，不含初始信号。

EffectAdapter 的必需成员是 `kind`、`version`、`execute(request, context)`；可选 `reconcile()`、`cancel()`、`recoveryPolicy`、`shouldRetry()`。context 携带 Session / Task / Effect 身份、attemptId / 单调 attemptNumber、幂等键、AbortSignal、授权、进度 emit 和可选预算 / 共享状态端口。cancel 也获得同一 attempt 身份，供适配器对外部执行者实施 fencing。

`context.waitForObservation?<T>(observe: (signal: AbortSignal) => Promise<T>, options?: {confirmLocalStop?: boolean}): Promise<T>` 用于 execute/reconcile 内的只读等待，例如已拥有的子 Task 的 `wait({signal, until: 'settled'})`。进入等待时让出当前实例的 Effect 工作额度，观察成功或失败后都先重新取得额度，再返回值或抛出原错误；恢复者优先于新派发取得释放的额度。一个 Effect 只允许一次在途观察，重叠调用报 EFFECT_OBSERVATION_ACTIVE。旧宿主可缺少此端口，需要嵌套等待的适配器应明确拒绝，不能无条件退回占用工作额度等待。

让出的是进程内工作额度，原 Effect 仍为 leased，心跳、deadline、预算、取消和 cleanupPending 的语义保持；active Effect 仍计入 waitIdle/drain，不能将让出额度当成物理停止。maxConcurrentEffects 继续限制未让出额度的 Effect 工作，显式等待的 Effect 可以超过此数量。回调只能观察已有操作，须遵守传入 signal 并释放订阅；不得在其中提交任务、发送指令或执行外部写入。Kernel 可中断等待，但不能证明任意不合作的回调已物理退出；实际收尾仍由 cancel 核对。恢复不保存 Promise 或工作额度，仍按原 journal 和 reconcile 重新观察，不重新创建原操作。

返回后不额外授予执行或续接权限；新的副作用仍须使用 setIfRunning 及原宿主授权。该端口不提供跨 Session 事务、控制传播或业务调用所有权；取消清理上下文不提供工作额度等待端口。诊断使用 effect.observe 的 yield/resume/aborted 阶段，保留安全错误码、耗时及原执行身份，不输出观察正文。

恢复核对返回 `completed(result)`、`retry` 或 `indeterminate(error)`。未知结果需通过 `resolveEffect({requestId, effectId, outcome})` 裁决；outcome 为 `completed`、`failed` 或 `retry`。外部系统必须对相同逻辑操作的幂等键核对；库不能承诺任意副作用 exactly-once。

`cancel()` 仅在指定 attempt 已停止、旧执行者不能再启动该 attempt 时返回成功，并须支持重复调用。外部服务需利用 attempt 身份和单调编号拒绝迟到启动；内核的事务 fencing 不能代替外部系统的执行屏障。超时保存 `cleanupPending`，未确认前不能报告暂停已完成。同步死循环需宿主 Worker / 进程隔离；恢复从最后持久提交继续，不恢复任意 JavaScript 调用栈。

## 受管 Task 函数调用

Kernel 内置 `kernel.call@1` Program 和 `task.call@1` Effect。`TaskCallPlan` 描述已由宿主准备的目标 Session、版本化 TaskSpec、目标能力绑定、正整数 maxDepth 及可选 timeoutMs；不接受目标 requestId、parent、spawnKey、deferStart 或 ownedBy。Kernel 生成稳定调用身份和 deferred 目标 Task，再以原请求身份启动或核对。TaskSpec 的业务输入、授权策略及远端连接仍由宿主准备。

```ts
const plan: TaskCallPlan = {
    targetSessionId: 'worker-session', maxDepth: 4, timeoutMs: 60_000,
    task: {program: {kind: 'review', version: '1'}, input: {document: 'asset-reference'}},
};
const parent = await harness.openSession('caller-session');
const call = await parent.submit({program: {kind: 'kernel.call', version: '1'}, input: plan, deferStart: true});
await harness.bindTaskCall({sessionId: parent.id, taskId: call.id}, plan);
const exit = await call.wait({until: 'settled'});
```

kernel.call@1 保持既有冻结请求语义：省略 timeoutMs 使用 30 秒，正数为明确期限，不接受 null。新增 kernel.call@2 要求显式 timeoutMs（正数或 null），防止省略值被解释成隐含无限等待。已有 Task 不升级 Program 或改写计划；人工重新运行创建新调用者身份。自定义 Program 也可直接为 task.call@1 Effect 明确设置 null。调用 wait({timeoutMs}) 只是观察超时，不改变原执行期限或取消目标。

`snapshotTaskCallPlan(plan)` 在接纳前复用 Kernel 的调用信封和持久 JSON 校验，返回独立 JSON 快照；根入口和 `/core` 均提供。它不授予能力、创建 Session/Task 或启动执行。快照保留输入的序列化表示，不排序嵌套消息或改写摘要；原生协议可能校验消息的实际序列化内容。指纹计算可以规范化，实际派发不能因此重写已准备的输入。恢复应读取已冻结计划，不从最新定义重新构造“看起来相同”的请求。

`bindTaskCall(TaskRef, TaskCallPlan)` 是受信宿主能力授予入口，也在 `/core` 提供；它为确切计划创建 task capability 并启动原 deferred 调用者。`taskCallCapabilityUri(plan)` 可供使用根入口 bindCapabilities 的宿主复用；URI 只含计划摘要，不含正文。自定义 Program 可直接发出 task.call Effect，但须提供已获授予的 resourceHandleId 和 execute grant。能力不等于任意 Session 或任意 Task 的执行权；修改输入、目标、绑定或限额后，旧能力不再匹配。

成功输出为 `TaskCallResult {task: {sessionId, taskId}, exit: ExitRecord}`，保持目标真实结构化输出；内置 Program 将目标失败/取消传播为调用者失败，不改写目标终态。目标记录的 `ownedBy` 保存原 source Session/Task/Effect 及继承深度，不借用业务 Session ancestry 或 Invocation ID。深度按真实 Kernel 根任务继承，maxDepth 取祖先和本次计划的较小值；换 Session 不能重新获得深度预算。并发工作继续使用 Kernel 额度，观察时让出本实例额度；共享容量和实际用量沿 Resource/Budget API，跨调用 token 预留与结算不由此接口自动推导。

取消先核对原 Effect 的持久 binding；没有 binding 时，原 setIfRunning 围栏保证尚未产生目标调用。有 binding 时，取消可先创建同一 deferred 目标的取消记录：迟到创建只会取得这个原 Task，不能另建或启动第二个 Task。先完成的目标保留真实结果，仍在运行的本地后代通过原 remaining-tree 协议停止。调用等待整个目标任务树及其已持有资源 claim 的释放；仅 Task exit 不足以释放物理容量。Program/资源适配器仍须按原协议释放自己的 claim，未释放或无法确认时调用/清理保持未完成，不伪造成功，也不取消同 Session 的无关任务。

新 Session 有存储 incarnation `instanceId`；旧 Session 在获准的调用准备阶段条件补齐并记录 session.identity.assigned。调用绑定固定该身份，删除重建或归档导入后，旧调用以 SESSION_INSTANCE_CHANGED 拒绝进入替代实例。归档中有 ownedBy 时须执行宿主外部绑定验证。恢复核对同一 deferred Task 和原能力，不生成新调用；用户重新运行须创建新的调用者 Task。当前暂停为调用边界语义：已接纳的调用继续到边界，不承诺立即暂停子 Session 内部循环。

`KernelOptions.authorizeTaskCall(plan, ownerRef, phase)` 可注入宿主对实际 Session 写权及策略的检查，phase 为 execute/cleanup；参数为复制的计划和最小身份 DTO，不授予 Effect 状态访问。实际启动仍须满足原能力和 Kernel 围栏；cleanup 只处理原 binding 所属目标。诊断为 task.call.execute/cancel 的 admission/target/start/observe/stop/settlement 阶段，记录原关联身份、分类和耗时，不包含 Task 输入。

## 消息与分页

`SessionMessageRequest` 的必需字段为 `idempotencyKey`、`targetSessionId`、`topic`、`payload: JsonValue`；可选 `targetTaskId`、`correlationId`、`expiresAt`（绝对毫秒时间）。TaskMessageRequest 要求 targetTaskId。

| 事实 | 含义 |
| --- | --- |
| outbox `pending` | 源方已持久登记，等待 relay |
| `delivered` | 目标已持久接收；尚不代表业务消费 |
| `rejected` | 保存目标关闭、Task 终态、祖先取消或过期原因 |
| inbox `consumedAt` | 宿主显式确认，或 Task 输入随 Decision 消费 |
| `settlementAcknowledgedAt` | 双方已记录投递结算事实；不等于业务答复 |

指定 Task 的消息进入该 Task 的 pendingEvents；省略时留在宿主邮箱，不广播到所有 Task。暂停的 Session 仍可接收消息。源、目标不共享一个跨 Session 原子提交；两方通过持久回执去重、重试和结算。读取 inbox 不确认消费。宿主的外部副作用与 acknowledgeMessage 不同事务，须自行幂等。

不承诺同一发送者的 FIFO：并发 relay、重试和目标提交竞争可以改变接收顺序；deliverySequence 只表示目标提交顺序。需要顺序业务处理时，上层提供业务序号并按序消费。close 会等待 pending outbox；目标永不可达且未设 expiresAt 时可能持续 closing。v1 没有能够撤回已接纳消息的接口，不能把未知投递直接标为失败；需要有界关闭时应在发送时设置 expiresAt。过期只约束投递，不撤销目标已接纳或正在消费的消息。

| 游标 | 作用域 / 边界 |
| --- | --- |
| `watch({after})` / `readEvents({afterSequence})` | Session 事件 sequence，排除下界；Task 过滤后不连续 |
| `inboxPage({afterSequence, throughSequence?, limit?})` | 接收方 deliverySequence；limit 默认 100、范围 1–500 |
| `listTaskPage({afterIndex, throughIndex?, limit?})` | Session 的 Task 成员索引；固定成员上界，各页状态是读取时的当前值 |
| 根 `taskEventPage({afterIndex, throughIndex?, limit?})` | Task 专用事件索引序号，与 EventEnvelope.sequence 不同 |
| 根 `taskHistoryPage({afterVersion, throughVersion?, limit?})` | Task 快照 version |

首个 inbox 页返回 throughSequence；后续带上它和 nextAfterSequence，直到没有下一页，再把 throughSequence 保存为下一轮增量起点。Task / 事件 / 历史索引同样按各自 through 上界分页。不要把时间戳、事件序号、投递序号和快照版本混用。

跨进程通信要求共享事务存储及通知或兼容轮询；跨主机传输、网络身份和路由由宿主提供。幂等保证受回执保留窗口约束。

## 资源、缓存与扩展入口

`ResourceApi` 在 Harness / Session / Task 上按作用域提供 `create`、`share`、`revoke`、`destroy`、`query`、`open`、`acquire`、`release`、`close`、`read`、`write`、`claimAuthority`、`authority`、`request`、`stat`、`validate`、`list`。

变更请求使用稳定 requestId，并按命令带 expectedVersion / expectedRevision / authority epoch。返回 `DurableResourceRequest<T>` 的方法先返回持久请求句柄；通过其 `poll()`、`wait({timeoutMs?, signal?})`、`cancel()` 观察最终结果。受理与执行成功是不同事实。ResourceRef 包含 `{id, scope}`，跨 Session 共享还要显式授权。

`ManagedResourceAdapter.cleanup/destroy()` 返回关联 operationId / epoch 的 `stopped | pending | unknown` 回执。只有 stopped 表示容量可再次使用或资源已删除；不能用 HTTP 成功或发出取消信号代替它。

`CacheApi` 提供 `create(spec)`、`list()`、`read(request)`、`publish(request)`、`invalidate(handleId, expectedGeneration)`、`renew(handleId, expectedGeneration, ttlMs)`。读写通过 operationId 去重；写入还校验 generation 和可选版本。scope 为 step / task / session，usage 为 reusable / single-use。缓存命中不是业务操作完成证明。

根入口还提供按领域拆分的 `SessionSharedStateApi`、`SessionTaskBoardApi`、`SessionContextApi`、`SessionResourceApi`、`SessionBudgetApi`、`SessionWorkspaceApi`，以及旧句柄别名、history / attempts、只读检查与 retention / GC。`/core` 没有包含全部兼容成员。

冷读取使用 Harness.inspectTask / inspectSession；两者均不登记 Session 监听器、资源 worker 或执行调度。根 Kernel.inspectSession 另保留 listTasks / getShared / attachTask 等扩展；附加句柄的 status / snapshot / readEvents 仍不隐式激活。显式控制方法是写操作。

## 错误与日志

`KernelError` 携带 KernelErrorCode；控制冲突为 CONFLICT，未排空的 resume 为 CONTROL_NOT_SETTLED，等待超时为 WAIT_TIMEOUT，观察取消为 ABORTED，dispose 中止等待为 DISPOSED，非法分页 / 等待规格为 INVALID_SPEC。CURSOR_EXPIRED 附带安全的作用域和保留边界 details；STORAGE_CORRUPT 与 EVENT_INVALID 分别表示索引 / 记录损坏和事件协议错误。旧扩展入口仍可能返回普通 Error，不能只靠错误文本识别新核心流程。

`onDiagnostic` 是进程内诊断回调，不是持久事件或消息消费接口；回调失败不改变已经提交的业务结果。`status()`、journal 和消息可能包含业务内容，不要将其整体写入诊断日志。断流、物理清理确认及 retention 的已知限制见[事件契约](./events.md)。

## 事件补读与迁移

`readEvents()` 返回 `{items, throughSequence, nextAfterSequence?}`。使用首个 throughSequence 固定整批上界，读取到没有 nextAfterSequence 后保存 throughSequence。Task 查询通过索引定位该 Session sequence；Session 查询读取有界连续序号范围，不重复扫描全部历史。

收到 CURSOR_EXPIRED 时，丢弃旧事件投影，读取 `snapshot()`，用权威状态重建视图，再从 snapshot.asOfSequence 补读。快照内容与水位同事务读取；裁剪后缺失的历史不会被快照重新创造。Task 快照只包含该 Task，Session 快照显式包含全体成员状态，其成本与 Task 数量相关。

`watch({signal})` 取消仅释放观察资源并抛 ABORTED；Kernel dispose 结束事件流，未完成的 wait / waitStopped 抛 DISPOSED。恢复后重新 open / attach 句柄。底层不支持取消的只读 IO 可能继续完成，但不会因此启动 Task 或保存消费确认。

新事件 schemaVersion 为 2；`parseEventEnvelope()` 校验信封，`parseKernelEvent()` 校验已收录的 v2 内建事件并返回判别联合，旧 v1 与未收录 / 应用类型返回 undefined，保留原始记录供拥有者解析。首次新事件提交在同一事务将 Session manifest 的 events 和 index schema 提升到 2；读取支持这两个记录族的 1 / 2，旧写入者必须退出后再升级，不支持混用旧版本写入。

/core 收敛为 submit、send、inboxPage、stat、snapshot、readEvents、watch；spawn、sendToSession、inbox、outbox、status、events 等兼容方法保留在根句柄。旧 core.ResourceHandle 别名改用明确的 ManagedHandle。这是 0.1.0 的统一接口约定；使用旧名称的源码消费者应迁到 canonical 方法，或明确使用根兼容入口。


存储的事务、跨进程通知、掉电保证和 drain 生命周期见[存储契约](./storage.md)。SignalReceipt 的 requestId 在未提供请求身份时省略；同请求重放保持 sequence / acceptedAt，replayed 为 true。回执位于 Task 权威记录族，不随观察事件或历史快照裁剪。TASK_TERMINAL 和 SIGNAL_IDENTITY_CONFLICT 可由宿主明确展示；Promise 成功表示输入持久接纳，不表示业务执行完成。

## Session 执行摘要

`harness.sessionActivity(id)` 是总览查询，不激活 Session，不读取对话历史或已索引 Task 正文。它与 `Session.stat()` 分工不同：stat 判断控制是否确认，activity 汇总其中各 Task 的执行阶段和待办数量。不存在的执行 Session 抛 `SESSION_NOT_FOUND`；格式损坏抛 `STORAGE_CORRUPT`，宿主不得把读取失败显示为空闲。

```ts
const activity = await harness.sessionActivity(session.id);
console.log(activity.pending); // { approval, input, cleanup }
console.log(activity.counts);  // running, queued, created, waiting, paused, blocked,
                              // succeeded, failed, cancelled
console.log(activity.sessionStatus, activity.revision, activity.asOfSequence);
```

每个 Task 只计入一个 phase。pending 另按交互/操作计数，一个 Task 可以有多个待办；终态 Task 不贡献待确认/输入。等待依赖或消息属于 waiting；缺少 Program 或 Effect 结果不确定属于 blocked。已租赁的 Effect 仍计运行中，暂停请求不等于外部操作已经停止。`pending.cleanup` 保留待清理操作，包括已进入逻辑终态的 Task；停止是否确认仍以 stat 的 acknowledged / settled 为准。

`latestExit` 是索引中最近一次持久终态的种类和时间，不代表该 Session 所有任务都已结束。revision 只在摘要语义变化时递增；内容流会推进 asOfSequence，但不推进 activity revision。Session 自身生命周期变化需要同时比较 sessionStatus；不能只靠 revision 缓存整个响应。

宿主在订阅 structure 通知后加载初始摘要，通知发生时重新读取；通知可以合并，不应当作为待办增减计数器。冷查询不会登记监听与调度，本地 onChanged 及默认跨进程补扫只覆盖该实例已经接纳的 Session。宿主可以缓存可见行并按 Session 失效，显式刷新应重新查询。

摘要查询的冷读取成本与 Session 的小索引条数有关；不会把全 Session 快照用于侧栏。旧存储缺少投影时批读缺少摘要的 Task，包括终态，核对遗留清理责任；恢复时补齐索引。[Harness 场景验证](./harness-scenarios.md)列出了恢复、交互和性能证据。

### 实时 Effect 失败分类

`EffectAdapter.failureOutcome?(error)` 可在实时调用无法确认外部结果时返回 `indeterminate`，默认行为仍为 `failed`。Kernel 持久化 `effect.indeterminate` 并保留 Task 等待，即使 `shouldRetry` 为 true 也不安排自动重试。取消和超时继续走物理清理。使用现有显式 `TaskHandle.resolveEffect()` 提交核对后的成功/失败，或授权另一次尝试；适配器仍负责稳定外部身份及回执核对。


### Historical control admission inspection

The root `Kernel.readTaskControlReceipt(sessionId, taskId, requestId)` reads the existing durable control receipt without opening an execution Session, scheduling work, or replaying a mutation. It returns `undefined` if no receipt is retained; a missing Task or mismatched Session remains an error.

`TaskControlReceipt` contains `requestId`, the original `action` (`pause`, `run`, `interrupt`, or `withdraw-pause`), optional original `expectedEpoch`, immutable `accepted` control, and the separately observed `current` control. An old admission remains readable after replacement or Task completion. The original acknowledgement is not rewritten to imply historical physical stoppage; current control and task/tree statistics must be inspected separately. Request fingerprint and signal payload are never returned. No receipt write format or replay fingerprint changes.

### Cancellation admission receipts

`Task.cancel(reason?, {requestId?, expectedEpoch?, lease?})` keeps its `Promise<void>` return and existing cleanup behavior. Supplying `requestId` saves immutable `TaskCancellationReceipt` in the **same Session transaction** as the terminal outcome. `expectedEpoch`, when supplied, must match the observed Task control epoch and requires a nonempty request ID. Lease ownership is checked before admitting or replaying a cancellation. Reusing an ID with a different reason or expected epoch conflicts; a replay does not change Task version or duplicate terminal events. A parent's request ID and epoch apply only to that parent; descendant cancellation retains its own control ownership.

`Kernel.readTaskCancellationReceipt(sessionId, taskId, requestId)` is a cold, read-only query returning `undefined` when no receipt exists. It returns `{requestId, taskId, expectedEpoch?, controlEpoch, status}` without the reason, output or error payload. Cancellation IDs use a separate `cancellation/` receipt namespace from pause/resume's `control/` namespace. Receipts remain with the retained Task storage and survive Kernel reconstruction; deleting that storage removes the evidence.

`status` is the actual terminal outcome at admission: a completed or failed Task stays completed or failed when cancellation loses the race. The receipt **does not confirm physical release or descendant settlement**. It is readable while `cancel()` is awaiting cleanup or after cleanup fails. Continue observing `taskStat`/`taskStats`, `stopped` and `settled`; cold receipt reads never retry cleanup or start workers. Legacy calls without a request ID retain the original behavior and do not create admission evidence.

### Durable interaction response identity

`Task.respond(response, {requestId?, lease?})`, `Session.respond(taskId, response, options?)` and `Kernel.respondInteraction` accept `InteractionResponseOptions`. A supplied requestId is a non-empty string up to 256 characters, scoped to the target Task. It is persisted as `InteractionRecord.responseRequestId` in the same transaction as the response and the single `interaction-resolved` input event.

The original ID and identical answer may be replayed even after the Task has terminated. The same ID cannot answer another interaction or change its answer; another ID cannot adopt an already resolved answer, including an old answer without an ID. These cases return `CONFLICT`. Callers omitting requestId keep the legacy same-value replay behavior. A pending response still obeys the existing lease and cancellation fences; saving an answer does not release a pause.

`Kernel.readTaskInteractionResponseReceipt(sessionId, taskId, requestId)` reads persisted evidence without registering a worker. It returns `{requestId, taskId, interactionId, resolvedAt}`, or `undefined` when no matching receipt exists. It excludes the prompt and answer. Missing evidence never authorizes an automatic resend. The receipt lasts as long as the retained Task record; it is not a completion or cleanup acknowledgement. Successful and failed response writes use `task.interaction.respond` diagnostics without payloads.

### Stop remaining work under a terminal root

`kernel.cancelRemainingTaskTree(sessionId, taskId, {requestId, expectedEpoch, reason?, lease?})`
explicitly stops remaining work after the selected root has reached a terminal exit. This is
separate from `cancel`: a late ordinary cancellation that observes successful completion must
not silently acquire this meaning. An active root is rejected; use normal cancellation instead.

Admission validates the optional shared scheduler lease and root control epoch, then cancels
all nonterminal members of the actual same-Session parent tree in one transaction. Existing
terminal exits and outputs are preserved, including intermediate nodes. Active Effects on
terminal members are fenced and retain `cleanupPending`. No new tree state or scheduler is
introduced: terminal parents reject child creation, old claims cannot commit, and existing
recovery completes retained cleanup. A concurrent child is either included before admission
or rejected after it. Tree selection currently scans the Session task index.

The stable request identity rejects changed reason/epoch; replay only drives remaining cleanup.
`readTaskTreeCancellationReceipt(sessionId, taskId, requestId)` is a cold query and returns
`{requestId, taskId, expectedEpoch, controlEpoch, rootStatus, acceptedAt, taskCount}`. A receipt
means admission, not physical stop. Adapter failures keep cleanup responsibility durable;
independent cleanup branches proceed and errors are joined. Reopening a failed call uses a new
Task, never resume of a cancelled member. Restore preserves the receipt; fork uses new Tasks
and does not copy it. The operation does not control other Sessions or roll back external writes.


### Task submission admission receipts

`Kernel.readTaskSubmissionReceipt(sessionId, originalSpec)` and the `/core` Harness expose an exact-spec, cold query for ordinary public `Session.submit` admissions. The original spec must include its requestId and preserve its JSON representation, including field order, deferStart and labels. This reuses the existing submission key and fingerprint; it adds no record, schema, worker or recovery. Internal owned-target admissions use a separate owner fingerprint and remain reconciled by task.call.

The result is `TaskSubmissionReceipt {requestId, task: {sessionId, taskId}, acceptedAt}` or undefined when no receipt is retained. Different content throws CONFLICT, absent Sessions throw SESSION_NOT_FOUND, and malformed receipts or mismatched storage/Task scope throw TASK_SUBMISSION_RECEIPT_INVALID. The receipt and Task are checked in one Session transaction without enumerating Tasks. The fingerprint and input are never returned. task.submission.inspect diagnostics contain safe identities, stage, outcome, error classification and duration.

An admission proves only that the original Task was durably created. It does not prove capability binding, target dispatch, execution completion or resource release. Terminal Tasks retain the same admission. Receipt lifetime follows retained Session storage; missing evidence after removal or recreation does not prove an operation never ran. After a lost reply, reconcile and replay the original identity rather than inventing a new request. Inspection itself never retries the submission.


### Cold Task call target inspection

`Kernel.readTaskCallTarget({sessionId, taskId, effectId})` and `/core` Harness return the actual owned target TaskRef, or undefined when the original Effect/binding/target has not yet been observed. This never creates a Session or Task, registers a worker, grants capabilities or resumes execution. Source lookup failures remain errors; absent evidence never authorizes another submission.

Inspection reuses task.call binding validation: exact Effect protocol and plan fingerprint, inherited depth/owner, target Session incarnation, root identity and ownedBy. Incarnation and target record are read in one target Session transaction. There is no atomic cross-Session snapshot. Replaced incarnations throw SESSION_INSTANCE_CHANGED; corrupt bindings/targets throw the existing TASK_CALL_BINDING_INVALID/TASK_CALL_TARGET_INVALID. A missing target after a successful Effect is an error, not a downgrade to never dispatched. Failure and cancellation do not require success output to retain the relation.

The result contains identity only, not a completion or physical cleanup acknowledgement. Continue observing caller and target/tree facts separately. task.call.inspect diagnostics report safe captured identities, lookup stage, outcome, classification and duration, never the request or binding body. No storage format changes or second execution index are introduced.

SessionActivity.lastActivityAt 是最后一个有 Task 归属的持久执行事件时间，与事件同事务写入 events.seq 的 last-execution-activity-at。普通读取、租约心跳、无 Task 的结构事件及 task.events.pruned 不更新时间；冷读只查询摘要元数据。旧存储缺失时省略，不能用读取时间或 Session.updatedAt 补齐。

Worker lifecycle: `kernel.quiesce({timeoutMs})` stops admission, dispatch and polling, waits for local work to reach an idle boundary, and retains storage/query capability. A blocked acknowledgement retains leases and work; callers may wait again. `kernel.drain` additionally disposes the quiescent worker. Neither method pauses or cancels durable Session controls; recovery uses the existing committed Task state and identity.

`quiesce({detachObservations:true})` stops local read-only `waitForObservation` waits and retains the unresolved Effect and original identity for recovery. After local cleanup, it expires only the matching worker attempt lease so a new worker can reconcile immediately. It does not cancel the target Task or publish an Effect failure. The default drain continues to wait for observations. Active work outside an observation still drains to its boundary before storage is released; noncooperative work keeps `localIdle: false`.

An adapter that owns a local execution resource uses `waitForObservation(read, {confirmLocalStop:true})`. On drain the callback receives the stop signal, but the Kernel retains local ownership until that callback confirms resource cleanup and settles. A timeout returns `blocked`; it never treats a still-running managed child as detached remote observation. The original Effect remains leased for reconciliation after restart.

`kernel.beginObservationDrain()` begins irreversible local observer retirement before producers checkpoint their metadata. It aborts observations and prevents new Effect dispatch without cancelling durable Tasks; submission bookkeeping can still finish. A host then calls `quiesce` to await physical cleanup before releasing storage. It is a host lifecycle method, not a user pause or an API for stopping remote work.
