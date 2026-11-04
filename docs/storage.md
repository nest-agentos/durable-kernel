# 事务存储与宿主通知契约

内核的权威状态是事务性 SeqFile 记录，不是可见文件正文，也不是观察事件流。LocalFS 将 SeqFile 字段保存在 SQLite sidecar；只复制目录中的 `.seq` 文件不足以恢复 Session。

## 后端必须保证

Session 主记录可保存 instanceId，区分逻辑 ID 相同的不同存储实例；调用绑定在原 source shared.seq 的 kernel.task-call.<稳定摘要> 保存原归属、目标实例和计划摘要。目标 Task 仍在原 tasks/ 及 submission 请求索引中，先 deferred 再启动；ownedBy 只保存关联和深度，不复制执行状态。目标确定身份避免核对或取消重试制造空的 Task 目录。incarnation 的核对与目标提交在同一目标事务中完成，跨 source/target 不假定事务原子性；原取消 Task 是迟到启动的持久屏障。保留原 binding、请求索引和取消记录，不能在仍有活跃引用或在途旧操作时仅按年龄删除。

- `meta.seq.transaction` 在一个事务后端内跨 SeqFile 原子提交；异常时整笔回滚。
- 同一事务内的读取、revision / lease / epoch 检查和写入串行化，两个进程不能同时成功领取同一执行权。
- 事务提交成功后的新读取可见该提交；所有进程使用同一数据库和锁域。SQLite 不应通过任意网络文件共享代替受支持的锁协议。
- 不支持跨事务后端的隐式原子提交。跨 Session 消息依靠 outbox、目标回执和结算协议；归档 v1 的导入要求 catalog 和目标使用同一 IFileSystem 事务。
- 原子提交与持久介质同步分别定义。MemoryBackend 不跨重启；LocalFS/SQLite WAL 的 NORMAL 配置覆盖进程崩溃恢复，FULL 才提供更强的已提交事务掉电持久性，仍受底层文件系统和设备保证约束。

内核会检查事务入口是否存在，但不能仅凭 TypeScript 类型证明第三方后端正确。接入后端必须跑并发领取、失败回滚和杀进程测试；掉电保证需要单独验证，不能用 SIGKILL 结果代替。

## 通知和补扫

`seq:committed` 在事务成功后发出，表示本实例看到了提交；它本身不是跨进程的持久通知队列。通知只负责唤醒，worker 每次仍读取并校验权威状态。

默认 `rescanMs: 1000` 提供安全补扫，与默认 `pollMs: 0` 的兼容轮询独立。补扫按 IFileSystem 合并为一次事务，批量读取 Session journal 水位、活动修订、Session 控制记录及托管资源修订标记。水位未变时不枚举 Task、事件正文或资源列表；变化后才唤醒既有调度循环。定时器和租约仍按各自截止时间唤醒，不等待下一次补扫。

```ts
const harness = createHarness({
    catalog: { fs, rootPath: '/catalog' },
    rescanMs: 1000,
    wakeSource: {
        subscribe(wake) {
            return hostCommits.subscribe(sessionId => wake(sessionId));
        },
    },
});
```

`hostCommits` 是宿主提供的通知端口。`wake(sessionId)` 唤醒已打开的指定 Session，省略 ID 唤醒本实例全部已打开 Session；它不自动接纳 catalog 中其他 Session。清理函数在内核释放时调用。

只有提供 `wakeSource` 才允许 `rescanMs: 0`。此时宿主须保证订阅与首次读取之间无缺口、提交后发送者退出也能补发、断线重连会重新核对水位。普通 EventEmitter、`fs.watch` 或一次性 socket 消息不能单独提供这些保证。建议保留默认补扫，即使通知桥已经存在。

## 文件创建与修改回调

```ts
const offCreated = fs.on('node:created', event => onFilesCreated(event));
const offUpdated = fs.on('node:updated', event => onFilesUpdated(event));
const offKernel = harness.onChanged(event => refreshTaskView(event.sessionId));
// Release subscriptions with their owning view or host.
offCreated(); offUpdated(); offKernel();
```

这些 VFS 事件覆盖经过对应实例的操作；外部编辑器直接写磁盘是否可见，由宿主文件监听适配器决定。上层可把文件变化转换成带稳定 requestId 的 `task.signal()`。`wakeSource` 只唤醒内核，不会推断文件变化应发送给哪个 Task，也不把一次文件监听回调当作持久业务输入。

## worker 退出

`harness.drain({timeoutMs})` 先拒绝新提交和新领取，让已接纳的 reducer / Effect 完成并提交，然后释放本地 worker；不改变共享 Session 的运行意图。返回 `drained` 时 `localIdle: true`。返回 `blocked` 时仍保留正在执行工作的心跳和存储责任，可再次 drain；不要提前关闭存储。显式 dispose 可强制退出本实例，恢复者随后按租约与 Effect 核对协议处理。

`shutdown()` 保留原语义：持久请求本实例已打开的 Session 暂停。它适用于业务暂停；worker 滚动替换使用 drain。两种操作不能同时启动。

## 活动索引与在线恢复

index schema v2 在 `index.seq` 保存 `activity/task/<id>` 小投影和 `activity/revision` 水位。小投影包括 Task version、phase、pending input/approval/cleanup 数量和终态时间，不含 input、state、工具结果或用户正文。Task 权威记录与投影同事务更新；只有汇总语义变化才提升 activity revision，纯版本变化仍更新校验版本。事件流不写该投影。

跨进程补扫在原批次中增加 activity 水位及 Session 控制记录；前者或后者变化发 structure，单独 journal 水位变化发 content。空闲时仍按后端一次批读，不逐 Session 开事务或扫描任务列表。

恢复先枚举 Task，再在修复事务内核对小投影中的 Task version。仅对期间变化、新增或缺少版本投影的 Task 批量重读权威记录，再修复状态、活动及等待图，防止旧枚举覆盖新完成状态或删除新任务索引。无并发变化、索引完整的正常恢复仍只批读一次 Task 正文。

index v1 和 events v1 允许冷读取；首次新事件提交原子声明 index v2 / events v2，旧 writer 随后必须拒绝。升级需先退出旧执行进程，不支持新旧 writer 混写同一存储。派生索引不是执行检查点；跨进程恢复仍需要完整事务存储或合规归档。


## 无总期限 Effect 的格式声明

Effect request.timeoutMs=null 表示显式无总执行期限。第一次提交这类真实 Effect 时，同一 Decision 事务将 Session 的 recordSchemas.effect 提升为 2，并添加 requiredCapabilities 的 effect-unbounded-deadline；失败事务不留下格式升级。未使用 null 的新旧 Session 保持 effect v1，完成后也不降级已声明的 v2。

旧 Kernel 无法理解该契约，必须拒绝打开 effect v2；升级前退出旧 writer，不支持旧进程与新进程混写。归档保留原声明，导入时即使校验和有效，也拒绝包含 null Effect 却未声明对应 schema/能力的归档。执行期限缺失不代表停止、免费或释放资源；原 claim、心跳、取消及清理证据仍为权威。
