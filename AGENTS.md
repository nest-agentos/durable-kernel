# @itookit/durable-kernel

持久执行内核，可独立安装、构建和打包。唯一运行依赖为 vfs-core；不依赖应用、UI、LLM 或具体工具。使用、停止恢复和消息语义见 [README](./README.md)。

接口与已知限制见随包提供的 [API](./docs/api.md) 和[事件契约](./docs/events.md)；修改公开接口或事件时同步更新。interrupt 必须持久 fence 在途 Effect 并确认物理清理；Task watch 默认在 settled 后结束，wait 默认保留逻辑 exit 语义。

## 开发约定

- 交流使用中文，代码注释使用英文，提交使用 Conventional Commits。
- 保持 SOLID / DRY / KISS / YAGNI；新增函数不超过 30 行、圈复杂度不超过 10。
- 公共入口为根、`/core`、仅类型的 `/contracts`；外部消费者禁止深引用包内实现。
- 时间、随机数、网络、文件和进程副作用不得隐藏在可重放 reducer 中；业务外部操作通过 EffectAdapter 接入。
- ID 使用包内 `identity.ts` 兼容工具；原生 UUID 缺失或抛错的路径必须有回归测试。
- tsconfig 不引用父工程；LocalFS/SQLite 仅作为开发依赖用于真实进程集成测试。

## 不变量

- Decision 的 state、输入消费、本地 action 登记、wait 注册和状态迁移在同一 Session 事务提交。
- 所有持久输入、状态和结果须验证可序列化；恢复不得依赖进程内 Promise 或 Map。
- reducer 的有效提交权由 lease token/epoch 保证；暂停或失权后旧 Decision 不得提交。
- Task 终态不可 resume；人工重做创建带 retryOfTaskId 的新 Task。
- withdrawPause 仅按精确控制 epoch 撤销未确认 pause，不等于 resume，不允许绕过 interrupt 或树内 cleanupPending；其他控制所有者的 hold 必须保留。Effect 的 setIfRunning 在原共享写入事务检查 Session/祖先/任务屏障和 claim，暂停不妨碍原操作结算。
- Task 消息仅随 Decision 原子消费；Session 邮箱由宿主显式 acknowledgeMessage，不自动广播到 Task。
- 稳定幂等键同键异内容必须冲突；旧请求回执在保留窗口内可重放，包括发送 Task 已终态的情况。
- inbox 游标使用接收方 deliverySequence，不能使用发送时间；分页和消费确认是不同操作。
- Effect 超时保留 cleanupPending，物理取消确认成功前不得确认暂停；适配器缺失、失败或挂起都不能丢弃清理责任。
- drain 只退休本实例，不修改 Session 运行意图；blocked 保留心跳与存储责任。默认提交水位补扫按存储批量读取，禁止对未变化的 Session 每次全扫 Task。
- schedule 使用持久 timer-fired 时间、原子 spawn 和稳定轮次身份；不得在 reducer 读取当前时间。
- archive 先有界快照再输出，导入记录/catalog/回执同事务；恢复验证源端所有权，导入保持暂停，fork 不复制在途副作用。
- shutdown 对本地登记 Session 持久请求暂停，返回 stopped/blocked；重建后须显式 resume。dispose 不承诺物理停止，waitIdle 超时须报错。
- 观察投影 taskStat/taskStats/sessionStat 只读；inspectTask / inspectSession / snapshot / readEvents 不得登记 worker。
- 新事件写 schemaVersion 2 并原子提升 manifest；v1 可读，不能猜测补齐旧关联。Effect 生命周期须带 effectId 和已有 attemptId，Task 终态统一 ExitRecord。
- readEvents 和 watch 使用 Session sequence，过期游标抛 CURSOR_EXPIRED；snapshot 的状态和 asOfSequence 同事务读取。订阅取消只释放观察，不改变执行意图。
- Session 仅 events.seq 改变时通知 content，其他结构记录通知 structure；两类均须唤醒既有调度。
- Session 内 SeqFile 提交只调度所属 Session；跨 Session 投递由接收方提交及既有通知唤醒。catalog 变更仍调度已登记 Session，跨进程遗漏继续由 CommitRescanner 核对；普通局部提交不能无条件广播轮询全部 Session。
- Task 与 activity 小投影同事务提交；纯版本变化不提升 activity revision。恢复修复前按 Task version 复核，不能用目录枚举的旧快照覆盖并发完成或新增的 Task。
- 预算 usageId 和结算回执同事务；同键不同内容冲突，重放不重复计费。
- managed authority 的 ownerEpoch 在权威事务校验；过期执行者不得新写，已受理请求仍可重放。
- retention 不删除活跃引用、未交付消息、未确认的跨 Session 结算或仍处于重放窗口的幂等事实。
- 恢复只按当前 Session 的 task/<id> 修复 catalog；不得对每个 Session 全扫其他 Session 的 Task。
- 等待图恢复批量读取关系，不得为每个终态 Task 重复空范围查询。
- Session layout manifest 不支持或迁移未完成时 fail closed。

## 诊断

关键跨层步骤和失败必须通过 onDiagnostic/既有通道记录操作、阶段、结果、错误分类、耗时和可关联身份。不能记录 payload、完整参数、用户正文、密钥或令牌；观察回调失败不能改变提交结果。验证失败场景及日志脱敏。

## 验证

```bash
pnpm typecheck
pnpm test:unit
pnpm test:integration
pnpm build
pnpm test:package
```

- `src/reliability-contracts.test.ts`：幂等输入、worker drain、批量水位补扫、定时恢复、归档校验与导入。
- `src/activity-contracts.test.ts`：持久待办数量、冷读取、旧格式、在线恢复并发与内容/结构水位。
- `src/harness-scenarios.test.ts`：多 Task 协作、跨 Session 请求回复、独立控制、带审批的模型/工具循环、文件回调桥及关闭。
- `src/interface-contracts.test.ts`：中止 / 重建 / 整树确认、冷读取、v2 schema、补读与日志脱敏。
- `src/independent-contracts.test.ts`：多 Session 并发、幂等消息、消费确认、游标、停止确认、重建和 core 接口。
- `src/protocol.test.ts`：事务、租约、wait、控制、消息、retention、layout 和缓存不变量。
- `src/resources.test.ts`：托管资源、authority 与物理清理。
- `tests/ipc.test.ts` / `tests/ipc-worker.ts`：真实 LocalFS/SQLite、两个挂载模式、多进程竞争与 SIGKILL 恢复，无应用宿主依赖。
- `tests/harness-runtime.ts`：真实 Harness 进程组合，业务消息走 Kernel outbox/inbox，控制 RPC 仅作测试驱动。
- `scripts/check-package.mjs`：仓库外 ESM/CJS、公开入口、NodeNext 声明及运行消费；可传现成 tarball 避免重复打包。

源码入口与发布入口不同，使用 pnpm pack 或明确应用 publishConfig 的制品流程；发布前验证 dist 出口和纯注册表依赖，禁止直接发布源码 manifest。
