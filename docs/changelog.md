# 版本记录

## 0.1.0

- Effect 支持显式 timeoutMs=null；kernel.call@2 要求明确期限策略，@1 默认 30 秒保持兼容。实际使用时原子声明 effect schema v2 与 effect-unbounded-deadline 能力，归档保留并校验契约。
- Task.start 在目标事务校验可选 deadlineAt；受管调用恢复不能启动已过期的新目标，已接纳启动仍可幂等核对。执行期限与取消清理、观察超时分开。
- 期限增量验证：455 项单元/契约、60 项真实进程测试、仓库外 ESM/CJS/core 与 NodeNext 消费，以及上一提交构建产物拒绝新格式的实测。

- 受管调用派发保留已准备输入的序列化表示，避免指纹规范化破坏原生上下文摘要；公开 `snapshotTaskCallPlan` 供宿主在接纳前冻结和校验调用数据。

新增受管 Task 调用：kernel.call / task.call、确切计划能力、跨 Session 原任务核对与取消屏障、深度继承和资源收尾观察。Session 存储 incarnation 阻止迟到调用进入删除重建或导入的替代实例。该能力不包含应用的 Agent/Flow 定义解析、原生上下文或跨调用 token 预算预留策略。

首版独立持久执行内核，唯一运行依赖为 `@itookit/vfs-core@5.3.3`；公开根、`/core` 与仅类型的 `/contracts`，提供 ESM、CommonJS 和对应类型声明。

- Session 内并发 Task、依赖和任务树，Program / Decision / Effect 的事务提交与恢复。
- Session/Task 持久消息、消费确认与幂等请求；同机共享事务存储的多进程通信。
- pause / interrupt / resume / cancel、Session 控制、worker drain、Effect 核对和清理屏障。
- 持久定时运行、JSONL restore/fork 归档、资源和预算机制。
- v2 事件信封、snapshot 水位、游标补读、可取消观察、批量提交水位补扫。
- `sessionActivity` 轻量状态汇总；活动索引与 Task 同事务提交，恢复修复前复核版本，避免旧快照覆盖并发进展。

验证基线：401 项测试（349 单元/契约、52 真实进程），另有仓库外 tarball 的 ESM/CJS、NodeNext 声明及运行消费；场景和边界见 [Harness 验证](./harness-scenarios.md)。

index / events v1 支持冷读取；首次新事件提交声明 v2，升级前须退出旧 writer。内核不内置 Agent Loop、TTY 展示或跨主机网络传输；外部副作用仍需适配器的幂等、核对及停止确认协议。SIGKILL 测试不代表掉电保证。
