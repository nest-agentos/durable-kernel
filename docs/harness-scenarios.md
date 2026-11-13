# Harness 场景验证

这些测试验证 durable-kernel 能作为 Harness 的持久执行底座。模型、工具、审批规则和文件通知桥由测试宿主注入，内核不内置 Agent Loop、真实模型供应商协议或 TTY 渲染。

| 常见场景 | 验证内容 | 自动化证据 |
| --- | --- | --- |
| 同 Session 多任务团队 | 两个 worker 并行消费任务消息，原子登记回复，coordinator 收齐结果结束 | [src/harness-scenarios.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/harness-scenarios.test.ts) |
| 跨 Session 请求/回复 | 双向 outbox/inbox、稳定幂等身份、Task 终态后原请求重放、同键异内容冲突 | [src/harness-scenarios.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/harness-scenarios.test.ts) |
| Session 与 Task 独立控制 | Session 暂停期间保存输入，另一 Session 继续；Session 恢复保留 Task 自己的暂停，显式恢复后继续 | [src/harness-scenarios.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/harness-scenarios.test.ts) |
| Agent 的模型→工具→模型循环 | Effect 调用模型、持久审批、shutdown、重建、原 Task 回答、工具执行一次、第二次模型结果结束 | [src/harness-scenarios.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/harness-scenarios.test.ts) |
| 文件回调 | VFS 创建/修改回调经宿主转换成幂等 signal，暂停期间落盘、恢复后按序消费 | [src/harness-scenarios.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/harness-scenarios.test.ts) |
| 关闭 Session | 所属 Task 全部取消并 settled，汇总不再显示待办 | [src/harness-scenarios.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/harness-scenarios.test.ts) |
| 两个真实 Harness 进程 | 独立进程分别拥有 coordinator / reviewers Session，两个 worker 请求/回复、重试去重、drain 后替换进程冷读结果 | [tests/ipc.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/tests/ipc.test.ts)、[tests/harness-runtime.ts](https://github.com/nest-agentos/durable-kernel/blob/main/tests/harness-runtime.ts) |
| 菜单等待与进程退出 | 两个待选菜单经 SIGKILL 后由新进程读回；Session 暂停时回答不执行，恢复后原 Task 完成，requested/resolved 各一次 | 同上 |
| 定时运行与恢复 | 到期原子 spawn、固定间隔、skip/catch-up、暂停/恢复、kill 后恢复，不重复创建轮次 | [src/reliability-contracts.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/reliability-contracts.test.ts)、[tests/ipc.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/tests/ipc.test.ts) |
| Effect 不确定结果与停止屏障 | 外部执行后本地未提交、旧执行者迟到、取消未确认、核对/清理失败后重建 | [src/interface-contracts.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/interface-contracts.test.ts)、[src/independent-contracts.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/independent-contracts.test.ts)、[tests/ipc.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/tests/ipc.test.ts) |
| 状态总览和并发恢复 | 多个审批/输入计数、冷读、旧格式补读、完整索引不读 Task 正文、活动水位、恢复与完成/新增任务交错 | [src/activity-contracts.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/activity-contracts.test.ts) |
| 事件重连 | snapshot 水位后的 watch 补读、无重复 sequence、Effect/attempt 关联、已回答问题不重复创建 | [src/harness-scenarios.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/harness-scenarios.test.ts)、[src/public/event-stream.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/public/event-stream.test.ts) |
| 文本归档 | 完整记录校验、restore / fork、未知外部资源拒绝、导入事务前后 SIGKILL | [src/reliability-contracts.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/src/reliability-contracts.test.ts)、[tests/ipc.test.ts](https://github.com/nest-agentos/durable-kernel/blob/main/tests/ipc.test.ts) |

0.1.0 验收共 401 项测试：349 项单元/契约、52 项真实进程；独立 ESM/CJS、NodeNext 声明及 tarball 运行消费通过。表中的源码证据链接指向独立仓库，安装包不附带测试源码。

真实进程矩阵分别在根后端和挂载模块运行，使用 LocalFS / SQLite，不依赖 NestOS 应用。进程之间的 Node IPC 只传测试控制命令与断言结果，业务消息经 Kernel 自身持久 outbox/inbox 投递。

```bash
pnpm typecheck
pnpm test:unit
pnpm test:integration
pnpm build
pnpm test:package
```

`test:package` 在仓库外安装 tarball，核对唯一运行依赖、ESM/CJS/类型入口，并实际运行 Task、活动汇总、消息、定时、归档和停止。确定性模型适配器验证调度与恢复，不能代替真实供应商端到端测试。SIGKILL 验证进程故障，不等于机器掉电测试；外部副作用的恢复仍依赖对应适配器核对协议。
