# 持久定时运行

`session.schedule(spec)` 提交通用的 `kernel.schedule@1` Task。它使用已有的 timer wait 和事务 spawn，精确版本由 Kernel 自动注册；不用额外的调度数据库或每 Task 常驻计时器。

```ts
const schedule = await session.schedule({
    requestId: 'nightly-run',
    at: Date.now() + 60_000,
    everyMs: 24 * 60 * 60 * 1000,
    missed: 'skip',
    maxRuns: 30,
    task: { program: { kind: 'my-job', version: '1' }, input: { scope: 'daily' } },
});

await schedule.pause({requestId: 'pause-nightly'});
await schedule.waitStopped();
await schedule.resume({requestId: 'resume-nightly'});
// Permanently cancel the schedule and its unfinished child Tasks.
await schedule.cancel('schedule removed');
```

| 字段 | 契约 |
| --- | --- |
| `requestId` | Session 内稳定提交身份；同 ID 同规格重放，同 ID 异规格冲突 |
| `at` | UTC Unix 毫秒绝对时间，非负安全整数；过去的时间在恢复调度后按 missed 处理 |
| `everyMs` | 正安全整数；省略为单次运行；固定间隔以原时间轴计算，不按完成时间漂移 |
| `missed` | 默认 skip：执行最近一个已到期时间点，跳过更早时段；catch-up：每次事务补一个到期轮次 |
| `maxRuns` | 正安全整数，限制实际创建的轮次，不计被 skip 的时段 |
| `task` | 每轮子 Task 的规格；不能指定已有 parent、spawnKey 或 retryOfTaskId |

每轮使用 `scheduled:<UTC毫秒>` 作为持久 spawnKey，子 Task 登记与调度状态推进同事务，因此恢复和多 worker 竞争不会重复创建同一轮 Task。子任务的外部副作用仍遵守 Effect 幂等和核对契约。

错过时间点的计算使用已持久化 timer-fired 的 firedAt，reducer 不读取当前时间。旧 timer 事件无 firedAt 时按原到期时间解释。catch-up 一步只创建一个子任务，避免单笔事务无界扩大；长期停机后的大量补跑应配合 maxRuns 和执行并发限额。

暂停保留下一次绝对时间；恢复后按 missed 处理。进程重建后 recover 原 Session，显式暂停仍需 resume。单次或达到 maxRuns 时，调度 Task 等待最后一轮子 Task 退出后结束，output 含 runs 和 lastExit；该边界不代表所有更早的重叠轮次均已完成或物理资源均已释放。取消调度使用现有任务树取消协议。

轮次可以重叠，并发受普通 Task / Effect 配置限制。本接口提供单次与固定间隔机制，不解释 cron、时区夏令时或具体业务日历；这些策略由上层计算绝对时间或组合 Program。需要精确跨进程时间语义时，宿主须维持一致的时钟基准。
