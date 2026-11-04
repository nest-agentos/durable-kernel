# Session 文本归档

`harness.archives` 提供 `exportSession`、`validateArchive`、`importSession`。它独立于用于 UI 重同步的 snapshot，也独立于对现有存储执行恢复的 recover。

## 格式与一致性

v1 为 UTF-8 JSONL：manifest、每条 `{path,key,value}` 记录、含记录数量和 SHA-256 的 end。manifest 包含格式版本、Session、记录路径、asOfSequence、archiveId 和外部依赖声明。校验拒绝半条尾记录、尾部缺失、校验和不符、重复字段记录、非法路径、缺失 Task 关系及不支持的 layout。SHA-256 检测完整性，不提供来源认证。

默认上限 16 MiB / 100,000 条记录，可通过 maxBytes / maxRecords 调整。导出在一个有界事务中取得全部权威记录快照，之后才向慢消费者逐行输出，不长时间持有事务。导入先有界解码和校验，再提交一次原子事务。大于上限的数据需要宿主明确调整容量，v1 不声称无界流式导入。

归档包括 Session、Task、历史、attempt、输入与幂等回执、wait、Effect、共享值、上下文提交、图、索引、消息和 Session 资源记录。内核作用域资源、外部工作目录、附件正文和任意业务 input 中引用的外部数据不在这份记录快照中。

非记录文件会列入 externalPaths；resources、消息回执或显式 dependencies 也会使 externalBindings 为 true。宿主须声明并核对 opaque input 所依赖的数据。文件清单只用于绑定核对，不代表正文或文件快照已被嵌入。

## 导出与校验

```ts
for await (const line of harness.archives.exportSession(session.id, {purpose: 'recover'})) {
    await destination.write(line);
}
const validation = await harness.archives.validateArchive(source());
```

inspect 可观察运行中 Session；recover 要求 Session 已确认暂停或关闭，Task 没有当前执行 claim、leased Effect 或未确认清理。权威记录与 asOfSequence 同事务读取。宿主仍须单独核对物理资源、外部文件以及源端退出；导出本身不会撤销源端执行权。

## restore

```ts
const report = await target.archives.importSession(source(), {
    mode: 'restore', requestId: 'migration-2026-10-21',
    storage: {kind: 'local', locator: '/sessions/restored'},
    assertExclusive: manifest => ownership.assertRetired(manifest.session.id),
    validateBindings: validation => bindings.validate(validation),
});
// Inspect and recover the paused import before explicitly resuming it.
await target.recoverSession(report.sessionId);
const restored = await target.openSession(report.sessionId);
await restored.resume();
```

restore 保留 Session / Task / Effect / 幂等身份，只接受 recover 归档。assertExclusive 必需：宿主应在整个导入及激活期间持有所有权，并确保旧执行者不能继续工作；它不是由库内部取得的跨主机锁。存在外部依赖时 validateBindings 也必需，负责文件、资源 authority、另一 Session 已前进的消息状态等核对。精确 Program / Effect 版本必须已注册。

目标必须为空；不能覆盖已存在的 Session、Task 身份或 Session 记录。catalog 与目标使用同一 IFileSystem，记录、catalog 和导入回执同事务提交。稳定 requestId 同内容重试返回原报告，异内容冲突；事务提交前崩溃不发布 Session，提交后回复丢失可重放回执。导入始终处于暂停状态，不登记执行 worker；旧历史租约不成为新执行权。保留绝对 timer / deadline。

## fork

fork 为每个原始根 Task 创建新 ID，重建根 Task 之间的依赖，保留 Program / input 规格；所有新 Task 为 deferred。旧后代不复制，根 Program 重新执行时按自己的逻辑创建新任务树，避免重复保留旧子任务。根 Task 若依赖旧后代则拒绝通用 fork，须由应用层明确重映射。它是基于原输入的新执行，不复制 reducer checkpoint、Effect、租约、消息、资源、授权或未消费输入。任意业务 input 内部的 ID 不自动改写。宿主重新绑定资源并明确 start，Session 仍需 resume。

fork 可使用 inspect 归档。存在声明或检测到的外部依赖仍需宿主验证；无需继承源 Session 的执行所有权。报告中的 taskIds 给出旧根 Task → 新根 Task 映射；新的 session.created / task.created 与导入记录同事务写入。需要完整业务历史分支时，应由会话层基于自己的历史模型实现。

## 取消与诊断

signal 可中止输入解码、目录清单及记录准备；导入事务内观察到取消时整体回滚。提交成功是最终接纳边界，调用者不能因随后失去响应认定未提交，应使用相同 requestId 核对。

诊断区分解码、存储绑定、文件清单、记录快照、所有权、外部依赖、事务与完成阶段，只记录操作身份、结果、耗时和安全错误码，不输出归档正文或完整业务参数。
