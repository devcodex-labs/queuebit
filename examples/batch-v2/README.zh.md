# 收据快照示例 — 未发布 Batch v2

[English](README.md)

`receipt-task.ts` 是应用代码，不是假装可用的数据库驱动。注入持久、不可变快照仓储：固定成员集合、严格递增的正安全整数 ID 和冻结 payload。在该快照中每页按 `id > afterId ORDER BY id LIMIT 100` 读取。单独的时间戳或最大 ID 不能冻结可变的筛选条件/payload；需物化快照或等价的持久数据库合同。

`ReceiptSink.putOnce` 需将唯一键约束和外部写放进同一持久事务，或使用外部服务的持久幂等接口。键为 receipt/快照/记录的 JSON 元组；重试、响应丢失、同一快照的多个 Run 都不会重复发收据。`completeOnce` 使用 Event ID；死信 replay 可能再次调用它。

在源码目录执行 `npm ci`、`npm pack`，然后在应用中用 `npm install /absolute/path/to/queuebit-0.0.5.tgz` 安装实际根包（以 pack 输出的文件名为准）。保留的编号是历史元数据，不表示 v2 已发布。将 `receipt-task.ts` 复制进应用，从 `queuebit` 导入 `createBatchQueue`，在 `ready()` 前调用 `defineReceiptTask(queue, repository, sink)`，然后 `task.start({query:{snapshotId}})`。消费者保持运行直到关闭；start 不等待任务完成。producer 模式可只登记同一 contract、不带 handlers；部署方负责匹配消费者及一致的 namespace protocol。

不能吞掉仓储/发送失败后返回成功：让 throw/reject 按有限 Batch 预算重试，继续使用上次提交的 cursor。部分外部写成功而 ctx.next 尚未提交时崩溃，会重跑该页。将 AbortSignal 传给适配器；abort 是协作取消，不回滚外部事务。handler 必须最终退出，超时/close 后未退出的 Promise 仍占物理槽。

自动化示例仅用内存测试替身验证适配器合同，并实际连接 Redis、消费 fresh 安装包；注入一次外部写成功后的失败，校验安全重放和唯一完成通知。同一 TypeScript 文件在无仓库路径别名的目录编译。这不是生产存储或持久性认证。
