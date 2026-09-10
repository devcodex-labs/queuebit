# 批量处理数据库记录

<span class="manual-label">任务指南 · 固定快照、有限分页与安全重试</span>

<span id="sc-snapshot"></span>

收据批量发送、导出和受控回填适合走这条路径。应用先创建持久快照，冻结成员和 payload；可变记录上的时间戳不是快照。

## 完整示例路径

将 [receipt-task.ts](https://github.com/devcodex-labs/queuebit/blob/main/examples/batch-v2/receipt-task.ts) 复制到应用，实现它的 `ReceiptRepository` 与 `ReceiptSink`。[中文示例说明](https://github.com/devcodex-labs/queuebit/blob/main/examples/batch-v2/README.zh.md) 给出适配器合同。

## 从数据库批量处理到最终完成

<div class="qb-canonical-flow" role="img" aria-label="冻结快照、接收任务、读取并写入一页、提交游标、投递回调">
  <div class="qb-flow-stage">冻结快照</div>
  <div class="qb-flow-arrow" aria-hidden="true">→</div>
  <div class="qb-flow-stage">接收 Run</div>
  <div class="qb-flow-arrow" aria-hidden="true">→</div>
  <div class="qb-flow-stage">读取并写入一页</div>
  <div class="qb-flow-arrow" aria-hidden="true">→</div>
  <div class="qb-flow-stage">提交游标</div>
  <div class="qb-flow-arrow" aria-hidden="true">→</div>
  <div class="qb-flow-stage qb-flow-stage--final">投递回调</div>
</div>

回调是持久但独立的阶段：回调尚未完成，不会阻止下一正常页执行。

## 1. 确定有限处理范围

`query.snapshotId` 指向已经创建的不可变数据集。每页读取 `id > afterId ORDER BY id LIMIT 100`，ID 为严格递增正安全整数。100 是应用示例的读取上限，不是库的数据库配置项。

## 2. 安全写入一页

逐条调用 `sink.putOnce(JSON.stringify(['receipt', snapshotId, row.id]), row.payload, signal)`。唯一键和业务写入必须处于同一个持久事务，或使用服务提供方的幂等能力。将 `AbortSignal` 传给仓储和 sink。

## 3. 提交检查点

<span id="sc-checkpoint"></span>

本页全部写入成功后返回 `ctx.next({ afterId: lastId })`；下一次读取为空时返回 `ctx.end()`。控制值只能来自当前尝试，不可伪造或重复返回。每个 Run 串行推进页，不同 Run 可以并发；不提供 paced 调度或双正常游标。

## 4. 投递每批和最终结果

<span id="sc-callbacks"></span>

ready 前声明 `batchSettled`、`success` 或 `failure` 并提供匹配 handler。Event 保存结算时不可变快照，投递使用独立重试预算。同 Run 正常回调按创建顺序投递，进入死信会放行正常序列；回调失败不重做执行，也不把成功 Run 改成失败。按 `eventId` 持久去重。

## 5. 启动、查询与控制

`await queue.ready()` 后调用 `task.start({ query: { snapshotId }, idempotencyKey })`，返回 `{ runId, created }`，不是完成结果。用 `task.get(runId)` 或运维元数据查看进度；[运维指南](operations.md) 提供带 revision 的 pause/resume/cancel。

## 6. 选择正确恢复动作

外部写成功而结算前崩溃，会从上次提交状态重跑该页。不能吞掉适配器错误后返回成功。真实示例验证203条记录，注入一次写后失败，观察204次写调用、5次分页和1次完成通知；其中内存适配器仅为测试替身，不是生产存储。

## 7. 保持语义边界

执行和回调均为至少一次。Redis 异步复制切换可能丢已确认写。队列去重只在 Run 身份保留时有效，不是永久业务审计；取消无法撤回已发出的外部写。

## 下一步

[业务幂等](idempotency-patterns.md) · [故障恢复](failure-runbooks.md) · [完整合同](batch-v2.md)
