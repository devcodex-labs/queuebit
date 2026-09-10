# 上线后怎么查问题

<span class="manual-label">运行指南 · 从现象开始，明确控制意图</span>

<span id="sc-diagnostics"></span>

## 先判断是哪类问题

区分执行积压、回调积压、存储未就绪和本地 handler 饱和。重做工作前，除 Queuebit 状态外还要查看业务审计。

## 各视图分别看什么

| 视图 | 用途 |
|---|---|
| task.get(runId) | 保留的 query/state/error 及执行、回调进度 |
| operator.runs.getMetadata(runId) | 不带业务 payload 的有限诊断 |
| operator.runs.list({limit,cursor}) | 有上限的 live Run 列表 |
| operator.deadLetters.get/list | 保留的死信与 replay 元数据 |
| operator.health.snapshot() | 本地生命周期和有限Redis/成员/积压观测 |
| operator.capacity.snapshot() | 共享逻辑计费、计数与接收压力 |
| operator.metrics.snapshot() | 本地计数及遥测丢弃，不是全局总量 |

列表默认50、最多200条。cursor 固定15分钟到期，绑定筛选条件和首屏序号上界。结果是 live 而非快照：短页仍可带 nextCursor；应直到 nextCursor 为 null 才结束。

## 控制 Run

<span id="sc-control"></span>

读取最新元数据、认证操作者并持久保存命令身份。pause/resume/cancel 输入 `{ runId, expectedRevision, reason, commandId }`。pause 可能等待在途页，cancel 无法撤回外部写。结果为 applied/noop/not_found；not_found 没有虚构 revision。

REVISION_CONFLICT 后先读状态再重新决定；OUTCOME_UNKNOWN 后核对效果或重试同一完整命令，不能换新 commandId。父 Run 共享最多32条、24小时命令回执，完整编码回执≤2KiB，不是永久命令去重。

## 容量和背压

逻辑容量含预付结算/Event 依赖，不等于 Redis RSS。达到高水位时停止新接收，降到低水位再恢复；合法在途任务、控制和有限维护仍继续排空。先查未完成回调、保留 Run、匹配消费者和卡住的物理槽；不要手工改计数器或重建索引。

## 指标与告警

关联 namespace、任务/版本、runId/eventId、commandId、operation 和 outcomeKnown。telemetry 有限且可丢弃，其失败不会回滚业务。按自己的 SLO 和压测确定阈值，不使用通用固定延迟门槛。

## 优雅退出

先停止应用接收请求，await queue.close，检查 timedOut/remainingExecutions/remainingCallbacks。忽略 abort 的 handler 可能在 close 后仍存活；是否终止进程由服务管理者决定，Queuebit 不杀进程。

## 下一步

[事故恢复](failure-runbooks.md) · [配置](configuration-recipes.md)
