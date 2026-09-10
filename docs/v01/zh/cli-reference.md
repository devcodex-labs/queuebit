# 使用运维 SDK，不提供 CLI

<span class="manual-label">参考 · 应用自行托管操作工具</span>

BatchQueue 不安装命令行程序。保留此 URL 是为了让旧链接仍能找到当前说明，不支持旧命令及退出码合同。

## 运维查询

在经过认证的应用运维工具中使用进程内 `queue.operator`：

```ts
const metadata = await queue.operator.runs.getMetadata(runId);
const page = await queue.operator.runs.list({ limit: 50 });
const deadLetters = await queue.operator.deadLetters.list({ limit: 50 });
const health = await queue.operator.health.snapshot();
const capacity = await queue.operator.capacity.snapshot();
```

调用者提供 runId 和 ready 的 Queue。metadata 故意不带业务 payload；需要业务数据时使用匹配 Task 的 get。

## 控制 Run

pause/resume/cancel 接收 `{ runId, expectedRevision, reason, commandId }`。replay 接收 `{ eventId, expectedRevision, reason, commandId }`，使用 Event revision。跨重试持久保存 commandId，不在 OUTCOME_UNKNOWN 后换新 ID。结果为 applied/noop/not_found。

## 托管与关闭

应用负责身份认证、授权、HTTP/命令行展示、信号和退出码。关闭时 `await queue.close()`，检查 timedOut 和剩余任务数量。没有远程 drain 命令或隐藏 worker 守护进程。

## 下一步

[运维流程](operations.md) · [故障恢复](failure-runbooks.md) · [API 快查](target-api.md)
