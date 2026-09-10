# API 快查

<span class="manual-label">参考 · 公开方法、类型和返回边界</span>

## 先按任务找 API

| 任务 | API / 结果 |
|---|---|
| 配置参与者 | createBatchQueue(options): BatchQueue |
| ready前注册 | `queue.define<Q,S>(contract, handlers?): BatchTask<Q,S>` |
| 启停自有生命周期 | `queue.ready(): Promise<void>`；`queue.close(): Promise<CloseResult>` |
| 接收不可变query | task.start({query,idempotencyKey?}) → {runId,created} |
| 读取保留Run | `task.get(runId) → RunInfo<Q,S>` 或null |
| 直接取消 | task.cancel(runId) → found:false或found:true结果 |
| 检查/控制 | queue.operator.runs.getMetadata/list/pause/resume/cancel |
| 检查/重放投递 | queue.operator.deadLetters.get/list/replay |
| 观测 | operator.health/capacity.snapshot()；本地metrics.snapshot() |

## 公开输入和返回类型

以下仅为类型/形状示例，不是生产handler或启动工作脚本：

```ts
import {
  createBatchQueue, QueuebitError,
  type BatchQueue, type BatchTask, type RunControlInput, type EventReplayInput,
} from 'queuebit';

const queue: BatchQueue = createBatchQueue({
  namespace: 'api-reference',
  redis: { mode: 'direct', host: '127.0.0.1', port: 6379 },
});
const task: BatchTask<{ snapshotId: string }, { afterId: number }> = queue.define(
  { name: 'reference-only', version: '1', events: [] },
  { execute(ctx) { return ctx.end(); } },
);
// Type examples only. Real identities/revisions come from current operator metadata.
const control: RunControlInput = {
  runId: 'a'.repeat(32), expectedRevision: 1,
  reason: 'Operator decision', commandId: 'persisted-command-identity',
};
const replay: EventReplayInput = {
  eventId: 'a'.repeat(32) + ':1:success', expectedRevision: 1,
  reason: 'Provider recovered', commandId: 'persisted-replay-identity',
};
void [QueuebitError, task, control, replay];
// This type example performs no ready/start/replay operation.
```

BatchQueueOptions包含namespace、redis、runtime、defaults、protocol和telemetry。TaskContract绑定name/version/events/有效执行策略，不绑定handler源码。配置默认和边界见[字段字典](cli-and-config.md)。

## 执行上下文

`ExecuteContext<Q,S>` 包含深只读query/state、runId/batchId/page/attempt及AbortSignal。state初始null。返回当前尝试的不透明ctx.next(state?)或ctx.end()；非法、跨尝试或重复控制值会导致合同失败。

## 回调上下文

`CallbackContext<Q,S>` 包含原始不可变query/state/error/timestamp、eventId/kind/runId/batchId/sequence、deliveryAttempt/replayGeneration/lateReplay和signal。返回值忽略；consumer/all的声明事件必须有匹配handler。

## 运维返回

Run控制用RunControlInput；replay用EventReplayInput和Event revision。结果为applied/noop/not_found，not_found只有判别值和ID，没有虚构revision。列表包含items、nextCursor、consistency为live；死信列表按首次死信排序，metadata不带业务payload。

## 公开错误结构

QueuebitError含code/operation/retryable/outcomeKnown及可选runId/eventId/commandId。OUTCOME_UNKNOWN不是零写。返回记录为只读合同快照，修改它们不能控制任务。见[状态与错误](failure-modes.md)。

## 导入边界

运行时根导出只有createBatchQueue和QueuebitError，公开类型显式导出；内部storage/runtime/domain类和旧子路径不导出。ESM/CJS各有匹配声明映射。

## 下一步

[完整合同](batch-v2.md) · [第一个真实批处理](quick-start.md)
