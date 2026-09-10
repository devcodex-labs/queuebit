# 执行一个有限后台任务

<span class="manual-label">任务指南 · 小任务与分页工作使用同一接收合同</span>

<span id="sc-bounded-task"></span>

## 先确定业务边界

多记录处理使用[完整快照示例](quick-start.md)。已经有明确上限的单个外部操作，也可以使用同一 Task 一页结束，不存在另一套直接 job API。

```ts
const task = queue.define<{ operationId: string }, null>(
  { name: 'bounded-operation', version: '1', events: [] },
  { async execute(ctx) {
      await businessService.applyOnce(ctx.query.operationId, ctx.signal);
      return ctx.end();
  } },
);
await queue.ready();
const started = await task.start({
  query: { operationId },
  idempotencyKey: operationId,
});
const current = await task.get(started.runId);
```

这是 API 微型示例：queue 为已构造 Queue，operationId 由服务端确定，businessService.applyOnce 必须实现持久幂等，不能用进程缓存替代。完整收据指南给出真实分页路径。

## 重试与时间边界

<span id="sc-retry"></span>

执行默认3次尝试、30秒协作超时和 full-jitter 指数退避；需要时在不可变 Task 合同中设置 policy。没有首次延迟或 cron 选项。throw/reject 从持久状态重试，不能捕获外部写失败后返回成功。

## 处理函数需要什么上下文

ctx.query 不可变，ctx.state 为可选持久状态，另有 runId/batchId/page/attempt/signal。下一页返回当前尝试的 ctx.next(newState)，结束返回 ctx.end。重复、伪造或跨尝试使用控制值会导致合同失败。

## 取消和观察

task.cancel(runId) 返回 found 区分结果。取消阻止后续可执行工作，不能回滚既有外部效果；运维控制额外带 expectedRevision/reason/commandId。接收成功不等于完成，消费者应持续运行，并分别观察执行与回调结果。

## 下一步

[业务幂等](idempotency-patterns.md) · [回调恢复](failure-runbooks.md)
