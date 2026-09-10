# Run a Bounded Background Task

<span class="manual-label">Task guide · one admission contract for small and paged work</span>

<span id="sc-bounded-task"></span>

## Start with the business boundary

For many records, use the [snapshot example](quick-start.md). For one already-bounded external operation, the same Task contract can finish in one page. There is no separate direct-job API.

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

This is an API micro-example: `queue` is your constructed Queue, `operationId` is server-derived, and `businessService.applyOnce` must provide durable idempotency. Do not substitute a process-local cache. The full receipt guide supplies the real paging workflow.

## Delay and retry

<span id="sc-retry"></span>

Execution defaults to3 attempts with30-second cooperative timeout and full-jitter exponential backoff. Set `policy` in the immutable Task contract when its business needs differ. There is no initial-delay or cron option. Throw/reject to retry from committed state; never catch a failed external write and return success.

## When a handler needs more context

Use immutable `ctx.query`, optional persisted `ctx.state`, runId/batchId/page/attempt and signal. For a next page return its own `ctx.next(newState)`; to end return `ctx.end()`. Calling a control twice, fabricating it or returning a control from another attempt is a handler-contract failure.

## Cancel or inspect work

`task.cancel(runId)` returns a found-discriminated result. Cancellation prevents future eligible work but cannot roll back existing external effects. Operator controls add expectedRevision/reason/commandId for incident tooling. A successful admission only means the Run was accepted; keep consumers alive and inspect execution and callback outcomes separately.

## Next

[Business idempotency](idempotency-patterns.md) · [Callback recovery](failure-runbooks.md)
