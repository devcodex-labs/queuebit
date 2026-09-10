# API quick lookup

<span class="manual-label">Reference · public methods, types and return boundaries</span>

## Find the API by task

| Task | API / result |
|---|---|
| Configure a participant | `createBatchQueue(options): BatchQueue` |
| Register before ready | `queue.define<Q,S>(contract, handlers?): BatchTask<Q,S>` |
| Start/stop owned lifecycle | `queue.ready(): Promise<void>`, `queue.close(): Promise<CloseResult>` |
| Admit immutable query | `task.start({query,idempotencyKey?}) → {runId,created}` |
| Read retained Run | `task.get(runId) → RunInfo<Q,S> or null` |
| Cancel directly | `task.cancel(runId) → found:false or found:true result` |
| Inspect/control | `queue.operator.runs.getMetadata/list/pause/resume/cancel` |
| Inspect/replay delivery | `queue.operator.deadLetters.get/list/replay` |
| Observe | `operator.health/capacity.snapshot()`; local `metrics.snapshot()` |

## Public input and return types

The following is a type/shape example, not a production handler or a script that starts work:

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

`BatchQueueOptions` contains namespace, redis, runtime, defaults, protocol and telemetry. `TaskContract` binds name/version/events/effective execution policy; handler source is not its identity. See [configuration](cli-and-config.md) for defaults and bounds.

## Execution context

`ExecuteContext<Q,S>` contains deeply readonly query/state, runId/batchId/page/attempt and AbortSignal. State starts null. Return the current attempt's opaque `ctx.next(state?)` or `ctx.end()`. Invalid, foreign or repeated controls fail the handler contract.

## Callback context

`CallbackContext<Q,S>` carries immutable original query/state/error/timestamp, eventId/kind/runId/batchId/sequence, deliveryAttempt/replayGeneration/lateReplay and signal. Callback return values are ignored. Declared event kinds require matching handlers in consumer/all mode.

## Operator results

Run controls take RunControlInput; replay takes EventReplayInput and uses the Event revision. Result variants are applied/noop/not_found. A not_found result has only its discriminator and ID, no synthetic revision. List results contain items, nextCursor and `consistency: 'live'`; dead-letter pagination uses first-dead order. Metadata omits business payload.

## Public error shape

`QueuebitError` exposes code/operation/retryable/outcomeKnown and optional runId/eventId/commandId. OUTCOME_UNKNOWN is not zero writes. All return records are readonly contract snapshots; do not mutate them to control work. See [states and errors](failure-modes.md).

## Import boundary

Runtime exports are exactly createBatchQueue and QueuebitError. Public types are explicit root exports; internal storage/runtime/domain classes and old subpaths are not exported. ESM and CJS have distinct declaration mappings.

## Next

[Complete contract](batch-v2.md) · [First real batch](quick-start.md)
