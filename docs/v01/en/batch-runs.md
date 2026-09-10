# Process Many Database Records

<span class="manual-label">Task guide · a finite snapshot, bounded pages and safe retries</span>

<span id="sc-snapshot"></span>

Choose this path for receipt campaigns, exports and controlled backfills. Your application first materializes a snapshot that freezes membership and payload. A timestamp over mutable records is not a snapshot.

## Full Example Path

Copy [receipt-task.ts](https://github.com/devcodex-labs/queuebit/blob/main/examples/batch-v2/receipt-task.ts) into your application and implement its `ReceiptRepository` and `ReceiptSink`. The [example guide](https://github.com/devcodex-labs/queuebit/blob/main/examples/batch-v2/README.md) explains both contracts.

## Database batch to final completion

<div class="qb-canonical-flow" role="img" aria-label="Freeze a snapshot, admit a Run, read and write one page, commit its cursor, deliver callbacks">
  <div class="qb-flow-stage">Freeze snapshot</div>
  <div class="qb-flow-arrow" aria-hidden="true">→</div>
  <div class="qb-flow-stage">Admit Run</div>
  <div class="qb-flow-arrow" aria-hidden="true">→</div>
  <div class="qb-flow-stage">Read + write page</div>
  <div class="qb-flow-arrow" aria-hidden="true">→</div>
  <div class="qb-flow-stage">Commit cursor</div>
  <div class="qb-flow-arrow" aria-hidden="true">→</div>
  <div class="qb-flow-stage qb-flow-stage--final">Deliver callbacks</div>
</div>

The last stage is durable but independent: a pending callback is not a barrier preventing the next normal page from executing.

## 1. Define a finite processing range

`query.snapshotId` identifies the already-created immutable dataset. Read `id > afterId ORDER BY id LIMIT 100`; IDs must be strictly increasing positive safe integers. The limit is enforced by the application example, not a library database setting.

## 2. Write each page safely

For each row, call `sink.putOnce(JSON.stringify(['receipt', snapshotId, row.id]), row.payload, signal)`. Implement uniqueness and the write in one durable transaction, or use a provider's idempotency facility. Pass `AbortSignal` through to the repository and sink.

## 3. Commit the checkpoint

<span id="sc-checkpoint"></span>

Return `ctx.next({ afterId: lastId })` after all page writes succeed. When the next read is empty, return `ctx.end()`. These controls belong to the current attempt; do not fabricate them or return more than one. One Run executes pages serially; different Runs can use concurrent slots. There is no paced-dispatch mode or dual normal cursor.

## 4. Deliver batch and final results

<span id="sc-callbacks"></span>

Declare `batchSettled`, `success` or `failure` and matching handlers before ready. Events capture immutable settlement data and have their own delivery retries. Normal callbacks preserve creation order within a Run; a dead letter releases the normal sequence. Callback failure does not redo execution or change a successful Run into failed. Use `eventId` for durable callback deduplication.

## 5. Start and control a run

After `await queue.ready()`, call `task.start({ query: { snapshotId }, idempotencyKey })`. Its result is `{ runId, created }`, not completion. Poll `task.get(runId)` or inspect operator metadata. See [operations](operations.md) for revision-checked pause/resume/cancel.

## 6. Choose the correct recovery

A crash after external writes but before settlement repeats the page from the last committed state. Do not swallow adapter errors and return success. The real example test processes 203 rows, injects one post-write failure and observes 204 write calls, five page reads and one completion; the in-memory adapters used there are test doubles, not production storage.

## 7. Summary invariants

Execution and callbacks are at least once. Redis replication may lose acknowledged writes during failover. Queue deduplication lasts only while the Run identity is retained; it is not a permanent business audit. A cancelled Run cannot undo external writes already issued.

## Next

[Prevent duplicates](idempotency-patterns.md) · [Recover failures](failure-runbooks.md) · [Complete contract](batch-v2.md)
