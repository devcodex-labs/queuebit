# Quick Start: Process a Receipt Snapshot

<span class="manual-label">Quick start · your first real batch, from fixed input to verified result</span>

## 1. Install Queuebit and prepare Redis

Use Node.js22+ and reachable Redis7.2+ with noeviction. This source tree is unreleased; build its root package and install the tarball into your application:

```bash
# Source checkout
npm ci
npm pack
# Your application (use the filename printed by pack)
npm install /absolute/path/to/queuebit-0.0.5.tgz
```

The historical0.0.5 filename is not a newly selected v2 release.

## 2. Prepare the business snapshot

Create a durable snapshot of the receipt records before starting. It must freeze both membership and payload; a timestamp over mutable records is not enough. Copy [receipt-task.ts](https://github.com/devcodex-labs/queuebit/blob/main/examples/batch-v2/receipt-task.ts) into your application.

Implement its two small adapter contracts: a repository that reads strictly increasing `id > afterId` pages of at most100 records, and a sink whose `putOnce` atomically combines a durable uniqueness key with the business write. Its `completeOnce` likewise deduplicates completion effects. These adapters are your real database/provider integration, not code supplied by Queuebit.

## 3. Register the task once in your service

```ts
import { createBatchQueue } from 'queuebit';
import { defineReceiptTask } from './receipt-task.js';
import { receiptRepository, receiptSink } from './your-application-adapters.js';

const queue = createBatchQueue({
  namespace: 'receipt-service',
  redis: { mode: 'direct', host: '127.0.0.1', port: 6379 },
});
const task = defineReceiptTask(queue, receiptRepository, receiptSink);
await queue.ready();
```

The adapter module is the implementation you provide in step2. Import/construct/define do not connect; `ready()` starts the participant. Create it once, not per HTTP request.

## 4. Admit the snapshot

```ts
const { runId, created } = await task.start({
  query: { snapshotId: 'snapshot-2026-09' },
  idempotencyKey: 'receipt-snapshot-2026-09',
});
console.log({ runId, created });
```

Use the actual snapshot ID produced by your business service. Derive authorization and tenant scope on the server, not from an untrusted request alone. start means accepted, not finished; keep this consumer service alive.

## 5. Confirm the result

```ts
const current = await task.get(runId);
// Repeat in your monitoring path until terminal:
// current?.status === 'success'
// current?.callbacks.delivered === 1 for this success-callback example

// From the application shutdown hook, not immediately after admission:
const closed = await queue.close();
console.log(closed.timedOut, closed.remainingExecutions, closed.remainingCallbacks);
```

The task reads100 rows, writes them with stable business keys, returns `ctx.next({afterId})`, and ends when a later page is empty. Execution and callbacks are at least once. The example qualification uses203 rows and a forced post-write failure to verify safe page repetition; its memory test doubles do not replace your durable adapters.

If readiness fails, check Redis/address/protocol. If work does not advance, check a matching consumer and the Run error. If an external reply is lost, retry with the same business key rather than assuming zero effects.

## Next

[Understand the paging workflow](batch-runs.md) · [Choose Redis settings](configuration-recipes.md) · [Recover safely](failure-runbooks.md)
