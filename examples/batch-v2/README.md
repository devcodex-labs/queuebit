# Receipt snapshot example — unreleased Batch v2

[中文](README.zh.md)

`receipt-task.ts` is application code, not a fake database driver. Supply a repository backed by a durable immutable snapshot: fixed membership, strictly increasing positive safe-integer IDs and frozen payload. Each query reads `id > afterId ORDER BY id LIMIT 100` within that snapshot. A timestamp or high-water ID alone does not freeze mutable eligibility or payload; materialize the snapshot or use an equivalent durable database contract.

Implement `ReceiptSink.putOnce` with a durable unique key and the side effect in the same transaction, or use the provider's durable idempotency facility. The key is a JSON tuple of receipt/snapshot/record, so retries, lost replies and multiple Runs for one snapshot cannot duplicate the receipt. `completeOnce` uses Event ID; callback replay may invoke it again.

Run `npm ci` and `npm pack` in the source checkout, then install the resulting root tarball in your application with `npm install /absolute/path/to/queuebit-0.0.5.tgz` (use the filename printed by pack). The retained number is historical metadata, not a v2 release. Copy `receipt-task.ts` into your application, import `createBatchQueue` from `queuebit`, call `defineReceiptTask(queue, repository, sink)` before `ready()`, then `task.start({query:{snapshotId}})`. Keep the consumer process running until shutdown; `start()` does not wait for completion. The same contract can be registered without handlers in producer mode. Deployment must provide matching consumers and keep identical namespace protocol configuration.

Do not catch repository/sink failures and return success: throw/reject so the bounded Batch attempt can retry from the previous committed cursor. A crash after some external writes but before `ctx.next()` repeats that page. Pass `AbortSignal` to adapters; abort is cooperative, not transaction rollback. Handlers must eventually exit; unresolved handlers retain physical slots after timeout/close.

The automated example uses an in-memory test double only to exercise this adapter contract against real Redis and a fresh-installed package. It injects a failure after one applied external write, verifies safe replay and one completion notification, and compiles the same TypeScript file without repository aliases. This is not production storage or a durability certification.
