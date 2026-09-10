# Business idempotency: one result after repeated execution

<span class="manual-label">Task guide · durable protection for external side effects</span>

<span id="sc-idempotency"></span>

## Remember this first

Queuebit execution and callbacks are at least once. A crash or lost response can repeat work that already succeeded externally. Library Run identity retention is not a permanent business deduplication system.

## How to choose the key

Use a durable business operation identity. For receipts, `JSON.stringify(['receipt', snapshotId, row.id])` avoids delimiter collisions and stays stable across page retries and replacement Runs. Do not use a random attempt ID. For callback effects, Event ID is stable across replay; use a business key instead if multiple Events intentionally represent the same business operation.

## Pattern 1: provider idempotency

Send the same key to the provider on every retry and query its operation status after an uncertain response. Ensure the provider's idempotency retention covers your recovery window.

## Pattern 2: database transaction

Enforce a unique operation key and apply the business state change in one durable transaction. A separate process-memory Set, or a separate uncommitted “seen” check before writing, is not sufficient.

## Pattern 3: transactional outbox

Write the business change and outbox row together, then deliver with the destination's durable idempotency boundary. An outbox alone does not guarantee a non-idempotent remote side effect cannot repeat.

## Queue admission identity

`task.start({ query, idempotencyKey })` returns the retained Run when the task/key and canonical query/effective policy match. Changed input produces IDEMPOTENCY_CONFLICT. The key is at most256 UTF-8 bytes, valid Unicode, with empty distinct from omitted. It is not trimmed or normalized, and stops deduplicating after the associated identity is collected.

## Acceptance drill

Execute a page, force a failure after its first external write succeeds, then retry from the persisted cursor. Verify one durable business effect per row despite repeated calls. The receipt example's 203-row test demonstrates this adapter contract with test doubles; your production database/provider needs its own durability drill.

## Next

[Receipt pages](batch-runs.md) · [Unknown outcome recovery](failure-runbooks.md)
