# Use It First, Then Learn Queuebit

<span class="manual-label">Quick start · a small set of concepts for business tasks</span>

## The five terms you need first

| Term | What it means for your application |
|---|---|
| Queue | A configured process participant in one Redis namespace |
| Task | A versioned contract and, in consumers, handlers |
| Run | One admitted immutable query and its durable progress |
| Batch | One bounded execution attempt that advances a page |
| Event | An immutable settlement notification delivered with its own retries |

## Start with a task

Follow [the receipt snapshot guide](quick-start.md). Prepare a fixed dataset, read one small page, perform business-idempotent writes, and commit the cursor with `ctx.next`. Empty input ends with `ctx.end`.

## Learn when needed

Choose [runtime modes](distributed-workers.md) when separating request admission from background work. Use [configuration](configuration-recipes.md) when moving beyond local Redis. Read [operator controls](operations.md) to pause, inspect or replay delivery safely.

## Common misunderstandings

`start` does not wait for completion. A successful Run can still have pending or failed callbacks. A lease protects Redis state transitions, not external side effects. A timeout or cancel signal cannot forcibly interrupt arbitrary JavaScript. Query deduplication is retained identity, not a permanent business audit.

## Next

[First batch](quick-start.md) · [Page business records](batch-runs.md) · [Complete contract](batch-v2.md)
