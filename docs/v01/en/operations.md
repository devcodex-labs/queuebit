# Check Problems After Launch

<span class="manual-label">Operations · start from symptoms, then make an explicit control decision</span>

<span id="sc-diagnostics"></span>

## First decide what kind of problem it is

Separate execution backlog, callback backlog, unready storage and saturated local handlers. Inspect the business audit as well as Queuebit state before repeating work.

## What each view answers

| View | Purpose |
|---|---|
| `task.get(runId)` | Full retained query/state/error and execution/callback progress |
| `operator.runs.getMetadata(runId)` | Bounded diagnostic fields without business payload |
| `operator.runs.list({limit,cursor})` | Live bounded Run listing |
| `operator.deadLetters.get/list` | Retained dead-letter/replay metadata |
| `operator.health.snapshot()` | Local lifecycle plus bounded Redis/member/backlog observations |
| `operator.capacity.snapshot()` | Shared logical charges, counts and admission pressure |
| `operator.metrics.snapshot()` | Local counters and telemetry loss, not global totals |

List pages default to50/max200 items. Cursors have fixed15-minute expiry and bind filters and the first page's upper sequence. Pages are live, not snapshots: changes can shorten a page, and a short page can still have nextCursor. Do not stop until nextCursor is null.

## Control a Run

<span id="sc-control"></span>

Read fresh metadata, authenticate the operator and persist a command identity. Send `{ runId, expectedRevision, reason, commandId }` to pause/resume/cancel. Pause can wait for an in-flight page; cancel cannot undo external writes. Results are applied/noop/not_found; not_found has no fabricated revision.

On REVISION_CONFLICT inspect current state before deciding a new command. On OUTCOME_UNKNOWN retry the same full command identity or reconcile its effect; do not convert an uncertain result into a new command. A shared parent-Run ring retains at most32 command receipts for24 hours, with an encoded receipt limit of2KiB. It is not permanent command deduplication.

## Capacity and backpressure

Logical capacity prepays settlement/Event dependencies and is not Redis RSS. Admission stops at high thresholds and resumes below low thresholds; valid in-flight work, controls and bounded maintenance continue draining. Investigate unfinished callbacks, retained Runs, matching consumers and stuck physical slots before raising infrastructure capacity. Do not edit counters or rebuild indexes manually.

## Metrics and alerts

Collect namespace, task/version, runId/eventId, commandId, operation and error outcomeKnown where relevant. Telemetry is bounded and may drop records; its failure cannot roll back work. Choose alert thresholds from your workload SLO and load test, not universal latency numbers.

## Graceful shutdown

Stop application request admission, await `queue.close()`, then inspect timedOut/remainingExecutions/remainingCallbacks. A handler that ignores abort can remain alive after close. The service owner must decide any process termination; Queuebit does not kill processes.

## Next

[Incident recovery](failure-runbooks.md) · [Configuration](configuration-recipes.md)
