# Operator SDK, no CLI

<span class="manual-label">Reference · application-owned operational tooling</span>

BatchQueue does not install a command-line binary. This URL is retained so existing links remain useful; old shell commands and exit-code contracts are not supported.

## Operational inspection

Use the in-process `queue.operator` SDK from your authenticated application tooling:

```ts
const metadata = await queue.operator.runs.getMetadata(runId);
const page = await queue.operator.runs.list({ limit: 50 });
const deadLetters = await queue.operator.deadLetters.list({ limit: 50 });
const health = await queue.operator.health.snapshot();
const capacity = await queue.operator.capacity.snapshot();
```

The caller supplies `runId` and a ready Queue. Metadata intentionally omits payload; use the matching Task's `get` when business data is required.

## Control a Run

Send `{ runId, expectedRevision, reason, commandId }` to pause/resume/cancel. Replay uses `{ eventId, expectedRevision, reason, commandId }` with the Event revision. Persist the command ID across retries; do not mint a new one after `OUTCOME_UNKNOWN`. Results discriminate applied/noop/not_found.

## Hosting and shutdown

Your service owns authentication, authorization, HTTP or shell presentation, process signals and exit codes. Use `await queue.close()` during shutdown and inspect `timedOut` and remaining counts. There is no remote drain command or hidden worker daemon.

## Next

[Operator workflow](operations.md) · [Failure runbooks](failure-runbooks.md) · [API lookup](target-api.md)
