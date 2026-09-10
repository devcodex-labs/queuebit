# Recover From Failures

<span class="manual-label">Operations · preserve identity and business evidence</span>

## Remember three rules first

Capture Run/Event identity and current state before changing anything. External effects are at least once. An uncertain reply is not proof of zero writes; never erase a namespace to make an error disappear.

## Triage by symptom

| Symptom | Inspect | Recovery |
|---|---|---|
| No progress | ready state, matching definitions/members, health/backlog | Restore compatible consumers and connection |
| Handler fails | task.get error, state, business audit | Repair the adapter; finite retries resume from persisted state |
| Execution times out | AbortSignal handling, residual physical slots | Make I/O bounded and cooperative; cancellation is not rollback |
| Revision conflict | current Run/Event metadata | Reconsider action against the latest revision |
| Unknown command result | original commandId and full request | Read state; retry identical request when appropriate |
| Capacity pressure | reserved bytes, unfinished events, stuck handlers | Drain or repair; do not mutate counters directly |
| Inconsistent storage | exact namespace schema/index evidence | Stop unsafe work and restore validated state |

## Redis unavailable or network partition

See [Redis outage semantics](distributed-semantics.md). Do not send fresh identities repeatedly during an uncertain window. Reconcile acknowledged-write loss with the durable business store after failover.

## Resume or replace business work

Pause/resume only applies where the current Run state allows it. A failed/cancelled terminal Run is not transformed into a new execution by callback replay. To reprocess business data, make a deliberate new Run over the appropriate immutable snapshot and preserve stable external keys. There is no old failed-job migration/recovery-run API.

## Recover callback delivery

<span id="sc-replay"></span>

Read `queue.operator.deadLetters.get(eventId)` and confirm the handler, original business snapshot and external idempotency are safe to retry. Use its current Event revision:

```ts
const replay = await queue.operator.deadLetters.replay({
  eventId,
  expectedRevision: event.revision,
  reason: 'Provider restored; original event remains safe to retry',
  commandId: recoveryCommandId,
});
```

Here `eventId`, non-null `event` and the persisted `recoveryCommandId` come from your authenticated incident tooling. Replay reuses the immutable Event; it does not rerun business pages. The first dead letter fixes `deadLetterExpiresAt` at30 days. Replays never extend it. At/after expiry, only an already-valid attempt may settle before its frozen `replayDrainDeadline` and timeout; no new claim/retry/renewal is allowed.

## Incident exit criteria

Confirm the intended Run outcome and separate callback state, check the business audit for duplicates or missing effects, observe stable capacity/health, and document any accepted Redis loss window. Do not call an incident resolved merely because one request returned successfully.

## Next

[Operator procedures](operations.md) · [Full callback contract](batch-v2.md)
