# Run multiple consumers together

<span class="manual-label">Task guide · scale out, crash recovery and rolling releases</span>

<span id="sc-scale"></span>

## Start with what you need

One `runtime.mode: 'all'` process can admit and consume tasks. Use a producer mode in request-facing services and consumer mode in background services when lifecycle or scaling differs. Producer mode registers contracts without handlers; consumer/all require exactly the declared handlers. Consumer mode cannot start Runs.

## Minimal deployment

Share Redis, namespace, protocol options and task name/version/events/effective execution policy. Define before `ready()`. The package does not load modules or launch worker processes for you. There is no separate coordinator or scheduler role.

## Calculate concurrency

Each process defaults to4 physical execution slots and4 callback slots. Across N identically configured consumers the nominal local slot sum is N×4 for each class, not a global rate limit. Measure database/provider capacity and event-loop delay. One Run advances its pages serially; independent Runs can execute concurrently.

## When a consumer crashes

Lease recovery can run the same page again. Late settlements from the previous token are rejected. External effects can still repeat, so every retry uses durable business keys. A timeout-aborted handler that keeps running occupies its physical slot until it exits; admitting a replacement does not magically free CPU or sockets.

## Scale out

Start a process with the same immutable task contract, await readiness, and verify membership/health before sending traffic. Protocol mismatches fail explicitly. Do not change shared limits or callback policy on just one process.

## Rolling release and drain

Deploy matching consumers before routing producers to a new task version. Keep old-version consumers while retained work still needs them. This is versioned task operation within BatchQueue, not support for removed legacy APIs. On shutdown stop new request admission and await `queue.close()`; inspect remainingExecutions, remainingCallbacks and timedOut. Your process manager owns any later termination decision.

## Next

[Choose configuration](configuration-recipes.md) · [Idempotency](idempotency-patterns.md)
