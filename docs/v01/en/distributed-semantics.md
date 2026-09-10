# What happens when Redis is down

<span class="manual-label">Operations · outage, failover and data-loss boundaries</span>

## Classify the situation first

Distinguish connection loss, an uncertain command reply and confirmed Redis history loss. These have different recovery actions. Never treat a retryable error as evidence that nothing was written.

## Temporary Redis outage

Connection/command retries share a bounded operation deadline, with offline queuing disabled. `OUTCOME_UNKNOWN` means the write may already have committed; preserve the original query, idempotency key or commandId and inspect the relevant Run/Event before deciding what to retry. Starting a new identity can duplicate business work.

## Sentinel failover

The same Queue can discover a new primary. Leases fence stale commits in the current Redis history, but asynchronous replication can lose acknowledged writes. Neither Sentinel nor a successful client response creates an exactly-once business guarantee. Reconcile with the durable business store after a loss window.

## Recover a blocked Run

Inspect `reason`, current metadata and compatible consumer membership. A definition without a matching live consumer cannot safely execute. Restore the correct contract/version and protocol, then use revision-checked operator control where the state allows it. Do not edit Run hashes or indexes by hand.

## Confirmed Redis state loss

Stop new admission while assessing the exact namespace and business audit. Restore according to your declared RPO/RTO or deliberately create new business Runs after reconciliation. Queuebit provides no old-data migration or automatic destructive reset. Stable external idempotency must outlive the queue's retained identity.

## Next

[Failure recovery](failure-runbooks.md) · [Production deployment](production-deployment.md)
