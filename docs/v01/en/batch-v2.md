# BatchQueue complete contract

<span class="manual-label">Reference · full BatchQueue behavior and limits</span>

This manual describes the current, unreleased BatchQueue root package. Node.js 22/24 and Redis 7.2 are the qualification targets. No CLI, framework-specific adapter, separate Worker/Coordinator, job mapper or legacy data migration is included.

## 1. Install and lifecycle

Run `npm ci` and `npm pack` in the source checkout, then install the actual root tarball using the filename printed by pack, for example `npm install /absolute/path/queuebit-0.0.5.tgz`. The retained 0.0.5 is historical metadata, not a new v2 release; the package is private until a release is selected. ESM and CommonJS import the same public runtime names, `createBatchQueue` and `QueuebitError`. Types resolve independently for NodeNext import/require and Bundler. Only the root and `queuebit/package.json` are exported.

Construct `createBatchQueue({namespace,redis})`, call `queue.define(contract,handlers)` before `await queue.ready()`, then use `task.start/get/cancel` and `queue.operator`. Construction/import performs no I/O. The first ready opens the owned connection and registers immutable definitions; ready failures are explicit. Repeated ready/close share their in-flight promise; a closed Queue is not reusable. Keep the consumer process alive; start returns admission, not completion. Always await close on application shutdown and inspect remainingExecutions/remainingCallbacks/timedOut.

runtime.mode is all (default), producer or consumer. Producer registers definitions and performs maintenance but invokes no handler; consumer cannot start work. Consumer/all require handlers matching the declared events. Definitions bind name/version/event set/effective policy, not function source; change version when deploying incompatible handler behavior. Different policies under the same task version are not interchangeable.

## 2. Run and Batch

`queue.define<Q,S>({name,version,events,policy?}, {execute,onBatchSettled?,onSuccess?,onFailure?})` returns a typed Task. `start({query,idempotencyKey?})` returns `{runId,created}`; `get(runId)` returns the immutable Run snapshot or null; `cancel(runId)` returns a found-discriminated result. Query is frozen for the Run; state starts null. Within each Run, one valid leased Batch at a time advances serially. Different Runs may execute concurrently.

An execute callback must return its own `ctx.next(state?)` or `ctx.end()` exactly once. These are context-owned opaque controls, not root helpers or arbitrary `{kind:...}` objects. Throw/reject uses the finite business attempt budget; malformed/duplicate controls are terminal contract failures, even if caught. next commits the next state and resets the next Batch's budget. page, batchId, attempt and AbortSignal are available. Cooperatively stop work after abort; Redis fencing prevents late state commits, not external side effects.

## 3. Input and idempotency

Use finite JSON primitives, dense arrays and plain data objects. No Date/BigInt/undefined/NaN/Infinity/functions/accessors/cycles/symbol keys; strict config rejects unknown, null and explicit undefined fields. Query ≤256KiB, state ≤64KiB, original error envelope ≤32KiB after encoding. Legal JSON strings preserve UTF-16 values; business idempotency keys must be valid Unicode, ≤256 UTF-8 bytes. Empty key differs from omitted key; text is not trimmed or normalized.

The same task/key and exactly matching canonical query/effective policy returns the existing Run while retained; changed input gives IDEMPOTENCY_CONFLICT. It is not permanent deduplication. Use durable business keys at the external sink: execution and callback delivery are at least once; Redis asynchronous replication may lose acknowledged writes. A successful library commit does not prove exactly-once business delivery.

## 4. Defaults and limits

| Group | Defaults |
|---|---|
| Local runtime | all; concurrency 4; callbackConcurrency 4; closeGraceMs 30000 |
| Execute policy | attempts 3; timeoutMs 30000; full-jitter backoff base1000/max30000 ms |
| Callback policy | attempts 10; timeoutMs 30000; full-jitter base1000/max60000 ms |
| Lease | leaseMs30000; heartbeatMs10000; pollMs1000; recoveryLimit20 |
| Retention | Run minimum7d; delivered Event minimum7d; dead letter absolute30d |
| Capacity | nonterminal10000; Runs20000; objects100000; unfinishedEvents20000; definitions10000; members1000; total512MiB logical |
| Maintenance | batchSize100; maxBatchesPerTick10; timeBudgetMs50 |

Namespace protocol options (callback/lease/retention/limits/maintenance) must agree across producers and consumers. Local runtime/concurrency and per-Task execute policy are separate. Charged logical bytes include prepaid settlement/Event space, not Redis RSS. High/low admission thresholds stop new starts while existing prepaid work, controls and maintenance can drain. CAPACITY_EXCEEDED is explicit; do not bypass it by creating another consumer schema. Runtime membership has its own bounded partition.

## 5. Connections and failure boundaries

redis is one of `{mode:'direct',host,port,username?,password?,database?,tls?}`, `{mode:'url',url,tls?}` or `{mode:'sentinel',name,seeds,nodeAuth?,sentinelAuth?,nodeTls?,sentinelTls?,database?,addressMap?}`. Supply PEM strings for tls.ca/cert/key plus optional servername; rediss URL is required with URL TLS. Certificates are verified; no disable-verification switch. Data and Sentinel credentials/TLS options are separate. Prefer at least three Sentinels in independent failure domains. Tests using one machine do not certify independent-failure-domain availability.

Redis must be 7.2+, writable primary, noeviction and compatible schema/protocol. The isolated prefix is `qb:batch:v1:{namespace}:`; legacy keys are not scanned for migration or consumed. Namespace is organizational isolation, not a hostile-tenant security boundary. Strict preflight can reject uncertain/orphaned storage; do not delete meta alone or rebuild indexes manually to suppress the error.

Connection/command attempts are bounded with one shared operation deadline (≤10s); offline queue is disabled. OUTCOME_UNKNOWN means a write might have committed: reconcile its returned runId/commandId and reuse the identical request, never invent a new command ID to hide uncertainty. Transport retry never restarts a business handler itself. Redis failover history rollback cannot be eliminated by client-side fencing.

## 6. Durable callbacks and replay

Declare batchSettled/success/failure and matching handlers. Event query/state/error/timestamp are immutable snapshots of the original settlement; callback return values are ignored. Callbacks have an independent attempt budget, lease and physical concurrency. Failure does not rerun execute or flip a successful Run. Normal Events preserve creation sequence within a Run; a retry blocks its normal successors. delivered/dead_letter advances that sequence; late replay never rewinds it and shares the Run's Event lock.

The first dead letter fixes firstDeadAt, an independent list sequence and expiry E=firstDeadAt+30d. Replay reuses the Event/parent query, resets delivery attempts for a new generation, consumes unfinished capacity, and never extends E. Before E, the first lease grant crossing E freezes replayDrainDeadline permanently. At/after E no new claim, renewal, recovery or retry is permitted; only the already-valid attempt can settle before its frozen deadline and timeout. Replayed callbacks can repeat external effects: deduplicate by Event ID or a durable business key.

## 7. Operator SDK

`queue.operator.runs.getMetadata/list/pause/resume/cancel`, `deadLetters.get/list/replay`, `health.snapshot()`, `capacity.snapshot()` and local `metrics.snapshot()` are in-process SDK groups, not HTTP endpoints. Run controls take `{runId,expectedRevision,reason,commandId}`; replay takes `{eventId,expectedRevision,reason,commandId}` using the Event revision. Results discriminate applied/noop/not_found; no synthetic revision on not_found. Shared parent-Run command history retains at most32 receipts/24h; same ID with different full operation/target/text conflicts, even before CAS. A complete encoded receipt is ≤2KiB. After that retention window, command IDs are not permanent idempotency keys.

Lists are live, not snapshots: default50/max200, cursor15min fixed expiry, original upper sequence/filter binding; no created-after-first-page rows. A sparse page may be short and still have nextCursor. Stable bad indexes fail explicitly, not a fake empty last page. deadLetters list uses first-dead sequence (not Run sequence), excludes expired/successfully delivered replay; get can observe retained replay metadata until GC. Metadata methods omit business payload. Health can be degraded/unavailable; local metrics do not represent exact cluster totals. Telemetry sink is bounded/droppable and cannot roll back business work.

## 8. Retention and shutdown

No permanent tombstones or whole-namespace automatic purge. Unfinished normal Events and valid frozen replay drains protect dependencies. GC removes Event references/fees, then eligible terminal Run/query, idempotency and unreferenced definitions. Graceful close stops new claims, allows bounded in-flight settlement, then revokes authority. An unresolved Promise continues occupying its physical slot until it actually exits; inspect close results and fix non-cooperative adapters. close/ready never clean old Redis data.

## 9. Application example and operations

The repository's `examples/batch-v2/receipt-task.ts` accepts an immutable snapshot repository and durable idempotent receipt sink. It reads a maximum100 strictly increasing IDs, uses ctx.next({afterId}) and emits a completion callback. Do not replace snapshot isolation with a timestamp over mutable data. Tests compile that exact file in a fresh consumer, inject a post-write failure and execute against real Redis. Supply real database/provider adapters; test arrays are not production storage.

On REVISION_CONFLICT read fresh metadata before deciding a new operation. On SCHEMA_MISMATCH/INDEX_INCONSISTENT/STORAGE_INCONSISTENT stop and inspect the exact namespace; do not silently reset. On capacity/dead-letter growth inspect consumers, stuck handlers and retention. Pausing does not cancel valid leases; cancellation cannot undo already-issued external writes. A replay is an operator decision with possible repeated effects.

## 10. Qualification and release status

Local qualification scripts cover unit/model/Redis faults, fresh install/type consumers, TLS, three-Sentinel actual failover, examples and bilingual docs under Node22/24. Qualification builds and installs the actual root tarball in fresh directories; no synthesized package metadata or repository aliases are used. Local qualification is not npm publication. A new release version and publication remain separate decisions; no lifecycle method cleans legacy Redis data.
