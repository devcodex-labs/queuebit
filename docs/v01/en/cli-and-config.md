# Configuration field dictionary

<span class="manual-label">Reference · types, defaults, constraints and scope</span>

Start with [configuration recipes](configuration-recipes.md). Configuration is passed directly to `createBatchQueue`; there is no CLI loader, automatic environment lookup or framework configuration overlay.

## Common fields first

| Field | Default | Meaning |
|---|---|---|
| `namespace` | required | ASCII letter/digit first, then letters/digits/dot/underscore/hyphen; at most 128 UTF-8 bytes |
| `redis` | required | direct, URL or Sentinel connection |
| `runtime.mode` | all | producer, consumer or both |
| `runtime.concurrency` | 4 | physical execution slots, 1–1024 |
| `runtime.callbackConcurrency` | 4 | physical callback slots, 1–1024 |
| `runtime.closeGraceMs` | 30000 | local drain grace, 0–86400000 ms |
| `defaults` | policy below | default execution policy |
| `protocol` | table below | must match every process in the namespace |
| `telemetry.sink` | absent | bounded, droppable local observations |

## Naming and static validation

Unknown fields, explicit undefined, null configuration values and accessor properties are rejected with `CONFIG_INVALID`. Omit optional fields instead. Task names and versions use the same safe identifier rules. This strictness applies to configuration, not JSON payloads where null is valid.

## Execution and callback policies

Execution defaults: attempts 3, timeoutMs 30000, full-jitter backoff baseMs 1000/maxMs 30000. Callback defaults: attempts 10, timeoutMs 30000, baseMs 1000/maxMs 60000. Task `policy` overrides execution defaults field by field; callback policy belongs to the namespace protocol. Attempts are 1–1000, timeout 1–86400000 ms, backoff base 1–3600000 ms and max up to 86400000 ms with max ≥ base. Only `jitter: 'full'` is accepted.

## Protocol and retention

| Group | Defaults and limits |
|---|---|
| lease | leaseMs 30000 (min3000); heartbeatMs10000 (min1000, ≤lease/3); pollMs1000 (1–1000); recoveryLimit20 (1–1000) |
| retention | runMs 7d and deliveredEventMs7d (7–365d); deadLetterMs exactly30d |
| limits | nonterminalRunMax10000, runMax20000, objectMax100000, unfinishedEventMax20000, definitionMax10000, memberMax1000, totalBytes512MiB |
| maintenance | batchSize100, maxBatchesPerTick10, timeBudgetMs50; positive values may be lowered, not raised |

Capacity values are upper bounds and may be lowered only when their interdependent budgets remain valid. Logical bytes reserve future settlement/Event space; they are not Redis RSS. Retention is eligibility for bounded GC, not a promise of deletion at an exact wall-clock second.

## Redis connection

Direct accepts host/port, username/password, database and tls. URL accepts a redis/rediss URL and optional tls; use rediss when supplying TLS. Sentinel accepts name/seeds plus separate nodeAuth/sentinelAuth, nodeTls/sentinelTls, database and addressMap. Database defaults to 0; seeds contain 1–32 addresses; addressMap contains at most256 host:port remappings. TLS fields ca/cert/key are PEM strings; servername is optional, certificate verification cannot be disabled.

## Payload limits

Query ≤256KiB, state ≤64KiB and the original encoded error envelope ≤32KiB. Business idempotency key ≤256 UTF-8 bytes with valid Unicode; empty differs from omitted. See the [complete JSON contract](batch-v2.md).

## Next

[States and errors](failure-modes.md) · [Run controls](operations.md)
