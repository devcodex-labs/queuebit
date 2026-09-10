# Redis model and atomic invariants

<span class="manual-label">Maintainer · internal storage contract, not a public command API</span>

## User boundary

Consumers use BatchQueue, not raw Redis key manipulation. The internal prefix is `qb:batch:v1:{namespace}:`. Old key prefixes are not migrated or consumed. Startup only uses a bounded SCAN with MATCH restricted to the exact new namespace to detect orphaned state; there is no old-key scan.

## Conceptual keyspace

Namespace metadata binds schema/protocol. Definitions bind task identity. Runs store the immutable query and committed state; indexes support bounded live listings and due/lease work. Events hold immutable settlement snapshots, independent delivery state and references protecting parent data. Runtime members are bounded and distinct from durable business identity.

## Required atomic transitions

Static Lua receives every key explicitly in KEYS. Lease/token/revision checks fence claim, renew and settlement. Partial object/index inconsistency fails rather than becoming executable work. Capacity accounting prepays dependent settlement/Event space and refuses unsafe new admission without preventing valid drain work.

## Canonical input

JSON validation precedes storage; canonical identity preserves exact accepted string/number/array/object semantics. Query/state/error have encoded size bounds. Business keys are valid Unicode bytes without trimming or normalization.

## Retention and non-removable state

Unfinished normal callbacks and valid frozen replay drains protect dependencies. Dead-letter expiry is anchored to firstDeadAt; replay does not move it. GC removes fees and references before eligible Run/query/idempotency/definition cleanup. There are no permanent tombstones or whole-namespace automatic purge.

Never issue FLUSHDB, FLUSHALL or manual metadata/index deletion as a library repair step. Namespace lifecycle methods do not clear old data. Recovery decisions belong to an explicit operator workflow.

## Verification matrix

Verify CAS/token races, unknown replies, partial indexes, bounded memory, capacities, Event/Run coupling, expiry/drain behavior and cleanup against actual Redis. The root package test additionally seeds old keys and observes zero old-key commands while confirming new-key traffic.

## Next

[Runtime lifecycle](worker-lifecycle.md) · [Qualification](development-contract.md)
