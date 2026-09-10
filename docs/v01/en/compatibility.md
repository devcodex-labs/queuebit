# Can my environment use Queuebit?

<span class="manual-label">Reference · supported runtime and workload boundaries</span>

## One-minute check

- Node.js `>=22`; qualification targets Node22 and Node24.
- Redis `>=7.2`, writable single primary and `noeviction`.
- Standalone or managed single-primary Redis, optionally discovered by Sentinel.
- ESM/CommonJS with conditional TypeScript declarations for NodeNext/Bundler.
- Install the locally built root tarball for this unreleased API. Historical npm versions are not interchangeable.

## Good fits

Finite snapshot processing, exports, receipt delivery and controlled backfills where your application supplies durable input and idempotent external writes. Multiple Runs can execute concurrently; each Run advances one serial page cursor.

## Not a fit

Redis Cluster, non-Redis backends, unbounded streams/CDC, cron, DAG orchestration, priorities, global rate limiting and a built-in administration UI. Namespace separation is not hostile-tenant security isolation.

## Connection safety

TLS validates CA and hostname. Sentinel discovery/data credentials and TLS settings are separate. Three Sentinels in independent failure domains are a deployment recommendation; same-machine tests do not certify availability across failure domains. Asynchronous replication can lose acknowledged writes.

## Compatibility boundary

No old API aliases, old Redis data migration, CLI, worker/coordinator package entry or framework-specific adapter is provided. The retained `docs/v01` directory is a URL convention only. Old Redis keys are not read or deleted by queue lifecycle operations.

## Next

[First batch](quick-start.md) · [Choose Redis settings](configuration-recipes.md)
