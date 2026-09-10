# Deploy Queuebit in production

<span class="manual-label">Operations · application hosting and single-primary Redis</span>

<span id="sc-redis"></span>

## Choose the path for your environment

Run Node.js `>=22` with Redis `>=7.2`. Install the actual local root tarball while this API is unreleased; do not use a historical npm package with the new manual.

## Redis requirements

Use a writable single primary with noeviction, capacity headroom, persistence and backups appropriate to your RPO/RTO. Restrict application access to the intended deployment. TLS verifies CA/hostname; manage credentials in your application's chosen configuration system. Namespace is organizational isolation, not a hostile-tenant authorization boundary.

Readiness checks `INFO memory` for `maxmemory_policy:noeviction` before writing namespace metadata. A different or unobservable policy produces `CONFIG_INVALID`; Queuebit never changes the server configuration. The check also runs when the background runtime revalidates a reconnected client. It is a sampled preflight, not continuous enforcement: operators must retain noeviction throughout the deployment.

Sentinel discovery should use at least three independent failure domains and an appropriate quorum. Data and discovery authentication/TLS are configured separately. Redis asynchronous replication can lose acknowledged writes; no client-side lease can repair rolled-back history. Redis Cluster is not supported.

## Processes to deploy

A producer service admits work and registers contracts without executing handlers. Consumer services execute pages and callbacks. An all-mode service can do both. Every participant uses the same namespace protocol and matching immutable task contract. Application repositories and sinks are deployment-owned dependencies, not bundled database implementations.

## Startup order

1. Prepare Redis and a durable immutable business snapshot store.
2. Start consumers with registered handlers; await `ready()`.
3. Check compatible membership and health.
4. Start producer/request admission with server-derived identities.
5. Observe Run and callback outcomes plus the business audit.

The library does not install a CLI daemon, start extra worker processes or bind global shutdown signals.

## Containers and service managers

Use your standard process manager. Give shutdown enough time for configured close grace and bounded I/O, then inspect the close result. Do not interpret a process being alive as Queue readiness. Avoid per-request Queue creation; maintain long-lived participants.

## Configuration version and rolling release

Shared protocol options must match; local concurrency can differ. Deploy matching consumers before producers use a new task version and retain needed versions until their work drains. Incompatible handler behavior needs a new version even if its JavaScript function name stays the same.

## Production acceptance

Exercise actual database/provider idempotency, startup failure, TLS/auth rejection, process crash, uncertain replies, failover, callback replay, capacity pressure and owned-service cleanup in your environment. Local same-machine Sentinel fixtures verify client behavior, not independent-fault-domain availability or disk-persistence guarantees.

## Next

[Connection recipes](configuration-recipes.md) · [Outage boundaries](distributed-semantics.md)
