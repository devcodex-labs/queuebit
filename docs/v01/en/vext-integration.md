# Host Queuebit in your framework

<span class="manual-label">Reference · application integration without a framework-specific adapter</span>

<span id="sc-hosting"></span>

The former adapter is removed. This page retains its URL but does not describe a compatibility layer. Use the ordinary `createBatchQueue` API with your framework's native startup, dependency injection and shutdown facilities.

## Own the lifecycle

Create one long-lived producer/all participant at application startup, register contracts, await `ready()` and expose only the needed Task operations to routes. Run consumers separately when deployment/scaling differs. On shutdown stop admission then await `queue.close()`. There is no per-request connection factory or hidden daemon.

## Authenticate before admission

Validate the request and derive tenant/snapshot ownership on the server. Pass only finite JSON to `task.start`; stable business idempotency must include the intended operation boundary. Namespace is not a substitute for authorization. Queuebit defines no HTTP status mapping or framework route schema.

## Handle outcomes explicitly

Return the runId after successful admission without waiting for all business work. Map invalid input, identity conflict, unavailable storage and capacity pressure according to your application contract. Preserve identity across uncertain replies and inspect current Run state; do not silently mint replacement requests.

## Deploy matching consumers

Consumers share namespace/protocol and register the same task version/events/policy with real handlers. A new incompatible handler requires a new task version. The [receipt module](https://github.com/devcodex-labs/queuebit/blob/main/examples/batch-v2/receipt-task.ts) composes with any host because repositories/sinks are injected by the application.

## Next

[First batch](quick-start.md) · [Scale consumers](distributed-workers.md) · [Production deployment](production-deployment.md)
