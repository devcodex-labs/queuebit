# Configure Redis and Consumers

<span class="manual-label">Operations · choose the smallest connection that fits</span>

## Choose your situation

Use direct host/port for one local Redis, a URL for managed endpoints, or Sentinel when your platform provides replicated single-primary discovery. Keep namespace protocol settings identical across every producer and consumer. Change local concurrency independently according to downstream capacity.

## Minimal local configuration

```ts
import { createBatchQueue } from 'queuebit';
const queue = createBatchQueue({
  namespace: 'receipt-service',
  redis: { mode: 'direct', host: '127.0.0.1', port: 6379 },
});
```

Define tasks before `await queue.ready()`. There is no config-file loader or automatic environment interpolation; your application supplies values through its own configuration system.

## Managed Redis with TLS

```ts
const redis = {
  mode: 'url' as const,
  url: 'rediss://redis.example.internal:6380/0',
  tls: { ca: trustedCaPem, servername: 'redis.example.internal' },
};
```

Supply `trustedCaPem` as the provider CA PEM string and actual credentials in the URL when required. The hostname must match the certificate. Never disable verification to fix a wrong CA or address.

## Sentinel

```ts
const redis = {
  mode: 'sentinel' as const,
  name: 'receipt-primary',
  seeds: [
    { host: 'sentinel-a.internal', port: 26379 },
    { host: 'sentinel-b.internal', port: 26379 },
    { host: 'sentinel-c.internal', port: 26379 },
  ],
  nodeAuth: { username: redisUser, password: redisPassword },
  sentinelAuth: { username: discoveryUser, password: discoveryPassword },
  database: 0,
};
```

The four credential variables come from your application configuration. Add nodeTls/sentinelTls where those channels use TLS; supply addressMap when advertised host:port addresses need deliberate remapping. Separate discovery and data permissions. Sentinel does not eliminate asynchronous replication loss.

## Tune work, not shared semantics

Runtime execution and callback concurrency default to4 each. Start low and measure downstream latency, CPU and event-loop delay. Handlers must honor abort and eventually exit. Task execution policy merges over `defaults`; callbacks, leases, retention, capacity and maintenance are shared protocol options. A mismatched protocol fails readiness rather than silently adopting one process's settings.

## Validate before startup

Node.js22+, Redis7.2+, writable primary and noeviction are required. Unknown/null/explicit-undefined configuration fields fail synchronously with CONFIG_INVALID. Readiness errors are explicit; do not route requests to an unready Queue. See [all defaults and bounds](cli-and-config.md) and [deployment](production-deployment.md).
