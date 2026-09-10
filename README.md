# queuebit

Queuebit processes a durable, bounded snapshot of business records in pages. It saves progress in Redis, retries interrupted work, and delivers durable completion callbacks. Use it for receipt delivery, exports or controlled backfills when your application supplies the data repository and idempotent external writes.

## Install

Requires Node.js 22+ and Redis 7.2+ with `noeviction`. This working tree contains the unreleased Batch v2 API; it is not the existing npm release. Until a release is selected, build and install a local tarball:

```bash
# In this source checkout
npm ci
npm pack
# In your consuming application; use the filename printed by npm pack
npm install /absolute/path/to/queuebit-0.0.5.tgz
```

The retained `0.0.5` is historical package metadata, not a new v2 release. The package is marked private to prevent accidental publication.

## Process a Snapshot

Use the [receipt task application module](https://github.com/devcodex-labs/queuebit/blob/main/examples/batch-v2/receipt-task.ts) with your snapshot repository and sink:

```ts
import { createBatchQueue } from 'queuebit';
import { defineReceiptTask } from './receipt-task.js';
import { receiptRepository, receiptSink } from './your-application-adapters.js';

const queue = createBatchQueue({
  namespace: 'receipt-service',
  redis: { mode: 'direct', host: '127.0.0.1', port: 6379 },
});
const task = defineReceiptTask(queue, receiptRepository, receiptSink);
await queue.ready();
const { runId } = await task.start({
  query: { snapshotId: 'snapshot-2026-09' },
  idempotencyKey: 'receipt-snapshot-2026-09',
});
console.log(runId);
// Keep this managed service alive while it consumes work.
// From your application shutdown hook: await queue.close();
```

Copy the linked task module into your application. Implement `your-application-adapters` with a snapshot that fixes record membership and payload, keyset pages of at most 100 rows, and a durable `putOnce` operation. The snapshot must already exist before `start`. The [full example](https://github.com/devcodex-labs/queuebit/blob/main/examples/batch-v2/README.md) describes these contracts and safe recovery after an external write succeeds but its response is lost. Qualification compiles that exact task module outside the repository and exercises 203 records with a forced page retry; its in-memory repository/sink are test fixtures, not production persistence.

`start` means admitted, not finished. Check `task.get(runId)` for status and callback progress. Execution and callback delivery are at least once. Store idempotency with the business write, not in process memory. Imports, construction and `define` do not open connections; `ready` starts the selected producer/consumer mode. Abort is cooperative, so a handler that does not exit still occupies a physical slot.

## Configure and Recover

Use direct host/port for a local Redis, a `redis://` or `rediss://` URL when provided by your host, or Sentinel discovery for a replicated single-primary topology. TLS verifies CA and hostname; Sentinel discovery credentials and Redis data credentials are separate. Redis Cluster is not supported. Replication is asynchronous: failover can lose acknowledged writes.

The operator SDK provides Run inspection/listing/pause/resume/cancel, dead-letter inspection/replay, health, capacity and local metrics. A control request carries `expectedRevision`, `reason` and `commandId`; after an uncertain response, inspect current state and retry the same command identity instead of assuming nothing was written. Dead-letter replay never extends the first-dead 30-day expiry.

## Documentation

The bilingual manual keeps its existing `docs/v01` URLs but describes only the new BatchQueue contract:

- [Start your first batch](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/quick-start.md)
- [Page business records safely](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/batch-runs.md)
- [Configuration and defaults](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/configuration-recipes.md)
- [API reference](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/target-api.md)
- [Failure recovery](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/failure-runbooks.md)
- [中文用户手册](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/zh/index.md)

中文：Queuebit 将固定业务快照按页执行，保存进度并持久投递回调。应用负责真实快照仓储与业务幂等写入；`start` 只表示接收，消费者需要持续运行。本工作树是未发布的新接口，安装时使用本地打包产物，不要把 npm 旧版本当作本手册对应实现。失败后按 Run 状态和控制命令标识恢复；不确定响应不代表零写入。

## Scope

Only `createBatchQueue`, `QueuebitError` and their public TypeScript types are exported from the root. There is no legacy API/data migration, CLI, framework-specific adapter or internal subpath. The package does not provide cron, DAGs, priorities, global rate limiting, a dashboard, CDC or unbounded streams. Your service owns hosting, signals, application repositories and sinks.

## Development Checks

```bash
npm ci
npm --prefix website ci
npm run typecheck
npm test
npm run docs:validate
```

Required Redis/TLS/Sentinel tests provision isolated fixtures and fail if prerequisites are missing; a skip is not qualification. Full qualification requires Node.js 22 or 24, Redis 7.2+ and OpenSSL; see the [testing guide](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/development-contract.md).

For local documentation review, `npm run docs:preview` builds then serves generated pages on `127.0.0.1:4180`. `npm run docs:dev` serves generated pages on `127.0.0.1:4181`; `npm run docs:edit` uses the hot editor on `127.0.0.1:4182`. Open `http://localhost:4180/queuebit/` or `http://localhost:4180/queuebit/zh/`.

## Package

`npm pack` builds the actual root entry. The tarball contains `dist`, `README.md`, `LICENSE` and `package.json`; its only runtime dependency is `@redis/client@6.1.0`. Publication requires a separately selected version and explicit release authorization.

## License

Apache-2.0
