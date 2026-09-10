import { createBatchQueue, QueuebitError, type BatchControl, type JsonValue } from 'queuebit';
const value: JsonValue = { valid: true };
const queue = createBatchQueue({ namespace: 'typing', redis: { mode: 'direct', host: 'localhost', port: 6379 } });
const task = queue.define<{ snapshot: number }, { cursor: number }>({ name: 'typed', version: '1', events: ['success'] }, {
  execute(ctx) {
    // @ts-expect-error query is deeply immutable
    ctx.query.snapshot = 2;
    // @ts-expect-error state requires the declared shape
    ctx.next({ wrong: true });
    return ctx.next({ cursor: ctx.query.snapshot });
  }, onSuccess(ctx) { const readonlyTime: number = ctx.timestamp; void readonlyTime; }
});
// @ts-expect-error opaque controls cannot be fabricated
const forged: BatchControl = {};
// @ts-expect-error strict start query
void task.start({ query: { missing: true } });
// @ts-expect-error removed root API
import { createQueuebitClient } from 'queuebit';
// @ts-expect-error internal subpath is not exported
import { BatchRedisStore } from 'queuebit/storage';
// @ts-expect-error root runtime internals are not public types
import type { QueueRuntime } from 'queuebit';
void [value, QueuebitError, forged];
