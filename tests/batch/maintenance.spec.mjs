import assert from 'node:assert/strict';
import { test } from 'node:test';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { createBatchQueue } from '../../.temp/batch/index.js';
import { withRedis } from './helpers.mjs';
import { waitFor } from './redis-harness.mjs';

test('maintenance: actual terminal Run GC returns all references/counters and allows business-key reuse', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'gc', version: '1', events: [] }, undefined, 'producer');
    try {
      await store.ready();
      const { runId } = await store.start(definition, { query: { x: 1 }, idempotencyKey: '' }); await store.cancel(runId);
      assert.equal((await store.gcRun(runId)).changed, false, 'minimum retention still protects actual entity');
      const terminalAt = Date.now() - config.protocol.retention.runMs - 1000;
      await target.client.hSet(store.keys.run(runId), 'terminalAt', String(terminalAt));
      await target.client.zAdd(store.keys.gcRuns, { score: terminalAt + config.protocol.retention.runMs, value: runId });
      assert.equal((await store.gcRun(runId)).changed, true);
      assert.equal((await store.gcRun(runId)).changed, false);
      assert.equal(await store.get(runId), null);
      for (const index of [store.keys.runs(), store.keys.runs('gc'), store.keys.runs(undefined, 'cancelled'), store.keys.runs('gc', 'cancelled'), store.keys.gcRuns]) assert.equal(await target.client.zScore(index, runId), null);
      assert.equal(await target.client.get(store.keys.idempotency('gc', '')), null);
      assert.equal(await target.client.hGet(store.keys.capacity, 'runCount'), '0');
      assert.equal(await target.client.hGet(store.keys.capacity, 'chargedBytes'), String(16 * 1024));
      assert.equal((await store.gcDefinition(definition.identity)).changed, true);
      assert.equal(await target.client.exists(store.keys.definition(definition.identity)), 0);
      assert.equal(await target.client.zCard(store.keys.definitions), 0);
      assert.equal(await target.client.hGet(store.keys.capacity, 'chargedBytes'), '0');
      const reused = await store.start(definition, { query: { x: 2 }, idempotencyKey: '' }); assert.notEqual(reused.runId, runId);
    } finally { await connection.close(); }
  });
});

test('maintenance: producer without any local definition rotates shared catalog through tail definitions', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const options = { namespace: target.namespace, redis: target.redis, runtime: { mode: 'producer' }, protocol: {
      lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10 }, maintenance: { batchSize: 2, maxBatchesPerTick: 10, timeBudgetMs: 50 } } };
    const config = normalizeOptions(options); const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const queue = createBatchQueue(options);
    try {
      await store.ready(); const runs = [];
      for (let i = 0; i < 12; i++) {
        const definition = normalizeDefinition({ name: `tail-${i}`, version: '1', events: [] }, undefined, 'producer');
        runs.push((await store.start(definition, { query: null })).runId);
      }
      await queue.ready();
      await waitFor(async () => (await store.get(runs.at(-1))).reason === 'definition_unavailable', 20000);
      for (const id of runs) assert.equal((await store.get(id)).status, 'blocked');
      assert.equal(await target.client.hGet(store.keys.capacity, 'memberCount'), '0');
    } finally { await queue.close(); await connection.close(); }
  });
});
