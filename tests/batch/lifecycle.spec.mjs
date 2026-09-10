import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createBatchQueue } from '../../.temp/batch/index.js';
import { BatchKeys } from '../../.temp/batch/storage/redis/keys.js';
import { withRedis } from './helpers.mjs';
import { setTimeout as delay } from 'node:timers/promises';
import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

test('lifecycle: explicit readiness, definition freeze, task identity and producer membership', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis, runtime: { mode: 'producer' } });
    const first = queue.define({ name: 'one', version: '1', events: [] });
    const second = queue.define({ name: 'two', version: '1', events: [] });
    assert.throws(() => queue.define({ name: 'one', version: '2', events: [] }), { code: 'CONFIG_INVALID' });
    await assert.rejects(first.start({ query: null }), { code: 'QUEUE_NOT_READY' });
    assert.equal((await target.keys()).length, 0);
    try {
      const ready = queue.ready();
      assert.throws(() => queue.define({ name: 'late', version: '1', events: [] }), { code: 'CONFIG_INVALID' });
      await ready;
      assert.equal(await target.client.hGet(`qb:batch:v1:{${target.namespace}}:capacity`, 'memberCount'), '0');
      const started = await first.start({ query: null });
      await assert.rejects(second.get(started.runId), { code: 'TASK_IDENTITY_MISMATCH' });
      await assert.rejects(second.cancel(started.runId), { code: 'TASK_IDENTITY_MISMATCH' });
      await queue.close();
      await assert.rejects(first.get(started.runId), { code: 'QUEUE_CLOSED' });
      await assert.rejects(queue.ready(), { code: 'QUEUE_CLOSED' });
    } finally { await queue.close(); }
  });
});

test('lifecycle: close revokes a not-yet-ready startup and never registers or polls late', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    let calls = 0;
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis, runtime: { closeGraceMs: 0 } });
    queue.define({ name: 'one', version: '1', events: [] }, { execute(ctx) { calls++; return ctx.end(); } });
    const ready = queue.ready();
    const rejected = assert.rejects(ready, error => ['QUEUE_CLOSING', 'QUEUE_FAILED'].includes(error.code));
    await queue.close(); await rejected;
    assert.equal(calls, 0);
    assert.equal(await target.client.zCard(new BatchKeys(target.namespace).members), 0);
    assert.equal((await target.keys()).filter(key => key.includes(':runtime:')).length, 0);
  });
});

test('lifecycle: a failed ready is terminal and a consumer never starts Runs', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const failed = createBatchQueue({ namespace: target.namespace, redis: { ...target.redis, password: 'invalid-on-this-no-auth-fixture' } });
    try {
      await assert.rejects(failed.ready());
      await assert.rejects(failed.ready(), { code: 'QUEUE_FAILED' });
    } finally { await failed.close(); }
    const consumer = createBatchQueue({ namespace: target.namespace, redis: target.redis, runtime: { mode: 'consumer' } });
    const task = consumer.define({ name: 'only-consume', version: '1', events: [] }, { execute: ctx => ctx.end() });
    try {
      await consumer.ready();
      await assert.rejects(task.start({ query: null }), { code: 'MODE_OPERATION_NOT_ALLOWED' });
    } finally { await consumer.close(); }
  });
});

test('lifecycle: repeated public operations and failed startups release owned resources', { timeout: 90000 }, async () => {
  assert.equal(typeof global.gc, 'function', 'This qualification requires node --expose-gc');
  await withRedis(async target => {
    const options = { namespace: target.namespace, redis: target.redis };
    const declaration = { name: 'pressure', version: '1', events: [] };
    const handlers = { execute: ctx => ctx.end() };
    const warmup = createBatchQueue(options); warmup.define(declaration, handlers); await warmup.ready(); await warmup.close();
    const sample = async () => {
      await delay(100); global.gc();
      const resources = {};
      for (const resource of process.getActiveResourcesInfo()) resources[resource] = (resources[resource] ?? 0) + 1;
      return { ...process.memoryUsage(), resources,
        listeners: Object.fromEntries(process.eventNames().map(name => [String(name), process.listenerCount(name)])),
        clients: (await target.client.sendCommand(['CLIENT', 'LIST'])).trim().split('\n').length,
        members: Number(await target.client.hGet(new BatchKeys(target.namespace).capacity, 'memberCount')) };
    };
    const before = await sample(); const samples = []; const began = Date.now();
    for (let iteration = 0; iteration < 30; iteration++) {
      const queue = createBatchQueue(iteration >= 25 ? { ...options, redis: { ...target.redis, password: 'invalid-on-no-auth-fixture' } } : options);
      const task = queue.define(declaration, handlers);
      try {
        if (iteration < 20) {
          const ready = queue.ready(); assert.equal(queue.ready(), ready); await ready;
          const run = await task.start({ query: { iteration } });
          assert.equal((await task.get(run.runId)).query.iteration, iteration);
          await task.cancel(run.runId);
        } else if (iteration < 25) {
          const ready = assert.rejects(queue.ready()); await queue.close(); await ready;
        } else await assert.rejects(queue.ready());
      } finally { const closing = queue.close(); assert.equal(queue.close(), closing); await closing; }
      if ((iteration + 1) % 5 === 0) samples.push(await sample());
    }
    await delay(500); const after = await sample();
    const receipt = { iterations: 30, normalCycles: 20, revokedStartups: 5, failedStartups: 5,
      elapsedMs: Date.now() - began, cooldownMs: 500, before, samples, after,
      limits: { heapGrowthBytes: 8 * 1024 * 1024, rssGrowthBytes: 64 * 1024 * 1024, lateHeapGrowthBytes: 2 * 1024 * 1024,
        connections: before.clients, members: 0, extraTimersOrSockets: 0 } };
    await writeFile(resolve(target.directory, 'leak-pressure.json'), JSON.stringify(receipt, null, 2));
    assert.equal(after.clients, before.clients); assert.equal(after.members, 0);
    assert.deepEqual(after.listeners, before.listeners);
    for (const resource of new Set([...Object.keys(before.resources), ...Object.keys(after.resources)])) {
      if (/Socket|Timeout|TCP/i.test(resource)) assert.ok((after.resources[resource] ?? 0) <= (before.resources[resource] ?? 0), `Resource did not return: ${resource}`);
    }
    assert.ok(after.heapUsed - before.heapUsed <= receipt.limits.heapGrowthBytes);
    assert.ok(after.rss - before.rss <= receipt.limits.rssGrowthBytes);
    assert.ok(samples.at(-1).heapUsed - samples.at(-3).heapUsed <= receipt.limits.lateHeapGrowthBytes);
  });
});
