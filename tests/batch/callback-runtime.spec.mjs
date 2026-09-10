import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { createBatchQueue } from '../../.temp/batch/index.js';
import { BatchKeys } from '../../.temp/batch/storage/redis/keys.js';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { randomBytes } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { withRedis } from './helpers.mjs';
import { waitFor } from './redis-harness.mjs';

test('callback runtime: actual handlers receive immutable ordered snapshots, ignore return values and never re-execute after callback failure', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis, protocol: {
      callback: { attempts: 2, backoff: { baseMs: 1, maxMs: 1 } }, lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10 } } });
    let executed = 0; const observed = [];
    const task = queue.define({ name: 'callback', version: '1', events: ['batchSettled', 'success'] }, {
      execute(ctx) { executed++; return executed === 1 ? ctx.next({ n: 1 }) : ctx.end(); },
      onBatchSettled(ctx) {
        assert(Object.isFrozen(ctx)); assert(Object.isFrozen(ctx.query)); assert(Object.isFrozen(ctx.state));
        assert.equal(ctx.timestamp > 0, true); assert.equal(ctx.query.original, '\ud800'); assert.equal(ctx.state.n, 1);
        observed.push([ctx.sequence, ctx.deliveryAttempt]);
        if (ctx.sequence === 1 && ctx.deliveryAttempt === 1) throw Error('callback failure only');
        return { ignored: true };
      }, onSuccess(ctx) { observed.push([ctx.sequence, ctx.deliveryAttempt]); return 'not a BatchControl'; }
    });
    try {
      await queue.ready(); const { runId } = await task.start({ query: { original: '\ud800' } });
      await waitFor(async () => (await task.get(runId))?.callbacks.delivered === 3, 10000);
      assert.deepEqual(observed, [[1, 1], [1, 2], [2, 1], [3, 1]]); assert.equal(executed, 2);
      assert.deepEqual((await task.get(runId)).callbacks, { pending: 0, delivered: 3, deadLetters: 0 });
      assert.equal((await task.get(runId)).businessFailures, 0);
      assert.equal(queue.operator.metrics.snapshot().counters.callback_settled, 4);
    } finally { await queue.close(); }
  });
});

test('callback runtime: timed-out unresolved Promise retains its separate physical slot while execution continues and close reports the residual', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis,
      runtime: { concurrency: 1, callbackConcurrency: 1, closeGraceMs: 0 },
      protocol: { callback: { attempts: 1, timeoutMs: 150 }, lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10 } } });
    let release; let signal; let executed = 0; let called = 0;
    const pending = new Promise(resolve => { release = resolve; });
    const task = queue.define({ name: 'held-callback', version: '1', events: ['success'] }, {
      execute(ctx) { executed++; return ctx.end(); }, onSuccess(ctx) { called++; signal = ctx.signal; return pending; }
    });
    try {
      await queue.ready(); const first = await task.start({ query: null }); await waitFor(() => signal, 5000);
      const second = await task.start({ query: null }); await waitFor(() => executed === 2, 5000);
      await waitFor(() => signal.aborted, 5000); await delay(100); assert.equal(called, 1);
      const health = await queue.operator.health.snapshot(); assert.equal(health.runtime.callbacks, 1); assert.equal(health.runtime.residualCallbacks, 1);
      assert.equal(health.reason, 'RESIDUAL_CALLBACK');
      const result = await queue.close(); assert.deepEqual(result, { status: 'closed', timedOut: true, remainingExecutions: 0, remainingCallbacks: 1 });
      const key = new BatchKeys(target.namespace).event(first.runId, 1, 'success');
      const before = await target.client.hGetAll(key); assert.equal(before.status, 'dead_letter');
      release(); await delay(100); assert.deepEqual(await target.client.hGetAll(key), before);
      assert.equal(called, 1); assert.notEqual(second.runId, first.runId);
    } finally { release(); await queue.close(); }
  });
});

test('callback runtime: grace renews an actual callback beyond one lease; producer never invokes handlers', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const common = { namespace: target.namespace, redis: target.redis, protocol: { lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10 } } };
    const producer = createBatchQueue({ ...common, runtime: { mode: 'producer' } });
    const consumer = createBatchQueue({ ...common, runtime: { mode: 'consumer', closeGraceMs: 5000 } });
    const contract = { name: 'grace-callback', version: '1', events: ['success'] };
    const task = producer.define(contract); let entered = false;
    consumer.define(contract, { execute: ctx => ctx.end(), async onSuccess() { entered = true; await delay(3500); } });
    try {
      await producer.ready(); const { runId } = await task.start({ query: null }); await delay(60); assert.equal(entered, false);
      await consumer.ready(); await waitFor(() => entered, 5000);
      assert.deepEqual(await consumer.close(), { status: 'closed', timedOut: false, remainingExecutions: 0, remainingCallbacks: 0 });
      assert.equal((await task.get(runId)).callbacks.delivered, 1);
    } finally { await consumer.close(); await producer.close(); }
  });
});

test('callback runtime: bounded metadata rotation reaches unmatched tails and both normal/replay queues make progress', { timeout: 90000 }, async () => {
  for (const unmatched of [false, true]) await withRedis(async target => {
    const options = { namespace: target.namespace, redis: target.redis, protocol: { callback: { attempts: 1 },
      maintenance: { batchSize: 2 }, lease: { pollMs: 10 } } };
    const config = normalizeOptions(options); const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const uid = () => randomBytes(16).toString('hex'); const member = uid(); const observed = [];
    const local = { name: 'local', version: '1', events: ['success'] };
    const other = { name: 'unmatched', version: '1', events: ['success'] };
    const handlers = { execute: ctx => ctx.end(), onSuccess() {} };
    const definitions = [local, other].map(value => normalizeDefinition(value, handlers, 'all'));
    const queue = createBatchQueue({ ...options, runtime: { mode: 'consumer', callbackConcurrency: 1 } });
    queue.define(local, { execute: ctx => ctx.end(), onSuccess(ctx) { observed.push(ctx.lateReplay); } });
    try {
      await store.ready(); await store.registerRuntime(member, 1, definitions);
      const seed = async (definition, replay) => {
        const { runId } = await store.start(definition, { query: null }); await store.settle(await store.claim(definition, member, 1), { kind: 'end' });
        const eventId = `${runId}:1:success`;
        if (replay) { await store.settleEvent(await store.claimEvent(eventId, member, 1), { kind: 'failure' });
          await store.replayEvent({ eventId, expectedRevision: (await store.getEvent(eventId)).revision, reason: 'fair', commandId: uid() }); }
        return eventId;
      };
      if (unmatched) for (let i = 0; i < 7; i++) {
        const eventId = await seed(definitions[1], false);
        await target.client.hSet(store.keys.eventById(eventId), 'state', 'invalid payload must not be read for an unmatched candidate');
      }
      for (let i = 0; i < 3; i++) { await seed(definitions[0], false); await seed(definitions[0], true); }
      await queue.ready(); await waitFor(() => observed.length === 6, 6000);
      assert.equal(observed.filter(Boolean).length, 3); assert.equal(observed.filter(value => !value).length, 3);
      if (!unmatched) assert.deepEqual(observed, [false, true, false, true, false, true]);
      assert.equal((await store.capacitySnapshot()).counts.unfinishedEvents, unmatched ? 7 : 0);
    } finally { await queue.close(); await connection.close(); }
  });
});

test('callback runtime: twenty public callback lifecycles and failing sinks return owned resources within fixed pressure limits', { timeout: 90000 }, async () => {
  assert.equal(typeof global.gc, 'function', 'Callback pressure requires --expose-gc');
  await withRedis(async target => {
    const options = { namespace: target.namespace, redis: target.redis, protocol: { lease: { pollMs: 10 }, callback: { attempts: 2, backoff: { baseMs: 1, maxMs: 1 } } },
      telemetry: { sink() { throw Error('observation only'); } } };
    const warmup = createBatchQueue(options); await warmup.ready(); await warmup.close();
    const sample = async () => {
      await delay(100); global.gc(); const resources = {};
      for (const name of process.getActiveResourcesInfo()) resources[name] = (resources[name] ?? 0) + 1;
      return { ...process.memoryUsage(), resources, listeners: Object.fromEntries(process.eventNames().map(name => [String(name), process.listenerCount(name)])),
        clients: (await target.client.sendCommand(['CLIENT', 'LIST'])).trim().split('\n').length };
    };
    const before = await sample(); const samples = [];
    for (let iteration = 0; iteration < 20; iteration++) {
      let calls = 0; let executions = 0; const queue = createBatchQueue(options);
      const task = queue.define({ name: 'callback-pressure', version: '1', events: ['success'] }, {
        execute(ctx) { executions++; return ctx.end(); }, onSuccess(ctx) { calls++; if (ctx.deliveryAttempt === 1) throw Error('first delivery'); }
      });
      try {
        await queue.ready(); const { runId } = await task.start({ query: { iteration } });
        await waitFor(async () => (await task.get(runId)).callbacks.delivered === 1, 3000);
        assert.equal(executions, 1); assert.equal(calls, 2); assert.equal((await queue.operator.deadLetters.list()).items.length, 0);
        assert.equal((await queue.operator.health.snapshot()).runtime.callbacks, 0);
        assert(queue.operator.metrics.snapshot().telemetry.sinkFailures > 0);
      } finally { assert.equal((await queue.close()).remainingCallbacks, 0); }
      if ((iteration + 1) % 5 === 0) samples.push(await sample());
    }
    await delay(500); const after = await sample();
    const limits = { heapGrowthBytes: 8 * 1024 * 1024, rssGrowthBytes: 64 * 1024 * 1024, lateHeapGrowthBytes: 2 * 1024 * 1024 };
    await writeFile(resolve(target.directory, 'callback-pressure.json'), JSON.stringify({ iterations: 20, cooldownMs: 500, before, samples, after, limits }, null, 2));
    assert.equal(after.clients, before.clients); assert.deepEqual(after.listeners, before.listeners);
    for (const name of new Set([...Object.keys(before.resources), ...Object.keys(after.resources)])) {
      if (/Socket|Timeout|TCP/i.test(name)) assert((after.resources[name] ?? 0) <= (before.resources[name] ?? 0), name);
    }
    assert(after.heapUsed - before.heapUsed <= limits.heapGrowthBytes); assert(after.rss - before.rss <= limits.rssGrowthBytes);
    assert(samples.at(-1).heapUsed - samples.at(-3).heapUsed <= limits.lateHeapGrowthBytes);
  });
});
