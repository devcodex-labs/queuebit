import assert from 'node:assert/strict';
import { test } from 'node:test';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { createBatchQueue } from '../../.temp/batch/index.js';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { withRedis } from './helpers.mjs';
import { startWorker, startProxy, importProbe, waitFor } from './redis-harness.mjs';

const protocol = { lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10 } };
const contract = { name: 'fault', version: '1', events: [] };

test('faults: two real Node processes have one legal claimant for a Run', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const producer = createBatchQueue({ namespace: target.namespace, redis: target.redis, protocol, runtime: { mode: 'producer' } });
    const task = producer.define(contract);
    const workers = [];
    try {
      await producer.ready();
      for (let i = 0; i < 2; i++) workers.push(await startWorker(target, { protocol, behavior: 'finish' }));
      const started = await task.start({ query: { delayMs: 100 } });
      const result = await waitFor(async () => { const run = await task.get(started.runId); return run.status === 'success' && run; });
      assert.equal(result.dispatchCount, 1);
      assert.equal(workers.flatMap(worker => worker.messages).filter(message => message.kind === 'entered').length, 1);
      assert.notEqual(workers[0].pid, workers[1].pid);
    } finally { for (const worker of workers) await worker.stop(); await producer.close(); }
  });
});

test('faults: killing only the owning worker permits expiry recovery without a business failure', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const producer = createBatchQueue({ namespace: target.namespace, redis: target.redis, protocol, runtime: { mode: 'producer' } });
    const task = producer.define(contract);
    let owner; let survivor;
    try {
      await producer.ready(); owner = await startWorker(target, { protocol, behavior: 'hold' });
      const started = await task.start({ query: null });
      const entered = await waitFor(() => owner.messages.find(message => message.kind === 'entered'));
      survivor = await startWorker(target, { protocol, behavior: 'finish' });
      await owner.stop(true);
      const result = await waitFor(async () => { const run = await task.get(started.runId); return run.status === 'success' && run; }, 20000);
      assert.deepEqual([result.dispatchCount, result.recoveries, result.businessFailures, result.page], [2, 1, 0, 1]);
      const resumed = survivor.messages.find(message => message.kind === 'entered');
      assert.equal(resumed.batchId, entered.batchId);
    } finally { await owner?.stop(); await survivor?.stop(); await producer.close(); }
  });
});

test('faults: an expired token accepts zero late commits before any replacement owner', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis, protocol });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition(contract, { execute: ctx => ctx.end() }, 'all');
    const member = randomBytes(16).toString('hex');
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]);
      const started = await store.start(definition, { query: null }); const lease = await store.claim(definition, member, 1);
      await delay(3100);
      const time = await target.client.sendCommand(['TIME']);
      assert.ok(Number(time[0]) * 1000 + Math.floor(Number(time[1]) / 1000) >= lease.deadline);
      const before = await target.snapshot();
      await assert.rejects(store.renew(lease), { code: 'LEASE_LOST' });
      await assert.rejects(store.settle(lease, { kind: 'end' }), { code: 'LEASE_LOST' });
      assert.deepEqual(await target.snapshot(), before);
      assert.equal((await store.recover(started.runId)).changed, true);
      assert.equal((await store.recover(started.runId)).changed, false);
      const run = await store.get(started.runId);
      assert.deepEqual([run.recoveries, run.businessFailures, run.dispatchCount], [1, 0, 1]);
      assert.equal(await target.client.hGet(store.keys.capacity, 'reservedBytes'), '0');
    } finally { await connection.close(); }
  });
});

test('faults: TCP proxy drops committed write replies and identical commands recover their outcomes', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const proxy = await startProxy(target, { start: 1, claim: 1, renew: 1, settle: 1 });
    const config = normalizeOptions({ namespace: target.namespace, redis: { ...target.redis, port: proxy.port } });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ ...contract, events: ['success'] }, { execute: ctx => ctx.end(), onSuccess() {} }, 'all');
    const member = randomBytes(16).toString('hex');
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]);
      const started = await store.start(definition, { query: null }); assert.equal(started.created, true);
      const lease = await store.claim(definition, member, 1); assert.ok(lease);
      const renewed = await store.renew(lease);
      assert.equal((await store.settle(renewed, { kind: 'end' })).status, 'success');
      const run = await store.get(started.runId);
      assert.equal(run.dispatchCount, 1); assert.equal(run.callbacks.pending, 1);
      assert.equal(await target.client.hGet(store.keys.capacity, 'reservedBytes'), '0');
      assert.deepEqual(proxy.dropped.map(value => value.op), ['start', 'claim', 'renew', 'settle']);
      for (const dropped of proxy.dropped) {
        const requests = proxy.requests.filter(value => value.commandId === dropped.commandId);
        assert.ok(requests.length >= 2); assert.equal(new Set(requests.map(value => value.canonical)).size, 1);
      }
    } finally { await connection.close(); await proxy.stop(); }
  });
});

test('faults: exhausted lost-reply budget exposes OUTCOME_UNKNOWN without corrupting namespace metadata', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const proxy = await startProxy(target, { start: 10 });
    const config = normalizeOptions({ namespace: target.namespace, redis: { ...target.redis, port: proxy.port } });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition(contract, undefined, 'producer');
    try {
      await store.ready();
      const began = Date.now(); let uncertainty;
      await assert.rejects(store.start(definition, { query: null }), error => {
        uncertainty = error;
        return error.code === 'OUTCOME_UNKNOWN' && error.outcomeKnown === false && typeof error.runId === 'string' && typeof error.commandId === 'string';
      });
      assert.ok(Date.now() - began < 10000);
      assert.equal(proxy.dropped.length, 3);
      assert.equal((await store.get(uncertainty.runId)).status, 'pending');
      assert.equal(await target.client.hGet(store.keys.meta, 'status'), 'ready');
      assert.equal(await target.client.hGet(store.keys.capacity, 'runCount'), '1');
    } finally { await connection.close(); await proxy.stop(); }
  });
});

test('faults: fresh ESM and CJS import/construction/define add zero sockets, timers or process hooks', { timeout: 30000 }, async () => {
  for (const format of ['esm', 'cjs']) {
    assert.deepEqual(await importProbe(format), { format, connects: 0, timers: 0, hooks: 0, exports: ['QueuebitError', 'createBatchQueue'] });
  }
});

test('faults: a consumer replaces its expired membership after a temporary Redis outage', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis, protocol });
    const task = queue.define(contract, { execute: ctx => ctx.end() });
    try {
      await queue.ready();
      // The pause belongs solely to this isolated server, and expires without external cleanup.
      await target.client.sendCommand(['CLIENT', 'PAUSE', '4500', 'ALL']);
      await delay(4700);
      const started = await task.start({ query: null });
      const run = await waitFor(async () => { const value = await task.get(started.runId); return value.status === 'success' && value; }, 6000);
      assert.equal(run.dispatchCount, 1);
      assert.equal(await target.client.hGet(`qb:batch:v1:{${target.namespace}}:capacity`, 'memberCount'), '1');
    } finally { await queue.close(); }
  });
});

test('faults: idempotency lookup and the write share one transport retry budget', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const proxy = await startProxy(target, { get: 1, start: 2 });
    const config = normalizeOptions({ namespace: target.namespace, redis: { ...target.redis, port: proxy.port } });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition(contract, undefined, 'producer');
    try {
      await store.ready();
      await assert.rejects(store.start(definition, { query: null, idempotencyKey: 'shared-budget' }), { code: 'OUTCOME_UNKNOWN', outcomeKnown: false });
      assert.deepEqual(proxy.dropped.map(value => value.op), ['get', 'start', 'start']);
      assert.equal(proxy.requests.filter(value => value.op === 'start').length, 2);
      assert.equal(await target.client.hGet(store.keys.capacity, 'runCount'), '1');
    } finally { await connection.close(); await proxy.stop(); }
  });
});

test('faults: a registration reply arriving after close is cleaned without late polling', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const proxy = await startProxy(target, {}); proxy.hold('register');
    const queue = createBatchQueue({ namespace: target.namespace, redis: { ...target.redis, port: proxy.port }, protocol,
      runtime: { closeGraceMs: 0 } });
    let invoked = 0;
    queue.define(contract, { execute(ctx) { invoked++; return ctx.end(); } });
    try {
      const ready = assert.rejects(queue.ready(), { code: 'QUEUE_CLOSING' });
      await waitFor(() => proxy.held.length === 1);
      const closing = queue.close(); await delay(50); proxy.release();
      await ready; assert.equal((await closing).status, 'closed');
      assert.equal(invoked, 0);
      assert.equal(proxy.requests.some(request => request.op === 'claim'), false);
      assert.equal(await target.client.hGet(`qb:batch:v1:{${target.namespace}}:capacity`, 'memberCount'), '0');
      assert.equal((await target.keys()).filter(key => key.includes(':runtime:')).length, 0);
    } finally { proxy.release(); await queue.close(); await proxy.stop(); }
  });
});
