import assert from 'node:assert/strict';
import { test } from 'node:test';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { withRedis } from './helpers.mjs';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';

test('real Redis: readiness requires observable noeviction before any namespace write', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis);
    const store = new BatchRedisStore(connection, config);
    try {
      await target.client.configSet('maxmemory-policy', 'allkeys-lru');
      await assert.rejects(store.ready(), { code: 'CONFIG_INVALID' });
      assert.deepEqual(await target.keys(), [], 'Rejected eviction policy must not initialize metadata');
      await target.client.configSet('maxmemory-policy', 'noeviction');
      const original = connection.command.bind(connection);
      connection.command = (args, context) => args[0] === 'INFO' && args[1] === 'memory'
        ? Promise.resolve('# Memory\r\n') : original(args, context);
      await assert.rejects(store.ready(), { code: 'CONFIG_INVALID' });
      assert.deepEqual(await target.keys(), [], 'Unobservable policy must fail closed');
      connection.command = original;
      await store.ready();
      assert.equal(await target.client.hGet(store.keys.meta, 'status'), 'ready');
    } finally { await connection.close(); }
  });
});

test('real Redis: orphan execution lease is rejected without repair or repeated silent no-op', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis);
    const store = new BatchRedisStore(connection, config);
    const runId = randomBytes(16).toString('hex');
    try {
      await store.ready();
      assert.deepEqual(await store.recover(runId), { changed: false });
      await target.client.zAdd(store.keys.leases, { score: 1, value: runId });
      const before = await target.snapshot();
      await assert.rejects(store.recoverExpired(), { code: 'INDEX_INCONSISTENT' });
      assert.deepEqual(await target.snapshot(), before, 'Recovery must not guess how to repair orphaned data');
    } finally { await connection.close(); }
  });
});

test('real Redis: JSON strings preserve lone UTF-16 units while business idempotency keys reject them', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'json-text', version: '1', events: ['success'] },
      { execute: ctx => ctx.end(), onSuccess() {} }, 'all');
    const member = randomBytes(16).toString('hex');
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]);
      const first = await store.start(definition, { query: { text: '\ud800', literalEscape: '\\ud800', pair: '😀' } });
      assert.equal((await store.get(first.runId)).query.text, '\ud800');
      const lease = await store.claim(definition, member, 1);
      await store.settle(lease, { kind: 'next', stateCanonical: JSON.stringify({ text: '\udfff' }) });
      const next = await store.claim(definition, member, 1);
      await store.settle(next, { kind: 'end' });
      assert.equal((await store.get(first.runId)).state.text, '\udfff');
      await assert.rejects(store.start(definition, { query: null, idempotencyKey: '\ud800' }));
    } finally { await connection.close(); }
  });
});

test('real Redis: retry budget, next reset, timeout and cancellation settle funded state only once', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis);
    const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'retry', version: '1', events: ['batchSettled', 'failure'],
      policy: { attempts: 2, backoff: { baseMs: 1, maxMs: 1 } } },
      { execute: ctx => ctx.end(), onBatchSettled() {}, onFailure() {} }, 'all');
    const timeout = normalizeDefinition({ name: 'timeout', version: '1', events: ['failure'], policy: { attempts: 1, timeoutMs: 1 } },
      { execute: ctx => ctx.end(), onFailure() {} }, 'all');
    const member = randomBytes(16).toString('hex');
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition, timeout]);
      const first = await store.start(definition, { query: null });
      let lease = await store.claim(definition, member, 1);
      await store.settle(lease, { kind: 'business' });
      let run = await store.get(first.runId);
      assert.deepEqual([run.status, run.businessFailures, run.batchFailures, run.scheduledRetries, run.callbacks.pending], ['retrying', 1, 1, 1, 0]);
      await delay(10);
      lease = await store.claim(definition, member, 1);
      await store.settle(lease, { kind: 'next', stateCanonical: '{"cursor":1}' });
      run = await store.get(first.runId);
      assert.deepEqual([run.page, run.businessFailures, run.batchFailures, run.consecutiveRecoveries], [2, 1, 0, 0]);
      assert.deepEqual(run.state, { cursor: 1 });
      lease = await store.claim(definition, member, 1);
      await store.settle(lease, { kind: 'business' }); await delay(10);
      lease = await store.claim(definition, member, 1);
      await store.settle(lease, { kind: 'business' });
      run = await store.get(first.runId);
      assert.deepEqual([run.status, run.businessFailures, run.batchFailures, run.scheduledRetries, run.callbacks.pending], ['failed', 3, 2, 2, 3]);
      const second = await store.start(timeout, { query: null });
      lease = await store.claim(timeout, member, 1); await delay(10);
      await store.settle(lease, { kind: 'end' });
      run = await store.get(second.runId);
      assert.equal(run.status, 'failed'); assert.equal(run.error.code, 'EXECUTION_TIMEOUT'); assert.equal(run.businessFailures, 1);
      const third = await store.start(definition, { query: null });
      lease = await store.claim(definition, member, 1);
      await store.cancel(third.runId);
      await assert.rejects(store.settle(lease, { kind: 'end' }), { code: 'LEASE_LOST' });
      assert.equal(await target.client.hGet(store.keys.capacity, 'reservedBytes'), '0');
      assert.equal(await target.client.zCard(store.keys.events(third.runId)), 0);
    } finally { await connection.close(); }
  });
});

test('real Redis: safe integer boundaries remain exact and exhaustion rejects before any write', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis);
    const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'safe', version: '1', events: ['batchSettled', 'success'] },
      { execute: ctx => ctx.end(), onBatchSettled() {}, onSuccess() {} }, 'all');
    const member = randomBytes(16).toString('hex');
    try {
      await store.ready();
      await store.registerRuntime(member, 1, [definition]);
      await target.client.hSet(store.keys.meta, 'createdSequence', String(Number.MAX_SAFE_INTEGER - 1));
      const first = await store.start(definition, { query: null });
      assert.equal(await target.client.hGet(store.keys.meta, 'createdSequence'), String(Number.MAX_SAFE_INTEGER));
      const before = await target.snapshot();
      await assert.rejects(store.start(definition, { query: null }), { code: 'SEQUENCE_EXHAUSTED' });
      assert.deepEqual(await target.snapshot(), before);
      await target.client.hSet(store.keys.run(first.runId), { page: String(Number.MAX_SAFE_INTEGER), leaseRevision: String(Number.MAX_SAFE_INTEGER - 1) });
      const lease = await store.claim(definition, member, 1);
      assert.equal(lease.run.page, Number.MAX_SAFE_INTEGER);
      assert.equal(JSON.parse(lease.tokenCanonical).leaseRevision, Number.MAX_SAFE_INTEGER);
      assert.equal(lease.run.batchId, `${first.runId}:${Number.MAX_SAFE_INTEGER}`);
      const leased = await target.snapshot();
      await assert.rejects(store.renew(lease), { code: 'SEQUENCE_EXHAUSTED' });
      await assert.rejects(store.settle(lease, { kind: 'next', stateCanonical: 'null' }), { code: 'SEQUENCE_EXHAUSTED' });
      assert.deepEqual(await target.snapshot(), leased);
      await store.settle(lease, { kind: 'end' });
      assert.deepEqual(await target.client.zRange(store.keys.events(first.runId), 0, -1),
        [`${first.runId}:${Number.MAX_SAFE_INTEGER}:batchSettled`, `${first.runId}:${Number.MAX_SAFE_INTEGER}:success`]);
    } finally { await connection.close(); }
  });
});

test('real Redis: identical transport replay after lost replies never repeats a state transition', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis);
    const store = new BatchRedisStore(connection, config);
    const original = connection.command.bind(connection);
    const replayed = [];
    connection.command = async (args, context) => {
      const result = await original(args, context);
      if (['EVAL', 'EVALSHA'].includes(args[0])) {
        const request = JSON.parse(args.at(-1));
        if (['start', 'claim', 'renew', 'settle', 'cancel'].includes(request.op) && result !== 'null') {
          replayed.push(request.op);
          // First response is deliberately withheld; repeat the exact bytes, not another public call.
          return original(args, context);
        }
      }
      return result;
    };
    const definition = normalizeDefinition({ name: 'replies', version: '1', events: ['success'] },
      { execute: ctx => ctx.end(), onSuccess() {} }, 'all');
    const member = randomBytes(16).toString('hex');
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]);
      const first = await store.start(definition, { query: null });
      assert.equal(first.created, true);
      const lease = await store.claim(definition, member, 1);
      assert.ok(lease, 'the confirmed claim must be recovered from the repeated request');
      const renewed = await store.renew(lease);
      const settled = await store.settle(renewed, { kind: 'end' });
      assert.equal(settled.status, 'success');
      assert.equal((await store.get(first.runId)).dispatchCount, 1);
      assert.equal(await target.client.zCard(store.keys.events(first.runId)), 1);
      assert.equal(await target.client.hGet(store.keys.capacity, 'reservedBytes'), '0');
      const second = await store.start(definition, { query: null });
      assert.equal((await store.cancel(second.runId)).changed, true);
      assert.deepEqual(new Set(replayed), new Set(['start', 'claim', 'renew', 'settle', 'cancel']));
    } finally { await connection.close(); }
  });
});

test('real Redis: two members cannot claim one Run and funded settlement creates two immutable Events', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis);
    const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'scan', version: '1', events: ['batchSettled', 'success'] },
      { execute: ctx => ctx.end(), onBatchSettled() {}, onSuccess() {} }, 'all');
    const members = [randomBytes(16).toString('hex'), randomBytes(16).toString('hex')];
    try {
      await store.ready();
      await Promise.all(members.map(member => store.registerRuntime(member, 1, [definition])));
      const start = await store.start(definition, { query: { pageSize: 10 } });
      const claims = await Promise.all(members.map(member => store.claim(definition, member, 1)));
      assert.equal(claims.filter(Boolean).length, 1);
      const lease = claims.find(Boolean);
      assert.equal(lease.run.runId, start.runId);
      assert.equal(lease.run.dispatchCount, 1);
      assert.equal(Number(await target.client.hGet(store.keys.capacity, 'reservedBytes')), 384 * 1024);
      const renewed = await store.renew(lease);
      await assert.rejects(store.settle(lease, { kind: 'end', stateCanonical: 'null' }), { code: 'LEASE_LOST' });
      await store.settle(renewed, { kind: 'end', stateCanonical: 'null' });
      const run = await store.get(start.runId);
      assert.equal(run.status, 'success');
      assert.equal(run.callbacks.pending, 2);
      assert.equal(Number(await target.client.hGet(store.keys.capacity, 'reservedBytes')), 0);
      const eventIds = await target.client.zRange(store.keys.events(start.runId), 0, -1);
      assert.deepEqual(eventIds, [`${start.runId}:1:batchSettled`, `${start.runId}:1:success`]);
      await assert.rejects(store.settle(renewed, { kind: 'end', stateCanonical: 'null' }), { code: 'LEASE_LOST' });
      assert.equal(await target.client.zCard(store.keys.events(start.runId)), 2);
      await Promise.all(members.map(member => store.unregisterRuntime(member, 1)));
      assert.equal(Number(await target.client.hGet(store.keys.capacity, 'memberCount')), 0);
    } finally { await connection.close(); }
  });
});

test('real Redis: schema isolation, idempotency, frozen input and cancellation', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis);
    const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'scan', version: '1', events: [] }, undefined, 'producer');
    try {
      await store.ready();
      const first = await store.start(definition, { query: { after: 0 }, idempotencyKey: '' });
      const repeated = await store.start(definition, { query: { after: 0 }, idempotencyKey: '' });
      assert.equal(first.created, true);
      assert.deepEqual(repeated, { runId: first.runId, created: false });
      await assert.rejects(store.start(definition, { query: { after: 1 }, idempotencyKey: '' }), { code: 'IDEMPOTENCY_CONFLICT' });
      const run = await store.get(first.runId);
      assert.equal(run.status, 'pending');
      assert.deepEqual(run.query, { after: 0 });
      assert.equal(run.state, null);
      assert.equal(run.page, 1);
      assert.equal(run.error, null);
      assert.equal(run.dispatchCount, 0);
      assert.deepEqual(await store.cancel(first.runId), { found: true, runId: first.runId, status: 'cancelled', revision: 2, changed: true });
      assert.equal((await store.cancel(first.runId)).changed, false);
      assert.equal((await store.get(first.runId)).status, 'cancelled');
      const keys = await target.keys();
      assert.ok(keys.every(key => key.startsWith(`qb:batch:v1:{${target.namespace}}:`)));
    } finally { await connection.close(); }
  });
});

test('real Redis: incompatible protocol and wrong key types produce no domain writes', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis);
    const store = new BatchRedisStore(connection, config);
    try {
      await store.ready();
      const second = new BatchRedisStore(connection, normalizeOptions({ namespace: target.namespace, redis: target.redis,
        protocol: { lease: { pollMs: 10 } } }));
      await assert.rejects(second.ready(), { code: 'SCHEMA_MISMATCH' });
      await target.client.set(`qb:batch:v1:{${target.namespace}}:idx:runs`, 'wrongtype-fixture');
      const before = await target.snapshot();
      const definition = normalizeDefinition({ name: 'scan', version: '1', events: [] }, undefined, 'producer');
      await assert.rejects(store.start(definition, { query: null }), { code: 'STORAGE_INCONSISTENT' });
      assert.deepEqual(await target.snapshot(), before);
    } finally { await connection.close(); }
  });
});

test('real Redis: missing meta with an orphan is not silently initialized', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis);
    const store = new BatchRedisStore(connection, config);
    try {
      await target.client.set(`qb:batch:v1:{${target.namespace}}:orphan`, 'retained');
      await assert.rejects(store.ready(), { code: 'NAMESPACE_ORPHANED' });
      assert.equal(await target.client.exists(`qb:batch:v1:{${target.namespace}}:meta`), 0);
    } finally { await connection.close(); }
  });
});
