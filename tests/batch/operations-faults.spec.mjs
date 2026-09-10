import assert from 'node:assert/strict';
import { test } from 'node:test';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { createBatchQueue } from '../../.temp/batch/index.js';
import { withRedis } from './helpers.mjs';
import { startProxy, waitFor } from './redis-harness.mjs';

const uid = () => randomBytes(16).toString('hex');
const input = (run, extra = {}) => ({ runId: run.runId, expectedRevision: run.revision, reason: 'race', commandId: uid(), ...extra });
const definition = (name, policy) => normalizeDefinition({ name, version: '1', events: [], ...(policy ? { policy } : {}) }, { execute: ctx => ctx.end() }, 'all');
const open = async (target, protocol) => {
  const config = normalizeOptions({ namespace: target.namespace, redis: target.redis, ...(protocol ? { protocol } : {}) });
  const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
  try { await store.ready(); return { connection, store }; } catch (error) { await connection.close(); throw error; }
};
const expectRace = results => {
  for (const result of results) if (result.status === 'rejected') {
    assert(['REVISION_CONFLICT', 'LEASE_LOST'].includes(result.reason.code), String(result.reason));
  }
};
// DUMP is an RDB serialization, not a canonical digest: a hash lookup can advance internal rehashing.
// Compare all logical fields/indices and require zero calls to the mutation primitives as well.
async function logicalSnapshot(target) {
  const snapshot = {};
  for (const key of await target.keys()) {
    const type = await target.client.type(key);
    const value = type === 'hash' ? await target.client.hGetAll(key) : type === 'zset'
      ? await target.client.sendCommand(['ZRANGE', key, '0', '-1', 'WITHSCORES']) : type === 'string' ? await target.client.get(key) : assert.fail(`Unexpected fixture type ${type}`);
    snapshot[key] = { type, value, ttl: await target.client.pTTL(key) };
  }
  const stats = await target.client.info('commandstats');
  const writes = Object.fromEntries(['hset', 'hdel', 'del', 'unlink', 'set', 'zadd', 'zrem', 'expire', 'pexpire'].map(command =>
    [command, Number(new RegExp(`^cmdstat_${command}:calls=(\\d+)`, 'm').exec(stats)?.[1] ?? 0)]));
  return { snapshot, writes };
}

test('operations: concurrently submitted pause/resume/cancel versus claim/renew/next/failure preserve one transition and funded ownership', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const { connection, store } = await open(target); const member = uid(); const evidence = [];
    const cases = [];
    for (const operation of ['pause', 'resume', 'cancel']) for (const execution of ['claim', 'renew', 'next', 'business']) {
      for (const order of ['control-first', 'execution-first']) cases.push({ operation, execution, order, definition: definition(`race-${cases.length}`) });
    }
    try {
      await store.registerRuntime(member, 1, cases.map(value => value.definition));
      for (const item of cases) {
        const { operation, execution, order, definition: def } = item;
        const { runId } = await store.start(def, { query: null });
        const lease = execution === 'claim' ? null : await store.claim(def, member, 1);
        if (operation === 'resume') await store.control('pause', input(await store.get(runId)));
        const before = await store.get(runId); const command = input(before);
        const control = () => store.control(operation, command);
        const execute = () => execution === 'claim' ? store.claim(def, member, 1)
          : execution === 'renew' ? store.renew(lease) : store.settle(lease, { kind: execution });
        // Both calls are active before either is awaited; the server, not the test, chooses their linearization.
        const results = order === 'control-first' ? await Promise.allSettled([control(), execute()])
          : (await Promise.allSettled([execute(), control()])).reverse();
        expectRace(results);
        const [controlled, executed] = results; const after = await store.get(runId);
        assert(after.dispatchCount <= 1); assert(after.businessFailures <= 1); assert(after.page <= 2);
        if (controlled.status === 'fulfilled') {
          assert.equal(controlled.value.kind, 'applied');
          if (operation === 'cancel') assert.equal(after.status, 'cancelled');
          if (operation === 'pause') assert(['paused', 'pausing'].includes(after.status));
          assert.deepEqual(await store.control(operation, command), controlled.value, 'same request remains replayable after the racing mutation');
        } else assert.equal(controlled.reason.code, 'REVISION_CONFLICT');
        if (executed.status === 'fulfilled' && ['next', 'business'].includes(execution)) {
          assert.equal(execution === 'next' ? after.page : after.businessFailures, execution === 'next' ? 2 : 1);
        }
        const raw = await target.client.hGetAll(store.keys.run(runId));
        assert.equal(Number(raw.reservationBytes), raw.token === '' ? 0 : 131072);
        assert.equal(Number(await target.client.hGet(store.keys.capacity, 'reservedBytes')), Number(raw.reservationBytes));
        evidence.push({ operation, execution, order, result: results.map(value => value.status === 'fulfilled' ? 'fulfilled' : value.reason.code), status: after.status });
        await store.cancel(runId);
        assert.equal(await target.client.hGet(store.keys.capacity, 'reservedBytes'), '0');
      }
      assert.equal(evidence.length, 24);
      await writeFile(resolve(target.directory, 'operator-races.json'), JSON.stringify(evidence, null, 2));
    } finally { await connection.close(); }
  });
});

test('operations: real expiry recovery races with all three controls without double recovery or double refund', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const { connection, store } = await open(target, { lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10 } });
    const definitions = ['pause', 'resume', 'cancel'].map(value => definition(`recover-${value}`)); const member = uid();
    try {
      await store.registerRuntime(member, 1, definitions); const cases = [];
      for (const [index, operation] of ['pause', 'resume', 'cancel'].entries()) {
        const { runId } = await store.start(definitions[index], { query: null });
        const lease = await store.claim(definitions[index], member, 1);
        if (operation === 'resume') await store.control('pause', input(await store.get(runId)));
        cases.push({ runId, lease, operation, command: input(await store.get(runId)) });
      }
      await delay(3100);
      const clock = await target.client.sendCommand(['TIME']); const now = Number(clock[0]) * 1000 + Math.floor(Number(clock[1]) / 1000);
      for (const item of cases) {
        assert(now >= item.lease.deadline);
        const results = await Promise.allSettled([store.control(item.operation, item.command), store.recover(item.runId)]);
        expectRace(results);
        const after = await store.get(item.runId);
        assert(after.recoveries <= 1); assert.equal(after.businessFailures, 0); assert.equal(after.dispatchCount, 1);
        if (results[0].status === 'fulfilled' && item.operation === 'cancel') assert.equal(after.status, 'cancelled');
        assert.equal((await store.recover(item.runId)).changed, false);
        await assert.rejects(store.settle(item.lease, { kind: 'end' }), { code: 'LEASE_LOST' });
        assert.equal(await target.client.hGet(store.keys.run(item.runId), 'reservationBytes'), '0');
      }
      assert.equal(await target.client.hGet(store.keys.capacity, 'reservedBytes'), '0');
    } finally { await connection.close(); }
  });
});

test('operations: recovery_exhausted resume clears only its consecutive budget and preserves frozen due and all business counters', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const { connection, store } = await open(target, { lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10, recoveryLimit: 1 } });
    const def = definition('budget', { attempts: 3, backoff: { baseMs: 1, maxMs: 1 } }); let member = uid();
    try {
      await store.registerRuntime(member, 1, [def]); const { runId } = await store.start(def, { query: { original: true } });
      await store.settle(await store.claim(def, member, 1), { kind: 'business' });
      const lease = await waitFor(() => store.claim(def, member, 1)); await delay(3100);
      assert.equal((await store.recover(runId)).changed, true);
      const exhausted = await store.get(runId); assert.equal(exhausted.reason, 'recovery_exhausted');
      assert.equal(exhausted.consecutiveRecoveries, 1); assert.equal(exhausted.businessFailures, 1);
      member = uid(); await store.registerRuntime(member, 1, [def]);
      await store.control('resume', input(exhausted)); const resumed = await store.get(runId);
      assert.equal(resumed.consecutiveRecoveries, 0); assert.equal(resumed.status, 'retrying');
      for (const field of ['runId', 'page', 'dispatchCount', 'businessFailures', 'batchFailures', 'scheduledRetries', 'recoveries', 'dueAt']) {
        assert.equal(resumed[field], exhausted[field], field);
      }
      assert.deepEqual(resumed.query, exhausted.query); assert.deepEqual(resumed.effectivePolicy, exhausted.effectivePolicy);
      await assert.rejects(store.settle(lease, { kind: 'end' }), { code: 'LEASE_LOST' });
      const paused = await store.control('pause', input(resumed)); await store.control('resume', { ...input(resumed), expectedRevision: paused.revision });
      const again = await store.get(runId); assert.equal(again.dueAt, exhausted.dueAt); assert.equal(again.businessFailures, 1); assert.equal(again.recoveries, 1);
    } finally { await connection.close(); }
  });
});

test('operations: lost control replies expose original command identity; exact retry finds one receipt and conflicting text never mutates', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const proxy = await startProxy(target, { control: 3 }); let connection;
    try {
      const opened = await open({ ...target, redis: { ...target.redis, port: proxy.port } }); connection = opened.connection; const { store } = opened;
      const { runId } = await store.start(definition('lost-control'), { query: null });
      const request = input(await store.get(runId), { commandId: 'raw-\ud800', reason: 'full original / \udfff' });
      await assert.rejects(store.control('pause', request), { code: 'OUTCOME_UNKNOWN', outcomeKnown: false, runId, commandId: request.commandId });
      assert.equal((await store.get(runId)).revision, 2);
      const replay = await store.control('pause', request); assert.equal(replay.kind, 'applied'); assert.equal(replay.revision, 2);
      const before = await target.snapshot();
      await assert.rejects(store.control('cancel', request), { code: 'COMMAND_CONFLICT' });
      await assert.rejects(store.control('pause', { ...request, reason: request.reason + 'different tail' }), { code: 'COMMAND_CONFLICT' });
      assert.deepEqual(await target.snapshot(), before);
      const ring = JSON.parse(await target.client.hGet(store.keys.run(runId), 'controlRing')); assert.equal(ring.length, 1);
      assert.equal(proxy.dropped.length, 3); assert.equal(new Set(proxy.requests.filter(value => value.op === 'control').slice(0, 4).map(value => value.canonical)).size, 1);
      assert.equal(await target.client.hGet(store.keys.meta, 'status'), 'ready');
    } finally { await connection?.close(); await proxy.stop(); }
  });
});

test('operations: exhausted read-response retries fail explicitly without freezing metadata or returning empty snapshots', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const proxy = await startProxy(target, { getMetadata: 3, listPrepare: 3, capacitySnapshot: 3, healthSample: 3 }); let connection;
    try {
      const opened = await open({ ...target, redis: { ...target.redis, port: proxy.port } }); connection = opened.connection; const { store } = opened;
      const { runId } = await store.start(definition('reads'), { query: null }); const before = await logicalSnapshot(target);
      for (const read of [() => store.getMetadata(runId), () => store.listRuns(), () => store.capacitySnapshot(), () => store.healthSample()]) {
        await assert.rejects(read(), { code: 'CONNECTION_UNAVAILABLE' });
        assert.deepEqual(await logicalSnapshot(target), before);
      }
      assert.equal(proxy.dropped.length, 12); assert.equal((await store.getMetadata(runId)).runId, runId);
      assert.equal((await store.capacitySnapshot()).counts.runs, 1);
    } finally { await connection?.close(); await proxy.stop(); }
  });
});

test('operations: definition GC racing a new start preserves the new reference and never deletes its catalog', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const { connection, store } = await open(target); const def = definition('gc-race'); const member = uid();
    try {
      await store.registerRuntime(member, 1, [def]); await store.unregisterRuntime(member, 1);
      const results = await Promise.all([store.gcDefinition(def.identity), store.start(def, { query: null })]);
      const run = await store.get(results[1].runId); assert.equal(run.status, 'pending');
      assert.equal(await target.client.hGet(store.keys.definition(def.identity), 'runRefs'), '1');
      assert.notEqual(await target.client.zScore(store.keys.definitions, def.identity), null);
      assert.equal((await store.gcDefinition(def.identity)).changed, false);
      assert.equal((await store.capacitySnapshot()).counts.definitions, 1);
    } finally { await connection.close(); }
  });
});

test('operations: stable missing maintenance entities fail explicitly, unlike a legitimate concurrent exact GC', { timeout: 90000 }, async () => {
  for (const broken of ['member', 'due-run', 'gc-run', 'definition']) await withRedis(async target => {
    const { connection, store } = await open(target); const def = definition(`broken-${broken}`); const member = uid();
    try {
      if (broken === 'member') {
        await store.registerRuntime(member, 1, [def]); await target.client.del(store.keys.runtime(member));
        await target.client.zAdd(store.keys.members, { score: 1, value: member });
        await assert.rejects(store.reclaimExpiredMembers(), { code: 'INDEX_INCONSISTENT' });
      } else if (broken === 'definition') {
        await store.registerRuntime(member, 1, [def]); await store.unregisterRuntime(member, 1);
        await target.client.del(store.keys.definition(def.identity));
        await assert.rejects(store.gcDefinition(def.identity), { code: 'INDEX_INCONSISTENT' });
      } else {
        const { runId } = await store.start(def, { query: null });
        if (broken === 'gc-run') { await store.cancel(runId); await target.client.zAdd(store.keys.gcRuns, { score: 1, value: runId }); }
        await target.client.del(store.keys.run(runId));
        await assert.rejects(broken === 'gc-run' ? store.gcRun(runId) : store.maintain(), { code: 'INDEX_INCONSISTENT' });
      }
    } finally { await connection.close(); }
  });
});

test('operations: repeated public operator and failing telemetry return owned resources within fixed pressure bounds', { timeout: 90000 }, async () => {
  assert.equal(typeof global.gc, 'function', 'This qualification requires node --expose-gc');
  await withRedis(async target => {
    const options = { namespace: target.namespace, redis: target.redis, runtime: { mode: 'producer' }, telemetry: { sink() { throw Error('observation'); } } };
    const warmup = createBatchQueue(options); await warmup.ready(); await warmup.close();
    const sample = async () => {
      await delay(100); global.gc(); const resources = {};
      for (const name of process.getActiveResourcesInfo()) resources[name] = (resources[name] ?? 0) + 1;
      return { ...process.memoryUsage(), resources,
        listeners: Object.fromEntries(process.eventNames().map(name => [String(name), process.listenerCount(name)])),
        clients: (await target.client.sendCommand(['CLIENT', 'LIST'])).trim().split('\n').length };
    };
    const before = await sample(); const samples = [];
    for (let iteration = 0; iteration < 20; iteration++) {
      const queue = createBatchQueue(options); const task = queue.define({ name: 'operator-pressure', version: '1', events: [] });
      try {
        await queue.ready(); const { runId } = await task.start({ query: { iteration } });
        const metadata = await queue.operator.runs.getMetadata(runId);
        const paused = await queue.operator.runs.pause(input(metadata));
        const resumed = await queue.operator.runs.resume(input({ runId, revision: paused.revision }));
        await queue.operator.runs.cancel(input({ runId, revision: resumed.revision }));
        assert.equal((await queue.operator.runs.list({ taskName: 'operator-pressure', limit: 1 })).items[0].runId, runId);
        assert.equal((await queue.operator.capacity.snapshot()).origin, 'redis');
        assert.equal((await queue.operator.health.snapshot()).origin, 'redis');
        assert(queue.operator.metrics.snapshot().telemetry.sinkFailures > 0);
      } finally { await queue.close(); }
      if ((iteration + 1) % 5 === 0) samples.push(await sample());
    }
    await delay(500); const after = await sample();
    const limits = { heapGrowthBytes: 8 * 1024 * 1024, rssGrowthBytes: 64 * 1024 * 1024, lateHeapGrowthBytes: 2 * 1024 * 1024 };
    await writeFile(resolve(target.directory, 'operator-pressure.json'), JSON.stringify({ iterations: 20, cooldownMs: 500, before, samples, after, limits }, null, 2));
    assert.equal(after.clients, before.clients); assert.deepEqual(after.listeners, before.listeners);
    for (const name of new Set([...Object.keys(before.resources), ...Object.keys(after.resources)])) {
      if (/Socket|Timeout|TCP/i.test(name)) assert((after.resources[name] ?? 0) <= (before.resources[name] ?? 0), name);
    }
    assert(after.heapUsed - before.heapUsed <= limits.heapGrowthBytes); assert(after.rss - before.rss <= limits.rssGrowthBytes);
    assert(samples.at(-1).heapUsed - samples.at(-3).heapUsed <= limits.lateHeapGrowthBytes);
  });
});
