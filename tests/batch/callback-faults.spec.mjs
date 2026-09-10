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
import { startWorker, startProxy, waitFor } from './redis-harness.mjs';
const uid = () => randomBytes(16).toString('hex');
const contract = { name: 'callback-fault', version: '1', events: ['success'] };
const protocol = { callback: { attempts: 2, timeoutMs: 10000, backoff: { baseMs: 1, maxMs: 1 } }, lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10 } };

test('callback fixtures: startup port collisions retry without connecting to or stopping the existing owner and never replay the test action', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const retries = []; let calls = 0;
    await withRedis(async another => {
      calls++; assert.notEqual(another.port, target.port); assert.equal(await target.client.ping(), 'PONG');
    }, { firstPort: target.port, onPortRetry: info => { retries.push(info); } });
    assert.equal(calls, 1); assert.equal(retries.length, 1); assert.equal(retries[0].port, target.port);
    assert.equal(await target.client.ping(), 'PONG', 'the original test-owned Redis must remain alive');
  });
});

test('callback faults: two real Node workers deliver one Event without repeating execution', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const producer = createBatchQueue({ namespace: target.namespace, redis: target.redis, protocol, runtime: { mode: 'producer' } });
    const task = producer.define(contract); const workers = [];
    try {
      await producer.ready(); for (let i = 0; i < 2; i++) workers.push(await startWorker(target, { protocol, callback: true, behavior: 'finish' }));
      const { runId } = await task.start({ query: { delayMs: 150 } });
      const run = await waitFor(async () => { const value = await task.get(runId); return value.callbacks.delivered === 1 && value; }, 5000);
      assert.deepEqual([run.dispatchCount, run.businessFailures], [1, 0]);
      assert.equal(workers.flatMap(worker => worker.messages).filter(message => message.kind === 'callback-entered').length, 1);
      assert.notEqual(workers[0].pid, workers[1].pid);
    } finally { for (const worker of workers) await worker.stop(); await producer.close(); }
  });
});

test('callback faults: only the callback owner is killed; the next process consumes attempt two without re-running the Batch', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const producer = createBatchQueue({ namespace: target.namespace, redis: target.redis, protocol, runtime: { mode: 'producer' } });
    const task = producer.define(contract); let owner; let survivor;
    try {
      await producer.ready(); owner = await startWorker(target, { protocol, callback: true, behavior: 'hold' });
      const { runId } = await task.start({ query: null });
      const entered = await waitFor(() => owner.messages.find(message => message.kind === 'callback-entered'), 5000);
      survivor = await startWorker(target, { protocol, callback: true, behavior: 'finish' }); await owner.stop(true);
      const run = await waitFor(async () => { const value = await task.get(runId); return value.callbacks.delivered === 1 && value; }, 15000);
      assert.deepEqual([run.dispatchCount, run.businessFailures, run.recoveries], [1, 0, 0]);
      const resumed = survivor.messages.find(message => message.kind === 'callback-entered');
      assert.deepEqual([resumed.eventId, resumed.deliveryAttempt], [entered.eventId, 2]);
    } finally { await owner?.stop(); await survivor?.stop(); await producer.close(); }
  });
});

test('callback faults: committed TCP reply loss replays each identical grant, renewal, settlement and public replay exactly once', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const proxy = await startProxy(target, { claimEvent: 1, renewEvent: 1, settleEvent: 1, replayEvent: 1 });
    const config = normalizeOptions({ namespace: target.namespace, redis: { ...target.redis, port: proxy.port }, protocol: { callback: { attempts: 1 } } });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition(contract, { execute: ctx => ctx.end(), onSuccess() {} }, 'all'); const member = uid();
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]); const { runId } = await store.start(definition, { query: null });
      await store.settle(await store.claim(definition, member, 1), { kind: 'end' }); const eventId = `${runId}:1:success`;
      const lease = await store.claimEvent(eventId, member, 1); assert.equal(lease.event.deliveryAttempt, 1);
      await store.settleEvent(await store.renewEvent(lease), { kind: 'failure' });
      const revision = (await store.getEvent(eventId)).revision;
      const replay = await store.replayEvent({ eventId, expectedRevision: revision, reason: 'lost reply', commandId: uid() }); assert.equal(replay.replayGeneration, 1);
      await store.settleEvent(await store.claimEvent(eventId, member, 1), { kind: 'success' });
      assert.deepEqual((await store.get(runId)).callbacks, { pending: 0, delivered: 1, deadLetters: 0 });
      assert.equal((await store.get(runId)).dispatchCount, 1);
      assert.deepEqual(proxy.dropped.map(item => item.op), ['claimEvent', 'renewEvent', 'settleEvent', 'replayEvent']);
      for (const item of proxy.dropped) {
        const requests = proxy.requests.filter(value => value.commandId === item.commandId);
        assert(requests.length >= 2); assert.equal(new Set(requests.map(value => value.canonical)).size, 1);
      }
    } finally { await connection.close(); await proxy.stop(); }
  });
});

test('callback faults: exhausted claim confirmation reports uncertainty, consumes one permit and leaves metadata ready', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const proxy = await startProxy(target, { claimEvent: 10 });
    const config = normalizeOptions({ namespace: target.namespace, redis: { ...target.redis, port: proxy.port } });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition(contract, { execute: ctx => ctx.end(), onSuccess() {} }, 'all'); const member = uid();
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]); const { runId } = await store.start(definition, { query: null });
      await store.settle(await store.claim(definition, member, 1), { kind: 'end' }); const eventId = `${runId}:1:success`;
      await assert.rejects(store.claimEvent(eventId, member, 1), error => error.code === 'OUTCOME_UNKNOWN' && !error.outcomeKnown && error.runId === runId);
      const event = await store.getEvent(eventId); assert.equal(event.deliveryAttempt, 1); assert.equal(event.status, 'delivering');
      assert.equal(proxy.dropped.length, 3); assert.equal(await target.client.hGet(store.keys.meta, 'status'), 'ready');
      assert.equal((await store.get(runId)).dispatchCount, 1);
    } finally { await connection.close(); await proxy.stop(); }
  });
});

test('callback faults: real runtime crosses E without heartbeat revoking a frozen grant; reconnect cannot extend that grant', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const policy = { ...protocol, callback: { attempts: 1, timeoutMs: 10000 } };
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis, protocol: policy });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition(contract, { execute: ctx => ctx.end(), onSuccess() {} }, 'all'); const member = uid();
    const proxy = await startProxy(target, { renewRuntime: 1 }); let consumer; let signal; let entered = false;
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]); const { runId } = await store.start(definition, { query: null });
      await store.settle(await store.claim(definition, member, 1), { kind: 'end' }); const eventId = `${runId}:1:success`;
      await store.settleEvent(await store.claimEvent(eventId, member, 1), { kind: 'failure' });
      await store.replayEvent({ eventId, expectedRevision: (await store.getEvent(eventId)).revision, reason: 'drain', commandId: uid() });
      const parts = await target.client.sendCommand(['TIME']); const end = Number(parts[0]) * 1000 + Math.floor(Number(parts[1]) / 1000) + 1400;
      await target.client.hSet(store.keys.eventById(eventId), { firstDeadAt: String(end - 30 * 86400000), deadLetterExpiresAt: String(end) });
      await target.client.zAdd(store.keys.gcEvents, { score: end, value: eventId });
      consumer = createBatchQueue({ namespace: target.namespace, redis: { ...target.redis, port: proxy.port }, protocol: policy, runtime: { mode: 'consumer' } });
      consumer.define(contract, { execute: ctx => ctx.end(), async onSuccess(ctx) { entered = true; signal = ctx.signal; await delay(1800); assert.equal(signal.aborted, false); } });
      await consumer.ready(); await waitFor(() => entered, 1000);
      const grant = await store.getEvent(eventId); assert.equal(grant.replayDrainDeadline, grant.lease.deadline);
      await delay(1550); assert.equal(signal.aborted, false);
      assert.equal((await store.getEvent(eventId)).replayDrainDeadline, grant.replayDrainDeadline);
      await waitFor(async () => (await store.getEvent(eventId))?.status === 'delivered', 1000);
      assert.equal(proxy.requests.filter(item => item.op === 'renewEvent').length, 0, 'frozen runtime performs no ineffective renewal after E');
      assert.deepEqual(proxy.dropped.map(item => item.op), ['renewRuntime'], 'a committed membership reply loss forced a real TCP reconnect during the frozen grant');
      assert.equal((await store.get(runId)).dispatchCount, 1);
    } finally { await consumer?.close(); await connection.close(); await proxy.stop(); }
  });
});
