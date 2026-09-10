import assert from 'node:assert/strict';
import { test } from 'node:test';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { normalizeReplay, encodeReplayReceipt } from '../../.temp/batch/domain/events.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { withRedis } from './helpers.mjs';
import { waitFor } from './redis-harness.mjs';

const uid = () => randomBytes(16).toString('hex');
const command = (eventId, expectedRevision, extra = {}) => ({ eventId, expectedRevision, reason: 'replay', commandId: uid(), ...extra });
async function scenario(action, callback = {}) {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis, protocol: {
      callback: { attempts: 1, timeoutMs: 10000, backoff: { baseMs: 1, maxMs: 1 }, ...callback }, lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10 } } });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'replay', version: '1', events: ['batchSettled', 'success'] },
      { execute: ctx => ctx.end(), onBatchSettled() {}, onSuccess() {} }, 'all'); const member = uid();
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]);
      const { runId } = await store.start(definition, { query: { original: '\ud800' } });
      await store.settle(await store.claim(definition, member, 1), { kind: 'end' });
      const eventId = `${runId}:1:batchSettled`; const nextId = `${runId}:1:success`;
      while ((await store.getEvent(eventId)).status !== 'dead_letter') {
        await store.settleEvent(await waitFor(() => store.claimEvent(eventId, member, 1)), { kind: 'failure' });
      }
      await action({ target, store, definition, member, runId, eventId, nextId });
    } finally { await connection.close(); }
  });
}
async function expiry(target, store, eventId, ms) {
  const parts = await target.client.sendCommand(['TIME']); const now = Number(parts[0]) * 1000 + Math.floor(Number(parts[1]) / 1000);
  const end = now + ms;
  // Only retention dates are shortened in our own fixture; grants and cutoff decisions use real Redis TIME.
  await target.client.hSet(store.keys.eventById(eventId), { firstDeadAt: String(end - 30 * 86400000), deadLetterExpiresAt: String(end) });
  await target.client.zAdd(store.keys.gcEvents, { score: end, value: eventId }); return end;
}

test('replay: full parent Run ring precedes Event CAS and conflicts across operation, target and raw Unicode', { timeout: 90000 }, async () => {
  await scenario(async ({ store, target, eventId, nextId, runId, member }) => {
    const before = await store.getEvent(eventId);
    const input = command(eventId, before.revision, { reason: ' /\ud800\n', commandId: 'shared/\udfff' });
    const applied = await store.replayEvent(input); assert.equal(applied.kind, 'applied'); assert.equal(applied.replayGeneration, 1);
    const ring = JSON.parse(await target.client.hGet(store.keys.run(runId), 'controlRing')); assert.equal(ring.length, 1);
    assert.equal(JSON.stringify(ring[0]), encodeReplayReceipt(normalizeReplay(input).canonical, applied, ring[0].recordedAt));
    assert.deepEqual(await store.replayEvent(input), applied);
    await assert.rejects(store.replayEvent({ ...input, eventId: nextId }), { code: 'COMMAND_CONFLICT' });
    await assert.rejects(store.replayEvent({ ...input, reason: input.reason + 'tail' }), { code: 'COMMAND_CONFLICT' });
    await assert.rejects(store.control('cancel', { runId, expectedRevision: (await store.get(runId)).revision, reason: input.reason, commandId: input.commandId }), { code: 'COMMAND_CONFLICT' });
    await assert.rejects(store.replayEvent(command(eventId, before.revision)), { code: 'REVISION_CONFLICT' });
    const noop = await store.replayEvent(command(eventId, applied.revision)); assert.equal(noop.kind, 'noop');
    const normal = await store.claimEvent(nextId, member, 1); assert(normal);
    assert.equal(await store.claimEvent(eventId, member, 1), null, 'replay and normal share the Run lock');
    await store.settleEvent(normal, { kind: 'success' });
    const lease = await store.claimEvent(eventId, member, 1); assert.equal(lease.event.deliveryAttempt, 1);
    assert.equal(lease.event.lateReplay, true); assert.deepEqual(lease.event.query, before.query);
    await store.settleEvent(lease, { kind: 'failure' }); const after = await store.getEvent(eventId);
    assert.deepEqual([after.firstDeadAt, after.deadLetterExpiresAt, after.firstDeadLetterSequence], [before.firstDeadAt, before.deadLetterExpiresAt, before.firstDeadLetterSequence]);
    assert.equal(await target.client.hGet(store.keys.run(runId), 'normalEventCursor'), '2');
    assert.deepEqual((await store.get(runId)).callbacks, { pending: 0, delivered: 1, deadLetters: 1 });
    const absent = `${uid()}:1:success`; assert.deepEqual(await store.replayEvent(command(absent, 1)), { kind: 'not_found', id: absent });
  });
});

test('replay: complete receipt overflow and hard unfinished cap reject without writes; first-dead sequence exhaustion cannot block old replay', { timeout: 90000 }, async () => {
  await scenario(async ({ store, target, eventId, runId, member, nextId }) => {
    const before = await store.getEvent(eventId); const raw = await target.client.hGetAll(store.keys.eventById(eventId));
    await assert.rejects(store.replayEvent(command(eventId, before.revision, { reason: '\u0000'.repeat(300) })), { code: 'CONTROL_RECORD_TOO_LARGE' });
    assert.deepEqual(await target.client.hGetAll(store.keys.eventById(eventId)), raw);
    assert.equal(await target.client.hGet(store.keys.run(runId), 'controlRing'), '[]');
    const count = await target.client.hGet(store.keys.capacity, 'unfinishedEventCount');
    await target.client.hSet(store.keys.capacity, 'unfinishedEventCount', String(store.config.protocol.limits.unfinishedEventMax));
    await assert.rejects(store.replayEvent(command(eventId, before.revision)), { code: 'CAPACITY_EXCEEDED' });
    assert.deepEqual(await target.client.hGetAll(store.keys.eventById(eventId)), raw);
    await target.client.hSet(store.keys.capacity, 'unfinishedEventCount', count);
    await target.client.hSet(store.keys.meta, 'deadLetterSequence', String(Number.MAX_SAFE_INTEGER));
    await store.replayEvent(command(eventId, before.revision));
    await store.settleEvent(await store.claimEvent(eventId, member, 1), { kind: 'failure' });
    assert.equal((await store.getEvent(eventId)).firstDeadLetterSequence, 1);
    const normal = await store.claimEvent(nextId, member, 1);
    await assert.rejects(store.settleEvent(normal, { kind: 'failure' }), { code: 'SEQUENCE_EXHAUSTED' });
    await store.settleEvent(normal, { kind: 'success' });
  });
});

test('replay: claim crossing E freezes once, heartbeat never extends, E rejects renewal but original live grant can settle', { timeout: 90000 }, async () => {
  await scenario(async ({ store, target, eventId, member }) => {
    await store.replayEvent(command(eventId, (await store.getEvent(eventId)).revision));
    const end = await expiry(target, store, eventId, 1100);
    let lease = await store.claimEvent(eventId, member, 1); assert(lease.deadline >= end);
    assert.equal(lease.event.replayDrainDeadline, lease.deadline); const frozen = lease.deadline;
    await delay(150); lease = await store.renewEvent(lease); assert.equal(lease.deadline, frozen);
    await delay(1050); await assert.rejects(store.renewEvent(lease), { code: 'LEASE_LOST' });
    assert.equal((await store.recoverEvent(eventId)).changed, false);
    assert.equal((await store.gcEvent(eventId)).changed, false, 'GC postpones to the original valid grant');
    assert.equal(Number(await target.client.zScore(store.keys.gcEvents, eventId)), frozen);
    await store.settleEvent(lease, { kind: 'success' });
    assert.equal(Number(await target.client.zScore(store.keys.gcEvents, eventId)), end);
    assert.equal((await store.gcEvent(eventId)).changed, true); assert.equal(await store.getEvent(eventId), null);
  });
});

test('replay: pre-E renewal can first freeze a crossing grant; after E a failed attempt never retries and late owners lose', { timeout: 90000 }, async () => {
  await scenario(async ({ store, target, eventId, member }) => {
    await store.replayEvent(command(eventId, (await store.getEvent(eventId)).revision));
    const end = await expiry(target, store, eventId, 3500);
    let lease = await store.claimEvent(eventId, member, 1); assert.equal(lease.event.replayDrainDeadline, null);
    await delay(750); lease = await store.renewEvent(lease); assert(lease.deadline >= end);
    assert.equal((await store.getEvent(eventId)).replayDrainDeadline, lease.deadline);
    await delay(3100); await assert.rejects(store.settleEvent(lease, { kind: 'success' }), { code: 'LEASE_LOST' });
    assert.equal(await store.claimEvent(eventId, member, 1), null); assert.equal((await store.recoverEvent(eventId)).changed, false);
    assert.equal((await store.gcEvent(eventId)).changed, true); assert.equal(await store.getEvent(eventId), null);
  });
});

test('replay: failure after E ends the generation even when callback permits remain; ordinary timeout never borrows the frozen drain extension', { timeout: 90000 }, async () => {
  await scenario(async ({ store, target, eventId, member }) => {
    await store.replayEvent(command(eventId, (await store.getEvent(eventId)).revision));
    await expiry(target, store, eventId, 600);
    const lease = await store.claimEvent(eventId, member, 1); assert.equal(lease.event.deliveryAttempt, 1);
    await delay(750); await store.settleEvent(lease, { kind: 'failure' });
    const dead = await store.getEvent(eventId); assert.equal(dead.status, 'dead_letter'); assert.equal(dead.deliveryAttempt, 1);
    assert.equal(dead.dueAt, null); assert.equal(await target.client.zScore(store.keys.dueReplays, eventId), null);
    assert.equal(await store.claimEvent(eventId, member, 1), null); assert.equal((await store.recoverEvent(eventId)).changed, false);
  }, { attempts: 3 });
  await scenario(async ({ store, target, eventId, member }) => {
    await store.replayEvent(command(eventId, (await store.getEvent(eventId)).revision));
    await expiry(target, store, eventId, 100);
    const lease = await store.claimEvent(eventId, member, 1); assert.equal(lease.deadline, lease.attemptTimeoutAt);
    assert.equal(lease.event.replayDrainDeadline, lease.attemptTimeoutAt);
    await delay(250); await assert.rejects(store.settleEvent(lease, { kind: 'success' }), { code: 'LEASE_LOST' });
    assert.equal((await store.gcEvent(eventId)).changed, true);
  }, { timeoutMs: 200 });
});
