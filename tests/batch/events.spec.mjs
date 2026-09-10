import assert from 'node:assert/strict';
import { test } from 'node:test';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { withRedis } from './helpers.mjs';
import { waitFor } from './redis-harness.mjs';

const uid = () => randomBytes(16).toString('hex');
async function scenario(action, protocol = {}) {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis, protocol });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'events', version: '1', events: ['batchSettled', 'success'] },
      { execute: ctx => ctx.end(), onBatchSettled() {}, onSuccess() {} }, 'all'); const member = uid();
    try { await store.ready(); await store.registerRuntime(member, 1, [definition]); await action({ target, store, definition, member }); }
    finally { await connection.close(); }
  });
}

test('events: normal sequence blocks successors, preserves historical snapshots, and terminal settlement creates exactly two Events', { timeout: 90000 }, async () => {
  await scenario(async ({ store, definition, member, target }) => {
    const { runId } = await store.start(definition, { query: { original: '\ud800' } });
    await store.settle(await store.claim(definition, member, 1), { kind: 'next', stateCanonical: '{"cursor":1}' });
    await store.settle(await store.claim(definition, member, 1), { kind: 'next', stateCanonical: '{"cursor":2}' });
    await store.settle(await store.claim(definition, member, 1), { kind: 'end' });
    const ids = [`${runId}:1:batchSettled`, `${runId}:2:batchSettled`, `${runId}:3:batchSettled`, `${runId}:3:success`];
    assert.equal(await target.client.zCard(store.keys.events(runId)), 4);
    const first = await store.getEvent(ids[0]); assert.deepEqual(first.state, { cursor: 1 }); assert.equal(first.query.original, '\ud800');
    assert.equal((await store.getEvent(ids[1])).state.cursor, 2);
    assert.equal(await store.claimEvent(ids[1], member, 1), null);
    for (const [index, eventId] of ids.entries()) {
      const lease = await store.claimEvent(eventId, member, 1); assert(lease); assert.equal(lease.event.deliveryAttempt, 1);
      assert.equal(lease.event.sequence, index + 1); assert.equal(await store.claimEvent(eventId, member, 1), null);
      const renewed = await store.renewEvent(lease); await store.settleEvent(renewed, { kind: 'success' });
      await assert.rejects(store.settleEvent(lease, { kind: 'success' }), { code: 'LEASE_LOST' });
      assert.equal((await store.getEvent(eventId)).status, 'delivered');
    }
    const run = await store.get(runId); assert.deepEqual(run.callbacks, { pending: 0, delivered: 4, deadLetters: 0 });
    assert.deepEqual([run.status, run.dispatchCount, run.businessFailures], ['success', 3, 0]);
    assert.equal(await target.client.hGet(store.keys.run(runId), 'normalEventCursor'), '4');
    assert.equal((await store.capacitySnapshot()).counts.unfinishedEvents, 0);
  });
});

test('events: callback retry/timeout budget is independent of Run execution and dead letter unblocks the next normal event', { timeout: 90000 }, async () => {
  await scenario(async ({ store, definition, member }) => {
    const { runId } = await store.start(definition, { query: null }); await store.settle(await store.claim(definition, member, 1), { kind: 'end' });
    const eventId = `${runId}:1:batchSettled`; const nextId = `${runId}:1:success`;
    const first = await store.claimEvent(eventId, member, 1);
    await store.settleEvent(first, { kind: 'failure', error: { code: 'DELIVERY_FAILED', name: 'Error', message: 'callback only', truncated: false } });
    assert.equal((await store.getEvent(eventId)).status, 'retrying'); assert.equal(await store.claimEvent(nextId, member, 1), null);
    const second = await waitFor(() => store.claimEvent(eventId, member, 1)); assert.equal(second.event.deliveryAttempt, 2);
    await store.settleEvent(second, { kind: 'timeout' }); const dead = await store.getEvent(eventId);
    assert.equal(dead.status, 'dead_letter'); assert.equal(dead.firstDeadLetterSequence, 1);
    assert.equal(dead.deadLetterExpiresAt - dead.firstDeadAt, 30 * 86400000); assert.equal(dead.error, null);
    await store.settleEvent(await store.claimEvent(nextId, member, 1), { kind: 'success' });
    const run = await store.get(runId); assert.deepEqual(run.callbacks, { pending: 0, delivered: 1, deadLetters: 1 });
    assert.deepEqual([run.status, run.dispatchCount, run.businessFailures, run.scheduledRetries], ['success', 1, 0, 0]);
  }, { callback: { attempts: 2, backoff: { baseMs: 1, maxMs: 1 } } });
});

test('events: each real expired claim consumes one callback permit and late owners accept zero commits', { timeout: 90000 }, async () => {
  await scenario(async ({ store, definition, member }) => {
    const { runId } = await store.start(definition, { query: null }); await store.settle(await store.claim(definition, member, 1), { kind: 'end' });
    const eventId = `${runId}:1:batchSettled`; const first = await store.claimEvent(eventId, member, 1);
    await delay(3100);
    await assert.rejects(store.renewEvent(first), { code: 'LEASE_LOST' }); await assert.rejects(store.settleEvent(first, { kind: 'success' }), { code: 'LEASE_LOST' });
    assert.equal((await store.recoverEvent(eventId)).changed, true); assert.equal((await store.recoverEvent(eventId)).changed, false);
    assert.equal((await store.getEvent(eventId)).deliveryAttempt, 1);
    const replacement = uid(); await store.registerRuntime(replacement, 1, [definition]);
    const second = await waitFor(() => store.claimEvent(eventId, replacement, 1)); assert.equal(second.event.deliveryAttempt, 2);
    await delay(3100); await store.recoverEvent(eventId);
    assert.equal((await store.getEvent(eventId)).status, 'dead_letter'); assert.equal((await store.get(runId)).businessFailures, 0);
    assert.equal((await store.capacitySnapshot()).counts.unfinishedEvents, 1);
  }, { lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10 }, callback: { attempts: 2, timeoutMs: 10000, backoff: { baseMs: 1, maxMs: 1 } } });
});

test('events: concurrent members share one Run callback lock and cancel leaves already-created callbacks deliverable', { timeout: 90000 }, async () => {
  await scenario(async ({ store, definition, member, target }) => {
    const another = uid(); await store.registerRuntime(another, 1, [definition]);
    const { runId } = await store.start(definition, { query: null });
    await store.settle(await store.claim(definition, member, 1), { kind: 'next' }); await store.cancel(runId);
    const eventId = `${runId}:1:batchSettled`;
    const claims = await Promise.all([store.claimEvent(eventId, member, 1), store.claimEvent(eventId, another, 1)]);
    assert.equal(claims.filter(Boolean).length, 1); await store.settleEvent(claims.find(Boolean), { kind: 'success' });
    assert.equal((await store.get(runId)).status, 'cancelled'); assert.equal((await store.getEvent(eventId)).status, 'delivered');
    assert.equal(await target.client.exists(store.keys.eventLock(runId)), 0);
  });
});

test('events: candidate admission reads only parent identity before a successful grant, never unclaimed query/state payload', { timeout: 90000 }, async () => {
  await scenario(async ({ store, definition, member }) => {
    const { runId } = await store.start(definition, { query: 'q'.repeat(262142) });
    await store.settle(await store.claim(definition, member, 1), { kind: 'end' });
    const eventId = `${runId}:1:batchSettled`; const commands = []; const original = store.connection.command.bind(store.connection);
    store.connection.command = async (args, options) => { commands.push(args); return original(args, options); };
    const lease = await store.claimEvent(eventId, member, 1); assert(lease);
    assert.equal(await store.claimNextEvent([definition], member, 1), null, 'successor cannot bypass the active Run lock');
    assert.equal(commands.filter(args => args[0] === 'HGETALL' && [store.keys.run(runId), store.keys.eventById(eventId)].includes(args[1])).length, 0,
      'preflight identity lookup cannot fetch the parent business payload');
    assert(commands.some(args => ['EVAL', 'EVALSHA'].includes(args[0]) && JSON.parse(args.at(-1)).op === 'eventParent'));
    assert.equal(lease.event.query.length, 262142, 'successful grant still returns the full immutable query');
  });
});
