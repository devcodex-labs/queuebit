import assert from 'node:assert/strict';
import { test } from 'node:test';
import { randomBytes } from 'node:crypto';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { withRedis } from './helpers.mjs';
const uid = () => randomBytes(16).toString('hex');
async function scenario(action) {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis, protocol: { callback: { attempts: 1 } } });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'gc', version: '1', events: ['batchSettled', 'success'] },
      { execute: ctx => ctx.end(), onBatchSettled() {}, onSuccess() {} }, 'all'); const member = uid();
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]);
      const { runId } = await store.start(definition, { query: { original: true }, idempotencyKey: 'business' });
      await store.settle(await store.claim(definition, member, 1), { kind: 'end' });
      await action({ target, store, definition, member, runId, first: `${runId}:1:batchSettled`, second: `${runId}:1:success` });
    } finally { await connection.close(); }
  });
}
test('event GC: pending normal objects protect their parent; delivered GC never deletes a different Event lock; full dependency graph really disappears', { timeout: 90000 }, async () => {
  await scenario(async ({ target, store, definition, member, runId, first, second }) => {
    const past = Date.now() - 8 * 86400000;
    await target.client.hSet(store.keys.run(runId), 'terminalAt', String(past));
    await target.client.zAdd(store.keys.gcRuns, { score: past + 7 * 86400000, value: runId });
    assert.equal((await store.gcRun(runId)).changed, false); assert.equal((await store.gcEvent(first)).changed, false);
    await store.settleEvent(await store.claimEvent(first, member, 1), { kind: 'success' });
    const other = await store.claimEvent(second, member, 1);
    await target.client.hSet(store.keys.eventById(first), 'deliveredAt', String(past));
    await target.client.zAdd(store.keys.gcEvents, { score: past + 7 * 86400000, value: first });
    const lock = await target.client.hGetAll(store.keys.eventLock(runId));
    assert.equal((await store.gcEvent(first)).changed, true); assert.equal((await store.gcEvent(first)).changed, false);
    assert.deepEqual(await target.client.hGetAll(store.keys.eventLock(runId)), lock);
    assert.equal((await store.gcRun(runId)).changed, false); assert.equal(await store.getEvent(first), null);
    await store.settleEvent(other, { kind: 'failure' });
    await target.client.hSet(store.keys.eventById(second), { firstDeadAt: String(Date.now() - 31 * 86400000), deadLetterExpiresAt: String(Date.now() - 86400000) });
    await target.client.zAdd(store.keys.gcEvents, { score: Date.now() - 86400000, value: second });
    assert.equal((await store.gcEvent(second)).changed, true); assert.equal(await store.getEvent(second), null);
    await assert.rejects(store.settleEvent(other, { kind: 'success' }), { code: 'LEASE_LOST' });
    await assert.rejects(store.renewEvent(other), { code: 'LEASE_LOST' });
    assert.equal((await store.gcRun(runId)).changed, true); assert.equal(await store.get(runId), null);
    await store.unregisterRuntime(member, 1); assert.equal((await store.gcDefinition(definition.identity)).changed, true);
    const capacity = await store.capacitySnapshot(); assert.equal(capacity.bytes.charged, 0);
    assert.deepEqual(Object.values(capacity.counts), Array(8).fill(0));
    const replacement = await store.start(definition, { query: null, idempotencyKey: 'business' }); assert.notEqual(replacement.runId, runId);
  });
});

test('event GC: a stable Event without its parent is a storage error, not a nonexistent Event', { timeout: 90000 }, async () => {
  await scenario(async ({ target, store, first, runId, member }) => {
    await target.client.del(store.keys.run(runId));
    await assert.rejects(store.getEvent(first), { code: 'STORAGE_INCONSISTENT' });
    await assert.rejects(store.claimEvent(first, member, 1), { code: 'STORAGE_INCONSISTENT' });
    assert.equal(await target.client.exists(store.keys.eventById(first)), 1);
  });
});

test('event GC: expired unclaimed replay releases unfinished exactly once and stable broken references fail closed without repair', { timeout: 90000 }, async () => {
  await scenario(async ({ target, store, first, member, runId }) => {
    await store.settleEvent(await store.claimEvent(first, member, 1), { kind: 'failure' });
    await store.replayEvent({ eventId: first, expectedRevision: (await store.getEvent(first)).revision, reason: 'gc', commandId: uid() });
    await target.client.hSet(store.keys.eventById(first), { firstDeadAt: String(Date.now() - 31 * 86400000), deadLetterExpiresAt: String(Date.now() - 86400000) });
    await target.client.zAdd(store.keys.gcEvents, { score: Date.now() - 86400000, value: first });
    assert.equal((await store.gcEvent(first)).changed, true); assert.equal((await store.capacitySnapshot()).counts.unfinishedEvents, 1);
    assert.equal((await store.get(runId)).callbacks.pending, 1);
    await target.client.zAdd(store.keys.gcEvents, { score: Date.now() - 1, value: first });
    await assert.rejects(store.gcEvent(first), { code: 'INDEX_INCONSISTENT' });
    assert.equal(await target.client.zCard(store.keys.gcEvents), 1, 'no silent pointer repair');
  });
});
