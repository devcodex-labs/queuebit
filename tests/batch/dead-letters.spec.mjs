import assert from 'node:assert/strict';
import { test } from 'node:test';
import { randomBytes } from 'node:crypto';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { decodeDeadLetterCursor, encodeDeadLetterCursor } from '../../.temp/batch/domain/cursor.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { createBatchQueue } from '../../.temp/batch/index.js';
import { withRedis } from './helpers.mjs';
const uid = () => randomBytes(16).toString('hex');
async function scenario(action) {
  await withRedis(async target => {
    const options = { namespace: target.namespace, redis: target.redis, protocol: { callback: { attempts: 1 } } };
    const config = normalizeOptions(options); const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definitions = ['first', 'second'].map(name => normalizeDefinition({ name, version: '1', events: ['batchSettled', 'success'] },
      { execute: ctx => ctx.end(), onBatchSettled() {}, onSuccess() {} }, 'all')); const member = uid();
    const admin = createBatchQueue({ ...options, runtime: { mode: 'producer' } });
    try {
      await store.ready(); await store.registerRuntime(member, 1, definitions);
      const ids = [];
      for (const definition of definitions) {
        const { runId } = await store.start(definition, { query: { secretPayload: true } });
        await store.settle(await store.claim(definition, member, 1), { kind: 'end' });
        for (const kind of definition.events) {
          const eventId = `${runId}:1:${kind}`; await store.settleEvent(await store.claimEvent(eventId, member, 1), { kind: 'failure' }); ids.push(eventId);
        }
      }
      await action({ target, store, admin, ids, member, definitions });
    } finally { await admin.close(); await connection.close(); }
  });
}
test('deadLetters SDK: no local definition required; metadata never fetches payload, list uses independent first-dead ordering and replay keeps identity', { timeout: 90000 }, async () => {
  await scenario(async ({ store, admin, target, ids, member }) => {
    await assert.rejects(admin.operator.deadLetters.get(ids[0]), { code: 'QUEUE_NOT_READY' }); await admin.ready();
    const metadata = await admin.operator.deadLetters.get(ids[0]); assert(metadata); assert(Object.isFrozen(metadata.lease));
    for (const key of ['query', 'state', 'error', 'deliveryError', 'definitionIdentity', 'token']) assert.equal(Object.hasOwn(metadata, key), false);
    const original = await target.client.hGet(store.keys.eventById(ids[0]), 'state');
    await target.client.hSet(store.keys.eventById(ids[0]), 'state', 'not-json');
    assert.deepEqual(await admin.operator.deadLetters.get(ids[0]), metadata, 'metadata has no hidden payload dependency');
    await target.client.hSet(store.keys.eventById(ids[0]), 'state', original);
    const first = await admin.operator.deadLetters.list({ limit: 2 }); assert.deepEqual(first.items.map(x => x.eventId), ids.slice(2).reverse());
    assert.deepEqual(first.items.map(x => x.firstDeadLetterSequence), [4, 3]); assert(first.nextCursor);
    const second = await admin.operator.deadLetters.list({ limit: 2, cursor: first.nextCursor }); assert.deepEqual(second.items.map(x => x.eventId), ids.slice(0, 2).reverse());
    assert.equal(second.nextCursor, null); assert.equal(first.consistency, 'live');
    assert.deepEqual((await admin.operator.deadLetters.list({ taskName: 'first' })).items.map(x => x.firstDeadLetterSequence), [2, 1]);
    const input = { eventId: ids[0], expectedRevision: metadata.revision, reason: 'SDK replay', commandId: uid() };
    const replay = await admin.operator.deadLetters.replay(input); assert.equal(replay.kind, 'applied');
    assert.equal((await admin.operator.deadLetters.get(ids[0])).status, 'pending');
    assert.equal((await admin.operator.deadLetters.list()).items.length, 4);
    await store.settleEvent(await store.claimEvent(ids[0], member, 1), { kind: 'success' });
    assert.equal((await admin.operator.deadLetters.get(ids[0])).status, 'delivered');
    assert.equal((await admin.operator.deadLetters.list()).items.length, 3);
    assert.deepEqual(await admin.operator.deadLetters.replay(input), replay);
    await admin.close(); await assert.rejects(admin.operator.deadLetters.list(), { code: 'QUEUE_CLOSED' });
  });
});

test('deadLetters SDK: strict independent cursor binds task and original window; sparse expired rows and stable corrupt pointers are not silent success', { timeout: 90000 }, async () => {
  await scenario(async ({ store, admin, target, ids }) => {
    await admin.ready(); const first = await admin.operator.deadLetters.list({ limit: 1 });
    const state = decodeDeadLetterCursor(first.nextCursor, target.namespace, {}, Date.now());
    await assert.rejects(admin.operator.deadLetters.list({ taskName: 'first', cursor: first.nextCursor }), { code: 'CURSOR_INVALID' });
    const expired = encodeDeadLetterCursor(target.namespace, {}, state.upperSequence, state.lastSequence, Date.now() - 1);
    await assert.rejects(admin.operator.deadLetters.list({ cursor: expired }), { code: 'CURSOR_EXPIRED' });
    const runPage = await admin.operator.runs.list({ limit: 1 });
    await assert.rejects(admin.operator.deadLetters.list({ cursor: runPage.nextCursor }), { code: 'CURSOR_INVALID' });
    await target.client.hSet(store.keys.eventById(ids[1]), 'deadLetterExpiresAt', String(Date.now() - 1));
    const later = await admin.operator.deadLetters.list({ limit: 1, cursor: first.nextCursor });
    assert.equal(later.items[0].eventId, ids[2]);
    const last = await admin.operator.deadLetters.list({ limit: 1, cursor: later.nextCursor }); assert.equal(last.items[0].eventId, ids[0]);
    const missing = `${uid()}:1:success`; await target.client.zAdd(store.keys.deadLetters(), { score: 2, value: missing });
    await assert.rejects(admin.operator.deadLetters.list(), { code: 'INDEX_INCONSISTENT' });
    assert.equal(await target.client.zScore(store.keys.deadLetters(), missing), 2);
    await target.client.zRem(store.keys.deadLetters(), missing);
    await target.client.zAdd(store.keys.deadLetters(), { score: 5, value: missing });
    await assert.rejects(admin.operator.deadLetters.list(), { code: 'INDEX_INCONSISTENT' });
  });
});
