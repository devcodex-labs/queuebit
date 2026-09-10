import assert from 'node:assert/strict';
import { test } from 'node:test';
import { randomBytes } from 'node:crypto';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { withRedis } from './helpers.mjs';

async function scenario(action) {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definitions = ['mail', 'other'].map(name => normalizeDefinition({ name, version: '1', events: [] }, undefined, 'producer'));
    try { await store.ready(); await action({ target, store, definitions }); }
    finally { await connection.close(); }
  });
}

test('real Redis metadata/list: four exact indexes, no payload, newest fence and stable keyset traversal', { timeout: 90000 }, async () => {
  await scenario(async ({ store, definitions }) => {
    const ids = [];
    for (let i = 0; i < 12; i++) {
      const run = await store.start(definitions[i % 2], { query: { large: 'x'.repeat(250000) } }); ids.push(run.runId);
      if (i % 3 === 0) await store.cancel(run.runId);
    }
    for (const filter of [{}, { taskName: 'mail' }, { status: 'cancelled' }, { taskName: 'mail', status: 'cancelled' }]) {
      const expected = ids.filter((_, i) => (!filter.taskName || i % 2 === 0) && (!filter.status || i % 3 === 0)).reverse();
      const page = await store.listRuns({ ...filter, limit: 200 }); assert.deepEqual(page.items.map(x => x.runId), expected);
      assert.equal(page.consistency, 'live'); assert.equal(page.nextCursor, null);
      for (const metadata of page.items) for (const forbidden of ['query', 'state', 'error', 'stack', 'effectivePolicy', 'token']) assert.equal(Object.hasOwn(metadata, forbidden), false);
    }
    const first = await store.listRuns({ limit: 3 });
    await store.start(definitions[0], { query: null });
    const seen = first.items.map(x => x.runId); let cursor = first.nextCursor;
    while (cursor) { const page = await store.listRuns({ limit: 3, cursor }); seen.push(...page.items.map(x => x.runId)); cursor = page.nextCursor; }
    assert.deepEqual(seen, ids.toReversed());
    assert.equal(await store.getMetadata(randomBytes(16).toString('hex')), null);
    assert.equal((await store.getMetadata(ids[0])).status, 'cancelled');
  });
});

test('real Redis list: stable bad references/scores/types are explicit errors and never an empty success', { timeout: 90000 }, async () => {
  for (const corrupt of ['missing', 'score', 'wrongtype']) await scenario(async ({ target, store, definitions }) => {
    const { runId } = await store.start(definitions[0], { query: null });
    const beforeMeta = await target.client.hGetAll(store.keys.meta);
    if (corrupt === 'missing') await target.client.del(store.keys.run(runId));
    if (corrupt === 'score') await target.client.zAdd(store.keys.runs(), { value: runId, score: 2 });
    if (corrupt === 'wrongtype') { await target.client.del(store.keys.runs()); await target.client.set(store.keys.runs(), 'wrong'); }
    await assert.rejects(store.listRuns(), { code: 'INDEX_INCONSISTENT' });
    assert.deepEqual(await target.client.hGetAll(store.keys.meta), beforeMeta, 'read failure never mutates shared protocol');
  });
});

test('real Redis live list: removed candidates advance sparse page cursor without omitting unchanged older matches', { timeout: 90000 }, async () => {
  await scenario(async ({ store, definitions }) => {
    const ids = [];
    for (let i = 0; i < 9; i++) ids.push((await store.start(definitions[0], { query: null })).runId);
    const command = store.connection.command.bind(store.connection); let injected = false;
    store.connection.command = async (args, options) => {
      if (!injected && (args[0] === 'EVAL' || args[0] === 'EVALSHA') && JSON.parse(args.at(-1)).op === 'listCheck') {
        injected = true;
        for (const id of ids.slice(4)) await store.cancel(id);
      }
      return command(args, options);
    };
    const sparse = await store.listRuns({ status: 'pending', limit: 1 });
    assert.deepEqual(sparse.items, []); assert(sparse.nextCursor);
    const next = await store.listRuns({ status: 'pending', limit: 200, cursor: sparse.nextCursor });
    assert.deepEqual(next.items.map(x => x.runId), ids.slice(0, 4).toReversed()); assert.equal(next.nextCursor, null);
    await assert.rejects(store.listRuns({ status: 'cancelled', cursor: sparse.nextCursor }), { code: 'CURSOR_INVALID' });
    const body = JSON.parse(Buffer.from(sparse.nextCursor, 'base64url').toString());
    body.expiresAt = Date.now() - 1000;
    const expired = Buffer.from(JSON.stringify(body)).toString('base64url');
    await assert.rejects(store.listRuns({ status: 'pending', cursor: expired }), { code: 'CURSOR_EXPIRED' });
  });
});

test('real Redis list: maximum safe creation sequence lists and controls existing Run without wrapping', { timeout: 90000 }, async () => {
  await scenario(async ({ target, store, definitions }) => {
    await target.client.hSet(store.keys.meta, 'createdSequence', String(Number.MAX_SAFE_INTEGER - 1));
    const { runId } = await store.start(definitions[0], { query: null });
    const page = await store.listRuns(); assert.deepEqual(page.items.map(x => x.runId), [runId]);
    await assert.rejects(store.start(definitions[0], { query: null }), { code: 'SEQUENCE_EXHAUSTED' });
    assert.equal((await store.cancel(runId)).status, 'cancelled');
  });
});
