import assert from 'node:assert/strict';
import { test } from 'node:test';
import { randomBytes } from 'node:crypto';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { withRedis } from './helpers.mjs';
import { normalizeRunControl, encodeControlReceipt } from '../../.temp/batch/domain/operator.js';
import { createBatchQueue } from '../../.temp/batch/index.js';
import { waitFor } from './redis-harness.mjs';

const uid = () => randomBytes(16).toString('hex');
const command = (runId, revision, extras = {}) => ({ runId, expectedRevision: revision, reason: 'operator request', commandId: uid(), ...extras });

test('public operator: all modes can administer an undefined local Task; lifecycle guards and immutable metadata remain intact', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const producer = createBatchQueue({ namespace: target.namespace, redis: target.redis, runtime: { mode: 'producer' } });
    const admin = createBatchQueue({ namespace: target.namespace, redis: target.redis, runtime: { mode: 'consumer' } });
    const task = producer.define({ name: 'remote-task', version: '1', events: [] });
    try {
      await assert.rejects(admin.operator.runs.getMetadata(uid()), { code: 'QUEUE_NOT_READY' });
      await producer.ready(); await admin.ready();
      const { runId } = await task.start({ query: { private: 'payload' } });
      const before = await admin.operator.runs.getMetadata(runId); assert(before); assert.equal(Object.hasOwn(before, 'query'), false);
      const paused = await admin.operator.runs.pause(command(runId, before.revision)); assert.equal(paused.status, 'paused');
      const page = await admin.operator.runs.list({ taskName: 'remote-task', status: 'paused' });
      assert.equal(page.items[0].runId, runId); assert(Object.isFrozen(page.items[0])); assert(Object.isFrozen(page.items[0].lease));
      const cancel = await admin.operator.runs.cancel(command(runId, paused.revision)); assert.equal(cancel.status, 'cancelled');
      assert.equal((await task.get(runId)).status, 'cancelled');
      await admin.close();
      await assert.rejects(admin.operator.runs.list(), { code: 'QUEUE_CLOSED' });
    } finally { await admin.close(); await producer.close(); }
  });
});

test('public operator: local cancel aborts the invocation but never pretends an unresolved handler released its physical slot', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis, runtime: { closeGraceMs: 0, concurrency: 1 },
      protocol: { lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10 } } });
    let release; let signal;
    const pending = new Promise(resolve => { release = resolve; });
    const task = queue.define({ name: 'held', version: '1', events: [] }, { async execute(ctx) { signal = ctx.signal; await pending; return ctx.end(); } });
    try {
      await queue.ready(); const { runId } = await task.start({ query: null }); await waitFor(() => signal);
      const metadata = await queue.operator.runs.getMetadata(runId);
      await queue.operator.runs.cancel(command(runId, metadata.revision));
      assert.equal(signal.aborted, true);
      const close = await queue.close(); assert.equal(close.remainingExecutions, 1); assert.equal(close.timedOut, true);
    } finally { release(); await queue.close(); }
  });
});
async function scenario(action, policy) {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'controls', version: '1', events: [], ...(policy ? { policy } : {}) }, { execute: ctx => ctx.end() }, 'all');
    const member = uid();
    try { await store.ready(); await store.registerRuntime(member, 1, [definition]); await action({ target, store, definition, member }); }
    finally { await connection.close(); }
  });
}

test('real Redis operator: shared ring precedes CAS, compares full raw text, and keeps terminal immutable', { timeout: 90000 }, async () => {
  await scenario(async ({ target, store, definition }) => {
    const { runId } = await store.start(definition, { query: null });
    const pause = command(runId, 1, { reason: ' /\ud800\n', commandId: 'shared/\udfff' });
    const paused = await store.control('pause', pause);
    assert.deepEqual(paused, { kind: 'applied', id: runId, revision: 2, status: 'paused', changed: true });
    const stored = JSON.parse(await target.client.hGet(store.keys.run(runId), 'controlRing'))[0];
    assert.equal(JSON.stringify(stored), encodeControlReceipt(normalizeRunControl('pause', pause).canonical, paused, stored.recordedAt), 'Lua and JS count exactly the same complete encoding');
    assert.equal(JSON.parse(stored.request.reason), pause.reason);
    assert.equal(JSON.parse(stored.request.commandId), pause.commandId);
    assert.deepEqual(await store.control('pause', pause), paused);
    await assert.rejects(store.control('resume', pause), { code: 'COMMAND_CONFLICT' });
    await assert.rejects(store.control('pause', { ...pause, reason: pause.reason + 'tail' }), { code: 'COMMAND_CONFLICT' });
    await assert.rejects(store.control('cancel', command(runId, 1)), { code: 'REVISION_CONFLICT' });
    const cancelled = await store.control('cancel', command(runId, 2));
    assert.equal(cancelled.status, 'cancelled');
    assert.deepEqual(await store.control('pause', pause), paused, 'historical replay need not equal current state');
    const noop = await store.control('resume', command(runId, 3));
    assert.equal(noop.kind, 'noop'); assert.equal(noop.revision, 3);
    await assert.rejects(store.control('pause', command(runId, 2)), { code: 'REVISION_CONFLICT' });
    const absent = uid();
    assert.deepEqual(await store.control('cancel', command(absent, 1)), { kind: 'not_found', id: absent });
  });
});

test('real Redis operator: complete 2 KiB receipt preflight is zero-write, bounded ring/audit have exact time and count windows', { timeout: 90000 }, async () => {
  await scenario(async ({ target, store, definition }) => {
    const { runId } = await store.start(definition, { query: null });
    const before = await target.snapshot();
    await assert.rejects(store.control('pause', command(runId, 1, { reason: '\u0000'.repeat(300) })), { code: 'CONTROL_RECORD_TOO_LARGE' });
    assert.deepEqual(await target.snapshot(), before);
    const first = command(runId, 1); await store.control('pause', first);
    for (let i = 0; i < 32; i++) await store.control('pause', command(runId, 2));
    const ring = JSON.parse(await target.client.hGet(store.keys.run(runId), 'controlRing'));
    const audit = JSON.parse(await target.client.hGet(store.keys.run(runId), 'audit'));
    assert.equal(ring.length, 32); assert.equal(audit.length, 8);
    for (const receipt of ring) { assert(Buffer.byteLength(JSON.stringify(receipt)) <= 2048); assert.equal(receipt.expiresAt - receipt.recordedAt, 86400000); }
    for (const entry of audit) assert(Buffer.byteLength(JSON.stringify(entry)) <= 1024);
    await assert.rejects(store.control('pause', first), { code: 'REVISION_CONFLICT' });
    // Self-owned fixture backdates the receipt, not Redis clock or user data.
    const expired = command(runId, 2); await store.control('pause', expired);
    const records = JSON.parse(await target.client.hGet(store.keys.run(runId), 'controlRing'));
    records.at(-1).recordedAt = 1; records.at(-1).expiresAt = 86400001;
    await target.client.hSet(store.keys.run(runId), 'controlRing', JSON.stringify(records));
    const changed = await store.control('resume', expired); assert.equal(changed.status, 'pending');
  });
});

test('real Redis operator: pausing permits renewal, failure consumes budget and preserves retry due on resume', { timeout: 90000 }, async () => {
  await scenario(async ({ store, definition, member }) => {
    const { runId } = await store.start(definition, { query: null });
    let lease = await store.claim(definition, member, 1);
    assert.equal((await store.control('pause', command(runId, 2))).status, 'pausing');
    lease = await store.renew(lease);
    assert.equal((await store.get(runId)).revision, 3, 'heartbeat does not alter operator CAS revision');
    await store.settle(lease, { kind: 'business' });
    const paused = await store.get(runId);
    assert.equal(paused.status, 'paused'); assert.equal(paused.businessFailures, 1); assert.equal(paused.batchFailures, 1);
    const resumed = await store.control('resume', command(runId, paused.revision)); assert.equal(resumed.status, 'retrying');
    const actual = await store.get(runId); assert.equal(actual.dueAt, paused.dueAt); assert.equal(actual.batchFailures, 1);
    assert.equal(actual.scheduledRetries, 1);
  }, { attempts: 3, backoff: { baseMs: 30000, maxMs: 30000 } });
});

test('real Redis operator: terminal wins pause; cancel revokes token and releases prepaid capacity once', { timeout: 90000 }, async () => {
  await scenario(async ({ target, store, definition, member }) => {
    const first = await store.start(definition, { query: null });
    const lease = await store.claim(definition, member, 1);
    await store.control('pause', command(first.runId, 2));
    await store.settle(lease, { kind: 'end' }); assert.equal((await store.get(first.runId)).status, 'success');
    const second = await store.start(definition, { query: null });
    const cancelledLease = await store.claim(definition, member, 1);
    const cancellation = command(second.runId, 2); await store.control('cancel', cancellation);
    const after = await target.snapshot();
    assert.equal((await store.control('cancel', cancellation)).changed, true);
    await assert.rejects(store.settle(cancelledLease, { kind: 'end' }), { code: 'LEASE_LOST' });
    assert.deepEqual(await target.snapshot(), after);
    assert.equal(await target.client.hGet(store.keys.capacity, 'reservedBytes'), '0');
  });
});
