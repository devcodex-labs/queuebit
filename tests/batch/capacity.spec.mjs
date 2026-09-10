import assert from 'node:assert/strict';
import { test } from 'node:test';
import { randomBytes } from 'node:crypto';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { canonicalJson } from '../../.temp/batch/domain/json.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { withRedis } from './helpers.mjs';
import { normalizeRunControl, encodeControlReceipt } from '../../.temp/batch/domain/operator.js';
import { normalizeReplay, encodeReplayReceipt } from '../../.temp/batch/domain/events.js';
import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

test('capacity: actual high-water load still settles maximum state/error plus two prepaid Events and replaces an expired consumer', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis, protocol: { limits: { memberMax: 1, totalBytes: 2 * 1024 * 1024 } } });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'full', version: '1', events: ['batchSettled', 'failure'] }, { execute: ctx => ctx.end(), onBatchSettled() {}, onFailure() {} }, 'all');
    const member = randomBytes(16).toString('hex');
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]);
      const first = await store.start(definition, { query: 'q'.repeat(262142) });
      const one = await store.claim(definition, member, 1);
      await store.settle(one, { kind: 'next', stateCanonical: canonicalJson('s'.repeat(65534), 65536) });
      const two = await store.claim(definition, member, 1); assert(two);
      let created = 0;
      for (; created < 20; created++) {
        try { await store.start(definition, { query: 'f'.repeat(100000) }); }
        catch (error) { assert.equal(error.code, 'CAPACITY_EXCEEDED'); break; }
      }
      assert(created > 0 && created < 20); assert.equal(await target.client.hGet(store.keys.capacity, 'latch'), '1');
      const base = { code: 'BOUNDARY', name: 'Error', message: '', truncated: false };
      const error = { ...base, message: 'e'.repeat(32768 - Buffer.byteLength(canonicalJson(base))) };
      assert.equal(Buffer.byteLength(canonicalJson(error)), 32768);
      await store.settle(two, { kind: 'contract', error });
      const run = await store.get(first.runId); assert.equal(run.status, 'failed'); assert.equal(run.state.length, 65534);
      assert.equal(await target.client.zCard(store.keys.events(first.runId)), 3);
      await target.client.hSet(store.keys.run(first.runId), 'terminalAt', String(Date.now() - config.protocol.retention.runMs - 1000));
      assert.equal((await store.gcRun(first.runId)).changed, false, 'immutable Events still protect maximum parent payload');
      assert.equal(await target.client.hGet(store.keys.capacity, 'reservedBytes'), '0');
      await target.client.hSet(store.keys.runtime(member), 'deadline', '1'); await target.client.zAdd(store.keys.members, { score: 1, value: member });
      await target.client.zAdd(store.keys.definitionMembers(definition.identity), { score: 1, value: member });
      const replacement = randomBytes(16).toString('hex'); await store.registerRuntime(replacement, 1, [definition]);
      assert.equal(await target.client.hGet(store.keys.capacity, 'memberCount'), '1');
      assert.equal(await target.client.hGet(store.keys.definition(definition.identity), 'runtimeRefs'), '1');
      const charge = await target.client.hGetAll(store.keys.capacity);
      assert(Number(charge.chargedBytes) + Number(charge.reservedBytes) <= config.protocol.limits.totalBytes - 98304);
      const ids = [`${first.runId}:1:batchSettled`, `${first.runId}:2:batchSettled`, `${first.runId}:2:failure`];
      for (const eventId of ids) {
        const callback = await store.claimEvent(eventId, replacement, 1); assert(callback, 'already-paid callback drains at the high-water latch');
        await store.settleEvent(callback, { kind: 'success' });
        const past = Date.now() - config.protocol.retention.deliveredEventMs - 1000;
        await target.client.hSet(store.keys.eventById(eventId), 'deliveredAt', String(past));
        await target.client.zAdd(store.keys.gcEvents, { score: past + config.protocol.retention.deliveredEventMs, value: eventId });
        assert.equal((await store.gcEvent(eventId)).changed, true);
      }
      assert.equal((await store.gcRun(first.runId)).changed, true);
      assert.equal((await store.capacitySnapshot()).counts.unfinishedEvents, 0);
    } finally { await connection.close(); }
  });
});

test('capacity: full shared replay ring and maximum Event snapshots fit their actual fixed textual slots', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis, protocol: { callback: { attempts: 1 } } });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const uid = () => randomBytes(16).toString('hex'); const member = uid();
    const definition = normalizeDefinition({ name: 'n'.repeat(128), version: 'v'.repeat(128), events: ['batchSettled', 'failure'] },
      { execute: ctx => ctx.end(), onBatchSettled() {}, onFailure() {} }, 'all');
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]);
      const { runId } = await store.start(definition, { query: 'q'.repeat(262142), idempotencyKey: 'i'.repeat(256) });
      await store.settle(await store.claim(definition, member, 1), { kind: 'next', stateCanonical: canonicalJson('s'.repeat(65534), 65536) });
      const base = { code: 'MAX', name: 'Error', message: '', truncated: false };
      await store.settle(await store.claim(definition, member, 1), { kind: 'contract', error: { ...base, message: 'e'.repeat(32768 - Buffer.byteLength(canonicalJson(base))) } });
      for (const eventId of [`${runId}:1:batchSettled`, `${runId}:2:batchSettled`]) await store.settleEvent(await store.claimEvent(eventId, member, 1), { kind: 'success' });
      const eventId = `${runId}:2:failure`; await store.settleEvent(await store.claimEvent(eventId, member, 1), { kind: 'failure' });
      const dead = await store.getEvent(eventId);
      await store.replayEvent({ eventId, expectedRevision: dead.revision, reason: 'first replay', commandId: uid() });
      for (let index = 0; index < 32; index++) {
        const event = await store.getEvent(eventId); let chosen;
        const result = { kind: 'noop', id: eventId, revision: event.revision, status: event.status, changed: false };
        for (let n = 300; n >= 0; n--) {
          const input = { eventId, expectedRevision: event.revision, reason: '\u0000'.repeat(n), commandId: `${String(index).padStart(3, '0')}${'c'.repeat(125)}` };
          try { if (Buffer.byteLength(encodeReplayReceipt(normalizeReplay(input).canonical, result, Date.now())) <= 2048) { chosen = input; break; } } catch (error) { if (error.code !== 'CONTROL_RECORD_TOO_LARGE') throw error; }
        }
        assert(chosen); await store.replayEvent(chosen);
      }
      const lease = await store.claimEvent(eventId, member, 1); assert(lease);
      const event = await target.client.hGetAll(store.keys.eventById(eventId)); const run = await target.client.hGetAll(store.keys.run(runId));
      const bytes = object => Object.entries(object).reduce((sum, [key, value]) => sum + Buffer.byteLength(key) + Buffer.byteLength(value), 0);
      const eventMetadata = bytes(Object.fromEntries(Object.entries(event).filter(([key]) => !['state', 'error'].includes(key)))) + Buffer.byteLength('stateerror');
      const eventIndexes = [store.keys.events(runId), store.keys.dueEvents, store.keys.dueReplays, store.keys.leases, store.keys.gcEvents,
        store.keys.deadLetters(), store.keys.deadLetters(definition.name)].reduce((sum, key) => sum + Buffer.byteLength(key) + Buffer.byteLength(eventId) + 16, 0);
      const ring = JSON.parse(run.controlRing); assert.equal(ring.length, 32);
      for (const receipt of ring) { const size = Buffer.byteLength(JSON.stringify(receipt)); assert(size > 2000 && size <= 2048); }
      const runMetadata = bytes(Object.fromEntries(Object.entries(run).filter(([key]) => !['query', 'state', 'error', 'controlRing', 'audit'].includes(key))));
      const lock = bytes(await target.client.hGetAll(store.keys.eventLock(runId)));
      const runIndexes = [store.keys.runs(), store.keys.runs(definition.name), store.keys.runs(undefined, run.status), store.keys.runs(definition.name, run.status),
        store.keys.due(definition.identity), store.keys.blocked(definition.identity), store.keys.leases, store.keys.gcRuns, store.keys.events(runId), store.keys.eventLock(runId), run.idempotencyKey]
        .reduce((sum, key) => sum + Buffer.byteLength(key) + 32 + 16, 0);
      const fixedRun = runMetadata + Buffer.byteLength(run.controlRing) + Buffer.byteLength(run.audit) + runIndexes + lock;
      const fixedEvent = eventMetadata + eventIndexes + Buffer.byteLength(event.state) + Buffer.byteLength(event.error);
      await writeFile(resolve(target.directory, 'callback-capacity-calibration.json'), JSON.stringify({ fixedRun, runMetadata, runIndexes, ringBytes: Buffer.byteLength(run.controlRing),
        lock, fixedEvent, eventMetadata, eventIndexes, stateBytes: Buffer.byteLength(event.state), errorBytes: Buffer.byteLength(event.error), bounds: { run: 98304, event: 131072, eventMetadata: 32768 } }, null, 2));
      assert.equal(Buffer.byteLength(event.state), 65536); assert.equal(Buffer.byteLength(event.error), 32768);
      assert(eventMetadata + eventIndexes <= 32768); assert(fixedEvent <= 131072); assert(fixedRun <= 98304); assert(lock <= 1024);
      await store.settleEvent(lease, { kind: 'success' });
    } finally { await connection.close(); }
  });
});

test('capacity: oversized Event metadata fails before any lease, cursor or accounting mutation', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'event-bound', version: '1', events: ['success'] }, { execute: ctx => ctx.end(), onSuccess() {} }, 'all');
    const member = randomBytes(16).toString('hex');
    try {
      await store.ready(); await store.registerRuntime(member, 1, [definition]); const { runId } = await store.start(definition, { query: null });
      await store.settle(await store.claim(definition, member, 1), { kind: 'end' }); const eventId = `${runId}:1:success`;
      await target.client.hSet(store.keys.eventById(eventId), 'padding', 'x'.repeat(32768));
      const before = await Promise.all([store.keys.eventById(eventId), store.keys.run(runId), store.keys.capacity].map(key => target.client.hGetAll(key)));
      await assert.rejects(store.claimEvent(eventId, member, 1), { code: 'STORAGE_INCONSISTENT' });
      assert.deepEqual(await Promise.all([store.keys.eventById(eventId), store.keys.run(runId), store.keys.capacity].map(key => target.client.hGetAll(key))), before);
      assert.equal(await target.client.exists(store.keys.eventLock(runId)), 0); assert.equal(await target.client.zCard(store.keys.leases), 0);
      assert.equal(await target.client.hGet(store.keys.meta, 'status'), 'ready');
    } finally { await connection.close(); }
  });
});

test('capacity: maximum control-ring encoding and actual metadata/index bytes fit the fixed Run slot', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'n'.repeat(128), version: 'v'.repeat(128), events: [] }, undefined, 'producer');
    try {
      await store.ready(); const { runId } = await store.start(definition, { query: null, idempotencyKey: 'k'.repeat(256) });
      await store.control('pause', { runId, expectedRevision: 1, commandId: 'first', reason: 'pause' });
      const result = { kind: 'noop', id: runId, revision: 2, status: 'paused', changed: false };
      let count = 1;
      for (; count <= 1024; count++) {
        const normalized = normalizeRunControl('pause', { runId, expectedRevision: 2, commandId: 'c'.repeat(128), reason: '\n'.repeat(count) });
        try { encodeControlReceipt(normalized.canonical, result, Date.now()); } catch { break; }
      }
      for (let i = 0; i < 32; i++) await store.control('pause', { runId, expectedRevision: 2,
        commandId: String(i).padStart(128, 'c'), reason: '\n'.repeat(count - 1) });
      const raw = await target.client.hGetAll(store.keys.run(runId));
      assert.equal(JSON.parse(raw.controlRing).length, 32);
      const size = value => Buffer.byteLength(value, 'utf8');
      const ring = size(raw.controlRing); const audit = size(raw.audit);
      assert(ring > 32 * 2040 && ring <= 32 * 2048 + 33);
      let metadata = 0;
      for (const [key, value] of Object.entries(raw)) if (!['query','state','error','controlRing','audit'].includes(key)) metadata += size(key) + size(value);
      const associated = [store.keys.runs(), store.keys.runs(definition.name), store.keys.runs(undefined, 'paused'), store.keys.runs(definition.name, 'paused'),
        store.keys.due(definition.identity), store.keys.blocked(definition.identity), store.keys.leases, store.keys.gcRuns,
        store.keys.events(runId), store.keys.eventLock(runId), store.keys.idempotency(definition.name, Buffer.from('k'.repeat(256)).toString('base64url'))];
      const indexes = associated.reduce((total, key) => total + size(key) + size(runId) + 16, 0);
      assert(metadata <= 16 * 1024); assert(metadata + ring + audit + indexes + 1024 <= 96 * 1024);
      await writeFile(resolve(target.directory, 'capacity-calibration.json'), JSON.stringify({ model: 'UTF-8 logical fields, not Redis allocator memory',
        metadataBytes: metadata, controlRingBytes: ring, auditBytes: audit, associatedIndexBytes: indexes, eventLockHeaderAllowance: 1024,
        totalFixedBytes: metadata + ring + audit + indexes + 1024, fixedSlotBytes: 96 * 1024, requestReceiptCount: 32 }, null, 2));
      assert.equal(await target.client.hGet(store.keys.capacity, 'chargedBytes'), String(16 * 1024 + 96 * 1024 + 12));
    } finally { await connection.close(); }
  });
});

test('capacity: real Run GC crosses low-water and reopens new admission without a ledger reset', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis, protocol: { limits: {
      memberMax: 1, nonterminalRunMax: 10, runMax: 10, objectMax: 20, unfinishedEventMax: 10, definitionMax: 10, totalBytes: 2 * 1024 * 1024 } } });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'low-water', version: '1', events: [] }, undefined, 'producer');
    try {
      await store.ready(); const runs = [];
      for (let i = 0; i < 9; i++) runs.push((await store.start(definition, { query: null })).runId);
      await assert.rejects(store.start(definition, { query: null }), { code: 'CAPACITY_EXCEEDED' });
      await store.cancel(runs[0]); assert.equal(await target.client.hGet(store.keys.capacity, 'latch'), '1');
      await target.client.hSet(store.keys.run(runs[0]), 'terminalAt', String(Date.now() - config.protocol.retention.runMs - 1000));
      await store.gcRun(runs[0]); assert.equal(await target.client.hGet(store.keys.capacity, 'latch'), '0');
      assert.equal((await store.start(definition, { query: null })).created, true);
    } finally { await connection.close(); }
  });
});

test('capacity: oversized persisted metadata is rejected before any control mutation', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'corrupt-meta', version: '1', events: [] }, undefined, 'producer');
    try {
      await store.ready(); const { runId } = await store.start(definition, { query: null });
      await target.client.hSet(store.keys.run(runId), 'unexpectedMetadata', 'x'.repeat(16384));
      const before = await target.snapshot();
      await assert.rejects(store.control('pause', { runId, expectedRevision: 1, reason: 'pause', commandId: 'size-check' }), { code: 'STORAGE_INCONSISTENT' });
      assert.deepEqual(await target.snapshot(), before);
    } finally { await connection.close(); }
  });
});
