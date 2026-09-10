import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { createBatchQueue } from '../../.temp/batch/index.js';
import { Telemetry } from '../../.temp/batch/runtime/telemetry.js';
import { withRedis } from './helpers.mjs';
import { waitFor } from './redis-harness.mjs';

test('telemetry: one in-flight sink, bounded queue/labels/encoding, truthful residual and late rejection cleanup', async () => {
  let release; let calls = 0;
  const pending = new Promise((_, reject) => { release = reject; });
  const telemetry = new Telemetry({ sink: () => { calls++; return pending; } }); telemetry.registerTask('allowed');
  telemetry.emit('run_created', 'allowed'); await delay(0);
  for (let i = 0; i < 2000; i++) telemetry.emit('run_created', i % 2 ? 'allowed' : 'arbitrary-run-id');
  const busy = telemetry.snapshot(); assert.equal(calls, 1); assert.equal(busy.telemetry.inFlight, true);
  assert.equal(busy.telemetry.buffered, 1024); assert(busy.telemetry.dropped > 0); assert.equal(busy.counters.run_created, 2001);
  telemetry.close(); const closed = telemetry.snapshot(); assert.equal(closed.telemetry.buffered, 0); assert.equal(closed.telemetry.inFlight, true);
  release(Error('late sink failure')); await delay(0);
  assert.equal(telemetry.snapshot().telemetry.inFlight, false); assert.equal(telemetry.snapshot().telemetry.sinkFailures, 1);
  const records = []; const observed = new Telemetry({ sink: record => { records.push(record); } }); observed.registerTask('allowed');
  observed.emit('run_created', 'secret-id'); observed.emit('run_created', 'allowed'); await delay(0); observed.close();
  assert.equal(records[0].taskName, undefined); assert.equal(records[1].taskName, 'allowed');
  for (const record of records) assert(Buffer.byteLength(JSON.stringify(record)) <= 2048);
});

test('telemetry: synchronous sink exceptions and rejected promises never escape or stop later observations', async () => {
  let calls = 0;
  const telemetry = new Telemetry({ sink() { calls++; if (calls === 1) throw Error('sync'); return Promise.reject(Error('async')); } });
  telemetry.emit('queue_ready'); telemetry.emit('run_created'); await delay(0);
  assert.equal(calls, 2); assert.equal(telemetry.snapshot().telemetry.sinkFailures, 2); telemetry.close();
});

test('public observation: capacity is an atomic Redis namespace snapshot, metrics are local, health exposes bounded missing-definition sample', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis, runtime: { mode: 'producer' } });
    const task = queue.define({ name: 'observe', version: '1', events: [] });
    try {
      await assert.rejects(queue.operator.capacity.snapshot(), { code: 'QUEUE_NOT_READY' });
      assert.throws(() => queue.operator.metrics.snapshot(), { code: 'QUEUE_NOT_READY' });
      await queue.ready();
      const healthy = await queue.operator.health.snapshot(); assert.equal(healthy.status, 'ready'); assert.equal(healthy.protocol, 'matched');
      await task.start({ query: { secret: 'never-observed' } });
      const capacity = await queue.operator.capacity.snapshot(); assert.equal(capacity.origin, 'redis'); assert.equal(capacity.scope, 'namespace');
      assert.equal(capacity.counts.runs, 1); assert.equal(capacity.counts.members, 0);
      assert.equal(capacity.bytes.memberPartition, 1000 * 96 * 1024);
      assert.equal(capacity.bytes.businessLimit + capacity.bytes.memberPartition, capacity.bytes.totalLimit);
      assert.equal(capacity.bytes.memberUsed, 0);
      const health = await queue.operator.health.snapshot(); assert.equal(health.status, 'degraded');
      assert.equal(health.definitions.withoutMember, 1); assert.equal(health.definitions.complete, true);
      const metrics = queue.operator.metrics.snapshot(); assert.equal(metrics.origin, 'local'); assert.equal(metrics.counters.run_created, 1);
      assert(!JSON.stringify({ capacity, health, metrics }).includes('never-observed'));
    } finally { await queue.close(); }
  });
});

test('public observation: schema failure is explicit degraded health, never successful empty capacity', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis, runtime: { mode: 'producer' } });
    try {
      await queue.ready(); await target.client.hSet(`qb:batch:v1:{${target.namespace}}:meta`, 'schema', 'wrong-schema');
      const health = await queue.operator.health.snapshot(); assert.equal(health.status, 'degraded'); assert.equal(health.protocol, 'mismatch');
      assert.equal(health.backlog, null); assert.equal(health.reason, 'SCHEMA_MISMATCH');
      await assert.rejects(queue.operator.capacity.snapshot(), { code: 'SCHEMA_MISMATCH' });
    } finally { await queue.close(); }
  });
});

test('public telemetry: throwing or hung sinks cannot retry a successful business execution or prevent bounded close', { timeout: 90000 }, async () => {
  for (const behavior of ['throw', 'hold']) await withRedis(async target => {
    let executions = 0; let sinkCalls = 0; let release;
    const held = new Promise(resolve => { release = resolve; });
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis,
      protocol: { lease: { pollMs: 10 } }, telemetry: { sink() { sinkCalls++; if (behavior === 'throw') throw Error('observation'); return held; } } });
    const task = queue.define({ name: 'isolated', version: '1', events: [] }, { execute(ctx) { executions++; return ctx.end(); } });
    try {
      await queue.ready(); const { runId } = await task.start({ query: null });
      await waitFor(async () => (await task.get(runId)).status === 'success');
      assert.equal(executions, 1); assert(sinkCalls > 0);
      const metrics = queue.operator.metrics.snapshot(); assert.equal(metrics.counters.run_claimed, 1); assert.equal(metrics.counters.run_settled, 1);
      if (behavior === 'hold') assert.equal(metrics.telemetry.inFlight, true); else assert(metrics.telemetry.sinkFailures > 0);
      const began = Date.now(); const close = await queue.close(); assert(Date.now() - began < 10000); assert.equal(close.remainingExecutions, 0);
    } finally { release(); await queue.close(); }
  });
});
