import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { createBatchQueue } from '../../.temp/batch/index.js';
import { withRedis } from './helpers.mjs';

async function waitRun(task, runId, predicate, timeout = 15000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const run = await task.get(runId);
    if (predicate(run)) return run;
    await delay(10);
  }
  assert.fail(`Run ${runId} did not reach the expected state`);
}

test('runtime: public factory advances batches serially and preserves frozen query/state', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis, protocol: { lease: { pollMs: 10 } } });
    const observed = [];
    let active = 0;
    const task = queue.define({ name: 'pages', version: '1', events: [] }, {
      async execute(ctx) {
        assert.equal(++active, 1);
        try {
          assert.ok(Object.isFrozen(ctx)); assert.ok(Object.isFrozen(ctx.query));
          observed.push([ctx.page, ctx.state, ctx.batchId]);
          await delay(5);
          return ctx.page < 3 ? ctx.next({ cursor: ctx.page }) : ctx.end();
        } finally { active--; }
      }
    });
    try {
      const ready = queue.ready(); assert.equal(queue.ready(), ready); await ready;
      const started = await task.start({ query: { count: 3 } });
      const result = await waitRun(task, started.runId, run => run.status === 'success');
      assert.deepEqual(observed.map(value => value.slice(0, 2)), [[1, null], [2, { cursor: 1 }], [3, { cursor: 2 }]]);
      assert.equal(new Set(observed.map(value => value[2])).size, 3);
      assert.equal(result.dispatchCount, 3); assert.deepEqual(result.state, { cursor: 2 });
      assert.equal((await queue.close()).remainingExecutions, 0);
    } finally { await queue.close(); }
  });
});

test('runtime: caught invalid control is terminal, while business failures retry within the frozen budget', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis,
      protocol: { lease: { pollMs: 10 } }, defaults: { attempts: 2, backoff: { baseMs: 1, maxMs: 1 } } });
    let count = 0;
    const task = queue.define({ name: 'business', version: '1', events: [] }, { execute(ctx) { count++; if (count === 1) throw new Error('retry'); return ctx.end(); } });
    const poisoned = queue.define({ name: 'poison', version: '1', events: [] }, { execute(ctx) { const end = ctx.end(); try { ctx.next(); } catch {} return end; } });
    try {
      await queue.ready();
      const first = await task.start({ query: null });
      const second = await poisoned.start({ query: null });
      const ok = await waitRun(task, first.runId, run => run.status === 'success');
      const failed = await waitRun(poisoned, second.runId, run => run.status === 'failed');
      assert.deepEqual([count, ok.businessFailures, ok.scheduledRetries], [2, 1, 1]);
      assert.deepEqual([failed.dispatchCount, failed.businessFailures, failed.error.code], [1, 0, 'HANDLER_CONTRACT_INVALID']);
    } finally { await queue.close(); }
  });
});

test('runtime: timed-out unresolved handlers retain physical slots and cannot commit after close', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis,
      runtime: { concurrency: 1, closeGraceMs: 0 }, protocol: { lease: { pollMs: 10 } }, defaults: { attempts: 1, timeoutMs: 20 } });
    let release; let invoked = 0; let signal;
    const task = queue.define({ name: 'stuck', version: '1', events: [] }, { async execute(ctx) { invoked++; signal = ctx.signal; await new Promise(resolve => { release = resolve; }); return ctx.end(); } });
    try {
      await queue.ready();
      const first = await task.start({ query: null });
      const second = await task.start({ query: null });
      await waitRun(task, first.runId, run => run.status === 'failed');
      await delay(80);
      assert.equal(signal.aborted, true); assert.equal(invoked, 1);
      assert.equal((await task.get(second.runId)).status, 'pending');
      const closing = queue.close(); assert.equal(queue.close(), closing);
      assert.deepEqual(await closing, { status: 'closed', timedOut: true, remainingExecutions: 1, remainingCallbacks: 0 });
      release(); await delay(30);
      assert.equal(await target.client.hGet(`qb:batch:v1:{${target.namespace}}:run:${first.runId}`, 'status'), 'failed');
    } finally { release?.(); await queue.close(); }
  });
});

test('runtime: close grace keeps the latest lease eligible until a real in-flight handler settles', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const queue = createBatchQueue({ namespace: target.namespace, redis: target.redis,
      runtime: { closeGraceMs: 6000 }, protocol: { lease: { leaseMs: 3000, heartbeatMs: 1000, pollMs: 10 } } });
    let entered;
    const started = new Promise(resolve => { entered = resolve; });
    const task = queue.define({ name: 'grace', version: '1', events: [] }, {
      async execute(ctx) { entered(); await delay(3500); assert.equal(ctx.signal.aborted, false); return ctx.end(); }
    });
    try {
      await queue.ready(); const run = await task.start({ query: null }); await started;
      assert.deepEqual(await queue.close(), { status: 'closed', timedOut: false, remainingExecutions: 0, remainingCallbacks: 0 });
      assert.equal(await target.client.hGet(`qb:batch:v1:{${target.namespace}}:run:${run.runId}`, 'status'), 'success');
    } finally { await queue.close(); }
  });
});
