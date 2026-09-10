import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
const before = process.getActiveResourcesInfo().sort();
const api = await import('queuebit');
assert.deepEqual(Object.keys(api).sort(), ['QueuebitError', 'createBatchQueue']);
assert.deepEqual(process.getActiveResourcesInfo().sort(), before);
const config = JSON.parse(process.env.QUEUEBIT_CONSUMER_CONFIG);
const queue = api.createBatchQueue(config);
assert.deepEqual(process.getActiveResourcesInfo().sort(), before, 'Construction must not start timers or sockets');
let executed = 0, delivered = 0;
const task = queue.define({ name: 'consumer', version: '1', events: ['success'] }, {
  execute(ctx) { executed++; return ctx.page === 1 ? ctx.next({ cursor: 1 }) : ctx.end(); },
  onSuccess(ctx) { assert.equal(ctx.state.cursor, 1); delivered++; }
});
try {
  await queue.ready();
  const result = await task.start({ query: { snapshot: 1 }, idempotencyKey: 'esm' });
  let state;
  const deadline = Date.now() + 15000;
  do { state = await task.get(result.runId); if (state?.callbacks.delivered === 1) break; await delay(20); } while (Date.now() < deadline);
  assert.equal(state?.status, 'success'); assert.equal(state.callbacks.delivered, 1);
  assert.equal(executed, 2); assert.equal(delivered, 1);
  assert.equal((await queue.operator.runs.getMetadata(result.runId)).taskName, 'consumer');
  assert.equal((await queue.operator.deadLetters.list({})).items.length, 0);
  for (const subpath of ['queuebit/vext', 'queuebit/cli', 'queuebit/dist/index.js', 'queuebit/storage', 'queuebit/runtime']) {
    await assert.rejects(import(subpath), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' });
  }
  const metadata = (await import('queuebit/package.json', { with: { type: 'json' } })).default;
  assert.equal(metadata.engines.node, '>=22'); assert.equal(metadata.private, true);
  assert.equal(metadata.bin, undefined); assert.equal(metadata.peerDependencies, undefined);
  assert.deepEqual(metadata.dependencies, { '@redis/client': '6.1.0' });
} finally { const result = await queue.close(); assert.equal(result.remainingExecutions + result.remainingCallbacks, 0); }
await delay(100);
console.log(JSON.stringify({ status: 'PASS', mode: 'esm', node: process.version, executed, delivered }));
