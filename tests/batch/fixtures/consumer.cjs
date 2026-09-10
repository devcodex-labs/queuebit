const assert = require('node:assert/strict');
const { setTimeout: delay } = require('node:timers/promises');
const before = process.getActiveResourcesInfo().sort();
const api = require('queuebit');
assert.deepEqual(Object.keys(api).sort(), ['QueuebitError', 'createBatchQueue']);
assert.deepEqual(process.getActiveResourcesInfo().sort(), before);
const queue = api.createBatchQueue(JSON.parse(process.env.QUEUEBIT_CONSUMER_CONFIG));
assert.deepEqual(process.getActiveResourcesInfo().sort(), before);
const task = queue.define({ name: 'cjs', version: '1', events: [] }, { execute(ctx) { return ctx.end(); } });
(async () => {
  try {
    await queue.ready(); const started = await task.start({ query: null });
    let run; const deadline = Date.now() + 15000;
    do { run = await task.get(started.runId); if (run?.status === 'success') break; await delay(20); } while (Date.now() < deadline);
    assert.equal(run?.status, 'success');
    const metadata = require('queuebit/package.json');
    assert.equal(metadata.private, true);
    assert.equal(metadata.exports['.'].require.types, './dist/index.d.cts');
    assert.equal(metadata.bin, undefined);
    for (const name of ['queuebit/vext', 'queuebit/cli', 'queuebit/dist/index.cjs']) assert.throws(() => require(name), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' });
  } finally { const closed = await queue.close(); assert.equal(closed.remainingExecutions + closed.remainingCallbacks, 0); }
  console.log(JSON.stringify({ status: 'PASS', mode: 'cjs', node: process.version }));
})().catch(error => { console.error(error); process.exitCode = 1; });
