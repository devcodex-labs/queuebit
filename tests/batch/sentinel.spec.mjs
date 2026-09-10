import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { installCandidate } from './package-harness.mjs';
import { BatchKeys } from '../../.temp/batch/storage/redis/keys.js';
import { withTopology, eventually } from './topology-harness.mjs';

test('Sentinel: fresh package three authenticated sentinels discover two replicas; real primary loss and same Queue rediscovery', { timeout: 240000 }, async () => {
  const candidate = await installCandidate();
  const { createBatchQueue } = await import(pathToFileURL(resolve(candidate.consumer, 'node_modules/queuebit/dist/index.js')).href);
  await withTopology({ sentinel: true }, async target => {
    const addressMap = Object.fromEntries(target.ports.slice(0, 3).map(port => [`127.0.0.1:${port}`, { host: 'localhost', port }]));
    const queue = createBatchQueue({ namespace: target.namespace, redis: { ...target.redis, addressMap }, runtime: { mode: 'producer' } });
    const task = queue.define({ name: 'sentinel', version: '1', events: [] });
    try {
      await queue.ready(); const first = await task.start({ query: { before: true }, idempotencyKey: 'before' });
      // WAIT tracks writes on its own connection, so the diagnostic client's WAIT alone proves nothing about task.start().
      const replicas = await Promise.all(target.ports.slice(1, 3).map(port => target.connectData(port)));
      const runKey = new BatchKeys(target.namespace).run(first.runId);
      const expected = await target.data.hGetAll(runKey); assert.equal(expected.runId, first.runId);
      await eventually(async () => {
        for (const replica of replicas) {
          const observed = await replica.hGetAll(runKey);
          if (JSON.stringify(Object.entries(observed).sort()) !== JSON.stringify(Object.entries(expected).sort())) return false;
        }
        return true;
      }, 20000);
      const oldPort = target.ports[0]; await target.stopPrimary();
      const newPort = await eventually(async () => {
        const addresses = await Promise.all(target.sentinels.map(client => client.sendCommand(['SENTINEL', 'GET-MASTER-ADDR-BY-NAME', 'batchprimary'])));
        const port = Number(addresses[0]?.[1]);
        return port && port !== oldPort && addresses.every(address => Number(address[1]) === port) ? port : false;
      }, 30000);
      const promoted = await target.connectData(newPort); assert.equal((await promoted.sendCommand(['ROLE']))[0], 'master');
      const before = await task.get(first.runId); assert.equal(before.query.before, true);
      const second = await task.start({ query: { after: true }, idempotencyKey: 'after' });
      assert.notEqual(second.runId, first.runId); assert.equal((await task.get(second.runId)).query.after, true);
      assert.equal((await task.cancel(second.runId)).status, 'cancelled');
      target.evidence.package = { sha256: candidate.sha256, directory: candidate.directory };
      target.evidence.failover = { oldPort, newPort, firstRunId: first.runId, secondRunId: second.runId, sameQueue: true, sentinels: 3, replicasBefore: 2 };
      await target.save();
    } finally { await queue.close(); }
    for (const change of [{ nodeAuth: { password: target.sentinelPassword } }, { sentinelAuth: { password: target.password } }]) {
      const invalid = createBatchQueue({ namespace: `${target.namespace}-bad`, redis: { ...target.redis, ...change } });
      try { await assert.rejects(invalid.ready(), error => ['CONNECTION_UNAVAILABLE', 'RESOURCE_CLEANUP_FAILED'].includes(error.code)); }
      finally { await invalid.close(); }
    }
    target.evidence.assertions.push('data addressMap 127.0.0.1 to localhost before and after failover', 'separate data and Sentinel credentials', 'wrong credentials rejected', 'real primary stopped', '3 Sentinel convergence', 'same Queue read/start/cancel after promotion');
  });
});
