import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { installCandidate } from './package-harness.mjs';
import { withTopology, eventually } from './topology-harness.mjs';

test('TLS: fresh package direct and rediss authenticated database operations; bad CA/hostname/password reject with bounded close', { timeout: 240000 }, async () => {
  const candidate = await installCandidate();
  const { createBatchQueue } = await import(pathToFileURL(resolve(candidate.consumer, 'node_modules/queuebit/dist/index.js')).href);
  await withTopology({ tls: true }, async target => {
    const url = `rediss://default:${target.password}@127.0.0.1:${target.ports[0]}/2`;
    for (const redis of [{ ...target.redis, database: 2 }, { mode: 'url', url, tls: target.certs }]) {
      const queue = createBatchQueue({ namespace: target.namespace, redis, protocol: { lease: { pollMs: 10 } } });
      const task = queue.define({ name: 'tls', version: '1', events: [] }, { execute: ctx => ctx.end() });
      try { await queue.ready(); const started = await task.start({ query: null }); await eventually(async () => (await task.get(started.runId))?.status === 'success'); }
      finally { const closed = await queue.close(); assert.equal(closed.remainingExecutions, 0); }
    }
    assert.equal(await target.data.dbSize(), 0, 'Database 0 must remain empty');
    for (const redis of [{ ...target.redis, tls: { servername: 'localhost' } },
      { ...target.redis, tls: { ...target.certs, servername: 'not-localhost.invalid' } }, { ...target.redis, password: 'wrong' }]) {
      const queue = createBatchQueue({ namespace: `${target.namespace}-negative`, redis });
      const started = Date.now();
      try { await assert.rejects(queue.ready(), error => ['CONNECTION_UNAVAILABLE', 'RESOURCE_CLEANUP_FAILED'].includes(error.code)); }
      finally { await queue.close(); }
      assert(Date.now() - started < 12000, 'Handshake and cleanup must be bounded');
    }
    target.evidence.package = { sha256: candidate.sha256, directory: candidate.directory };
    target.evidence.assertions.push('direct TLS database2', 'URL TLS database2', 'untrusted CA rejected', 'hostname mismatch rejected', 'wrong auth rejected'); await target.save();
  });
});
