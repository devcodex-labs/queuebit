import { test } from 'node:test';
import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { withRedis } from './helpers.mjs';
import { installCandidate, runConsumer, verifyTypes } from './package-harness.mjs';

test('package: fresh tarball only, ESM/CJS actual Redis, conditional NodeNext/Bundler declarations removed exports and zero old-key access', { timeout: 240000 }, async () => {
  assert([22, 24].includes(Number(process.versions.node.split('.')[0])), 'Run qualification with Node 22 or 24');
  const candidate = await installCandidate();
  await verifyTypes(candidate);
  await withRedis(async target => {
    const oldPrefix = `qb:{${target.namespace}}:`;
    const oldKeys = ['meta', 'run:legacy-run', 'q:legacy:waiting', 'completions:due'].map(key => oldPrefix + key);
    for (const key of oldKeys) await target.client.set(key, 'legacy-data-must-not-be-touched');
    const before = await Promise.all(oldKeys.map(key => target.client.dump(key)));
    const monitor = target.client.duplicate(); monitor.on('error', () => {});
    const commands = []; let overflow = false;
    await monitor.connect();
    try {
      await monitor.monitor(line => { if (commands.length >= 20000) overflow = true; else commands.push(String(line)); });
      // Each fresh process uses the same namespace as the old keys, exercising a real collision boundary.
      for (const mode of ['mjs', 'cjs']) await runConsumer(candidate, `consumer.${mode}`, {
        namespace: target.namespace, redis: target.redis, protocol: { lease: { pollMs: 10 } }
      });
      const marker = `probe-finished-${target.namespace}`;
      await target.client.echo(marker);
      const deadline = Date.now() + 5000;
      while (!commands.some(line => line.includes(marker)) && Date.now() < deadline) await delay(10);
      await writeFile(resolve(candidate.directory, 'old-key-monitor.json'), JSON.stringify({ commands, overflow, marker }, null, 2));
      assert(commands.some(line => line.includes(marker)), 'Monitor must observe the end barrier');
      assert.equal(overflow, false, 'Monitor capture cannot be truncated');
      assert(commands.some(line => line.includes(`qb:batch:v1:{${target.namespace}}:`)), 'Positive control: actual new-key traffic');
      assert(!commands.some(line => line.includes('qb:{')), 'No command or Lua subcommand may access any old key namespace');
      const commandName = line => /^\S+ \[[^\]]+\] "([^\"]+)"/.exec(line)?.[1].toLowerCase();
      assert(!commands.some(line => ['keys', 'flushdb', 'flushall'].includes(commandName(line))), 'No broad key discovery or clearing');
      for (const line of commands.filter(line => commandName(line) === 'scan')) {
        assert(line.includes(`"MATCH" "qb:batch:v1:{${target.namespace}}:*"`), 'SCAN must stay inside the exact new namespace');
      }
      await writeFile(resolve(candidate.directory, 'old-key-isolation.json'), JSON.stringify({ node: process.version,
        sha256: candidate.sha256, fixture: target.directory, oldKeys, oldKeyAccesses: 0, positiveNewKeyTraffic: true,
        commands, status: 'PASS' }, null, 2));
    } finally { if (monitor.isOpen) monitor.destroy(); }
    assert.deepEqual(await Promise.all(oldKeys.map(key => target.client.dump(key))), before, 'All seeded legacy values remain unchanged');
  });
});
