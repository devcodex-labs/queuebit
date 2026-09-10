import assert from 'node:assert/strict';
import { test } from 'node:test';
import { randomBytes } from 'node:crypto';
import { normalizeOptions } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { BatchRedisConnection } from '../../.temp/batch/storage/redis/connection.js';
import { BatchRedisStore } from '../../.temp/batch/storage/redis/store.js';
import { withRedis } from './helpers.mjs';
import { QueuebitError } from '../../.temp/batch/api/errors.js';
import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const uid = () => randomBytes(16).toString('hex');
test('registry: incomplete chunk manifest is never published as an executable member; cleanup releases only its actual references', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definitions = Array.from({ length: 33 }, (_, i) => normalizeDefinition({ name: `partial-${i}`, version: '1', events: [] }, { execute: ctx => ctx.end() }, 'all'));
    const member = uid(); const command = connection.command.bind(connection); let chunks = 0;
    try {
      await store.ready();
      connection.command = async (args, options) => {
        if (args[0] === 'EVAL' || args[0] === 'EVALSHA') {
          const request = JSON.parse(args.at(-1));
          if (request.op === 'register' && request.definitions[0].identity === definitions[32].identity) { chunks++; throw new QueuebitError('CONNECTION_UNAVAILABLE', 'injected pre-send second-chunk interruption'); }
        }
        return command(args, options);
      };
      await assert.rejects(store.registerRuntime(member, 1, definitions)); assert(chunks > 0);
      for (const definition of definitions) assert.equal(await target.client.zCard(store.keys.definitionMembers(definition.identity)), 0);
      connection.command = command;
      // Known pre-send interruption leaves only the first chunk; cleanup targets that precise manifest.
      await store.unregisterRuntime(member, 1);
      assert.equal(await target.client.hGet(store.keys.capacity, 'memberCount'), '0');
      for (const definition of definitions.slice(0, 32)) assert.equal(await target.client.hGet(store.keys.definition(definition.identity), 'runtimeRefs'), '0');
    } finally { connection.command = command; await connection.close(); }
  });
});

test('registry: full 128-definition manifest and exact membership index encoding fit one fixed 96 KiB member slot', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definitions = Array.from({ length: 128 }, (_, i) => normalizeDefinition({ name: `task-${i}`.padEnd(128, 'n'), version: 'v'.repeat(128), events: [] }, { execute: ctx => ctx.end() }, 'all'));
    const member = uid();
    try {
      await store.ready(); await store.registerRuntime(member, 1, definitions);
      const record = await target.client.hGetAll(store.keys.runtime(member));
      const hashBytes = Object.entries(record).reduce((total, [key, value]) => total + Buffer.byteLength(key) + Buffer.byteLength(value), 0);
      let indexBytes = Buffer.byteLength(store.keys.members) + 32 + 16;
      for (const definition of definitions) {
        indexBytes += Buffer.byteLength(store.keys.definitionMembers(definition.identity)) + 32 + 16;
        assert.equal(await target.client.hGet(store.keys.definition(definition.identity), 'runtimeRefs'), '1');
        assert.equal(await target.client.zScore(store.keys.definitionMembers(definition.identity), member), Number(record.deadline));
      }
      assert(Buffer.byteLength(record.manifest) <= 64 * 1024); assert(hashBytes + indexBytes <= 96 * 1024);
      await writeFile(resolve(target.directory, 'member-calibration.json'), JSON.stringify({ definitions: 128, hashBytes, indexBytes,
        totalBytes: hashBytes + indexBytes, fixedSlotBytes: 96 * 1024, model: 'UTF-8 logical fields, not used_memory' }, null, 2));
      await store.unregisterRuntime(member, 1);
      assert.equal(await target.client.hGet(store.keys.capacity, 'memberCount'), '0');
      for (const definition of definitions) assert.equal(await target.client.hGet(store.keys.definition(definition.identity), 'runtimeRefs'), '0');
    } finally { await connection.close(); }
  });
});

test('registry: expired member is reclaimed before full-slot replacement; referenced definitions cannot GC', { timeout: 90000 }, async () => {
  await withRedis(async target => {
    const config = normalizeOptions({ namespace: target.namespace, redis: target.redis, protocol: { limits: { memberMax: 1 } } });
    const connection = new BatchRedisConnection(config.redis); const store = new BatchRedisStore(connection, config);
    const definition = normalizeDefinition({ name: 'replace', version: '1', events: [] }, { execute: ctx => ctx.end() }, 'all');
    const old = uid(); const replacement = uid();
    try {
      await store.ready(); await store.registerRuntime(old, 1, [definition]);
      assert.equal((await store.gcDefinition(definition.identity)).changed, false);
      await target.client.hSet(store.keys.runtime(old), 'deadline', '1');
      await target.client.zAdd(store.keys.members, { score: 1, value: old });
      await target.client.zAdd(store.keys.definitionMembers(definition.identity), { score: 1, value: old });
      await store.registerRuntime(replacement, 1, [definition]);
      assert.equal(await target.client.exists(store.keys.runtime(old)), 0);
      assert.equal(await target.client.hGet(store.keys.capacity, 'memberCount'), '1');
      assert.equal(await target.client.hGet(store.keys.definition(definition.identity), 'runtimeRefs'), '1');
      assert.deepEqual(await target.client.zRange(store.keys.definitionMembers(definition.identity), 0, -1), [replacement]);
      await store.unregisterRuntime(replacement, 1); assert.equal((await store.gcDefinition(definition.identity)).changed, true);
    } finally { await connection.close(); }
  });
});
