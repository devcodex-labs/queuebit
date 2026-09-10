import { createClient, RESP_TYPES } from '@redis/client';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { createServer } from 'node:net';
import { mkdir, mkdtemp, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

const repo = fileURLToPath(new URL('../../', import.meta.url));
const scratch = resolve(repo, '../.devcodex/queuebit/.tmp');
const linuxPath = path => path.replace(/^([A-Za-z]):/, (_match, drive) => `/mnt/${drive.toLowerCase()}`).replaceAll('\\', '/');

async function freePort() {
  const server = createServer();
  await new Promise((resolve, reject) => server.once('error', reject).listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}

class FixturePortCollision extends Error {
  constructor(directory, port, log) {
    super(`Test-owned Redis did not acquire port ${port}: ${log}`);
    this.code = 'TEST_REDIS_PORT_COLLISION'; this.directory = directory; this.port = port;
  }
}
/** Retry only the private pre-action startup collision type, never a test assertion or a business operation. */
export async function withRedis(action, { firstPort, onPortRetry } = {}) {
  if (firstPort !== undefined && (!Number.isInteger(firstPort) || firstPort < 1 || firstPort > 65535)) throw Error('Invalid fixture port');
  for (let attempt = 0; attempt < 3; attempt++) {
    try { return await withRedisAttempt(action, attempt === 0 ? firstPort : undefined); }
    catch (error) {
      if (!(error instanceof FixturePortCollision) || attempt === 2) throw error;
      onPortRetry?.({ attempt: attempt + 1, directory: error.directory, port: error.port });
    }
  }
}

/** Every attempt owns a distinct process and data directory; it never connects before its own Redis is ready. */
async function withRedisAttempt(action, requestedPort) {
  await mkdir(scratch, { recursive: true });
  const directory = await mkdtemp(resolve(scratch, 'batch-redis-'));
  const port = requestedPort ?? await freePort();
  const namespace = `test-${randomBytes(16).toString('hex')}`;
  const windows = process.platform === 'win32';
  const binary = process.env.QUEUEBIT_BATCH_REDIS_BINARY ?? (windows
    ? linuxPath(resolve(scratch, 'redis-7.2.16/root/usr/bin/redis-server')) : 'redis-server');
  const args = ['--bind', '127.0.0.1', '--port', String(port), '--save', '', '--appendonly', 'no',
    '--maxmemory-policy', 'noeviction', '--dir', windows ? linuxPath(directory) : directory];
  const command = windows ? 'wsl.exe' : binary;
  const commandArgs = windows ? ['-d', 'Ubuntu-24.04', '--', binary, ...args] : args;
  const processChild = spawn(command, commandArgs, { cwd: directory, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = '';
  let spawnFailure;
  processChild.on('error', error => { spawnFailure = error; });
  const append = chunk => { output = (output + chunk.toString()).slice(-65536); };
  processChild.stdout.on('data', append); processChild.stderr.on('data', append);
  const client = createClient({ socket: { host: '127.0.0.1', port, connectTimeout: 1000, reconnectStrategy: false }, disableOfflineQueue: true });
  client.on('error', () => {});
  const receipt = { command, args: commandArgs, cwd: directory, launcherPid: processChild.pid, port,
    namespace, startedAt: new Date().toISOString(), redisPid: null, stopped: false, portReleased: false, portOwnership: 'attempting' };
  await writeFile(resolve(directory, 'service.json'), JSON.stringify(receipt, null, 2));
  const prefix = `qb:batch:v1:{${namespace}}:`;
  const keys = async () => {
    let cursor = '0'; const found = [];
    for (let call = 0; call < 100; call++) {
      const reply = await client.sendCommand(['SCAN', cursor, 'MATCH', `${prefix}*`, 'COUNT', '100']);
      cursor = reply[0]; found.push(...reply[1]);
      if (found.length > 10000) throw Error('Test namespace scan budget exhausted');
      if (cursor === '0') return found.sort();
    }
    throw Error('Test namespace scan budget exhausted');
  };
  let primaryFailure;
  try {
    const deadline = Date.now() + 15000;
    while (!output.includes('Ready to accept connections')) {
      if (processChild.exitCode !== null && /bind: Address already in use/.test(output)) {
        receipt.portOwnership = 'not-acquired';
        throw new FixturePortCollision(directory, port, output);
      }
      if (spawnFailure || processChild.exitCode !== null || Date.now() >= deadline) throw Error(`Owned Redis failed to start: ${spawnFailure?.message ?? output}`);
      await delay(50);
    }
    receipt.redisPid = Number(/pid=(\d+)/.exec(output)?.[1]);
    receipt.portOwnership = 'acquired';
    await writeFile(resolve(directory, 'service.json'), JSON.stringify(receipt, null, 2));
    // WSL reports Redis readiness before Windows has necessarily installed localhost forwarding.
    while (!client.isReady) {
      try { await client.connect(); }
      catch (error) {
        if (client.isOpen) client.destroy();
        if (Date.now() >= deadline) throw error;
        await delay(100);
      }
    }
    const info = await client.info('server');
    const version = /^redis_version:(.+)$/m.exec(info)?.[1]?.trim();
    if (!version?.startsWith('7.2.')) throw Error(`Redis 7.2 qualification required; observed ${version}`);
    receipt.redisPid = Number(/^process_id:(\d+)/m.exec(info)?.[1]);
    receipt.redisVersion = version;
    await writeFile(resolve(directory, 'service.json'), JSON.stringify(receipt, null, 2));
    await action({ namespace, redis: { mode: 'direct', host: '127.0.0.1', port }, client, port, directory, keys,
      snapshot: async () => {
        const result = {};
        for (const key of await keys()) result[key] = (await client.sendCommand(['DUMP', key], { typeMapping: { [RESP_TYPES.BLOB_STRING]: Buffer } })).toString('base64');
        return result;
      } });
  } catch (error) {
    primaryFailure = error;
    receipt.failure = { name: error.name, message: error.message, code: error.code };
    throw error;
  } finally {
    if (client.isOpen) {
      try { await client.sendCommand(['SHUTDOWN', 'NOSAVE'], { timeout: 2000 }); } catch {}
      if (client.isOpen) client.destroy();
    }
    receipt.redisPid ??= Number(/pid=(\d+)/.exec(output)?.[1]);
    let deadline = Date.now() + 2000;
    while (processChild.exitCode === null && Date.now() < deadline) await delay(50);
    if (processChild.exitCode === null && Number.isSafeInteger(receipt.redisPid) && receipt.redisPid > 0 && windows) {
      const stop = spawn('wsl.exe', ['-d', 'Ubuntu-24.04', '--', 'kill', '-TERM', String(receipt.redisPid)], { windowsHide: true });
      await new Promise(resolve => stop.once('exit', resolve));
      deadline = Date.now() + 5000;
      while (processChild.exitCode === null && Date.now() < deadline) await delay(50);
    }
    receipt.stopped = processChild.exitCode !== null;
    // A bind check tests actual port release, independently of the launcher exit status.
    const releaseDeadline = Date.now() + 5000;
    if (receipt.portOwnership === 'not-acquired') receipt.portReleased = null; // The listener belongs to somebody else; do not stop or claim its release.
    else do {
      const probe = createServer();
      try { await new Promise((resolve, reject) => probe.once('error', reject).listen(port, '127.0.0.1', resolve)); receipt.portReleased = true; }
      catch {} finally { if (probe.listening) await new Promise(resolve => probe.close(resolve)); }
      if (!receipt.portReleased) await delay(100);
    } while (!receipt.portReleased && Date.now() < releaseDeadline);
    receipt.stoppedAt = new Date().toISOString();
    await writeFile(resolve(directory, 'service.log'), output);
    await writeFile(resolve(directory, 'service.json'), JSON.stringify(receipt, null, 2));
    if (!receipt.stopped || (receipt.portOwnership !== 'not-acquired' && !receipt.portReleased)) throw new AggregateError(
      [...(primaryFailure ? [primaryFailure] : []), Error(`Owned Redis cleanup not verified: ${directory}`)],
      'Redis test or cleanup failed', { cause: primaryFailure });
  }
}
