import assert from 'node:assert/strict';
import { createClient } from '@redis/client';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { createServer } from 'node:net';
import { mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { command, repo } from './package-harness.mjs';

const windows = process.platform === 'win32';
const linux = path => windows ? path.replace(/^([A-Za-z]):/, (_, drive) => `/mnt/${drive.toLowerCase()}`).replaceAll('\\', '/') : path;
const scratch = resolve(repo, '../.devcodex/queuebit/.tmp');
const binary = process.env.QUEUEBIT_BATCH_REDIS_BINARY ?? (windows ? linux(resolve(scratch, 'redis-7.2.16/root/usr/bin/redis-server')) : 'redis-server');
const launch = (binary, args) => windows ? ['wsl.exe', ['-d', 'Ubuntu-24.04', '--', binary, ...args]] : [binary, args];

export async function eventually(action, timeout = 15000) {
  const deadline = Date.now() + timeout; let last;
  do { try { const result = await action(); if (result) return result; } catch (error) { last = error; } await delay(100); } while (Date.now() < deadline);
  throw new Error(`Topology condition deadline: ${last?.message ?? 'not satisfied'}`, { cause: last });
}
async function freePort() {
  const probe = createServer();
  await new Promise((resolve, reject) => probe.once('error', reject).listen(0, '127.0.0.1', resolve));
  const port = probe.address().port; await new Promise(resolve => probe.close(resolve)); return port;
}
async function released(port) {
  return eventually(async () => {
    const probe = createServer();
    try { await new Promise((resolve, reject) => probe.once('error', reject).listen(port, '127.0.0.1', resolve)); return true; }
    finally { if (probe.listening) await new Promise(resolve => probe.close(resolve)); }
  }, 5000);
}
async function certificates(directory) {
  const run = args => { const [bin, actual] = launch('openssl', args); return command(bin, actual, { cwd: directory, timeout: 15000 }); };
  const file = name => linux(resolve(directory, name));
  await run(['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-subj', '/CN=QueuebitFixtureCA',
    '-keyout', file('ca.key'), '-out', file('ca.crt'), '-addext', 'basicConstraints=critical,CA:TRUE']);
  await run(['req', '-new', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=localhost', '-keyout', file('server.key'), '-out', file('server.csr')]);
  await writeFile(resolve(directory, 'server.ext'), 'subjectAltName=DNS:localhost\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth,clientAuth\n');
  await run(['x509', '-req', '-in', file('server.csr'), '-CA', file('ca.crt'), '-CAkey', file('ca.key'), '-CAcreateserial',
    '-days', '1', '-extfile', file('server.ext'), '-out', file('server.crt')]);
  return { ca: await readFile(resolve(directory, 'ca.crt'), 'utf8'), cert: await readFile(resolve(directory, 'server.crt'), 'utf8'),
    key: await readFile(resolve(directory, 'server.key'), 'utf8'), servername: 'localhost' };
}

/** Owns every process listed here. Never connects to a port before its own process is ready. */
export async function withTopology({ tls = false, sentinel = false } = {}, action) {
  await mkdir(scratch, { recursive: true }); const directory = await mkdtemp(resolve(scratch, 'batch-topology-'));
  const namespace = `topology-${randomBytes(12).toString('hex')}`;
  const password = randomBytes(16).toString('hex'), sentinelPassword = randomBytes(16).toString('hex');
  const processes = [], clients = [], used = new Set(); let primaryFailure;
  const evidence = { directory, namespace, node: process.version, tls, sentinel, startedAt: new Date().toISOString(), services: [], assertions: [] };
  const save = () => writeFile(resolve(directory, 'topology-evidence.json'), JSON.stringify(evidence, null, 2));
  const stop = async owned => {
    if (owned.child.exitCode === null && owned.child.signalCode === null) {
      const pid = owned.receipt.redisPid ?? Number(/pid=(\d+)/.exec(owned.output)?.[1]); assert(Number.isSafeInteger(pid) && pid > 0, `Missing owned PID: ${owned.output}`);
      const [bin, args] = launch('kill', ['-TERM', String(pid)]);
      await command(bin, args, { cwd: directory, timeout: 10000 });
      await eventually(() => owned.child.exitCode !== null || owned.child.signalCode !== null, 15000);
    }
    owned.receipt.stopped = true;
    if (owned.receipt.portOwnership === 'acquired') { await released(owned.receipt.port); owned.receipt.portReleased = true; }
    else owned.receipt.portReleased = null;
    owned.receipt.stoppedAt = new Date().toISOString(); await save();
  };
  const connect = async (port, auth, certs) => {
    const client = createClient({ RESP: 2, socket: { host: '127.0.0.1', port, connectTimeout: 1000, reconnectStrategy: false,
      ...(certs ? { ...certs, tls: true, rejectUnauthorized: true } : {}) }, password: auth, disableOfflineQueue: true });
    client.on('error', () => {}); clients.push(client);
    await eventually(async () => { try { if (!client.isOpen) await client.connect(); return client.isReady; } catch { if (client.isOpen) client.destroy(); return false; } });
    return client;
  };
  try {
    const certs = tls ? await certificates(directory) : undefined;
    const ports = [];
    while (ports.length < (sentinel ? 6 : 1)) { const port = await freePort(); if (!used.has(port)) { used.add(port); ports.push(port); } }
    for (let i = 0; i < ports.length; i++) {
      const isSentinel = sentinel && i >= 3, port = ports[i], name = `node-${i}`;
      const lines = ['bind 127.0.0.1', `dir ${linux(directory)}`, 'daemonize no', 'protected-mode yes', `port ${tls ? 0 : port}`,
        `requirepass ${isSentinel ? sentinelPassword : password}`];
      if (tls) lines.push(`tls-port ${port}`, `tls-cert-file ${linux(resolve(directory, 'server.crt'))}`,
        `tls-key-file ${linux(resolve(directory, 'server.key'))}`, `tls-ca-cert-file ${linux(resolve(directory, 'ca.crt'))}`, 'tls-auth-clients no');
      if (isSentinel) lines.push(`sentinel monitor batchprimary 127.0.0.1 ${ports[0]} 2`, `sentinel auth-pass batchprimary ${password}`,
        'sentinel down-after-milliseconds batchprimary 1000', 'sentinel failover-timeout batchprimary 10000', 'sentinel parallel-syncs batchprimary 1');
      else { lines.push('save ""', 'appendonly no', 'maxmemory-policy noeviction', 'shutdown-timeout 1', 'repl-diskless-sync-delay 0',
        'repl-diskless-load swapdb', `dbfilename dump-${i}.rdb`, `masterauth ${password}`);
        if (i > 0) lines.push(`replicaof 127.0.0.1 ${ports[0]}`); }
      if (tls && sentinel) lines.push('tls-replication yes');
      const config = resolve(directory, `${name}.conf`); await writeFile(config, lines.join('\n') + '\n');
      const [bin, args] = launch(binary, [linux(config), ...(isSentinel ? ['--sentinel'] : [])]);
      const child = spawn(bin, args, { cwd: directory, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
      const receipt = { name, command: bin, args, cwd: directory, launcherPid: child.pid, port, redisPid: null,
        portOwnership: 'attempting', stopped: false, portReleased: false, startedAt: new Date().toISOString() };
      const owned = { child, receipt, output: '', spawnError: null }; processes.push(owned); evidence.services.push(receipt); await save();
      child.on('error', error => { owned.spawnError = error; });
      const append = data => { owned.output = (owned.output + data.toString()).slice(-131072); };
      child.stdout.on('data', append); child.stderr.on('data', append);
      const until = Date.now() + 15000;
      while (!owned.output.includes(isSentinel ? '+monitor master' : 'Ready to accept connections')) {
        if (owned.spawnError || child.exitCode !== null || Date.now() >= until) {
          if (/bind: Address already in use/.test(owned.output)) receipt.portOwnership = 'not-acquired';
          throw Error(`Owned topology startup failed: ${owned.spawnError?.message ?? owned.output}`);
        }
        await delay(50);
      }
      receipt.redisPid = Number(/pid=(\d+)/.exec(owned.output)?.[1]); receipt.portOwnership = 'acquired'; await save();
    }
    const data = await connect(ports[0], password, certs);
    const version = /^redis_version:(.+)$/m.exec(await data.info('server'))?.[1]?.trim(); assert(version?.startsWith('7.2.')); evidence.redisVersion = version;
    const sentinels = [];
    if (sentinel) {
      for (const port of ports.slice(3)) sentinels.push(await connect(port, sentinelPassword, certs));
      await eventually(async () => {
        const info = await data.info('replication');
        if (!info.includes('connected_slaves:2')) return false;
        for (const client of sentinels) if ((await client.sendCommand(['SENTINEL', 'SENTINELS', 'batchprimary'])).length !== 2) return false;
        return (await data.sendCommand(['WAIT', '2', '1000'])) === 2;
      }, 20000);
    }
    const redis = sentinel ? { mode: 'sentinel', name: 'batchprimary', seeds: ports.slice(3).map(port => ({ host: '127.0.0.1', port })),
      nodeAuth: { password }, sentinelAuth: { password: sentinelPassword }, ...(certs ? { nodeTls: certs, sentinelTls: certs } : {}) }
      : { mode: 'direct', host: '127.0.0.1', port: ports[0], password, ...(certs ? { tls: certs } : {}) };
    await action({ directory, namespace, redis, certs, data, sentinels, ports, password, sentinelPassword, evidence, save,
      stopPrimary: () => stop(processes[0]), connectData: port => connect(port, password, certs) });
  } catch (error) { primaryFailure = error; evidence.failure = { name: error.name, message: error.message }; throw error; }
  finally {
    const cleanupErrors = [];
    for (const client of clients) if (client.isOpen) client.destroy();
    for (const owned of processes.toReversed()) {
      try { await stop(owned); } catch (error) { cleanupErrors.push(error); }
      await writeFile(resolve(directory, `${owned.receipt.name}.log`), owned.output);
    }
    evidence.stoppedAt = new Date().toISOString(); evidence.cleanupVerified = cleanupErrors.length === 0; await save();
    if (cleanupErrors.length) throw new AggregateError([...(primaryFailure ? [primaryFailure] : []), ...cleanupErrors], 'Topology cleanup failed');
  }
}
