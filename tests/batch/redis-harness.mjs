import { fork, spawn } from 'node:child_process';
import { createConnection, createServer } from 'node:net';
import { createHash, randomBytes } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

export async function waitFor(probe, timeout = 15000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { const result = await probe(); if (result) return result; await delay(10); }
  throw Error('Bounded fault probe did not reach its expected state');
}

export async function startWorker(target, options) {
  const script = fileURLToPath(new URL('./process-worker.mjs', import.meta.url));
  const child = fork(script, [], { cwd: target.directory, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  const receiptPath = resolve(target.directory, `worker-${randomBytes(8).toString('hex')}.json`);
  const receipt = { command: process.execPath, args: [script], cwd: target.directory, pid: child.pid,
    namespace: target.namespace, redisPort: target.port, stopped: false, startedAt: new Date().toISOString() };
  const messages = []; let output = ''; let failure;
  child.on('message', message => { messages.push(message); });
  child.on('error', error => { failure = error; });
  child.stdout.on('data', chunk => { output = (output + chunk).slice(-32768); });
  child.stderr.on('data', chunk => { output = (output + chunk).slice(-32768); });
  let stopping;
  const stop = (crash = false) => stopping ??= (async () => {
    if (child.exitCode === null && child.signalCode === null) {
      if (crash) child.kill('SIGKILL');
      else if (child.connected) child.send({ kind: 'close' }, error => { if (error) failure = error; });
      try { await waitFor(() => child.exitCode !== null || child.signalCode !== null, 5000); }
      catch { child.kill('SIGKILL'); await waitFor(() => child.exitCode !== null || child.signalCode !== null, 2000); }
    }
    receipt.stopped = child.exitCode !== null || child.signalCode !== null;
    receipt.exitCode = child.exitCode; receipt.signal = child.signalCode; receipt.crashRequested = crash;
    receipt.output = output; receipt.messages = messages; receipt.stoppedAt = new Date().toISOString();
    await writeFile(receiptPath, JSON.stringify(receipt, null, 2));
    if (!receipt.stopped) throw Error('Owned worker cleanup failed');
  })();
  await writeFile(receiptPath, JSON.stringify(receipt, null, 2));
  try {
    child.send({ kind: 'start', namespace: target.namespace, redis: target.redis, ...options });
    await waitFor(() => {
      if (failure) throw failure;
      const error = messages.find(message => message.kind === 'error');
      if (error) throw Error(`Owned worker failed: ${error.code}: ${error.message}`);
      if (child.exitCode !== null || child.signalCode !== null) throw Error(`Owned worker exited early: ${output}`);
      return messages.some(message => message.kind === 'ready');
    });
    return { pid: child.pid, messages, stop };
  } catch (error) { await stop(true); throw error; }
}

// RESP2 frame decoder for the test proxy. It never parses or rewrites product payload JSON.
function frame(buffer, offset = 0, depth = 0) {
  if (depth > 64) throw Error('Proxy response nesting exceeded test bound');
  if (offset >= buffer.length) return null;
  const eol = buffer.indexOf('\r\n', offset);
  if (eol < 0) return null;
  const kind = String.fromCharCode(buffer[offset]);
  const head = buffer.toString('utf8', offset + 1, eol); let end = eol + 2;
  if (kind === '+' || kind === '-' || kind === ':') return { end, value: kind === '-' ? { error: head } : head };
  const length = Number(head);
  if (!Number.isInteger(length) || length < -1 || length > 4 * 1024 * 1024) throw Error('Proxy frame exceeds test bound');
  if (length === -1) return { end, value: null };
  if (kind === '$') {
    if (buffer.length < end + length + 2) return null;
    return { end: end + length + 2, value: buffer.toString('utf8', end, end + length) };
  }
  if (kind === '*' && length <= 4096) {
    const value = [];
    for (let i = 0; i < length; i++) { const item = frame(buffer, end, depth + 1); if (!item) return null; value.push(item.value); end = item.end; }
    return { end, value };
  }
  throw Error(`Unexpected proxy protocol type ${kind}`);
}

/** Drops an already-observed successful Redis response by destroying only this proxy's sockets. */
export async function startProxy(target, dropCounts) {
  const remaining = { ...dropCounts }; const sockets = new Set(); const dropped = []; const requests = []; const failures = [];
  const holdCounts = {}; const held = [];
  const server = createServer(front => {
    const back = createConnection({ host: '127.0.0.1', port: target.port });
    sockets.add(front); sockets.add(back);
    let input = Buffer.alloc(0); let output = Buffer.alloc(0); const pending = [];
    const destroy = () => { front.destroy(); back.destroy(); };
    for (const socket of [front, back]) {
      socket.on('error', destroy); socket.on('close', () => { sockets.delete(socket); destroy(); });
    }
    front.on('data', chunk => {
      try {
        input = Buffer.concat([input, chunk]);
        if (input.length > 4 * 1024 * 1024) throw Error('Proxy request buffer exceeded test bound');
        for (;;) {
          const parsed = frame(input); if (!parsed) break;
          let request;
          if (Array.isArray(parsed.value) && ['EVAL', 'EVALSHA'].includes(parsed.value[0])) {
            const canonical = parsed.value.at(-1); const value = JSON.parse(canonical);
            request = { op: value.op, commandId: value.commandId, canonical };
            requests.push(request);
          } else if (Array.isArray(parsed.value) && parsed.value[0] === 'GET') {
            request = { op: 'get', commandId: `read:${parsed.value[1]}`, canonical: JSON.stringify(parsed.value) };
            requests.push(request);
          }
          pending.push(request); back.write(input.subarray(0, parsed.end)); input = input.subarray(parsed.end);
        }
      } catch (error) { failures.push(error.message); destroy(); }
    });
    back.on('data', chunk => {
      try {
        output = Buffer.concat([output, chunk]);
        if (output.length > 4 * 1024 * 1024) throw Error('Proxy response buffer exceeded test bound');
        for (;;) {
          const parsed = frame(output); if (!parsed) break;
          const request = pending.shift(); let successful = false;
          if (request?.op === 'get') successful = parsed.value === null || typeof parsed.value === 'string';
          else if (request && typeof parsed.value === 'string') {
            try { const result = JSON.parse(parsed.value); successful = result !== null && !result.error; } catch {}
          }
          if (successful && remaining[request.op] > 0) {
            remaining[request.op]--; dropped.push({ op: request.op, commandId: request.commandId, replyObserved: true });
            destroy(); return;
          }
          if (successful && holdCounts[request.op] > 0) {
            holdCounts[request.op]--;
            const bytes = Buffer.from(output.subarray(0, parsed.end));
            held.push({ op: request.op, release: () => { if (!front.destroyed) front.write(bytes); } });
            output = output.subarray(parsed.end); continue;
          }
          front.write(output.subarray(0, parsed.end)); output = output.subarray(parsed.end);
        }
      } catch (error) { failures.push(error.message); destroy(); }
    });
  });
  await new Promise((resolve, reject) => server.once('error', reject).listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  const receipt = { command: 'node TCP fault proxy (test-owned)', cwd: target.directory, pid: process.pid,
    port, upstreamPort: target.port, namespace: target.namespace, startedAt: new Date().toISOString(), stopped: false, portReleased: false };
  const receiptPath = resolve(target.directory, `proxy-${port}.json`);
  await writeFile(receiptPath, JSON.stringify(receipt, null, 2));
  let stopping;
  return { port, dropped, requests, held, hold: (op, count = 1) => { holdCounts[op] = count; },
    release: () => { for (const item of held.splice(0)) item.release(); }, stop: () => stopping ??= (async () => {
    const closed = new Promise(resolve => server.close(resolve));
    held.length = 0;
    for (const socket of sockets) socket.destroy();
    await closed; receipt.stopped = !server.listening;
    const probe = createServer();
    try { await new Promise((resolve, reject) => probe.once('error', reject).listen(port, '127.0.0.1', resolve)); receipt.portReleased = true; }
    finally { if (probe.listening) await new Promise(resolve => probe.close(resolve)); }
    receipt.dropped = dropped;
    receipt.requests = requests.map(({ op, commandId, canonical }) => ({ op, commandId, digest: createHash('sha256').update(canonical).digest('hex') }));
    receipt.failures = failures; receipt.stoppedAt = new Date().toISOString();
    await writeFile(receiptPath, JSON.stringify(receipt, null, 2));
    if (!receipt.stopped || !receipt.portReleased || failures.length) throw Error(`Proxy validation or cleanup failed: ${failures.join(', ')}`);
  })() };
}

export async function importProbe(format) {
  const script = fileURLToPath(new URL('./import-worker.mjs', import.meta.url));
  const child = spawn(process.execPath, [script, format], { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = ''; let stderr = ''; let failure;
  child.stdout.on('data', chunk => { stdout += chunk; }); child.stderr.on('data', chunk => { stderr += chunk; });
  child.on('error', error => { failure = error; });
  try {
    await waitFor(() => { if (failure) throw failure; return child.exitCode !== null || child.signalCode !== null; }, 10000);
    if (child.exitCode !== 0) throw Error(`Import probe failed: ${stderr}`);
    return JSON.parse(stdout);
  } finally {
    if (child.exitCode === null && child.signalCode === null) { child.kill('SIGKILL'); await waitFor(() => child.exitCode !== null || child.signalCode !== null, 2000); }
  }
}
