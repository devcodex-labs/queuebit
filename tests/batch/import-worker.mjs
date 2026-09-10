import { Socket } from 'node:net';
import { createRequire } from 'node:module';

const format = process.argv[2];
const counts = { format, connects: 0, timers: 0, hooks: 0 };
const connect = Socket.prototype.connect;
const timeout = globalThis.setTimeout;
const interval = globalThis.setInterval;
const on = process.on;
const addListener = process.addListener;
Socket.prototype.connect = function (...args) { counts.connects++; return connect.apply(this, args); };
globalThis.setTimeout = (...args) => { counts.timers++; return timeout(...args); };
globalThis.setInterval = (...args) => { counts.timers++; return interval(...args); };
process.on = function (...args) { counts.hooks++; return on.apply(this, args); };
process.addListener = function (...args) { counts.hooks++; return addListener.apply(this, args); };
try {
  const library = format === 'esm' ? await import('../../.temp/batch/index.js') : createRequire(import.meta.url)('../../.temp/batch/index.cjs');
  const queue = library.createBatchQueue({ namespace: 'import-only', redis: { mode: 'direct', host: '127.0.0.1', port: 1 } });
  queue.define({ name: 'pure', version: '1', events: [] }, { execute: ctx => ctx.end() });
  counts.exports = Object.keys(library).sort();
} finally {
  Socket.prototype.connect = connect; globalThis.setTimeout = timeout; globalThis.setInterval = interval;
  process.on = on; process.addListener = addListener;
}
process.stdout.write(JSON.stringify(counts));
