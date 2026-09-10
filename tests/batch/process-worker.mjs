import { createBatchQueue } from '../../.temp/batch/index.js';
import { setTimeout as delay } from 'node:timers/promises';

let queue;
const send = value => { if (process.connected) process.send(value); };
process.on('message', async message => {
  try {
    if (message.kind === 'start') {
      queue = createBatchQueue({ namespace: message.namespace, redis: message.redis, protocol: message.protocol,
        runtime: { mode: 'consumer', concurrency: 1, callbackConcurrency: 1, closeGraceMs: 0 } });
      queue.define({ name: message.callback ? 'callback-fault' : 'fault', version: '1', events: message.callback ? ['success'] : [] }, {
        async execute(ctx) {
          send({ kind: 'entered', pid: process.pid, runId: ctx.runId, batchId: ctx.batchId });
          if (!message.callback && message.behavior === 'hold') await new Promise(() => {});
          await delay(ctx.query?.delayMs ?? 0);
          send({ kind: 'returned', pid: process.pid, runId: ctx.runId });
          return ctx.end();
        },
        ...(message.callback ? { async onSuccess(ctx) {
          send({ kind: 'callback-entered', pid: process.pid, eventId: ctx.eventId, runId: ctx.runId, deliveryAttempt: ctx.deliveryAttempt });
          if (message.behavior === 'hold') await new Promise(() => {});
          await delay(ctx.query?.delayMs ?? 0);
          send({ kind: 'callback-returned', pid: process.pid, eventId: ctx.eventId });
        } } : {})
      });
      await queue.ready(); send({ kind: 'ready' });
    } else if (message.kind === 'close') {
      const result = await queue?.close(); send({ kind: 'closed', result });
      process.disconnect();
    }
  } catch (error) {
    send({ kind: 'error', code: error.code, message: error.message });
    try { await queue?.close(); } catch {}
    if (process.connected) process.disconnect();
    process.exitCode = 1;
  }
});
