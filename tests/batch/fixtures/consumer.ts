import { createBatchQueue, type CloseResult } from 'queuebit';
const queue = createBatchQueue({ namespace: 'bundler', redis: { mode: 'direct', host: 'localhost', port: 6379 } });
const close: Promise<CloseResult> = queue.close();
// @ts-expect-error legacy next is context-only, not a root export
import { next } from 'queuebit';
// @ts-expect-error framework adapter removed from package exports
import { createQueuebitPlugin } from 'queuebit/vext';
void close;
