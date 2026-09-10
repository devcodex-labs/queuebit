import type { BatchQueue, BatchQueueOptions } from './api/types.js';
import { normalizeOptions } from './domain/config.js';
import { QueueRuntime } from './runtime/queue.js';

export { QueuebitError } from './api/errors.js';
export type { QueuebitErrorCode, QueuebitErrorOptions } from './api/errors.js';
export type * from './api/types.js';

/** Validates synchronously; import, construction and define perform no network I/O. */
export function createBatchQueue(options: BatchQueueOptions): BatchQueue {
  const runtime = new QueueRuntime(normalizeOptions(options));
  return Object.freeze({ operator: runtime.operator, define: runtime.define.bind(runtime), ready: runtime.ready.bind(runtime), close: runtime.close.bind(runtime) });
}
