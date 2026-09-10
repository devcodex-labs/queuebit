import { QueuebitError } from '../api/errors.js';
import type { BatchTask, QueueMode, RunInfo } from '../api/types.js';
import type { Definition } from '../domain/definition.js';
import type { BatchRedisStore } from '../storage/redis/store.js';
import type { Telemetry } from '../runtime/telemetry.js';

/** A task handle binds immutable identity; lifecycle and Redis checks have separate owners. */
export function taskHandle<Q, S>(store: BatchRedisStore, definition: Definition<Q, S>, mode: QueueMode,
  guard: () => void, cancelled: (runId: string) => void, telemetry: Telemetry): BatchTask<Q, S> {
  return Object.freeze({
    async start(input: { query: Q; idempotencyKey?: string }) {
      guard();
      if (mode === 'consumer') throw new QueuebitError('MODE_OPERATION_NOT_ALLOWED', 'Consumer mode cannot start Runs');
      const result = await store.start(definition, input);
      if (result.created) telemetry.emit('run_created', definition.name);
      return result;
    },
    async get(runId: string) {
      guard();
      const result = await store.get(runId, definition.identity) as RunInfo<Q, S> | null;
      telemetry.emit('run_read', definition.name); return result;
    },
    async cancel(runId: string) {
      guard();
      const result = await store.cancel(runId, definition.identity);
      if (result.found && result.status === 'cancelled') cancelled(runId);
      if (result.found && result.changed) telemetry.emit('run_cancelled', definition.name);
      return result;
    }
  });
}
