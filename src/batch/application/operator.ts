import type { RunControlInput, RunsOperator, RunListInput, BatchOperator, HealthSnapshot } from '../api/types.js';
import type { RunControlOperation } from '../domain/operator.js';
import type { BatchRedisStore } from '../storage/redis/store.js';
import type { Telemetry } from '../runtime/telemetry.js';
import { isQueuebitError } from '../api/errors.js';

/** The trusted operator shares the store's transitions; it never needs a local task handler or bypasses lifecycle. */
export function runsOperator(store: BatchRedisStore, guard: () => void, cancelled: (runId: string) => void, telemetry: Telemetry): RunsOperator {
  const control = async (operation: RunControlOperation, input: RunControlInput) => {
    guard();
    const result = await store.control(operation, input);
    telemetry.emit(operation === 'pause' ? 'operator_pause' : operation === 'resume' ? 'operator_resume' : 'operator_cancel');
    if (operation === 'cancel' && result.kind !== 'not_found' && result.status === 'cancelled') cancelled(result.id);
    return result;
  };
  return Object.freeze({
    async getMetadata(runId: string) { guard(); const result = await store.getMetadata(runId); telemetry.emit('metadata_read'); return result; },
    async list(input?: RunListInput) { guard(); const result = await store.listRuns(input); telemetry.emit('runs_listed'); return result; },
    pause: (input: RunControlInput) => control('pause', input),
    resume: (input: RunControlInput) => control('resume', input),
    cancel: (input: RunControlInput) => control('cancel', input)
  });
}

/** Global logical storage measurements and local observations remain separate contracts. */
export function observationOperator(store: BatchRedisStore, guard: () => void, telemetry: Telemetry,
  runtime: () => { executions: number; residualExecutions: number; callbacks: number; residualCallbacks: number; localDefinitions: number }): Pick<BatchOperator, 'health' | 'capacity' | 'metrics'> {
  return {
    capacity: Object.freeze({ async snapshot() { guard(); return store.capacitySnapshot(); } }),
    metrics: Object.freeze({ snapshot() { guard(); return telemetry.snapshot(); } }),
    health: Object.freeze({ async snapshot(): Promise<HealthSnapshot> {
      guard();
      const local = runtime();
      const physical = Object.freeze({ executions: local.executions, residualExecutions: local.residualExecutions,
        callbacks: local.callbacks, residualCallbacks: local.residualCallbacks, telemetryInFlight: telemetry.snapshot().telemetry.inFlight });
      try {
        const sample = await store.healthSample();
        const reason = sample.definitions.withoutMember > 0 ? 'DEFINITION_UNAVAILABLE'
          : local.residualExecutions > 0 ? 'RESIDUAL_EXECUTION' : local.residualCallbacks > 0 ? 'RESIDUAL_CALLBACK'
          : sample.backlog.expiredLeases > 0 ? 'RECOVERY_BACKLOG' : null;
        return Object.freeze({ origin: 'redis', sampledAt: sample.capacity.sampledAt, status: reason ? 'degraded' : 'ready',
          connection: 'available', protocol: 'matched', reason, runtime: physical,
          definitions: Object.freeze({ local: local.localDefinitions, ...sample.definitions }), backlog: sample.backlog });
      } catch (error) {
        const code = isQueuebitError(error) ? error.code : 'STORAGE_INCONSISTENT';
        const available = store.connection.ready;
        return Object.freeze({ origin: 'local', sampledAt: Date.now(), status: available ? 'degraded' : 'unavailable',
          connection: available ? 'available' : 'unavailable', protocol: code === 'SCHEMA_MISMATCH' ? 'mismatch' : 'unverified',
          reason: code, runtime: physical, definitions: Object.freeze({ local: local.localDefinitions, sampled: null, withoutMember: null, complete: false }), backlog: null });
      }
    } })
  };
}
