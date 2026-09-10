export type QueuebitErrorCode = 'CONFIG_INVALID' | 'JSON_INVALID' | 'PAYLOAD_TOO_LARGE'
  | 'QUEUE_NOT_READY' | 'QUEUE_FAILED' | 'QUEUE_CLOSING' | 'QUEUE_CLOSED'
  | 'RESOURCE_CLEANUP_FAILED' | 'MODE_OPERATION_NOT_ALLOWED' | 'TASK_IDENTITY_MISMATCH'
  | 'IDEMPOTENCY_CONFLICT' | 'DEFINITION_HASH_CONFLICT' | 'COMMAND_CONFLICT'
  | 'REVISION_CONFLICT' | 'CONTROL_RECORD_TOO_LARGE' | 'CAPACITY_EXCEEDED' | 'LEASE_LOST'
  | 'SCHEMA_MISMATCH' | 'OUTCOME_UNKNOWN' | 'STORAGE_INCONSISTENT' | 'CURSOR_INVALID'
  | 'CURSOR_EXPIRED' | 'INDEX_INCONSISTENT' | 'READ_CONFLICT' | 'SEQUENCE_EXHAUSTED'
  | 'NAMESPACE_ORPHANED' | 'HANDLER_CONTRACT_INVALID' | 'CONNECTION_UNAVAILABLE';

export interface QueuebitErrorOptions {
  operation?: string; runId?: string; eventId?: string; commandId?: string;
  retryable?: boolean; outcomeKnown?: boolean;
}

/** Stable SDK error. Transport uncertainty is explicit, never inferred from retryable. */
const errorBrand = Symbol.for('queuebit.batch.error.v1');
export class QueuebitError extends Error {
  readonly [errorBrand] = true;
  readonly code: QueuebitErrorCode;
  readonly operation: string;
  readonly runId?: string;
  readonly eventId?: string;
  readonly commandId?: string;
  readonly retryable: boolean;
  readonly outcomeKnown: boolean;

  constructor(code: QueuebitErrorCode, message: string, options: QueuebitErrorOptions = {}) {
    super(message);
    this.name = 'QueuebitError';
    this.code = code;
    this.operation = options.operation ?? 'validate';
    this.retryable = options.retryable ?? false;
    this.outcomeKnown = options.outcomeKnown ?? true;
    if (options.runId !== undefined) this.runId = options.runId;
    if (options.eventId !== undefined) this.eventId = options.eventId;
    if (options.commandId !== undefined) this.commandId = options.commandId;
  }
}

/** Internal guard also recognizes the same SDK error across isolated ESM/CJS test bundles. */
export function isQueuebitError(error: unknown): error is QueuebitError {
  return error instanceof Error && Object.getOwnPropertyDescriptor(error, errorBrand)?.value === true;
}
