/** JSON accepted by the persistent protocol; serialization is validated at runtime. */
export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export type DeepReadonly<T> = T extends (...args: never[]) => unknown ? T
  : T extends readonly (infer U)[] ? readonly DeepReadonly<U>[]
  : T extends object ? { readonly [K in keyof T]: DeepReadonly<T[K]> } : T;

export type EventKind = 'batchSettled' | 'success' | 'failure';
export type EventStatus = 'pending' | 'retrying' | 'delivering' | 'delivered' | 'dead_letter';
export type QueueMode = 'all' | 'producer' | 'consumer';
export type RunStatus = 'pending' | 'running' | 'retrying' | 'blocked' | 'pausing'
  | 'paused' | 'success' | 'failed' | 'cancelled';

export interface BackoffPolicy { baseMs: number; maxMs: number; jitter: 'full' }
export interface ExecutionPolicy { attempts: number; timeoutMs: number; backoff: BackoffPolicy }
export interface PolicyInput { attempts?: number; timeoutMs?: number; backoff?: Partial<BackoffPolicy> }
export interface TlsOptions { ca?: string; cert?: string; key?: string; servername?: string }
export interface RedisAddress { host: string; port: number }
export interface RedisAuth { username?: string; password?: string }
export type RedisOptions =
  | { mode: 'url'; url: string; tls?: TlsOptions }
  | ({ mode: 'direct'; database?: number; tls?: TlsOptions } & RedisAddress & RedisAuth)
  | { mode: 'sentinel'; name: string; seeds: RedisAddress[]; nodeAuth?: RedisAuth;
      sentinelAuth?: RedisAuth; nodeTls?: TlsOptions; sentinelTls?: TlsOptions;
      database?: number; addressMap?: Record<string, RedisAddress> };

export interface LeasePolicy { leaseMs: number; heartbeatMs: number; pollMs: number; recoveryLimit: number }
export interface RetentionPolicy { runMs: number; deliveredEventMs: number; deadLetterMs: number }
export interface CapacityLimits {
  nonterminalRunMax: number; runMax: number; objectMax: number; unfinishedEventMax: number;
  definitionMax: number; memberMax: number; totalBytes: number;
}
export interface MaintenancePolicy { batchSize: number; maxBatchesPerTick: number; timeBudgetMs: number }
export interface ProtocolOptions {
  callback?: PolicyInput; lease?: Partial<LeasePolicy>; retention?: Partial<RetentionPolicy>;
  limits?: Partial<CapacityLimits>; maintenance?: Partial<MaintenancePolicy>;
}
export interface Protocol {
  callback: ExecutionPolicy; lease: LeasePolicy; retention: RetentionPolicy;
  limits: CapacityLimits; maintenance: MaintenancePolicy;
}
export interface RuntimeOptions {
  mode?: QueueMode; concurrency?: number; callbackConcurrency?: number; closeGraceMs?: number;
}
export interface TelemetryRecord {
  readonly timestamp: number; readonly origin: 'local' | 'redis'; readonly code: string;
  readonly taskName?: string; readonly value: number;
}
export interface BatchQueueOptions {
  namespace: string; redis: RedisOptions; runtime?: RuntimeOptions; defaults?: PolicyInput;
  protocol?: ProtocolOptions; telemetry?: { sink: (record: TelemetryRecord) => void | Promise<void> };
}
export interface TaskContract { name: string; version: string; events: readonly EventKind[]; policy?: PolicyInput }
export interface ErrorEnvelope {
  readonly code: string; readonly name: string; readonly message: string;
  readonly truncated: boolean; readonly stack?: string;
}

declare const controlBrand: unique symbol;
/** Opaque attempt-owned value. Only ctx.next()/ctx.end() can create an accepted result. */
export interface BatchControl { readonly [controlBrand]: true }
export interface ExecuteContext<Q, S> {
  readonly query: DeepReadonly<Q>; readonly state: DeepReadonly<S> | null;
  readonly runId: string; readonly batchId: string; readonly page: number;
  readonly attempt: number; readonly signal: AbortSignal;
  next(state?: S | null): BatchControl;
  end(): BatchControl;
}
export interface CallbackContext<Q, S> {
  readonly eventId: string; readonly kind: EventKind; readonly runId: string;
  readonly timestamp: number;
  readonly batchId: string; readonly sequence: number; readonly query: DeepReadonly<Q>;
  readonly state: DeepReadonly<S> | null; readonly error: ErrorEnvelope | null;
  readonly deliveryAttempt: number; readonly replayGeneration: number;
  readonly lateReplay: boolean; readonly signal: AbortSignal;
}
export interface TaskHandlers<Q, S> {
  execute: (context: ExecuteContext<Q, S>) => BatchControl | Promise<BatchControl>;
  onBatchSettled?: (context: CallbackContext<Q, S>) => unknown;
  onSuccess?: (context: CallbackContext<Q, S>) => unknown;
  onFailure?: (context: CallbackContext<Q, S>) => unknown;
}
export interface RunInfo<Q, S> {
  readonly runId: string; readonly taskName: string; readonly version: string;
  readonly status: RunStatus; readonly reason: string | null; readonly revision: number;
  readonly page: number; readonly batchId: string; readonly dispatchCount: number;
  readonly businessFailures: number; readonly scheduledRetries: number; readonly recoveries: number;
  readonly batchFailures: number; readonly consecutiveRecoveries: number;
  readonly query: DeepReadonly<Q>; readonly state: DeepReadonly<S> | null;
  readonly error: ErrorEnvelope | null; readonly effectivePolicy: DeepReadonly<ExecutionPolicy>;
  readonly createdAt: number; readonly dueAt: number | null; readonly terminalAt: number | null;
  readonly callbacks: { readonly pending: number; readonly delivered: number; readonly deadLetters: number };
}
export type CancelResult = { found: false; runId: string }
  | { found: true; runId: string; status: 'cancelled' | 'success' | 'failed'; revision: number; changed: boolean };
export interface BatchTask<Q, S> {
  start(input: { query: Q; idempotencyKey?: string }): Promise<{ runId: string; created: boolean }>;
  get(runId: string): Promise<RunInfo<Q, S> | null>;
  cancel(runId: string): Promise<CancelResult>;
}
export interface CloseResult {
  status: 'closed'; timedOut: boolean; remainingExecutions: number; remainingCallbacks: number;
}

export interface RunControlInput { runId: string; expectedRevision: number; reason: string; commandId: string }
export type RunControlResult = { readonly kind: 'not_found'; readonly id: string }
  | { readonly kind: 'applied'; readonly id: string; readonly revision: number; readonly status: RunStatus; readonly changed: true }
  | { readonly kind: 'noop'; readonly id: string; readonly revision: number; readonly status: RunStatus; readonly changed: false };
export interface RunFilter { taskName?: string; status?: RunStatus }
export interface RunListInput extends RunFilter { limit?: number; cursor?: string }
/** Bounded diagnostic fields only; payload and error stack require the task's explicit get(). */
export interface RunMetadata extends Omit<RunInfo<never, never>, 'query' | 'state' | 'error' | 'effectivePolicy' | 'callbacks'> {
  readonly lease: { readonly revision: number; readonly deadline: number | null };
  readonly budget: { readonly reservedBytes: number; readonly reservedEvents: number };
}
export interface RunListResult {
  readonly items: readonly RunMetadata[]; readonly nextCursor: string | null; readonly consistency: 'live';
}
export interface RunsOperator {
  getMetadata(runId: string): Promise<RunMetadata | null>;
  list(input?: RunListInput): Promise<RunListResult>;
  pause(input: RunControlInput): Promise<RunControlResult>;
  resume(input: RunControlInput): Promise<RunControlResult>;
  cancel(input: RunControlInput): Promise<RunControlResult>;
}
export interface EventReplayInput { eventId: string; expectedRevision: number; reason: string; commandId: string }
export type EventReplayResult = { readonly kind: 'not_found'; readonly id: string }
  | { readonly kind: 'noop'; readonly id: string; readonly revision: number; readonly status: EventStatus; readonly changed: false }
  | { readonly kind: 'applied'; readonly id: string; readonly eventId: string; readonly revision: number;
    readonly status: 'pending'; readonly changed: true; readonly replayGeneration: number };
export interface DeadLetterFilter { taskName?: string }
export interface DeadLetterListInput extends DeadLetterFilter { limit?: number; cursor?: string }
/** Operator diagnostics do not include the immutable business query/state/error snapshots. */
export interface DeadLetterMetadata {
  readonly eventId: string; readonly runId: string; readonly batchId: string; readonly taskName: string; readonly version: string;
  readonly kind: EventKind; readonly sequence: number; readonly timestamp: number; readonly status: EventStatus; readonly revision: number;
  readonly deliveryAttempt: number; readonly replayGeneration: number; readonly lateReplay: boolean;
  readonly firstDeadAt: number; readonly deadLetterExpiresAt: number; readonly firstDeadLetterSequence: number;
  readonly replayDrainDeadline: number | null; readonly dueAt: number | null; readonly deliveredAt: number | null;
  readonly lease: { readonly revision: number; readonly deadline: number | null; readonly attemptTimeoutAt: number | null };
}
export interface DeadLetterListResult {
  readonly items: readonly DeadLetterMetadata[]; readonly nextCursor: string | null; readonly consistency: 'live';
}
export interface DeadLettersOperator {
  get(eventId: string): Promise<DeadLetterMetadata | null>;
  list(input?: DeadLetterListInput): Promise<DeadLetterListResult>;
  replay(input: EventReplayInput): Promise<EventReplayResult>;
}
export type MetricCode = 'queue_ready' | 'queue_closing' | 'queue_error' | 'run_created' | 'run_read'
  | 'run_cancelled' | 'operator_pause' | 'operator_resume' | 'operator_cancel' | 'metadata_read' | 'runs_listed'
  | 'run_claimed' | 'run_settled' | 'run_timeout' | 'maintenance_tick'
  | 'callback_claimed' | 'callback_settled' | 'callback_timeout' | 'event_replayed' | 'dead_letter_read' | 'dead_letters_listed';
export interface MetricsSnapshot {
  readonly origin: 'local'; readonly sampledAt: number; readonly counters: Readonly<Record<MetricCode, number>>;
  readonly telemetry: { readonly buffered: number; readonly inFlight: boolean; readonly dropped: number; readonly sinkFailures: number };
}
export interface CapacitySnapshot {
  readonly origin: 'redis'; readonly scope: 'namespace'; readonly sampledAt: number; readonly revision: number;
  readonly counts: { readonly runs: number; readonly nonterminalRuns: number; readonly events: number; readonly unfinishedEvents: number;
    readonly definitions: number; readonly members: number; readonly reservedObjects: number; readonly reservedEvents: number };
  readonly bytes: { readonly charged: number; readonly reserved: number; readonly businessLimit: number; readonly memberPartition: number;
    readonly memberUsed: number; readonly totalLimit: number };
  readonly newStarts: 'open' | 'closed';
}
export interface HealthSnapshot {
  readonly origin: 'redis' | 'local'; readonly sampledAt: number; readonly status: 'ready' | 'degraded' | 'unavailable';
  readonly connection: 'available' | 'unavailable'; readonly protocol: 'matched' | 'mismatch' | 'unverified'; readonly reason: string | null;
  readonly runtime: { readonly executions: number; readonly residualExecutions: number; readonly callbacks: number; readonly residualCallbacks: number; readonly telemetryInFlight: boolean };
  readonly definitions: { readonly local: number; readonly sampled: number | null; readonly withoutMember: number | null; readonly complete: boolean };
  readonly backlog: { readonly expiredLeases: number; readonly runGcDue: number; readonly eventGcDue: number; readonly pendingEvents: number } | null;
}
export interface BatchOperator {
  readonly runs: RunsOperator;
  readonly deadLetters: DeadLettersOperator;
  readonly health: { snapshot(): Promise<HealthSnapshot> };
  readonly capacity: { snapshot(): Promise<CapacitySnapshot> };
  readonly metrics: { snapshot(): MetricsSnapshot };
}

export interface BatchQueue {
  readonly operator: BatchOperator;
  define<Q, S>(contract: TaskContract, handlers?: TaskHandlers<Q, S>): BatchTask<Q, S>;
  ready(): Promise<void>;
  close(): Promise<CloseResult>;
}
