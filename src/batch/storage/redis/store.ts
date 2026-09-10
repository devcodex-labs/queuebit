import { randomBytes } from 'node:crypto';
import { ErrorReply } from '@redis/client';
import type { CancelResult, ErrorEnvelope, ExecutionPolicy, RunInfo, RunStatus, RunControlInput, RunControlResult, RunListInput, RunListResult, RunMetadata, CapacitySnapshot, HealthSnapshot } from '../../api/types.js';
import { isQueuebitError, QueuebitError } from '../../api/errors.js';
import type { QueuebitErrorCode } from '../../api/errors.js';
import type { DeadLetterMetadata, DeadLetterListInput, DeadLetterListResult, EventStatus, EventReplayInput, EventReplayResult } from '../../api/types.js';
import { canonicalJson, digest, encodeIdempotencyKey, errorEnvelope, frozenJson, JSON_LIMITS } from '../../domain/json.js';
import { backoffDelay, integer, mergePolicy } from '../../domain/config.js';
import type { NormalizedOptions } from '../../domain/config.js';
import type { Definition } from '../../domain/definition.js';
import { normalizeRunControl, normalizeRunList, validateControlResult } from '../../domain/operator.js';
import type { RunControlOperation } from '../../domain/operator.js';
import { decodeRunCursor, encodeRunCursor, decodeDeadLetterCursor, encodeDeadLetterCursor } from '../../domain/cursor.js';
import { eventIdentifier, EVENT_STATUSES, normalizeReplay, normalizeDeadLetterList, validateReplayResult } from '../../domain/events.js';
import { BatchRedisConnection, transportBudget } from './connection.js';
import type { TransportBudget } from './connection.js';
import { BatchKeys, identifier, RUN_STATUSES } from './keys.js';
import { BATCH_SCRIPT, BATCH_SCRIPT_SHA } from './scripts.js';

type KeyType = 'hash' | 'zset' | 'string';
interface KeyPlan { names: string[]; types: KeyType[]; roles: Record<string, number> }
export interface ExecutionLease {
  readonly tokenCanonical: string; readonly definitionIdentity: string; readonly deadline: number;
  readonly attemptTimeoutAt: number; readonly run: RunInfo<unknown, unknown>;
  readonly timeoutRemainingMs: number;
}
export interface SettlementInput {
  kind: 'next' | 'end' | 'business' | 'timeout' | 'contract'; stateCanonical?: string; error?: ErrorEnvelope;
}
export interface EventInfo extends DeadLetterMetadata {
  readonly definitionIdentity: string; readonly query: unknown; readonly state: unknown;
  readonly error: ErrorEnvelope | null; readonly deliveryError: ErrorEnvelope | null;
}
export interface EventLease {
  readonly tokenCanonical: string; readonly definitionIdentity: string; readonly deadline: number;
  readonly attemptTimeoutAt: number; readonly timeoutRemainingMs: number; readonly deadlineRemainingMs: number;
  readonly event: EventInfo;
}
interface EventReply { event: Record<string, string | number>; query: string }
interface EventOutcome { changed: boolean; status?: EventStatus; revision?: number }
export interface NamespaceHealthSample {
  capacity: CapacitySnapshot; definitions: { sampled: number; withoutMember: number; complete: boolean };
  backlog: NonNullable<HealthSnapshot['backlog']>;
}
const id = (): string => randomBytes(16).toString('hex');
const inconsistency = (): never => { throw new QueuebitError('STORAGE_INCONSISTENT', 'Persistent batch data is inconsistent'); };
function hashReply(value: unknown): Record<string, string> {
  if (!Array.isArray(value) || value.length % 2) return inconsistency();
  const result: Record<string, string> = Object.create(null) as Record<string, string>;
  for (let index = 0; index < value.length; index += 2) {
    if (typeof value[index] !== 'string' || typeof value[index + 1] !== 'string') return inconsistency();
    result[value[index]] = value[index + 1];
  }
  return result;
}
function number(value: unknown): number {
  if (typeof value !== 'string' || !/^\d+$/.test(value) || !Number.isSafeInteger(Number(value))) return inconsistency();
  return Number(value);
}

/** Internal Redis port; no root export and no externally supplied Redis client. */
export class BatchRedisStore {
  readonly connection: BatchRedisConnection;
  readonly config: NormalizedOptions;
  readonly keys: BatchKeys;
  #initialized = false;
  #inconsistent = false;
  #maintenancePhase = 0;
  #catalogSequence = 0;
  #eventOffsets = [0, 0];
  #eventTurn = 0;
  #blockedSequences = new Map<string, { sequence: number; blocked: boolean }>();
  constructor(connection: BatchRedisConnection, config: NormalizedOptions) {
    this.connection = connection; this.config = config; this.keys = new BatchKeys(config.namespace);
  }
  #guard(): void {
    if (this.#inconsistent) inconsistency();
    if (!this.#initialized) throw new QueuebitError('QUEUE_NOT_READY', 'Call ready before using the batch store');
  }
  #plan(): KeyPlan { return { names: [], types: [], roles: Object.create(null) as Record<string, number> }; }
  #key(plan: KeyPlan, role: string, key: string, type: KeyType): void {
    plan.names.push(key); plan.types.push(type); plan.roles[role] = plan.names.length;
  }
  #basePlan(): KeyPlan {
    const plan = this.#plan();
    this.#key(plan, 'meta', this.keys.meta, 'hash'); this.#key(plan, 'capacity', this.keys.capacity, 'hash');
    return plan;
  }
  #runPlan(runId: string, definitionIdentity: string, taskName: string, encoded?: string): KeyPlan {
    const plan = this.#basePlan();
    const values: [string, string, KeyType][] = [
      ['run', this.keys.run(runId), 'hash'], ['definition', this.keys.definition(definitionIdentity), 'hash'],
      ['definitions', this.keys.definitions, 'zset'], ['gcDefinitions', this.keys.gcDefinitions, 'zset'],
      ['runs', this.keys.runs(), 'zset'], ['taskRuns', this.keys.runs(taskName), 'zset'],
      ['due', this.keys.due(definitionIdentity), 'zset'], ['blocked', this.keys.blocked(definitionIdentity), 'zset'],
      ['leases', this.keys.leases, 'zset'], ['gcRuns', this.keys.gcRuns, 'zset']
    ];
    for (const status of RUN_STATUSES) {
      values.push([`status_${status}`, this.keys.runs(undefined, status), 'zset'], [`taskStatus_${status}`, this.keys.runs(taskName, status), 'zset']);
    }
    if (encoded !== undefined) values.push(['idem', this.keys.idempotency(taskName, encoded), 'string']);
    for (const [role, key, type] of values) this.#key(plan, role, key, type);
    return plan;
  }

  async #execute<T>(plan: KeyPlan, request: Record<string, unknown>, deadline: TransportBudget, write = true): Promise<T> {
    const encoded = canonicalJson({ ...request, keys: plan.roles, types: plan.types,
      protocolCanonical: this.config.protocolCanonical, protocolDigest: digest(this.config.protocolCanonical) }, 2 * 1024 * 1024);
    if (plan.names.length > 1024) throw new QueuebitError('CONFIG_INVALID', 'Too many transaction keys');
    const tail = [String(plan.names.length), ...plan.names, encoded];
    const context = { write, budget: deadline, ...(typeof request.runId === 'string' ? { runId: request.runId } : {}),
      ...(typeof request.commandId === 'string' ? { commandId: request.commandId } : {}) };
    let reply: unknown;
    try {
      try { reply = await this.connection.command(['EVALSHA', BATCH_SCRIPT_SHA, ...tail], context); }
      catch (error) {
        if (!(error instanceof ErrorReply) || !error.message.startsWith('NOSCRIPT')) throw error;
        // EVAL reloads the identical static body and executes the identical request in one round trip.
        reply = await this.connection.command(['EVAL', BATCH_SCRIPT, ...tail], context);
      }
    } catch (error) {
      if (isQueuebitError(error)) throw error;
      if (!write) throw new QueuebitError('STORAGE_INCONSISTENT', 'Read-only Redis query failed', { operation: String(request.op) });
      this.#inconsistent = true;
      // An unexpected script failure can be post-write. This mark is best-effort, not a cluster-freeze claim.
      try { await this.connection.command(['HSET', this.keys.meta, 'status', 'inconsistent'], { write: true, budget: deadline }); } catch {}
      throw new QueuebitError('STORAGE_INCONSISTENT', 'Redis transaction failed outside a validated outcome', { outcomeKnown: false });
    }
    if (typeof reply !== 'string') return inconsistency();
    let parsed: unknown;
    try { parsed = JSON.parse(reply); } catch { return inconsistency(); }
    if (parsed === null) return null as T;
    if (typeof parsed !== 'object' || Array.isArray(parsed)) return inconsistency();
    if ('error' in parsed) {
      const allowed = ['STORAGE_INCONSISTENT', 'SCHEMA_MISMATCH', 'NAMESPACE_ORPHANED', 'CAPACITY_EXCEEDED',
        'IDEMPOTENCY_CONFLICT', 'DEFINITION_HASH_CONFLICT', 'READ_CONFLICT', 'SEQUENCE_EXHAUSTED', 'LEASE_LOST', 'COMMAND_CONFLICT',
        'REVISION_CONFLICT', 'CONTROL_RECORD_TOO_LARGE', 'CURSOR_EXPIRED', 'INDEX_INCONSISTENT'];
      const code = typeof parsed.error === 'string' && allowed.includes(parsed.error) ? parsed.error as QueuebitErrorCode : 'STORAGE_INCONSISTENT';
      if (code === 'STORAGE_INCONSISTENT' && write) this.#inconsistent = true;
      throw new QueuebitError(code, `Batch transaction rejected: ${code}`, { operation: String(request.op),
        ...(typeof request.runId === 'string' ? { runId: request.runId } : {}),
        ...(typeof request.commandId === 'string' ? { commandId: request.commandId } : {}) });
    }
    return parsed as T;
  }

  /** Namespace creation is preceded by a bounded orphan check tied to one primary process identity. */
  async ready(): Promise<void> {
    const deadline = transportBudget();
    await this.connection.connect(deadline.deadline);
    const info = await this.connection.command(['INFO', 'server'], { budget: deadline });
    if (typeof info !== 'string') return inconsistency();
    const version = /^redis_version:(\d+)\.(\d+)/m.exec(info);
    if (!version || Number(version[1]) < 7 || (Number(version[1]) === 7 && Number(version[2]) < 2)
      || !/^redis_mode:standalone\r?$/m.test(info)) throw new QueuebitError('CONFIG_INVALID', 'Redis >=7.2 standalone primary is required');
    const primaryId = /^run_id:(.+)$/m.exec(info)?.[1]?.trim();
    if (!primaryId) return inconsistency();
    const role = await this.connection.command(['ROLE'], { budget: deadline });
    if (!Array.isArray(role) || role[0] !== 'master') throw new QueuebitError('CONFIG_INVALID', 'Redis connection must address a primary');
    // Independent eviction of an entity or index invalidates the persistent protocol.
    // INFO is already required; observing policy does not require CONFIG administration rights.
    const memory = await this.connection.command(['INFO', 'memory'], { budget: deadline });
    if (typeof memory !== 'string' || !/^maxmemory_policy:noeviction\r?$/m.test(memory)) {
      throw new QueuebitError('CONFIG_INVALID', 'Redis noeviction policy must be observable before readiness');
    }
    const meta = hashReply(await this.connection.command(['HGETALL', this.keys.meta], { budget: deadline }));
    if (!Object.keys(meta).length) {
      let cursor = '0'; let count = 0; let complete = false;
      const scanDeadline = Math.min(deadline.deadline, Date.now() + 2000);
      for (let call = 0; call < 100 && Date.now() < scanDeadline; call++) {
        const response = await this.connection.command(['SCAN', cursor, 'MATCH', `${this.keys.prefix}*`, 'COUNT', '100'], { deadline: scanDeadline, budget: deadline });
        if (!Array.isArray(response) || typeof response[0] !== 'string' || !Array.isArray(response[1])) return inconsistency();
        cursor = response[0]; count += response[1].length;
        if (count > 10000) break;
        if (response[1].length) {
          const winner = hashReply(await this.connection.command(['HGETALL', this.keys.meta], { budget: deadline }));
          if (winner.schema) { complete = true; break; }
          throw new QueuebitError('NAMESPACE_ORPHANED', 'Namespace contains keys without protocol metadata');
        }
        if (cursor === '0') { complete = true; break; }
      }
      if (!complete) throw new QueuebitError('NAMESPACE_ORPHANED', 'Orphan scan did not complete within its fixed budget');
      const current = await this.connection.command(['INFO', 'server'], { budget: deadline });
      if (typeof current !== 'string' || /^run_id:(.+)$/m.exec(current)?.[1]?.trim() !== primaryId) throw new QueuebitError('NAMESPACE_ORPHANED', 'Primary changed during namespace initialization');
    }
    await this.#execute(this.#basePlan(), { op: 'init', commandId: id() }, deadline);
    this.#initialized = true;
  }

  async start<Q, S>(definition: Definition<Q, S>, input: { query: Q; idempotencyKey?: string }): Promise<{ runId: string; created: boolean }> {
    this.#guard();
    if (!input || typeof input !== 'object' || (Object.getPrototypeOf(input) !== Object.prototype && Object.getPrototypeOf(input) !== null)) throw new QueuebitError('CONFIG_INVALID', 'Invalid start input');
    const descriptors = Object.getOwnPropertyDescriptors(input);
    for (const key of Reflect.ownKeys(descriptors)) {
      if ((key !== 'query' && key !== 'idempotencyKey') || !Object.hasOwn(descriptors[key]!, 'value')) throw new QueuebitError('CONFIG_INVALID', 'Invalid start field');
    }
    const query = canonicalJson(descriptors.query?.value, JSON_LIMITS.query);
    const encodedKey = Object.hasOwn(descriptors, 'idempotencyKey') ? encodeIdempotencyKey(descriptors.idempotencyKey!.value) : undefined;
    const policyCanonical = canonicalJson(mergePolicy(this.config.defaults, definition.policy));
    const commandId = id(); const candidateRunId = id(); const deadline = transportBudget();
    for (let attempt = 0; attempt < 3; attempt++) {
      let runId = candidateRunId;
      if (encodedKey !== undefined) {
        const existing = await this.connection.command(['GET', this.keys.idempotency(definition.name, encodedKey)], { budget: deadline });
        if (existing !== null) { try { runId = identifier(existing); } catch { return inconsistency(); } }
      }
      const plan = this.#runPlan(runId, definition.identity, definition.name, encodedKey);
      try {
        return await this.#execute(plan, { op: 'start', commandId, runId, taskName: definition.name, version: definition.version,
          definitionIdentity: definition.identity, definitionCanonical: definition.canonical, query, policyCanonical,
          hasIdempotency: encodedKey !== undefined, idempotencyKey: encodedKey === undefined ? '' : this.keys.idempotency(definition.name, encodedKey) }, deadline);
      } catch (error) { if (!isQueuebitError(error) || error.code !== 'READ_CONFLICT' || attempt === 2) throw error; }
    }
    throw new QueuebitError('READ_CONFLICT', 'Start identity changed repeatedly');
  }

  async #readRun(runId: string, deadline: TransportBudget): Promise<Record<string, string> | null> {
    let fields: Record<string, string>;
    try { fields = hashReply(await this.connection.command(['HGETALL', this.keys.run(runId)], { budget: deadline })); }
    catch (error) { if (error instanceof ErrorReply) return inconsistency(); throw error; }
    if (!Object.keys(fields).length) return null;
    if (fields.schema !== 'batch-v1' || fields.runId !== runId || !RUN_STATUSES.includes(fields.status as RunStatus)) return inconsistency();
    return fields;
  }
  async get(runId: string, definitionIdentity?: string): Promise<RunInfo<unknown, unknown> | null> {
    this.#guard(); identifier(runId);
    const run = await this.#readRun(runId, transportBudget());
    if (!run) return null;
    if (definitionIdentity !== undefined && run.definitionIdentity !== definitionIdentity) throw new QueuebitError('TASK_IDENTITY_MISMATCH', 'Run belongs to a different task definition');
    return this.#runInfo(run);
  }
  #runInfo(run: Record<string, string>): RunInfo<unknown, unknown> {
    const runId = identifier(run.runId);
    try {
      for (const field of ['taskName','version','definitionIdentity','definitionCanonical','policyCanonical','query','state','error','reason']) {
        if (typeof run[field] !== 'string') return inconsistency();
      }
      const result: RunInfo<unknown, unknown> = {
        runId, taskName: run.taskName!, version: run.version!, status: run.status as RunStatus,
        reason: run.reason || null, revision: number(run.revision), page: number(run.page), batchId: `${runId}:${number(run.page)}`,
        dispatchCount: number(run.dispatchCount), businessFailures: number(run.businessFailures), scheduledRetries: number(run.scheduledRetries),
        recoveries: number(run.recoveries), batchFailures: number(run.batchFailures), consecutiveRecoveries: number(run.consecutiveRecoveries),
        query: frozenJson(run.query!), state: frozenJson(run.state!), error: frozenJson<ErrorEnvelope | null>(run.error!),
        effectivePolicy: frozenJson<ExecutionPolicy>(run.policyCanonical!), createdAt: number(run.createdAt),
        dueAt: number(run.dueAt) || null, terminalAt: number(run.terminalAt) || null,
        callbacks: Object.freeze({ pending: number(run.callbackPending), delivered: number(run.callbackDelivered), deadLetters: number(run.callbackDeadLetters) })
      };
      return Object.freeze(result);
    } catch { return inconsistency(); }
  }
  async cancel(runId: string, definitionIdentity?: string): Promise<CancelResult> {
    this.#guard(); identifier(runId);
    const deadline = transportBudget(); const commandId = id();
    const run = await this.#readRun(runId, deadline);
    if (!run) return { found: false, runId };
    if (definitionIdentity !== undefined && run.definitionIdentity !== definitionIdentity) throw new QueuebitError('TASK_IDENTITY_MISMATCH', 'Run belongs to a different task definition');
    if (!run.definitionIdentity || !run.taskName) return inconsistency();
    return this.#execute(this.#runPlan(runId, run.definitionIdentity, run.taskName), {
      op: 'cancel', runId, commandId, definitionIdentity: run.definitionIdentity, taskName: run.taskName
    }, deadline);
  }

  /** Operator does not depend on local define(); all state decisions are made by the shared Lua transaction. */
  async control(operation: RunControlOperation, input: RunControlInput): Promise<RunControlResult> {
    this.#guard();
    const control = normalizeRunControl(operation, input);
    const deadline = transportBudget();
    const run = await this.#readRun(control.runId, deadline);
    if (!run) return Object.freeze({ kind: 'not_found', id: control.runId });
    if (!run.definitionIdentity || !run.taskName) return inconsistency();
    const plan = this.#runPlan(control.runId, run.definitionIdentity, run.taskName);
    this.#key(plan, 'definitionMembers', this.keys.definitionMembers(run.definitionIdentity), 'zset');
    const summary = Array.from(control.reason).slice(0, 40).join('');
    try {
      const result = await this.#execute(plan, { op: 'control', runId: control.runId,
        commandId: canonicalJson(control.commandId), definitionIdentity: run.definitionIdentity, taskName: run.taskName,
        control: JSON.parse(control.wireCanonical) as unknown, controlCanonical: control.wireCanonical,
        auditReason: canonicalJson(summary), auditTruncated: summary !== control.reason }, deadline);
      return validateControlResult(result);
    } catch (error) {
      if (!isQueuebitError(error)) throw error;
      throw new QueuebitError(error.code, error.message, { operation, runId: control.runId, commandId: control.commandId,
        retryable: error.retryable, outcomeKnown: error.outcomeKnown });
    }
  }

  /** This query cannot read payload fields or mutate the namespace on a read failure. */
  async getMetadata(runId: string): Promise<RunMetadata | null> {
    this.#guard(); identifier(runId);
    const plan = this.#basePlan(); this.#key(plan, 'run', this.keys.run(runId), 'hash');
    const result = await this.#execute<RunMetadata | null>(plan, { op: 'getMetadata', runId }, transportBudget(), false);
    return result === null ? null : frozenJson<RunMetadata>(canonicalJson(result, 16384));
  }

  async capacitySnapshot(): Promise<CapacitySnapshot> {
    this.#guard();
    return frozenJson<CapacitySnapshot>(canonicalJson(await this.#execute(this.#basePlan(), { op: 'capacitySnapshot' }, transportBudget(), false), 8192));
  }
  /** The health sample's scope is explicit: a bounded catalog prefix, not an invented global match count. */
  async healthSample(): Promise<NamespaceHealthSample> {
    this.#guard(); const deadline = transportBudget(); const plan = this.#basePlan();
    const definitions = await this.connection.command(['ZRANGE', this.keys.definitions, '0', String(this.config.protocol.maintenance.batchSize - 1)], { budget: deadline });
    if (!Array.isArray(definitions)) return inconsistency();
    this.#key(plan, 'definitions', this.keys.definitions, 'zset');
    for (const [role, key] of Object.entries({ leases: this.keys.leases, gcRuns: this.keys.gcRuns, gcEvents: this.keys.gcEvents })) this.#key(plan, role, key, 'zset');
    definitions.forEach((identity, index) => {
      if (typeof identity !== 'string') return inconsistency();
      this.#key(plan, `sampleDef_${index + 1}`, this.keys.definition(identity), 'hash');
      this.#key(plan, `sampleMembers_${index + 1}`, this.keys.definitionMembers(identity), 'zset');
    });
    return frozenJson<NamespaceHealthSample>(canonicalJson(await this.#execute(plan, { op: 'healthSample', definitions }, deadline, false), 16384));
  }

  /** Bounded candidate read followed by atomic entity/index validation; the original upper/expiry survive every page. */
  async listRuns(input?: RunListInput): Promise<RunListResult> {
    this.#guard();
    const { filter, limit, cursor } = normalizeRunList(input);
    const deadline = transportBudget();
    const time = await this.connection.command(['TIME'], { budget: deadline });
    if (!Array.isArray(time)) return inconsistency();
    const now = number(time[0]) * 1000 + Math.floor(number(time[1]) / 1000);
    const state = cursor === undefined ? {} : decodeRunCursor(cursor, this.config.namespace, filter, now);
    const base = this.#basePlan();
    this.#key(base, 'listIndex', this.keys.runs(filter.taskName, filter.status), 'zset');
    const prepared = await this.#execute<{ candidates: { id: string; score: number }[]; upperSequence: number; expiresAt: number }>(base,
      { op: 'listPrepare', ...state, limit, filter }, deadline, false);
    if (!Array.isArray(prepared.candidates) || prepared.candidates.length > limit * 4 + 1) return inconsistency();
    prepared.candidates.forEach((candidate, index) => {
      try { identifier(candidate.id); } catch { throw new QueuebitError('INDEX_INCONSISTENT', 'Invalid Run index member'); }
      this.#key(base, `listRun_${index + 1}`, this.keys.run(candidate.id), 'hash');
    });
    const page = await this.#execute<{ items: RunMetadata[]; hasMore: boolean; lastSequence: number }>(base, {
      op: 'listCheck', ...state, ...prepared, limit, filter
    }, deadline, false);
    if (!Array.isArray(page.items) || page.items.length > limit || typeof page.hasMore !== 'boolean') return inconsistency();
    return Object.freeze({ items: Object.freeze(page.items.map(item => frozenJson<RunMetadata>(canonicalJson(item, 16384)))),
      nextCursor: page.hasMore ? encodeRunCursor(this.config.namespace, filter, prepared.upperSequence, page.lastSequence, prepared.expiresAt) : null,
      consistency: 'live' });
  }

  async getDeadLetter(eventId: string): Promise<DeadLetterMetadata | null> {
    this.#guard(); eventIdentifier(eventId); const plan = this.#basePlan(); this.#key(plan, 'event', this.keys.eventById(eventId), 'hash');
    const result = await this.#execute<DeadLetterMetadata | null>(plan, { op: 'deadGet', eventId }, transportBudget(), false);
    return result === null ? null : frozenJson<DeadLetterMetadata>(canonicalJson(result, 32768));
  }
  async listDeadLetters(input?: DeadLetterListInput): Promise<DeadLetterListResult> {
    this.#guard(); const { filter, limit, cursor } = normalizeDeadLetterList(input); const deadline = transportBudget();
    const now = await this.#redisNow(deadline);
    const state = cursor === undefined ? {} : decodeDeadLetterCursor(cursor, this.config.namespace, filter, now);
    const plan = this.#basePlan(); this.#key(plan, 'listIndex', this.keys.deadLetters(filter.taskName), 'zset');
    const prepared = await this.#execute<{ candidates: { id: string; score: number }[]; upperSequence: number; expiresAt: number }>(plan,
      { op: 'deadPrepare', ...state, limit, filter }, deadline, false);
    if (!Array.isArray(prepared.candidates) || prepared.candidates.length > limit * 4 + 1) return inconsistency();
    prepared.candidates.forEach((candidate, index) => {
      try { eventIdentifier(candidate.id); } catch { throw new QueuebitError('INDEX_INCONSISTENT', 'Invalid Event index member'); }
      this.#key(plan, `listEvent_${index + 1}`, this.keys.eventById(candidate.id), 'hash');
    });
    const page = await this.#execute<{ items: DeadLetterMetadata[]; hasMore: boolean; lastSequence: number }>(plan,
      { op: 'deadCheck', ...state, ...prepared, limit, filter }, deadline, false);
    if (!Array.isArray(page.items) || page.items.length > limit || typeof page.hasMore !== 'boolean') return inconsistency();
    return Object.freeze({ items: Object.freeze(page.items.map(item => frozenJson<DeadLetterMetadata>(canonicalJson(item, 32768)))),
      nextCursor: page.hasMore ? encodeDeadLetterCursor(this.config.namespace, filter, prepared.upperSequence, page.lastSequence, prepared.expiresAt) : null,
      consistency: 'live' });
  }

  #runtimePlan(runtimeId: string, definitions: readonly { identity: string }[]): KeyPlan {
    const plan = this.#basePlan();
    this.#key(plan, 'runtime', this.keys.runtime(runtimeId), 'hash');
    this.#key(plan, 'members', this.keys.members, 'zset');
    this.#key(plan, 'definitions', this.keys.definitions, 'zset');
    this.#key(plan, 'gcDefinitions', this.keys.gcDefinitions, 'zset');
    definitions.forEach((definition, index) => {
      this.#key(plan, `registrationDef_${index + 1}`, this.keys.definition(definition.identity), 'hash');
      this.#key(plan, `registrationMembers_${index + 1}`, this.keys.definitionMembers(definition.identity), 'zset');
    });
    return plan;
  }
  async registerRuntime(runtimeId: string, generation: number, definitions: readonly Definition<unknown, unknown>[]): Promise<void> {
    this.#guard(); identifier(runtimeId); integer(generation, 1, Number.MAX_SAFE_INTEGER, 'generation');
    if (!definitions.length || definitions.length > 128) throw new QueuebitError('CONFIG_INVALID', 'Execution members need 1..128 definitions');
    const manifest = canonicalJson(definitions.map(definition => definition.identity), 64 * 1024);
    const deadline = transportBudget();
    await this.reclaimExpiredMembers(deadline);
    // Registration is invisible to claim until the final complete manifest is published.
    for (let offset = 0; offset < definitions.length; offset += 32) {
      const chunk = definitions.slice(offset, offset + 32).map(definition => ({ identity: definition.identity, canonical: definition.canonical }));
      await this.#execute(this.#runtimePlan(runtimeId, chunk), { op: 'register', runtimeId, generation, manifest,
        commandId: id(), definitions: chunk, complete: false }, deadline);
    }
    const all = definitions.map(definition => ({ identity: definition.identity, canonical: definition.canonical }));
    await this.#execute(this.#runtimePlan(runtimeId, all), { op: 'register', runtimeId, generation, manifest,
      commandId: id(), definitions: all, complete: true }, deadline);
  }
  async #runtimeOperation(op: 'renewRuntime' | 'unregister' | 'gcRuntime', runtimeId: string, generation: number, outerDeadline: number | TransportBudget = Date.now() + 10000): Promise<void> {
    const deadline = typeof outerDeadline === 'number' ? transportBudget(outerDeadline) : outerDeadline;
    const runtime = hashReply(await this.connection.command(['HGETALL', this.keys.runtime(runtimeId)], { budget: deadline }));
    if (!Object.keys(runtime).length) {
      if (op === 'unregister' || op === 'gcRuntime') return this.#verifyAbsentCandidate(this.keys.runtime(runtimeId), [this.keys.members], runtimeId, deadline);
      throw new QueuebitError('LEASE_LOST', 'Execution member expired');
    }
    let registered: unknown;
    try { registered = JSON.parse(runtime.registered ?? 'null'); } catch { return inconsistency(); }
    if (!Array.isArray(registered) || registered.length > 128 || registered.some(value => typeof value !== 'string')) return inconsistency();
    const definitions = (registered as string[]).map(identity => ({ identity }));
    await this.#execute(this.#runtimePlan(runtimeId, definitions), { op, runtimeId, generation, manifest: runtime.manifest,
      definitions, commandId: id() }, deadline);
  }
  renewRuntime(runtimeId: string, generation: number): Promise<void> { return this.#runtimeOperation('renewRuntime', runtimeId, generation); }
  unregisterRuntime(runtimeId: string, generation: number, deadline?: number): Promise<void> { return this.#runtimeOperation('unregister', runtimeId, generation, deadline); }

  async #redisNow(deadline: TransportBudget): Promise<number> {
    const time = await this.connection.command(['TIME'], { budget: deadline });
    if (!Array.isArray(time)) return inconsistency();
    return number(time[0]) * 1000 + Math.floor(number(time[1]) / 1000);
  }
  /** A missing pre-read is not evidence of a concurrent GC: check the object and exact index pointers atomically. */
  async #verifyAbsentCandidate(object: string, indexes: readonly string[], candidate: string, deadline: TransportBudget, missingField?: string): Promise<void> {
    const plan = this.#basePlan();
    this.#key(plan, 'absentObject', object, 'hash');
    indexes.forEach((index, offset) => { this.#key(plan, `absentIndex_${offset + 1}`, index, 'zset'); });
    await this.#execute(plan, { op: 'verifyAbsentCandidate', id: candidate, indexCount: indexes.length,
      ...(missingField ? { missingField } : {}) }, deadline, false);
  }
  /** Registration uses the same finite member partition; expired manifests are reclaimed from their actual registered subset. */
  async reclaimExpiredMembers(deadline = transportBudget(), workEnd = deadline.deadline): Promise<void> {
    const now = await this.#redisNow(deadline);
    const candidates = await this.connection.command(['ZRANGEBYSCORE', this.keys.members, '-inf', String(now), 'LIMIT', '0', String(this.config.protocol.maintenance.batchSize)], { budget: deadline });
    if (!Array.isArray(candidates)) return inconsistency();
    let processed = 0;
    for (const candidate of candidates) {
      if (processed++ > 0 && Date.now() >= workEnd) break;
      const runtimeId = identifier(candidate);
      const generation = await this.connection.command(['HGET', this.keys.runtime(runtimeId), 'generation'], { budget: deadline });
      if (generation === null) {
        await this.#verifyAbsentCandidate(this.keys.runtime(runtimeId), [this.keys.members], runtimeId, deadline, 'generation');
        continue;
      }
      await this.#runtimeOperation('gcRuntime', runtimeId, number(generation), deadline);
    }
  }
  async gcDefinition(definitionIdentity: string, deadline = transportBudget()): Promise<{ changed: boolean }> {
    const plan = this.#basePlan();
    const entries = { definition: this.keys.definition(definitionIdentity), definitions: this.keys.definitions,
      gcDefinitions: this.keys.gcDefinitions, due: this.keys.due(definitionIdentity), blocked: this.keys.blocked(definitionIdentity),
      definitionMembers: this.keys.definitionMembers(definitionIdentity) };
    for (const [role, key] of Object.entries(entries)) this.#key(plan, role, key, role === 'definition' ? 'hash' : 'zset');
    const result = await this.#execute<{ changed: boolean }>(plan, { op: 'gcDefinition', definitionIdentity }, deadline);
    if (result.changed) this.#blockedSequences.delete(definitionIdentity);
    return result;
  }
  async gcRun(runId: string, deadline = transportBudget()): Promise<{ changed: boolean }> {
    const run = await this.#readRun(identifier(runId), deadline);
    if (!run) {
      await this.#verifyAbsentCandidate(this.keys.run(runId), [this.keys.gcRuns], runId, deadline);
      return { changed: false };
    }
    if (!run.definitionIdentity || !run.taskName || typeof run.idempotencyKey !== 'string') return inconsistency();
    const base = this.keys.idempotency(run.taskName, '');
    if (run.idempotencyKey && !run.idempotencyKey.startsWith(base)) return inconsistency();
    const plan = this.#runPlan(runId, run.definitionIdentity, run.taskName, run.idempotencyKey ? run.idempotencyKey.slice(base.length) : undefined);
    this.#key(plan, 'events', this.keys.events(runId), 'zset'); this.#key(plan, 'eventLock', this.keys.eventLock(runId), 'hash');
    return this.#execute(plan, { op: 'gcRun', runId, taskName: run.taskName, definitionIdentity: run.definitionIdentity }, deadline);
  }
  async #maintainRun(runId: string, definitionIdentity: string, deadline: TransportBudget): Promise<void> {
    const run = await this.#readRun(runId, deadline);
    if (!run) return this.#verifyAbsentCandidate(this.keys.run(runId), [this.keys.due(definitionIdentity), this.keys.blocked(definitionIdentity)], runId, deadline);
    if (!run.taskName || run.definitionIdentity !== definitionIdentity) return inconsistency();
    const plan = this.#runPlan(runId, definitionIdentity, run.taskName);
    this.#key(plan, 'definitionMembers', this.keys.definitionMembers(definitionIdentity), 'zset');
    await this.#execute(plan, { op: 'maintainRun', runId, taskName: run.taskName, definitionIdentity }, deadline);
  }
  async #maintainCatalog(deadline: TransportBudget, workEnd: number): Promise<void> {
    const flat = await this.connection.command(['ZRANGEBYSCORE', this.keys.definitions, `(${this.#catalogSequence}`, '+inf', 'WITHSCORES', 'LIMIT', '0', '1'], { budget: deadline });
    if (!Array.isArray(flat)) return inconsistency();
    if (!flat.length) { this.#catalogSequence = 0; return; }
    if (typeof flat[0] !== 'string') return inconsistency();
    const definitionIdentity = flat[0]; this.keys.definition(definitionIdentity);
    this.#catalogSequence = number(flat[1]);
    const cursor = this.#blockedSequences.get(definitionIdentity) ?? { sequence: 0, blocked: false };
    const size = this.config.protocol.maintenance.batchSize;
    const candidates = await this.connection.command(cursor.blocked
      ? ['ZRANGEBYSCORE', this.keys.blocked(definitionIdentity), `(${cursor.sequence}`, '+inf', 'WITHSCORES', 'LIMIT', '0', String(size)]
      : ['ZRANGE', this.keys.due(definitionIdentity), '0', String(size - 1)], { budget: deadline });
    if (!Array.isArray(candidates)) return inconsistency();
    // LRU bound equals the maximum live catalog; stale historical definitions cannot grow memory forever.
    this.#blockedSequences.delete(definitionIdentity);
    if (this.#blockedSequences.size >= this.config.protocol.limits.definitionMax) this.#blockedSequences.delete(this.#blockedSequences.keys().next().value!);
    let last = cursor.sequence;
    for (let i = 0; i < candidates.length; i += cursor.blocked ? 2 : 1) {
      // Finish one bounded use case even if its pre-read was slow; otherwise high RTT could starve all writes.
      if (i > 0 && Date.now() >= workEnd) break;
      await this.#maintainRun(identifier(candidates[i]), definitionIdentity, deadline);
      if (cursor.blocked) last = number(candidates[i + 1]);
    }
    this.#blockedSequences.set(definitionIdentity, { sequence: cursor.blocked && !candidates.length ? 0 : last, blocked: !cursor.blocked });
  }

  /** Rotate maintenance classes and catalog identities; limits bound work, wall-time is checked between atomic use cases. */
  async maintain(): Promise<void> {
    this.#guard();
    const began = Date.now(); const deadline = transportBudget(began + 10000);
    const policy = this.config.protocol.maintenance;
    const workEnd = began + policy.timeBudgetMs;
    for (let batch = 0; batch < policy.maxBatchesPerTick; batch++) {
      if (batch > 0 && Date.now() - began >= policy.timeBudgetMs) break;
      const phase = this.#maintenancePhase; this.#maintenancePhase = (phase + 1) % 6;
      if (phase === 0) await this.reclaimExpiredMembers(deadline, workEnd);
      else if (phase === 1) await this.recoverExpired(deadline, workEnd);
      else if (phase === 2) await this.#maintainCatalog(deadline, workEnd);
      else {
        const now = await this.#redisNow(deadline);
        const key = phase === 3 ? this.keys.gcRuns : phase === 4 ? this.keys.gcDefinitions : this.keys.gcEvents;
        const candidates = await this.connection.command(['ZRANGEBYSCORE', key, '-inf', String(now), 'LIMIT', '0', String(policy.batchSize)], { budget: deadline });
        if (!Array.isArray(candidates)) return inconsistency();
        let processed = 0;
        for (const candidate of candidates) {
          if (processed++ > 0 && Date.now() >= workEnd) break;
          if (typeof candidate !== 'string') return inconsistency();
          if (phase === 3) await this.gcRun(identifier(candidate), deadline);
          else if (phase === 4) await this.gcDefinition(candidate, deadline);
          else await this.gcEvent(candidate, deadline);
        }
      }
    }
  }

  #eventPlan(eventId: string, definitionIdentity: string, taskName: string): KeyPlan {
    const { runId } = eventIdentifier(eventId);
    const plan = this.#runPlan(runId, definitionIdentity, taskName);
    for (const [role, key, type] of [
      ['event', this.keys.eventById(eventId), 'hash'], ['events', this.keys.events(runId), 'zset'],
      ['eventLock', this.keys.eventLock(runId), 'hash'], ['dueEvents', this.keys.dueEvents, 'zset'],
      ['dueReplays', this.keys.dueReplays, 'zset'], ['gcEvents', this.keys.gcEvents, 'zset'],
      ['dead', this.keys.deadLetters(), 'zset'], ['taskDead', this.keys.deadLetters(taskName), 'zset'],
      ['definitionMembers', this.keys.definitionMembers(definitionIdentity), 'zset']
    ] as [string, string, KeyType][]) this.#key(plan, role, key, type);
    return plan;
  }
  #eventRequest(eventId: string): { eventId: string; runId: string; page: number; eventKind: string } {
    const parsed = eventIdentifier(eventId);
    return { eventId, runId: parsed.runId, page: parsed.page, eventKind: parsed.kind };
  }
  #eventInfo(reply: EventReply): EventInfo {
    try {
      const fields = Object.fromEntries(Object.entries(reply.event).map(([key, value]) => [key, String(value)]));
      const parsed = eventIdentifier(fields.eventId);
      if (fields.schema !== 'batch-v1' || fields.runId !== parsed.runId || fields.queryRef !== parsed.runId
        || fields.batchId !== `${parsed.runId}:${parsed.page}` || fields.kind !== parsed.kind
        || !EVENT_STATUSES.includes(fields.status as EventStatus) || !['0', '1'].includes(fields.lateReplay!)) return inconsistency();
      for (const field of ['taskName', 'version', 'definitionIdentity', 'state', 'error', 'deliveryError', 'token']) {
        if (typeof fields[field] !== 'string') return inconsistency();
      }
      const deadline = fields.token ? number(String((JSON.parse(fields.token) as { deadline: number }).deadline)) : null;
      return Object.freeze({ eventId: parsed.eventId, runId: parsed.runId, batchId: fields.batchId!, kind: parsed.kind,
        taskName: fields.taskName!, version: fields.version!, definitionIdentity: fields.definitionIdentity!,
        sequence: number(fields.sequence), timestamp: number(fields.timestamp), status: fields.status as EventStatus,
        revision: number(fields.revision), deliveryAttempt: number(fields.deliveryAttempt), replayGeneration: number(fields.replayGeneration),
        lateReplay: fields.lateReplay === '1', firstDeadAt: number(fields.firstDeadAt), deadLetterExpiresAt: number(fields.deadLetterExpiresAt),
        firstDeadLetterSequence: number(fields.firstDeadLetterSequence), replayDrainDeadline: number(fields.replayDrainDeadline) || null,
        dueAt: number(fields.dueAt) || null, deliveredAt: number(fields.deliveredAt) || null,
        lease: Object.freeze({ revision: number(fields.leaseRevision), deadline, attemptTimeoutAt: number(fields.attemptTimeoutAt) || null }),
        query: frozenJson(reply.query), state: frozenJson(fields.state!), error: frozenJson<ErrorEnvelope | null>(fields.error!),
        deliveryError: frozenJson<ErrorEnvelope | null>(fields.deliveryError!) });
    } catch { return inconsistency(); }
  }
  async #readEventParent(eventId: string, deadline: TransportBudget): Promise<{ taskName: string; definitionIdentity: string } | null> {
    const { runId } = eventIdentifier(eventId); const plan = this.#basePlan();
    this.#key(plan, 'parent', this.keys.run(runId), 'hash'); this.#key(plan, 'event', this.keys.eventById(eventId), 'hash');
    for (const [role, key] of Object.entries({ events: this.keys.events(runId), dueEvents: this.keys.dueEvents,
      dueReplays: this.keys.dueReplays, leases: this.keys.leases, gcEvents: this.keys.gcEvents, dead: this.keys.deadLetters() })) this.#key(plan, role, key, 'zset');
    return this.#execute(plan, { op: 'eventParent', eventId, runId }, deadline, false);
  }
  async getEvent(eventId: string): Promise<EventInfo | null> {
    this.#guard(); const request = this.#eventRequest(eventId); const deadline = transportBudget();
    const run = await this.#readEventParent(eventId, deadline); if (!run) return null;
    if (!run.definitionIdentity || !run.taskName) return inconsistency();
    const reply = await this.#execute<EventReply | null>(this.#eventPlan(eventId, run.definitionIdentity, run.taskName),
      { op: 'getEvent', ...request }, deadline, false);
    return reply ? this.#eventInfo(reply) : null;
  }
  async claimEvent(eventId: string, runtimeId: string, generation: number, deadline = transportBudget()): Promise<EventLease | null> {
    this.#guard(); const request = this.#eventRequest(eventId); identifier(runtimeId); integer(generation, 1, Number.MAX_SAFE_INTEGER, 'generation');
    const began = Date.now();
    const run = await this.#readEventParent(eventId, deadline); if (!run) return null;
    if (!run.definitionIdentity || !run.taskName) return inconsistency();
    const plan = this.#eventPlan(eventId, run.definitionIdentity, run.taskName); this.#key(plan, 'runtime', this.keys.runtime(runtimeId), 'hash');
    const reply = await this.#execute<(EventReply & { token: string; now: number }) | null>(plan,
      { op: 'claimEvent', ...request, runtimeId, generation, nonce: id(), commandId: id() }, deadline);
    if (!reply) return null;
    const event = this.#eventInfo(reply); const grant = event.lease.deadline; const timeout = event.lease.attemptTimeoutAt;
    if (!grant || !timeout) return inconsistency();
    const elapsed = Date.now() - began; const now = number(String(reply.now));
    return Object.freeze({ tokenCanonical: reply.token, definitionIdentity: run.definitionIdentity, deadline: grant,
      attemptTimeoutAt: timeout, event, timeoutRemainingMs: Math.max(0, timeout - now - elapsed),
      deadlineRemainingMs: Math.max(0, grant - now - elapsed) });
  }
  async renewEvent(lease: EventLease): Promise<EventLease> {
    this.#guard(); const began = Date.now();
    const reply = await this.#execute<{ token: string; now: number; replayDrainDeadline: number }>(this.#eventPlan(lease.event.eventId, lease.definitionIdentity, lease.event.taskName),
      { op: 'renewEvent', ...this.#eventRequest(lease.event.eventId), token: lease.tokenCanonical, commandId: id() }, transportBudget());
    const token = JSON.parse(reply.token) as { deadline: number; leaseRevision: number };
    const deadline = number(String(token.deadline)); const now = number(String(reply.now));
    return Object.freeze({ ...lease, tokenCanonical: reply.token, deadline,
      deadlineRemainingMs: Math.max(0, deadline - now - (Date.now() - began)),
      timeoutRemainingMs: Math.max(0, lease.attemptTimeoutAt - now - (Date.now() - began)),
      event: Object.freeze({ ...lease.event, replayDrainDeadline: number(String(reply.replayDrainDeadline)) || null,
        lease: Object.freeze({ ...lease.event.lease, deadline, revision: number(String(token.leaseRevision)) }) }) });
  }
  #eventFailure(error?: ErrorEnvelope): string {
    const encoded = canonicalJson(error ?? errorEnvelope(undefined), JSON_LIMITS.error);
    return Buffer.byteLength(encoded) <= 2048 ? encoded : canonicalJson({ code: 'CALLBACK_FAILED', name: 'Error',
      message: 'Callback diagnostic exceeded its 2 KiB bound', truncated: true });
  }
  async settleEvent(lease: EventLease, input: { kind: 'success' | 'failure' | 'timeout'; error?: ErrorEnvelope }): Promise<EventOutcome> {
    this.#guard();
    return this.#execute(this.#eventPlan(lease.event.eventId, lease.definitionIdentity, lease.event.taskName),
      { op: 'settleEvent', ...this.#eventRequest(lease.event.eventId), token: lease.tokenCanonical, commandId: id(), kind: input.kind,
        error: this.#eventFailure(input.error), timeoutError: this.#eventFailure({ code: 'CALLBACK_TIMEOUT', name: 'TimeoutError',
          message: 'Callback attempt exceeded timeoutMs', truncated: false }),
        retryDelay: backoffDelay(this.config.protocol.callback, lease.event.deliveryAttempt, Math.random()) }, transportBudget());
  }
  async recoverEvent(eventId: string, deadline = transportBudget()): Promise<EventOutcome> {
    this.#guard(); const request = this.#eventRequest(eventId);
    const run = await this.#readEventParent(eventId, deadline); if (!run) return { changed: false };
    if (!run.definitionIdentity || !run.taskName) return inconsistency();
    const plan = this.#eventPlan(eventId, run.definitionIdentity, run.taskName);
    const attempt = await this.connection.command(['HGET', this.keys.eventById(eventId), 'deliveryAttempt'], { budget: deadline });
    if (attempt === null) { await this.#verifyAbsentCandidate(this.keys.eventById(eventId), [this.keys.leases], eventId, deadline); return { changed: false }; }
    return this.#execute(plan, { op: 'recoverEvent', ...request, commandId: id(),
      error: this.#eventFailure({ code: 'CALLBACK_LEASE_EXPIRED', name: 'Error', message: 'Callback owner lease expired', truncated: false }),
      timeoutError: this.#eventFailure({ code: 'CALLBACK_TIMEOUT', name: 'TimeoutError', message: 'Callback attempt exceeded timeoutMs', truncated: false }),
      retryDelay: backoffDelay(this.config.protocol.callback, number(attempt), Math.random()) }, deadline);
  }

  async replayEvent(input: EventReplayInput): Promise<EventReplayResult> {
    this.#guard(); const control = normalizeReplay(input); const request = this.#eventRequest(control.eventId); const deadline = transportBudget();
    const run = await this.#readEventParent(control.eventId, deadline); if (!run) return Object.freeze({ kind: 'not_found', id: control.eventId });
    if (!run.definitionIdentity || !run.taskName) return inconsistency();
    const summary = Array.from(control.reason).slice(0, 40).join('');
    try {
      return validateReplayResult(await this.#execute(this.#eventPlan(control.eventId, run.definitionIdentity, run.taskName),
        { op: 'replayEvent', ...request, commandId: canonicalJson(control.commandId), control: JSON.parse(control.wireCanonical) as unknown,
          controlCanonical: control.wireCanonical, auditReason: canonicalJson(summary), auditTruncated: summary !== control.reason }, deadline));
    } catch (error) {
      if (!isQueuebitError(error)) throw error;
      throw new QueuebitError(error.code, error.message, { operation: 'replay', runId: control.runId, commandId: control.commandId,
        retryable: error.retryable, outcomeKnown: error.outcomeKnown });
    }
  }
  /** Alternate queues and rotate bounded metadata pages. Only a successful grant returns business payloads. */
  async claimNextEvent(definitions: readonly Definition<unknown, unknown>[], runtimeId: string, generation: number): Promise<EventLease | null> {
    this.#guard(); const deadline = transportBudget(); const workEnd = Date.now() + this.config.protocol.maintenance.timeBudgetMs;
    const first = this.#eventTurn; this.#eventTurn = 1 - first;
    for (let queue = 0; queue < 2; queue++) {
      const which = (first + queue) % 2; const index = which ? this.keys.dueReplays : this.keys.dueEvents;
      const size = this.config.protocol.maintenance.batchSize; const offset = this.#eventOffsets[which]!;
      let pageOffset = offset;
      let flat = await this.connection.command(['ZRANGE', index, String(pageOffset), String(pageOffset + size - 1), 'WITHSCORES'], { budget: deadline });
      // A claim removes its row, so a formerly valid offset can move past a still-nonempty queue.
      // Wrap once in the same turn before falling back to the other queue; total candidates remain bounded.
      if (Array.isArray(flat) && !flat.length && pageOffset > 0) {
        pageOffset = 0;
        flat = await this.connection.command(['ZRANGE', index, '0', String(size - 1), 'WITHSCORES'], { budget: deadline });
      }
      if (!Array.isArray(flat) || flat.length % 2 || flat.length > size * 2) return inconsistency();
      this.#eventOffsets[which] = flat.length < size * 2 ? 0 : pageOffset + size;
      if (!flat.length) continue;
      const plan = this.#basePlan(); this.#key(plan, 'candidateIndex', index, 'zset');
      const candidates: { id: string; score: number }[] = [];
      for (let i = 0; i < flat.length; i += 2) {
        let parsed: ReturnType<typeof eventIdentifier>;
        try { parsed = eventIdentifier(flat[i]); } catch { throw new QueuebitError('INDEX_INCONSISTENT', 'Invalid Event index member'); }
        candidates.push({ id: parsed.eventId, score: number(flat[i + 1]) });
        this.#key(plan, `candidateEvent_${candidates.length}`, this.keys.eventById(parsed.eventId), 'hash');
      }
      const page = await this.#execute<{ candidates: { eventId: string; definitionIdentity: string; taskName: string }[] }>(plan,
        { op: 'eventCandidates', candidates, replay: which === 1 }, deadline, false);
      let attempted = false;
      for (const candidate of page.candidates) {
        if (attempted && Date.now() >= workEnd) break;
        if (!definitions.some(definition => definition.identity === candidate.definitionIdentity && definition.name === candidate.taskName)) continue;
        attempted = true;
        const lease = await this.claimEvent(candidate.eventId, runtimeId, generation, deadline);
        if (lease) { this.#eventTurn = 1 - which; return lease; }
      }
    }
    return null;
  }
  async gcEvent(eventId: string, deadline = transportBudget()): Promise<{ changed: boolean }> {
    this.#guard(); const request = this.#eventRequest(eventId); const run = await this.#readEventParent(eventId, deadline);
    if (!run) return { changed: false };
    if (!run.definitionIdentity || !run.taskName) return inconsistency();
    return this.#execute(this.#eventPlan(eventId, run.definitionIdentity, run.taskName), { op: 'gcEvent', ...request }, deadline);
  }

  #leasePlan(lease: ExecutionLease): KeyPlan {
    const plan = this.#runPlan(lease.run.runId, lease.definitionIdentity, lease.run.taskName);
    this.#key(plan, 'events', this.keys.events(lease.run.runId), 'zset');
    this.#key(plan, 'dueEvents', this.keys.dueEvents, 'zset');
    for (const kind of ['batchSettled', 'success', 'failure'] as const) {
      this.#key(plan, `event_${kind}`, this.keys.event(lease.run.runId, lease.run.page, kind), 'hash');
    }
    return plan;
  }
  async claim(definition: Definition<unknown, unknown>, runtimeId: string, generation: number): Promise<ExecutionLease | null> {
    this.#guard();
    const began = Date.now(); const deadline = transportBudget(began + 10000);
    const candidates = await this.connection.command(['ZRANGE', this.keys.due(definition.identity), '0', '0'], { budget: deadline });
    if (!Array.isArray(candidates)) return inconsistency();
    if (!candidates.length) return null;
    const runId = identifier(candidates[0]);
    const plan = this.#runPlan(runId, definition.identity, definition.name);
    this.#key(plan, 'runtime', this.keys.runtime(runtimeId), 'hash');
    this.#key(plan, 'definitionMembers', this.keys.definitionMembers(definition.identity), 'zset');
    const reply = await this.#execute<{ token: string; run: Record<string, string | number>; now: number } | null>(plan, {
      op: 'claim', runId, taskName: definition.name, definitionIdentity: definition.identity,
      runtimeId, generation, nonce: id(), commandId: id()
    }, deadline);
    if (!reply) return null;
    const raw = Object.fromEntries(Object.entries(reply.run).map(([key, value]) => [key, String(value)]));
    const token: unknown = JSON.parse(reply.token);
    if (!token || typeof token !== 'object' || !('deadline' in token) || typeof token.deadline !== 'number') return inconsistency();
    return Object.freeze({ tokenCanonical: reply.token, definitionIdentity: definition.identity,
      deadline: token.deadline, attemptTimeoutAt: number(raw.attemptTimeoutAt), run: this.#runInfo(raw),
      timeoutRemainingMs: Math.max(0, number(raw.attemptTimeoutAt) - number(String(reply.now)) - (Date.now() - began)) });
  }
  async renew(lease: ExecutionLease): Promise<ExecutionLease> {
    const reply = await this.#execute<{ token: string }>(this.#leasePlan(lease), {
      op: 'renew', runId: lease.run.runId, taskName: lease.run.taskName, definitionIdentity: lease.definitionIdentity,
      token: lease.tokenCanonical, commandId: id()
    }, transportBudget());
    const token = JSON.parse(reply.token) as { deadline: number };
    return Object.freeze({ ...lease, tokenCanonical: reply.token, deadline: token.deadline });
  }
  async settle(lease: ExecutionLease, input: SettlementInput): Promise<{ status: RunStatus; revision: number; page: number }> {
    const state = input.stateCanonical ?? canonicalJson(lease.run.state, JSON_LIMITS.state);
    if (canonicalJson(JSON.parse(state), JSON_LIMITS.state) !== state) throw new QueuebitError('HANDLER_CONTRACT_INVALID', 'Settlement state is not canonical');
    const failure = canonicalJson(input.error ?? errorEnvelope(undefined), JSON_LIMITS.error);
    const timeoutError = canonicalJson({ code: 'EXECUTION_TIMEOUT', name: 'TimeoutError', message: 'Execution attempt exceeded timeoutMs', truncated: false }, JSON_LIMITS.error);
    return this.#execute(this.#leasePlan(lease), { op: 'settle', runId: lease.run.runId, taskName: lease.run.taskName,
      definitionIdentity: lease.definitionIdentity, token: lease.tokenCanonical, commandId: id(), kind: input.kind,
      state, error: failure, timeoutError,
      retryDelay: backoffDelay(lease.run.effectivePolicy as ExecutionPolicy, lease.run.batchFailures + 1, Math.random())
    }, transportBudget());
  }
  async recover(runId: string, deadline = transportBudget()): Promise<{ changed: boolean; status?: RunStatus }> {
    const run = await this.#readRun(runId, deadline);
    if (!run) {
      await this.#verifyAbsentCandidate(this.keys.run(runId), [this.keys.leases], runId, deadline);
      return { changed: false };
    }
    if (!run.definitionIdentity || !run.taskName) return inconsistency();
    return this.#execute(this.#runPlan(runId, run.definitionIdentity, run.taskName), {
      op: 'recover', runId, taskName: run.taskName, definitionIdentity: run.definitionIdentity, commandId: id(),
      retryDelay: backoffDelay(mergePolicy(), number(run.consecutiveRecoveries) + 1, Math.random())
    }, deadline);
  }

  /** Lease recovery shares its caller's transport budget and yields between exact Run use cases. */
  async recoverExpired(deadline = transportBudget(), workEnd = Date.now() + this.config.protocol.maintenance.timeBudgetMs): Promise<void> {
    this.#guard();
    const time = await this.connection.command(['TIME'], { budget: deadline });
    if (!Array.isArray(time) || typeof time[0] !== 'string' || typeof time[1] !== 'string') return inconsistency();
    const now = number(time[0]) * 1000 + Math.floor(number(time[1]) / 1000);
    const candidates = await this.connection.command(['ZRANGEBYSCORE', this.keys.leases, '-inf', String(now), 'LIMIT', '0', String(this.config.protocol.maintenance.batchSize)], { budget: deadline });
    if (!Array.isArray(candidates)) return inconsistency();
    let processed = 0;
    for (const runId of candidates) {
      if (processed++ > 0 && Date.now() >= workEnd) break;
      if (typeof runId === 'string' && runId.includes(':')) await this.recoverEvent(eventIdentifier(runId).eventId, deadline);
      else await this.recover(identifier(runId), deadline);
    }
  }
}
