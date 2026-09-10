import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { isQueuebitError, QueuebitError } from '../api/errors.js';
import type { BatchQueue, BatchOperator, BatchTask, CloseResult, TaskContract, TaskHandlers } from '../api/types.js';
import type { NormalizedOptions } from '../domain/config.js';
import { mergePolicy } from '../domain/config.js';
import { normalizeDefinition } from '../domain/definition.js';
import type { Definition } from '../domain/definition.js';
import { taskHandle } from '../application/runs.js';
import { observationOperator, runsOperator } from '../application/operator.js';
import { deadLettersOperator } from '../application/dead-letters.js';
import { BatchRedisConnection, beforeDeadline } from '../storage/redis/connection.js';
import { BatchRedisStore } from '../storage/redis/store.js';
import { ExecutionSlot } from './execution.js';
import { CallbackSlot } from './callback.js';
import { Telemetry } from './telemetry.js';

type Lifecycle = 'created' | 'starting' | 'ready' | 'failed' | 'closing' | 'closed';

/** Runtime owns only this instance's client, members, timers and physical invocation slots. */
export class QueueRuntime implements BatchQueue {
  readonly operator: BatchOperator;
  readonly #telemetry: Telemetry;
  readonly #config: NormalizedOptions;
  readonly #connection: BatchRedisConnection;
  readonly #store: BatchRedisStore;
  readonly #definitions = new Map<string, Definition<unknown, unknown>>();
  readonly #slots = new Set<ExecutionSlot>();
  readonly #callbacks = new Set<CallbackSlot>();
  #state: Lifecycle = 'created';
  #generation = 0;
  #runtimeId = '';
  #registered = false;
  #validatedConnection = -1;
  #starting: Promise<void> | undefined;
  #closing: Promise<CloseResult> | undefined;
  #cleanup: Promise<void> | undefined;
  #tick: Promise<void> | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #nextMembership = 0;
  #nextMaintenance = 0;
  #rotation = 0;
  #lastError: unknown;

  constructor(config: NormalizedOptions) {
    this.#config = config;
    this.#connection = new BatchRedisConnection(config.redis);
    this.#store = new BatchRedisStore(this.#connection, config);
    this.#telemetry = new Telemetry(config.telemetry);
    this.operator = Object.freeze({ runs: runsOperator(this.#store, () => this.#guard(), runId => this.#cancelled(runId), this.#telemetry),
      deadLetters: deadLettersOperator(this.#store, () => this.#guard(), this.#telemetry),
      ...observationOperator(this.#store, () => this.#guard(), this.#telemetry, () => ({ executions: this.#slots.size,
        residualExecutions: [...this.#slots].filter(slot => slot.residual).length, callbacks: this.#callbacks.size,
        residualCallbacks: [...this.#callbacks].filter(slot => slot.residual).length, localDefinitions: this.#definitions.size })) });
  }
  define<Q, S>(contract: TaskContract, handlers?: TaskHandlers<Q, S>): BatchTask<Q, S> {
    if (this.#state !== 'created') throw new QueuebitError('CONFIG_INVALID', 'Definitions are frozen at the first ready or close call');
    const definition = normalizeDefinition<Q, S>(contract, handlers, this.#config.runtime.mode);
    if (this.#definitions.has(definition.name) || this.#definitions.size >= 128) throw new QueuebitError('CONFIG_INVALID', 'Task names must be unique; at most 128 definitions per queue');
    mergePolicy(this.#config.defaults, definition.policy);
    this.#definitions.set(definition.name, definition as unknown as Definition<unknown, unknown>);
    this.#telemetry.registerTask(definition.name);
    return taskHandle(this.#store, definition, this.#config.runtime.mode, () => this.#guard(), runId => this.#cancelled(runId), this.#telemetry);
  }
  #cancelled(runId: string): void { for (const slot of this.#slots) if (slot.runId === runId) slot.revoke(); }
  #guard(): void {
    if (this.#state === 'ready') return;
    const codes = { created: 'QUEUE_NOT_READY', starting: 'QUEUE_NOT_READY', failed: 'QUEUE_FAILED', closing: 'QUEUE_CLOSING', closed: 'QUEUE_CLOSED' } as const;
    throw new QueuebitError(codes[this.#state], `Queue is ${this.#state}`);
  }
  ready(): Promise<void> {
    if (this.#state === 'starting' || this.#state === 'ready') return this.#starting!;
    if (this.#state !== 'created') { try { this.#guard(); } catch (error) { return Promise.reject(error); } }
    this.#state = 'starting';
    const generation = ++this.#generation;
    this.#runtimeId = randomBytes(16).toString('hex');
    this.#starting = this.#start(generation);
    return this.#starting;
  }
  #startupGuard(generation: number): void {
    if (this.#state !== 'starting' || generation !== this.#generation) throw new QueuebitError('QUEUE_CLOSING', 'Startup generation was revoked');
  }
  async #start(generation: number): Promise<void> {
    try {
      await this.#store.ready(); this.#startupGuard(generation);
      if (this.#config.runtime.mode !== 'producer' && this.#definitions.size) {
        // Mark before awaiting so partially registered chunks are also cleaned up on failure.
        this.#registered = true;
        await this.#store.registerRuntime(this.#runtimeId, 1, [...this.#definitions.values()]);
        this.#startupGuard(generation);
      }
      this.#validatedConnection = this.#connection.generation;
      this.#nextMembership = Date.now() + this.#config.protocol.lease.heartbeatMs;
      this.#state = 'ready'; this.#telemetry.emit('queue_ready'); this.#schedule();
    } catch (error) {
      if (this.#state === 'closing' || this.#state === 'closed') throw new QueuebitError('QUEUE_CLOSING', 'Startup generation was revoked');
      this.#state = 'failed'; this.#lastError = error;
      await this.#clean(Date.now() + 10000);
      throw error;
    }
  }
  #schedule(): void {
    if (this.#state !== 'ready') return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.#tick = this.#cycle().catch(error => this.#record(error)).finally(() => { this.#tick = undefined; this.#schedule(); });
    }, this.#config.protocol.lease.pollMs);
  }
  #record(error: unknown): void {
    this.#lastError = error;
    this.#telemetry.emit('queue_error');
    if (isQueuebitError(error) && ['STORAGE_INCONSISTENT', 'SCHEMA_MISMATCH', 'RESOURCE_CLEANUP_FAILED'].includes(error.code)) {
      this.#state = 'failed';
      for (const slot of this.#slots) slot.revoke();
      for (const slot of this.#callbacks) slot.revoke();
      void this.#clean(Date.now() + 10000).catch(cleanup => { this.#lastError = cleanup; });
    }
  }
  async #pulse(): Promise<void> {
    await Promise.all([...this.#slots, ...this.#callbacks].map(slot => slot.pulse(Date.now())));
    if (this.#registered && Date.now() >= this.#nextMembership) {
      this.#nextMembership = Date.now() + this.#config.protocol.lease.heartbeatMs;
      try { await this.#store.renewRuntime(this.#runtimeId, 1); }
      catch (error) {
        if (!isQueuebitError(error) || error.code !== 'LEASE_LOST') throw error;
        for (const slot of this.#slots) slot.revoke();
        for (const slot of this.#callbacks) slot.revoke();
        if (this.#state !== 'ready') return;
        await this.#store.unregisterRuntime(this.#runtimeId, 1);
        if (this.#state !== 'ready') return;
        await this.#store.registerRuntime(this.#runtimeId, 1, [...this.#definitions.values()]);
      }
    }
  }
  async #cycle(): Promise<void> {
    if (this.#state !== 'ready') return;
    if (!this.#connection.ready || this.#validatedConnection !== this.#connection.generation) {
      await this.#store.ready(); this.#validatedConnection = this.#connection.generation;
    }
    await this.#pulse();
    if (this.#state !== 'ready') return;
    if (Date.now() >= this.#nextMaintenance) {
      this.#nextMaintenance = Date.now() + this.#config.protocol.lease.heartbeatMs;
      await this.#store.maintain();
      this.#telemetry.emit('maintenance_tick');
    }
    if (this.#config.runtime.mode === 'producer' || !this.#definitions.size) return;
    const definitions = [...this.#definitions.values()];
    const began = Date.now();
    for (let count = 0; count < Math.min(definitions.length, this.#config.protocol.maintenance.batchSize); count++) {
      if (this.#state !== 'ready' || this.#slots.size >= this.#config.runtime.concurrency
        || Date.now() - began >= this.#config.protocol.maintenance.timeBudgetMs) break;
      this.#rotation %= definitions.length;
      const definition = definitions[this.#rotation++]!;
      const lease = await this.#store.claim(definition, this.#runtimeId, 1);
      if (this.#state !== 'ready') break; // In-flight claim expires; close never creates or cancels business work.
      if (lease) {
        this.#telemetry.emit('run_claimed', definition.name);
        const slot = new ExecutionSlot(this.#store, lease, definition, error => this.#record(error), this.#telemetry);
        this.#slots.add(slot);
        void slot.done.then(() => { this.#slots.delete(slot); });
      }
    }
    // Callback admission has its own scheduling budget and physical limit; busy execution cannot starve it.
    const callbackEnd = Date.now() + this.#config.protocol.maintenance.timeBudgetMs;
    for (let count = 0; count < this.#config.protocol.maintenance.batchSize; count++) {
      if (this.#state !== 'ready' || this.#callbacks.size >= this.#config.runtime.callbackConcurrency
        || (count > 0 && Date.now() >= callbackEnd)) break;
      const lease = await this.#store.claimNextEvent(definitions, this.#runtimeId, 1);
      if (this.#state !== 'ready' || !lease) break;
      const definition = definitions.find(item => item.identity === lease.definitionIdentity);
      if (!definition) throw new QueuebitError('STORAGE_INCONSISTENT', 'Callback grant does not match local registration');
      this.#telemetry.emit('callback_claimed', definition.name);
      const slot = new CallbackSlot(this.#store, lease, definition, error => this.#record(error), this.#telemetry);
      this.#callbacks.add(slot); void slot.done.then(() => { this.#callbacks.delete(slot); });
    }
  }
  #clean(deadline: number): Promise<void> {
    this.#telemetry.close();
    if (this.#cleanup) return this.#cleanup;
    this.#cleanup = (async () => {
      try {
        if (this.#registered) {
          try { await this.#store.unregisterRuntime(this.#runtimeId, 1, deadline); }
          catch (error) { this.#lastError = error; } // Redis-side membership has a finite, non-renewed expiry.
          this.#registered = false;
        }
      } finally { await this.#connection.close(deadline); }
    })();
    return this.#cleanup;
  }
  close(): Promise<CloseResult> {
    if (this.#closing) return this.#closing;
    this.#telemetry.emit('queue_closing'); this.#telemetry.close();
    this.#state = 'closing'; this.#generation++;
    if (this.#timer !== undefined) { clearTimeout(this.#timer); this.#timer = undefined; }
    this.#closing = this.#close();
    return this.#closing;
  }
  async #close(): Promise<CloseResult> {
    const graceEnd = Date.now() + this.#config.runtime.closeGraceMs;
    while ((this.#slots.size || this.#callbacks.size) && Date.now() < graceEnd) {
      try { await beforeDeadline(this.#pulse(), graceEnd, () => {}); } catch (error) { this.#lastError = error; }
      if (this.#slots.size || this.#callbacks.size) await delay(Math.max(0, Math.min(25, graceEnd - Date.now())));
    }
    const remainingExecutions = this.#slots.size;
    const remainingCallbacks = this.#callbacks.size;
    for (const slot of this.#slots) slot.revoke();
    for (const slot of this.#callbacks) slot.revoke();
    const deadline = Date.now() + 10000;
    try {
      if (this.#starting) await beforeDeadline(this.#starting.catch(() => undefined), deadline, () => {});
      if (this.#tick) await beforeDeadline(this.#tick, deadline, () => {});
      await beforeDeadline(this.#clean(deadline), deadline, () => {});
      this.#state = 'closed';
      return Object.freeze({ status: 'closed', timedOut: remainingExecutions + remainingCallbacks > 0, remainingExecutions, remainingCallbacks });
    } catch {
      this.#state = 'failed';
      // Ensure cleanup remains observed even when the shared outer deadline has expired.
      void this.#clean(deadline).catch(error => { this.#lastError = error; });
      throw new QueuebitError('RESOURCE_CLEANUP_FAILED', 'Owned queue resources could not be verified closed', { operation: 'close' });
    }
  }
}
