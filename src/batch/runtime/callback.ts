import { isQueuebitError } from '../api/errors.js';
import type { ErrorEnvelope } from '../api/types.js';
import type { Definition } from '../domain/definition.js';
import { errorEnvelope } from '../domain/json.js';
import type { BatchRedisStore, EventLease } from '../storage/redis/store.js';
import type { Telemetry } from './telemetry.js';

type CallbackOutcome = { kind: 'success' | 'failure' | 'timeout'; error?: ErrorEnvelope };

/** Callback-specific physical slot: return values are ignored and frozen replay grants are never extended. */
export class CallbackSlot {
  readonly done: Promise<void>;
  readonly #abort = new AbortController();
  readonly #store: BatchRedisStore;
  readonly #onError: (error: unknown) => void;
  readonly #telemetry: Telemetry;
  #lease: EventLease;
  #serial = Promise.resolve();
  #eligible = true;
  #chosen = false;
  #physicalDone = false;
  #logicalDone = false;
  #nextHeartbeat: number;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #expiryTimer: ReturnType<typeof setTimeout> | undefined;
  #finish!: () => void;
  get residual(): boolean { return !this.#physicalDone && (!this.#eligible || this.#chosen); }

  constructor(store: BatchRedisStore, lease: EventLease, definition: Definition<unknown, unknown>, onError: (error: unknown) => void, telemetry: Telemetry) {
    this.#store = store; this.#lease = lease; this.#onError = onError; this.#telemetry = telemetry;
    this.done = new Promise(resolve => { this.#finish = resolve; });
    this.#nextHeartbeat = Date.now() + store.config.protocol.lease.heartbeatMs;
    const event = lease.event;
    const context = Object.freeze({ eventId: event.eventId, kind: event.kind, runId: event.runId, batchId: event.batchId,
      timestamp: event.timestamp, sequence: event.sequence, query: event.query, state: event.state, error: event.error,
      deliveryAttempt: event.deliveryAttempt, replayGeneration: event.replayGeneration, lateReplay: event.lateReplay, signal: this.#abort.signal });
    if (lease.timeoutRemainingMs <= 0 || lease.deadlineRemainingMs <= 0) {
      this.#physicalDone = true; this.#choose({ kind: 'timeout' }); return;
    }
    this.#timer = setTimeout(() => { this.#abort.abort(); this.#choose({ kind: 'timeout' }); }, lease.timeoutRemainingMs);
    this.#armFrozenExpiry();
    const handler = event.kind === 'batchSettled' ? definition.handlers!.onBatchSettled
      : event.kind === 'success' ? definition.handlers!.onSuccess : definition.handlers!.onFailure;
    void Promise.resolve().then(() => handler!(context)).then(() => {
      this.#physicalDone = true; this.#choose({ kind: 'success' });
    }, error => { this.#physicalDone = true; this.#choose({ kind: 'failure', error: errorEnvelope(error) }); })
      .finally(() => { this.#checkDone(); });
  }
  #armFrozenExpiry(): void {
    if (this.#expiryTimer !== undefined) clearTimeout(this.#expiryTimer);
    if (this.#lease.event.replayDrainDeadline !== null) {
      this.#expiryTimer = setTimeout(() => { this.revoke(); }, this.#lease.deadlineRemainingMs);
    }
  }
  #enqueue(action: () => Promise<void>): Promise<void> {
    this.#serial = this.#serial.then(action).catch(error => { this.#onError(error); this.revoke(); });
    return this.#serial;
  }
  #choose(input: CallbackOutcome): void {
    if (!this.#eligible || this.#chosen) return;
    this.#chosen = true; this.#clearTimers();
    void this.#enqueue(async () => {
      if (!this.#eligible) return;
      try {
        let settled = false;
        try { settled = !!(await this.#store.settleEvent(this.#lease, input))?.changed; }
        catch (error) {
          // Callback grants end at attempt timeout. A timeout at equality has no owner commit right;
          // exact-key recovery, not a late owner settlement, consumes the already granted permit.
          if (input.kind !== 'timeout' || !isQueuebitError(error) || error.code !== 'LEASE_LOST') throw error;
          settled = (await this.#store.recoverEvent(this.#lease.event.eventId)).changed;
        }
        if (settled) this.#telemetry.emit('callback_settled', this.#lease.event.taskName);
        if (input.kind === 'timeout') this.#telemetry.emit('callback_timeout', this.#lease.event.taskName);
      } finally { this.#eligible = false; this.#logicalDone = true; this.#checkDone(); }
    });
  }
  pulse(now: number): Promise<void> {
    if (!this.#eligible || this.#chosen || now < this.#nextHeartbeat || this.#lease.event.replayDrainDeadline !== null) return Promise.resolve();
    this.#nextHeartbeat = now + this.#store.config.protocol.lease.heartbeatMs;
    return this.#enqueue(async () => {
      if (!this.#eligible || this.#chosen || this.#lease.event.replayDrainDeadline !== null) return;
      this.#lease = await this.#store.renewEvent(this.#lease); this.#armFrozenExpiry();
    });
  }
  #clearTimers(): void {
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    if (this.#expiryTimer !== undefined) clearTimeout(this.#expiryTimer);
  }
  revoke(): void { this.#eligible = false; this.#logicalDone = true; this.#abort.abort(); this.#clearTimers(); this.#checkDone(); }
  #checkDone(): void { if (this.#physicalDone && this.#logicalDone) this.#finish(); }
}
