import { QueuebitError } from '../api/errors.js';
import type { Definition } from '../domain/definition.js';
import { createAttemptControl } from '../domain/control.js';
import { canonicalJson, errorEnvelope, JSON_LIMITS } from '../domain/json.js';
import type { BatchRedisStore, ExecutionLease, SettlementInput } from '../storage/redis/store.js';
import type { Telemetry } from './telemetry.js';

/** One physical invocation, one serialized lease owner, and a separately revocable commit right. */
export class ExecutionSlot {
  readonly runId: string;
  readonly done: Promise<void>;
  readonly #abort = new AbortController();
  readonly #store: BatchRedisStore;
  #lease: ExecutionLease;
  #serial = Promise.resolve();
  #eligible = true;
  #chosen = false;
  #physicalDone = false;
  #logicalDone = false;
  #nextHeartbeat = 0;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #finish!: () => void;
  readonly #onError: (error: unknown) => void;
  readonly #telemetry: Telemetry;
  get residual(): boolean { return !this.#physicalDone && (!this.#eligible || this.#chosen); }

  constructor(store: BatchRedisStore, lease: ExecutionLease, definition: Definition<unknown, unknown>, onError: (error: unknown) => void, telemetry: Telemetry) {
    this.#store = store; this.#lease = lease; this.#onError = onError; this.runId = lease.run.runId;
    this.#telemetry = telemetry;
    this.done = new Promise(resolve => { this.#finish = resolve; });
    this.#nextHeartbeat = Date.now() + store.config.protocol.lease.heartbeatMs;
    const control = createAttemptControl(canonicalJson(lease.run.state, JSON_LIMITS.state));
    const context = Object.freeze({ query: lease.run.query, state: lease.run.state, runId: lease.run.runId,
      batchId: lease.run.batchId, page: lease.run.page, attempt: lease.run.batchFailures + 1,
      signal: this.#abort.signal, next: control.next, end: control.end });
    if (lease.timeoutRemainingMs <= 0) {
      this.#physicalDone = true;
      this.#choose({ kind: 'timeout' });
      return;
    }
    this.#timer = setTimeout(() => { this.#abort.abort(); this.#choose({ kind: 'timeout' }); }, lease.timeoutRemainingMs);
    // This Promise remains observed and occupies its slot even after timeout, cancel or close.
    void Promise.resolve().then(() => definition.handlers!.execute(context)).then(value => {
      this.#physicalDone = true;
      if (!this.#eligible || this.#chosen) return;
      try { this.#choose(control.consume(value)); }
      catch { this.#choose({ kind: 'contract', error: errorEnvelope(new QueuebitError('HANDLER_CONTRACT_INVALID', 'Handler returned an invalid control')) }); }
    }, error => {
      this.#physicalDone = true;
      if (!this.#eligible || this.#chosen) return;
      this.#choose(control.poisoned
        ? { kind: 'contract', error: errorEnvelope(new QueuebitError('HANDLER_CONTRACT_INVALID', 'Handler created an invalid control')) }
        : { kind: 'business', error: errorEnvelope(error) });
    }).finally(() => { this.#checkDone(); });
  }
  #enqueue(action: () => Promise<void>): Promise<void> {
    this.#serial = this.#serial.then(action).catch(error => { this.#onError(error); this.revoke(); });
    return this.#serial;
  }
  #choose(input: SettlementInput): void {
    if (!this.#eligible || this.#chosen) return;
    this.#chosen = true;
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    void this.#enqueue(async () => {
      if (!this.#eligible) return;
      try {
        await this.#store.settle(this.#lease, input);
        this.#telemetry.emit('run_settled', this.#lease.run.taskName);
        if (input.kind === 'timeout') this.#telemetry.emit('run_timeout', this.#lease.run.taskName);
      }
      finally { this.#eligible = false; this.#logicalDone = true; this.#checkDone(); }
    });
  }
  pulse(now: number): Promise<void> {
    if (!this.#eligible || this.#chosen || now < this.#nextHeartbeat) return Promise.resolve();
    this.#nextHeartbeat = now + this.#store.config.protocol.lease.heartbeatMs;
    return this.#enqueue(async () => {
      if (this.#eligible && !this.#chosen) this.#lease = await this.#store.renew(this.#lease);
    });
  }
  revoke(): void {
    this.#eligible = false; this.#logicalDone = true; this.#abort.abort();
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#checkDone();
  }
  #checkDone(): void {
    if (this.#physicalDone && this.#logicalDone) this.#finish();
  }
}
