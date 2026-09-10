import type { BatchQueueOptions, MetricCode, MetricsSnapshot, TelemetryRecord } from '../api/types.js';
import { canonicalJson } from '../domain/json.js';

const CODES: readonly MetricCode[] = ['queue_ready','queue_closing','queue_error','run_created','run_read','run_cancelled',
  'operator_pause','operator_resume','operator_cancel','metadata_read','runs_listed','run_claimed','run_settled','run_timeout','maintenance_tick',
  'callback_claimed','callback_settled','callback_timeout','event_replayed','dead_letter_read','dead_letters_listed'];
const increment = (value: number): number => Math.min(Number.MAX_SAFE_INTEGER, value + 1);

/** Pure construction; a finite observation queue with one sink invocation, independent of business promises. */
export class Telemetry {
  readonly #sink: BatchQueueOptions['telemetry'];
  readonly #tasks = new Set<string>();
  readonly #counters = Object.fromEntries(CODES.map(code => [code, 0])) as Record<MetricCode, number>;
  #queue: TelemetryRecord[] = [];
  #pumping = false;
  #inFlight = false;
  #closed = false;
  #dropped = 0;
  #sinkFailures = 0;
  constructor(options?: BatchQueueOptions['telemetry']) { this.#sink = options; }
  registerTask(name: string): void { if (this.#tasks.size < 128) this.#tasks.add(name); }
  emit(code: MetricCode, taskName?: string): void {
    if (this.#closed || !CODES.includes(code)) return;
    this.#counters[code] = increment(this.#counters[code]);
    if (!this.#sink) return;
    if (this.#queue.length >= 1024) { this.#dropped = increment(this.#dropped); return; }
    const record: TelemetryRecord = Object.freeze({ timestamp: Date.now(), origin: 'local', code, value: 1,
      ...(taskName !== undefined && this.#tasks.has(taskName) ? { taskName } : {}) });
    try { canonicalJson(record, 2048); } catch { this.#dropped = increment(this.#dropped); return; }
    this.#queue.push(record);
    if (!this.#pumping) {
      this.#pumping = true;
      // Observations run in a separate microtask, so synchronous sink throws cannot escape to the caller.
      void this.#drain().catch(() => { this.#sinkFailures = increment(this.#sinkFailures); this.#pumping = false; this.#inFlight = false; });
    }
  }
  async #drain(): Promise<void> {
    await Promise.resolve();
    try {
      while (!this.#closed && this.#queue.length) {
        const record = this.#queue.shift()!; this.#inFlight = true;
        try { await this.#sink!.sink(record); }
        catch { this.#sinkFailures = increment(this.#sinkFailures); }
        finally { this.#inFlight = false; }
      }
    } finally { this.#pumping = false; }
  }
  snapshot(): MetricsSnapshot {
    return Object.freeze({ origin: 'local', sampledAt: Date.now(), counters: Object.freeze({ ...this.#counters }),
      telemetry: Object.freeze({ buffered: this.#queue.length, inFlight: this.#inFlight, dropped: this.#dropped, sinkFailures: this.#sinkFailures }) });
  }
  /** Stop new observations and discard waiting records; an arbitrary in-flight user promise remains observed, not forcibly terminated. */
  close(): void {
    this.#closed = true;
    this.#dropped = Math.min(Number.MAX_SAFE_INTEGER, this.#dropped + this.#queue.length);
    this.#queue = [];
  }
}
