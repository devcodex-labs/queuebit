import type { BatchQueue, JsonValue } from 'queuebit';

export interface ReceiptQuery { snapshotId: string }
export interface ReceiptState { afterId: number }
export interface ReceiptRow { id: number; payload: JsonValue }
/** The snapshot fixes membership and payload for the whole Run, including retries. */
export interface ReceiptRepository {
  readPage(snapshotId: string, afterId: number, limit: number, signal: AbortSignal): Promise<readonly ReceiptRow[]>;
}
/** Implement key uniqueness and the external write in one durable transaction / provider idempotency operation. */
export interface ReceiptSink {
  putOnce(key: string, payload: JsonValue, signal: AbortSignal): Promise<void>;
  completeOnce(eventId: string, snapshotId: string, signal: AbortSignal): Promise<void>;
}
export function defineReceiptTask(queue: BatchQueue, repository: ReceiptRepository, sink: ReceiptSink) {
  return queue.define<ReceiptQuery, ReceiptState>({ name: 'receipt-snapshot', version: '1', events: ['success'] }, {
    async execute(ctx) {
      ctx.signal.throwIfAborted();
      const afterId = ctx.state?.afterId ?? 0;
      const rows = await repository.readPage(ctx.query.snapshotId, afterId, 100, ctx.signal);
      if (rows.length > 100) throw new Error('Repository exceeded the page limit');
      if (rows.length === 0) return ctx.end();
      let cursor = afterId;
      for (const row of rows) {
        if (!Number.isSafeInteger(row.id) || row.id <= cursor) throw new Error('Repository returned an invalid or non-progressing keyset');
        ctx.signal.throwIfAborted();
        // JSON tuple avoids delimiter collisions between arbitrary snapshot IDs and record IDs.
        await sink.putOnce(JSON.stringify(['receipt', ctx.query.snapshotId, row.id]), row.payload, ctx.signal);
        cursor = row.id;
      }
      return ctx.next({ afterId: cursor });
    },
    async onSuccess(ctx) {
      ctx.signal.throwIfAborted();
      await sink.completeOnce(ctx.eventId, ctx.query.snapshotId, ctx.signal);
    }
  });
}
