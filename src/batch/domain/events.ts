import type { DeadLetterFilter, EventKind, EventReplayInput, EventReplayResult, EventStatus } from '../api/types.js';
import { QueuebitError } from '../api/errors.js';
import { configError, integer, objectFields, safeName } from './config.js';
import { controlCanonical, controlFields, encodeWireReceipt } from './operator.js';

export const EVENT_STATUSES: readonly EventStatus[] = Object.freeze(['pending', 'retrying', 'delivering', 'delivered', 'dead_letter']);
export function eventIdentifier(value: unknown): { eventId: string; runId: string; page: number; kind: EventKind } {
  if (typeof value !== 'string' || value.length > 64) configError('Invalid eventId');
  const match = /^([a-f0-9]{32}):([1-9][0-9]*):(batchSettled|success|failure)$/.exec(value);
  if (!match) configError('Invalid eventId');
  return Object.freeze({ eventId: value, runId: match[1]!, page: integer(Number(match[2]), 1, Number.MAX_SAFE_INTEGER, 'Event page'), kind: match[3] as EventKind });
}
export interface NormalizedReplay extends Readonly<EventReplayInput> {
  readonly runId: string; readonly operation: 'replay'; readonly canonical: string; readonly wireCanonical: string;
}
export function normalizeReplay(value: unknown): NormalizedReplay {
  const fields = objectFields(value, ['eventId', 'expectedRevision', 'reason', 'commandId'], 'replay');
  const { eventId, runId } = eventIdentifier(fields.eventId);
  const request = { operation: 'replay' as const, eventId, ...controlFields(fields) };
  return Object.freeze({ ...request, runId, ...controlCanonical(request) });
}
export function normalizeDeadLetterList(value: unknown = {}): { filter: Readonly<DeadLetterFilter>; limit: number; cursor?: string } {
  const fields = objectFields(value, ['taskName', 'limit', 'cursor'], 'dead-letter list');
  const filter: DeadLetterFilter = {};
  if (fields.taskName !== undefined) filter.taskName = safeName(fields.taskName, 'taskName');
  const limit = fields.limit === undefined ? 50 : integer(fields.limit, 1, 200, 'limit');
  if (fields.cursor !== undefined && (typeof fields.cursor !== 'string' || !fields.cursor.length || fields.cursor.length > 8192)) configError('Invalid cursor input');
  return Object.freeze({ filter: Object.freeze(filter), limit, ...(fields.cursor === undefined ? {} : { cursor: fields.cursor as string }) });
}
export function validateReplayResult(value: unknown): EventReplayResult {
  try {
    const fields = objectFields(value, ['kind', 'id', 'eventId', 'revision', 'status', 'changed', 'replayGeneration'], 'replay result');
    const id = eventIdentifier(fields.id).eventId;
    if (fields.kind === 'not_found' && Object.keys(fields).length === 2) return Object.freeze({ kind: 'not_found', id });
    const revision = integer(fields.revision, 1, Number.MAX_SAFE_INTEGER, 'revision');
    if (fields.kind === 'noop' && Object.keys(fields).length === 5 && fields.changed === false && EVENT_STATUSES.includes(fields.status as EventStatus)) {
      return Object.freeze({ kind: 'noop', id, revision, status: fields.status as EventStatus, changed: false });
    }
    if (fields.kind !== 'applied' || Object.keys(fields).length !== 7 || fields.status !== 'pending' || fields.changed !== true || fields.eventId !== id) throw Error('Invalid variant');
    return Object.freeze({ kind: 'applied', id, eventId: id, revision, status: 'pending', changed: true,
      replayGeneration: integer(fields.replayGeneration, 1, Number.MAX_SAFE_INTEGER, 'replayGeneration') });
  } catch { throw new QueuebitError('STORAGE_INCONSISTENT', 'Malformed replay result'); }
}
export function encodeReplayReceipt(request: string, result: EventReplayResult, recordedAt: number): string {
  validateReplayResult(result);
  const { operation, ...input } = JSON.parse(request) as EventReplayInput & { operation: unknown };
  if (operation !== 'replay') configError('Invalid replay operation');
  return encodeWireReceipt(normalizeReplay(input).wireCanonical, result, recordedAt);
}
