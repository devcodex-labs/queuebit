import { QueuebitError } from '../api/errors.js';
import type { RunFilter, DeadLetterFilter } from '../api/types.js';
import { integer, objectFields, safeName } from './config.js';
import { canonicalJson, digest } from './json.js';
import { normalizeRunList } from './operator.js';
import { normalizeDeadLetterList } from './events.js';

export const CURSOR_TTL_MS = 15 * 60 * 1000;
export interface RunCursor { readonly upperSequence: number; readonly lastSequence: number; readonly expiresAt: number }
type CursorSchema = 'batch-v1:runs' | 'batch-v1:dead';

/** An opaque traversal state, never an authorization token. Original expiry is reused across pages. */
export function encodeRunCursor(namespace: string, filter: RunFilter, upperSequence: number, lastSequence: number, expiresAt: number): string {
  return encodeCursor(namespace, filter, upperSequence, lastSequence, expiresAt, 'batch-v1:runs');
}
export function encodeDeadLetterCursor(namespace: string, filter: DeadLetterFilter, upperSequence: number, lastSequence: number, expiresAt: number): string {
  return encodeCursor(namespace, filter, upperSequence, lastSequence, expiresAt, 'batch-v1:dead');
}
function encodeCursor(namespace: string, filter: RunFilter | DeadLetterFilter, upperSequence: number, lastSequence: number, expiresAt: number, schema: CursorSchema): string {
  safeName(namespace, 'namespace');
  integer(upperSequence, 1, Number.MAX_SAFE_INTEGER, 'upperSequence');
  integer(lastSequence, 1, upperSequence, 'lastSequence');
  integer(expiresAt, CURSOR_TTL_MS, Number.MAX_SAFE_INTEGER, 'expiresAt');
  const normalizedFilter = canonicalJson((schema === 'batch-v1:runs' ? normalizeRunList(filter) : normalizeDeadLetterList(filter)).filter, 1024);
  return Buffer.from(canonicalJson({ schema, namespace, filter: normalizedFilter,
    filterDigest: digest(normalizedFilter), upperSequence, lastSequence, expiresAt }, 4096)).toString('base64url');
}

/** Check both the complete filter and digest; a digest collision never grants another traversal. */
export function decodeRunCursor(value: unknown, namespace: string, filter: RunFilter, redisNow: number): RunCursor {
  return decodeCursor(value, namespace, filter, redisNow, 'batch-v1:runs');
}
export function decodeDeadLetterCursor(value: unknown, namespace: string, filter: DeadLetterFilter, redisNow: number): RunCursor {
  return decodeCursor(value, namespace, filter, redisNow, 'batch-v1:dead');
}
function decodeCursor(value: unknown, namespace: string, filter: RunFilter | DeadLetterFilter, redisNow: number, schema: CursorSchema): RunCursor {
  let decoded: RunCursor;
  try {
    integer(redisNow, 0, Number.MAX_SAFE_INTEGER - CURSOR_TTL_MS, 'Redis time');
    if (typeof value !== 'string' || !value.length || value.length > 8192 || !/^[A-Za-z0-9_-]+$/.test(value)) throw new Error('Invalid encoding');
    const bytes = Buffer.from(value, 'base64url');
    if (bytes.toString('base64url') !== value || bytes.length > 4096) throw new Error('Noncanonical encoding');
    const text = bytes.toString('utf8');
    if (!Buffer.from(text, 'utf8').equals(bytes)) throw new Error('Invalid UTF-8');
    const body: unknown = JSON.parse(text);
    const fields = objectFields(body, ['schema', 'namespace', 'filter', 'filterDigest', 'upperSequence', 'lastSequence', 'expiresAt'], 'cursor');
    if (canonicalJson(body, 4096) !== text) throw new Error('Noncanonical JSON');
    const normalizedFilter = canonicalJson((schema === 'batch-v1:runs' ? normalizeRunList(filter) : normalizeDeadLetterList(filter)).filter, 1024);
    if (fields.schema !== schema || fields.namespace !== namespace || fields.filter !== normalizedFilter
      || fields.filterDigest !== digest(normalizedFilter)) throw new Error('Wrong traversal');
    const upperSequence = integer(fields.upperSequence, 1, Number.MAX_SAFE_INTEGER, 'upperSequence');
    const lastSequence = integer(fields.lastSequence, 1, upperSequence, 'lastSequence');
    const expiresAt = integer(fields.expiresAt, CURSOR_TTL_MS, redisNow + CURSOR_TTL_MS, 'expiresAt');
    decoded = Object.freeze({ upperSequence, lastSequence, expiresAt });
  } catch { throw new QueuebitError('CURSOR_INVALID', 'Invalid traversal cursor'); }
  if (redisNow >= decoded.expiresAt) throw new QueuebitError('CURSOR_EXPIRED', 'Traversal cursor has expired');
  return decoded;
}
