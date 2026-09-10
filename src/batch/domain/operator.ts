import { QueuebitError } from '../api/errors.js';
import type { RunControlInput, RunControlResult, RunFilter, RunStatus } from '../api/types.js';
import { configError, integer, objectFields, safeName } from './config.js';
import { canonicalJson } from './json.js';

export const RUN_STATUSES: readonly RunStatus[] = Object.freeze(['pending', 'running', 'retrying', 'blocked', 'pausing', 'paused', 'success', 'failed', 'cancelled']);
export const CONTROL_TTL_MS = 86400000;
export type RunControlOperation = 'pause' | 'resume' | 'cancel';
export interface NormalizedRunControl extends Readonly<RunControlInput> {
  readonly operation: RunControlOperation; readonly canonical: string; readonly wireCanonical: string;
}

/** Identifiers are opaque lowercase 128-bit IDs, not Redis keys or arbitrary paths. */
export function runIdentifier(value: unknown): string {
  if (typeof value !== 'string' || !/^[a-f0-9]{32}$/.test(value)) configError('Invalid runId');
  return value;
}

/** Preserve text verbatim; the complete escaped receipt is checked separately before persistence. */
export function normalizeRunControl(operation: RunControlOperation, value: unknown): NormalizedRunControl {
  if (!['pause', 'resume', 'cancel'].includes(operation)) configError('Invalid Run control operation');
  const fields = objectFields(value, ['runId', 'expectedRevision', 'reason', 'commandId'], 'control');
  const runId = runIdentifier(fields.runId);
  const request = { operation, runId, ...controlFields(fields) };
  return Object.freeze({ ...request, ...controlCanonical(request) });
}

/** Both Run controls and Event replay preserve the same raw-text and revision contract. */
export function controlFields(fields: Record<string, unknown>): { expectedRevision: number; reason: string; commandId: string } {
  const expectedRevision = integer(fields.expectedRevision, 1, Number.MAX_SAFE_INTEGER, 'expectedRevision');
  for (const [name, limit] of [['reason', 1024], ['commandId', 128]] as const) {
    const field = fields[name];
    if (typeof field !== 'string' || !field.length || Buffer.byteLength(field) > limit) configError(`Invalid ${name}`);
  }
  return { expectedRevision, reason: fields.reason as string, commandId: fields.commandId as string };
}

export function controlCanonical(request: { reason: string; commandId: string }): { canonical: string; wireCanonical: string } {
  // Each arbitrary text value is reversible JSON source inside the Lua-safe wire object.
  const wire = { ...request, reason: canonicalJson(request.reason, 8192), commandId: canonicalJson(request.commandId, 1024) };
  return { canonical: canonicalJson(request, 16384), wireCanonical: canonicalJson(wire, 16384) };
}

export function normalizeRunList(value: unknown = {}): { filter: Readonly<RunFilter>; limit: number; cursor?: string } {
  const fields = objectFields(value, ['taskName', 'status', 'limit', 'cursor'], 'Run list');
  const filter: RunFilter = {};
  if (fields.taskName !== undefined) filter.taskName = safeName(fields.taskName, 'taskName');
  if (fields.status !== undefined) {
    if (!RUN_STATUSES.includes(fields.status as RunStatus)) configError('Invalid Run status');
    filter.status = fields.status as RunStatus;
  }
  const limit = fields.limit === undefined ? 50 : integer(fields.limit, 1, 200, 'limit');
  if (fields.cursor !== undefined && (typeof fields.cursor !== 'string' || !fields.cursor.length || fields.cursor.length > 8192)) configError('Invalid cursor input');
  return Object.freeze({ filter: Object.freeze(filter), limit, ...(fields.cursor === undefined ? {} : { cursor: fields.cursor as string }) });
}

/** Validate storage's discriminated union, rather than casting a partial or mixed reply. */
export function validateControlResult(value: unknown): RunControlResult {
  try {
    const all = objectFields(value, ['kind', 'id', 'revision', 'status', 'changed'], 'control result');
    const id = runIdentifier(all.id);
    if (all.kind === 'not_found' && Object.keys(all).length === 2) return Object.freeze({ kind: 'not_found', id });
    if (Object.keys(all).length !== 5 || !RUN_STATUSES.includes(all.status as RunStatus)
      || !['applied', 'noop'].includes(all.kind as string) || all.changed !== (all.kind === 'applied')) throw new Error('Invalid variant');
    const revision = integer(all.revision, 1, Number.MAX_SAFE_INTEGER, 'revision');
    const status = all.status as RunStatus;
    return all.kind === 'applied' ? Object.freeze({ kind: 'applied', id, revision, status, changed: true })
      : Object.freeze({ kind: 'noop', id, revision, status, changed: false });
  } catch { throw new QueuebitError('STORAGE_INCONSISTENT', 'Malformed operator result'); }
}

/** Encode text fields losslessly for Redis cjson; account for every outer field and escape byte. */
export function encodeControlReceipt(request: string, result: RunControlResult, recordedAt: number): string {
  validateControlResult(result);
  const parsed = JSON.parse(request) as RunControlInput & { operation: RunControlOperation };
  const { operation, ...input } = parsed;
  return encodeWireReceipt(normalizeRunControl(operation, input).wireCanonical, result, recordedAt);
}

/** Callers validate their exact request/result variants before sharing the parent Run's finite receipt slot. */
export function encodeWireReceipt(wireCanonical: string, result: unknown, recordedAt: number): string {
  integer(recordedAt, 0, Number.MAX_SAFE_INTEGER - CONTROL_TTL_MS, 'recordedAt');
  const wire: unknown = JSON.parse(wireCanonical);
  const receipt = canonicalJson({ request: wire, result, recordedAt, expiresAt: recordedAt + CONTROL_TTL_MS }, 32768);
  if (Buffer.byteLength(receipt) > 2048) throw new QueuebitError('CONTROL_RECORD_TOO_LARGE', 'Entire control receipt exceeds 2 KiB');
  return receipt;
}
