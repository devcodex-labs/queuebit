import assert from 'node:assert/strict';
import { test } from 'node:test';
import { eventIdentifier, normalizeReplay, normalizeDeadLetterList, validateReplayResult, encodeReplayReceipt } from '../../.temp/batch/domain/events.js';
import { encodeRunCursor, encodeDeadLetterCursor, decodeDeadLetterCursor } from '../../.temp/batch/domain/cursor.js';

const runId = 'a'.repeat(32); const eventId = `${runId}:1:batchSettled`;
const request = { eventId, expectedRevision: 2, commandId: 'shared/\ud800', reason: '原始 / \udfff\n' };
const result = { kind: 'applied', id: eventId, eventId, revision: 3, status: 'pending', changed: true, replayGeneration: 1 };

test('Event identities are bounded canonical composite identities, not arbitrary Redis keys', () => {
  assert.deepEqual(eventIdentifier(eventId), { eventId, runId, page: 1, kind: 'batchSettled' });
  assert.equal(eventIdentifier(`${runId}:${Number.MAX_SAFE_INTEGER}:failure`).page, Number.MAX_SAFE_INTEGER);
  for (const value of [`${runId}:01:success`, `${runId}:0:success`, `${runId}:1:Success`, `${runId}:9007199254740992:success`, 'event:' + eventId, eventId + ':extra', 1]) {
    assert.throws(() => eventIdentifier(value), { code: 'CONFIG_INVALID' });
  }
});

test('replay input preserves raw text and separates the complete Event request from its derived parent Run identity', () => {
  const value = normalizeReplay(request); assert.equal(value.runId, runId); assert.equal(value.reason, request.reason);
  const wire = JSON.parse(value.wireCanonical); assert.equal(JSON.parse(wire.reason), request.reason); assert.equal(JSON.parse(wire.commandId), request.commandId);
  assert.equal(wire.eventId, eventId); assert.equal(wire.operation, 'replay'); assert.equal(Object.hasOwn(wire, 'runId'), false);
  for (const bad of [{ ...request, runId }, { ...request, expectedRevision: 0 }, { ...request, reason: '' }, { ...request, status: 'dead_letter' }, { ...request, commandId: undefined }]) {
    assert.throws(() => normalizeReplay(bad), { code: 'CONFIG_INVALID' });
  }
  let invoked = false; const getter = { ...request }; Object.defineProperty(getter, 'reason', { get() { invoked = true; return 'bad'; } });
  assert.throws(() => normalizeReplay(getter), { code: 'CONFIG_INVALID' }); assert.equal(invoked, false);
  assert.notEqual(normalizeReplay(request).canonical, normalizeReplay({ ...request, eventId: `${runId}:2:failure` }).canonical);
});

test('replay result variants require the Event revision and every applied field, without cross-variant aliases', () => {
  assert.deepEqual(validateReplayResult(result), result);
  assert.deepEqual(validateReplayResult({ kind: 'not_found', id: eventId }), { kind: 'not_found', id: eventId });
  assert.equal(validateReplayResult({ kind: 'noop', id: eventId, revision: 3, status: 'delivered', changed: false }).kind, 'noop');
  for (const field of Object.keys(result)) { const value = { ...result }; delete value[field]; assert.throws(() => validateReplayResult(value), { code: 'STORAGE_INCONSISTENT' }); }
  for (const value of [{ ...result, status: 'running' }, { ...result, replayGeneration: 0 }, { ...result, eventId: `${runId}:2:failure` },
    { kind: 'not_found', id: eventId, revision: 1 }, { kind: 'noop', id: eventId, revision: 3, status: 'dead_letter', changed: false, replayGeneration: 1 }]) {
    assert.throws(() => validateReplayResult(value), { code: 'STORAGE_INCONSISTENT' });
  }
});

test('replay receipts include full target and result plus both times in the shared 2 KiB budget', () => {
  const time = 1788900000000;
  const encoded = encodeReplayReceipt(normalizeReplay(request).canonical, result, time);
  assert(Buffer.byteLength(encoded) <= 2048); const receipt = JSON.parse(encoded);
  assert.equal(receipt.expiresAt - receipt.recordedAt, 86400000); assert.equal(receipt.request.eventId, eventId);
  assert.equal(JSON.parse(receipt.request.reason), request.reason);
  assert.throws(() => encodeReplayReceipt(normalizeReplay({ ...request, reason: '\u0000'.repeat(300) }).canonical, result, time), { code: 'CONTROL_RECORD_TOO_LARGE' });
});

test('dead-letter list allows only its exact task filter and bounded live traversal options', () => {
  assert.deepEqual(normalizeDeadLetterList(), { filter: {}, limit: 50 });
  assert.deepEqual(normalizeDeadLetterList({ taskName: 'a', limit: 200 }), { filter: { taskName: 'a' }, limit: 200 });
  for (const bad of [{ status: 'dead_letter' }, { limit: 201 }, { runId }, { taskName: 'bad:name' }, { cursor: '' }, null]) {
    assert.throws(() => normalizeDeadLetterList(bad), { code: 'CONFIG_INVALID' });
  }
});

test('dead-letter cursor has an independent schema and sequence fence, with complete filter binding and no expiry renewal', () => {
  const now = 1788900000000; const expiresAt = now + 900000;
  const cursor = encodeDeadLetterCursor('ns', { taskName: 'a' }, 100, 90, expiresAt);
  assert.deepEqual(decodeDeadLetterCursor(cursor, 'ns', { taskName: 'a' }, now), { upperSequence: 100, lastSequence: 90, expiresAt });
  assert.throws(() => decodeDeadLetterCursor(encodeRunCursor('ns', { taskName: 'a' }, 100, 90, expiresAt), 'ns', { taskName: 'a' }, now), { code: 'CURSOR_INVALID' });
  assert.throws(() => decodeDeadLetterCursor(cursor, 'ns', { taskName: 'b' }, now), { code: 'CURSOR_INVALID' });
  assert.throws(() => decodeDeadLetterCursor(cursor, 'other', { taskName: 'a' }, now), { code: 'CURSOR_INVALID' });
  assert.throws(() => decodeDeadLetterCursor(cursor, 'ns', { taskName: 'a' }, expiresAt), { code: 'CURSOR_EXPIRED' });
  const next = encodeDeadLetterCursor('ns', { taskName: 'a' }, 100, 80, expiresAt);
  assert.equal(decodeDeadLetterCursor(next, 'ns', { taskName: 'a' }, now + 800000).expiresAt, expiresAt);
});
