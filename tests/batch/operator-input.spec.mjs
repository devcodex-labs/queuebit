import assert from 'node:assert/strict';
import { test } from 'node:test';
import { normalizeRunControl, normalizeRunList, encodeControlReceipt, validateControlResult } from '../../.temp/batch/domain/operator.js';
import { encodeRunCursor, decodeRunCursor, CURSOR_TTL_MS } from '../../.temp/batch/domain/cursor.js';

const runId = 'a'.repeat(32);
const input = extras => ({ runId, expectedRevision: 1, commandId: 'command', reason: ' because ', ...extras });
const rejects = (fn, code = 'CONFIG_INVALID') => assert.throws(fn, error => error.code === code);

test('operator control preserves complete raw request and rejects sibling/missing fields without invoking accessors', () => {
  const value = input({ reason: ' \ud800/汉\n', commandId: '\udc00' });
  const normalized = normalizeRunControl('pause', value);
  assert.deepEqual(JSON.parse(normalized.canonical), { operation: 'pause', ...value });
  value.reason = 'changed';
  assert.notEqual(JSON.parse(normalized.canonical).reason, value.reason);
  assert(Object.isFrozen(normalized));
  for (const key of ['runId', 'expectedRevision', 'commandId', 'reason']) {
    const incomplete = input(); delete incomplete[key]; rejects(() => normalizeRunControl('pause', incomplete));
  }
  for (const extras of [{ eventId: 'x' }, { reason: '' }, { reason: null }, { expectedRevision: 0 },
    { expectedRevision: 1.5 }, { expectedRevision: Number.MAX_SAFE_INTEGER + 1 }, { commandId: '' },
    { commandId: 'x'.repeat(129) }, { reason: '汉'.repeat(342) }, { runId: 'A'.repeat(32) }]) {
    rejects(() => normalizeRunControl('pause', input(extras)));
  }
  let calls = 0;
  const unsafe = input(); Object.defineProperty(unsafe, 'reason', { get() { calls++; return 'x'; } });
  rejects(() => normalizeRunControl('pause', unsafe)); assert.equal(calls, 0);
  rejects(() => normalizeRunControl('replay', input()));
  assert.equal(normalizeRunControl('cancel', input({ expectedRevision: Number.MAX_SAFE_INTEGER, commandId: 'x'.repeat(128), reason: ' '.repeat(1024) })).reason.length, 1024);
});

test('whole stored control receipt includes timestamps and escaped request; equality never uses a truncated reason', () => {
  const result = { kind: 'applied', id: runId, revision: 2, status: 'paused', changed: true };
  const now = 1788930000000;
  const normalized = normalizeRunControl('pause', input());
  const receipt = JSON.parse(encodeControlReceipt(normalized.canonical, result, now));
  assert.deepEqual(receipt.request, JSON.parse(normalized.wireCanonical));
  assert.equal(JSON.parse(receipt.request.reason), normalized.reason);
  assert.equal(receipt.expiresAt, now + 86400000);
  assert.deepEqual(receipt.result, result);
  const escaped = normalizeRunControl('pause', input({ reason: '\u0000'.repeat(300) }));
  rejects(() => encodeControlReceipt(escaped.canonical, result, now), 'CONTROL_RECORD_TOO_LARGE');
  const tailA = normalizeRunControl('pause', input({ reason: 'x'.repeat(1000) + 'A' }));
  const tailB = normalizeRunControl('pause', input({ reason: 'x'.repeat(1000) + 'B' }));
  assert.notEqual(tailA.canonical, tailB.canonical);
  let largest = 0;
  for (let size = 1; size <= 1024; size++) {
    const control = normalizeRunControl('pause', input({ reason: '\n'.repeat(size) }));
    try { largest = Buffer.byteLength(encodeControlReceipt(control.canonical, result, now)); }
    catch (error) { assert.equal(error.code, 'CONTROL_RECORD_TOO_LARGE'); break; }
  }
  assert(largest >= 2045 && largest <= 2048, String(largest));
});

test('control result variants reject cross-fields and missing successful fields', () => {
  const good = { kind: 'applied', id: runId, revision: 2, status: 'paused', changed: true };
  assert.deepEqual(validateControlResult(good), good);
  assert.deepEqual(validateControlResult({ kind: 'not_found', id: runId }), { kind: 'not_found', id: runId });
  for (const key of ['id', 'revision', 'status', 'changed']) {
    const bad = { ...good }; delete bad[key]; rejects(() => validateControlResult(bad), 'STORAGE_INCONSISTENT');
  }
  for (const bad of [{ kind: 'not_found', id: runId, revision: 0 }, { ...good, kind: 'noop' },
    { ...good, eventId: 'x' }, { ...good, status: 'delivered' }, { ...good, revision: 1.2 }]) {
    rejects(() => validateControlResult(bad), 'STORAGE_INCONSISTENT');
  }
});

test('Run list normalizes exact filters and strict bounded limits', () => {
  assert.deepEqual(normalizeRunList(), { filter: {}, limit: 50 });
  assert.deepEqual(normalizeRunList({ taskName: 'mail', status: 'paused', limit: 200 }), { filter: { taskName: 'mail', status: 'paused' }, limit: 200 });
  for (const bad of [null, { limit: 0 }, { limit: 201 }, { status: ['paused'] }, { status: 'delivered' },
    { taskName: 'bad:name' }, { cursor: '' }, { cursor: null }, { limit: undefined }, { query: {} }]) rejects(() => normalizeRunList(bad));
});

test('cursor is strict, binds full filter/namespace, fences upper, and never renews its 15 minute deadline', () => {
  const now = 1788930000000;
  const filter = { status: 'paused', taskName: 'mail' };
  const token = encodeRunCursor('cursor-test', filter, 100, 91, now + CURSOR_TTL_MS);
  const decoded = decodeRunCursor(token, 'cursor-test', { taskName: 'mail', status: 'paused' }, now);
  assert.equal(decoded.upperSequence, 100); assert.equal(decoded.lastSequence, 91);
  const next = encodeRunCursor('cursor-test', filter, decoded.upperSequence, 80, decoded.expiresAt);
  assert.equal(decodeRunCursor(next, 'cursor-test', filter, now + CURSOR_TTL_MS - 1).expiresAt, decoded.expiresAt);
  rejects(() => decodeRunCursor(next, 'cursor-test', filter, now + CURSOR_TTL_MS), 'CURSOR_EXPIRED');
  rejects(() => decodeRunCursor(token, 'other', filter, now), 'CURSOR_INVALID');
  rejects(() => decodeRunCursor(token, 'cursor-test', { status: 'paused' }, now), 'CURSOR_INVALID');
  const body = JSON.parse(Buffer.from(token, 'base64url').toString());
  for (const patch of [{ lastSequence: 101 }, { upperSequence: 0 }, { expiresAt: now + CURSOR_TTL_MS + 1 },
    { schema: 'batch-v0' }, { filter: '{}' }, { filterDigest: '0'.repeat(64) }, { extra: true }]) {
    const bad = Buffer.from(JSON.stringify({ ...body, ...patch })).toString('base64url');
    rejects(() => decodeRunCursor(bad, 'cursor-test', filter, now), 'CURSOR_INVALID');
  }
  for (const bad of ['', token + '=', '%%%bad', 'a'.repeat(9000), Buffer.from('{').toString('base64url')]) {
    rejects(() => decodeRunCursor(bad, 'cursor-test', filter, now), 'CURSOR_INVALID');
  }
  // Non-UTF8 bytes must not be silently replaced into another valid traversal.
  const utf8 = Buffer.from(token, 'base64url');
  const marker = utf8.indexOf('cursor-test');
  const malformed = Buffer.concat([utf8.subarray(0, marker), Buffer.from([0xff]), utf8.subarray(marker + 'cursor-test'.length)]);
  rejects(() => decodeRunCursor(malformed.toString('base64url'), '\ufffd', filter, now), 'CURSOR_INVALID');
});
