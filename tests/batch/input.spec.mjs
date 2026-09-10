import assert from 'node:assert/strict';
import { test } from 'node:test';
import { canonicalJson, frozenJson, encodeIdempotencyKey, errorEnvelope } from '../../.temp/batch/domain/json.js';
import { normalizeOptions, mergePolicy, backoffDelay } from '../../.temp/batch/domain/config.js';
import { normalizeDefinition } from '../../.temp/batch/domain/definition.js';
import { createAttemptControl } from '../../.temp/batch/domain/control.js';

const invalid = fn => assert.throws(fn, error => ['JSON_INVALID', 'PAYLOAD_TOO_LARGE', 'CONFIG_INVALID', 'HANDLER_CONTRACT_INVALID'].includes(error.code));
const options = overrides => ({ namespace: 'input-tests', redis: { mode: 'direct', host: '127.0.0.1', port: 6379 }, ...overrides });

test('strict JSON canonical order, independent freeze, UTF-8 exact byte boundary', () => {
  assert.equal(canonicalJson({ z: -0, a: ['汉', true, null] }), '{"a":["汉",true,null],"z":0}');
  const text = canonicalJson({ '\ue000': 1, '😀': 2 });
  assert.equal(text, '{"😀":2,"":1}');
  const source = { nested: [1] };
  const snapshot = frozenJson(canonicalJson(source));
  source.nested[0] = 2;
  assert.equal(snapshot.nested[0], 1);
  assert.throws(() => { snapshot.nested[0] = 3; });
  assert.equal(canonicalJson('汉', 5), '"汉"');
  invalid(() => canonicalJson('汉', 4));
  assert.equal(canonicalJson(JSON.parse('{"__proto__":{"safe":true}}')), '{"__proto__":{"safe":true}}');
  assert.equal({}.safe, undefined);
});

test('strict JSON never invokes getters or toJSON and rejects ambiguous shapes', () => {
  let called = 0;
  const values = [undefined, NaN, Infinity, 1n, new Date(), new Map(), () => 1, Symbol('x'), [, 1],
    { get value() { called++; return 1; } }, { toJSON() { called++; return 1; } },
    { [Symbol('key')]: 1 }, Object.create({ x: 1 })];
  const cycle = {}; cycle.self = cycle; values.push(cycle);
  const extra = [1]; extra.extra = 1; values.push(extra);
  for (const value of values) invalid(() => canonicalJson(value));
  assert.equal(called, 0);
  assert.equal(canonicalJson({ left: source, right: source }), '{"left":{"x":1},"right":{"x":1}}');
});
const source = { x: 1 };

test('depth and total node limits include every visited value', () => {
  let value = null;
  for (let depth = 0; depth < 64; depth++) value = [value];
  canonicalJson(value);
  invalid(() => canonicalJson([value]));
  canonicalJson(Array(9999).fill(null));
  invalid(() => canonicalJson(Array(10000).fill(null)));
});

test('idempotency keys preserve empty/original Unicode and reject replacement collisions', () => {
  assert.equal(encodeIdempotencyKey(''), '');
  assert.notEqual(encodeIdempotencyKey('é'), encodeIdempotencyKey('e\u0301'));
  assert.equal(Buffer.from(encodeIdempotencyKey('😀'), 'base64url').toString(), '😀');
  for (const key of ['\ud800', '\udfff', 'x\ud800x', '😀'.repeat(65), undefined]) invalid(() => encodeIdempotencyKey(key));
  encodeIdempotencyKey('😀'.repeat(64));
});

test('error snapshots bound the whole JSON and do not run formatting hooks', () => {
  let called = 0;
  const thrown = { get message() { called++; throw Error(); }, toString() { called++; return 'bad'; } };
  assert.equal(errorEnvelope(thrown).message, 'Unrecognized thrown value');
  assert.equal(called, 0);
  const envelope = errorEnvelope({ message: '\u0000'.repeat(40000), stack: '汉'.repeat(40000) });
  assert.equal(envelope.truncated, true);
  assert.ok(Buffer.byteLength(canonicalJson(envelope)) <= 32768);
  assert.equal(errorEnvelope(new Error('business failed')).message, 'business failed');
});

test('normalization freezes complete defaults, policies merge only declared fields', () => {
  const normalized = normalizeOptions(options());
  assert.equal(normalized.runtime.mode, 'all');
  assert.equal(normalized.runtime.concurrency, 4);
  assert.equal(normalized.protocol.limits.totalBytes, 512 * 1024 * 1024);
  assert.equal(normalized.protocol.lease.leaseMs, 30000);
  assert.equal(normalized.protocolCanonical, canonicalJson(normalized.protocol));
  assert.throws(() => { normalized.protocol.lease.leaseMs = 10; });
  const policy = mergePolicy(normalized.defaults, { backoff: { baseMs: 2 } });
  assert.deepEqual(policy, { attempts: 3, timeoutMs: 30000, backoff: { baseMs: 2, maxMs: 30000, jitter: 'full' } });
  assert.equal(backoffDelay(policy, 1000, 1 - Number.EPSILON), 30000);
  assert.equal(backoffDelay(policy, 1, 0), 0);
});

test('unknown/sibling config fields, invalid budgets and omitted required fields fail synchronously', () => {
  const cases = [options({ namespace: '' }), options({ namespace: 'has:colon' }), options({ extra: true }),
    options({ redis: { mode: 'direct', host: 'x' } }), options({ redis: { mode: 'direct', host: 'x', port: 1, url: 'redis://x' } }),
    options({ redis: { mode: 'url', url: 'redis://x', tls: {} } }), options({ redis: { mode: 'url', url: 'https://x' } }),
    options({ redis: { mode: 'url', url: 'redis://x/1.2' } }), options({ redis: { mode: 'url', url: 'redis://x/0?database=2' } }),
    options({ redis: { mode: 'sentinel', name: 'primary', seeds: [], password: 'x' } }),
    options({ redis: { mode: 'direct', host: 'x', port: 1, tls: { rejectUnauthorized: false } } }),
    options({ runtime: { concurrency: 0 } }), options({ runtime: { mode: 'worker' } }),
    options({ defaults: { attempts: 0 } }), options({ defaults: { backoff: { jitter: 'none' } } }),
    options({ protocol: { lease: { leaseMs: 3000 } } }),
    options({ protocol: { limits: { totalBytes: 100 } } }), options({ protocol: { limits: { runMax: 2 } } }),
    options({ protocol: { retention: { deadLetterMs: 7 * 86400000 } } })];
  for (const value of cases) invalid(() => normalizeOptions(value));
  normalizeOptions(options({ redis: { mode: 'url', url: 'rediss://user:pass@localhost:6380/2', tls: { ca: 'certificate' } } }));
  normalizeOptions(options({ protocol: { lease: { leaseMs: 3000, heartbeatMs: 1000 }, limits: { memberMax: 1 } } }));
});

test('definition identity excludes local defaults/functions, validates two-generic event handlers', () => {
  const contract = { name: 'scan', version: '1', events: ['success'], policy: { attempts: 2 } };
  const handlers = { execute: ctx => ctx.end(), onSuccess() {} };
  const left = normalizeDefinition(contract, handlers, 'all');
  const right = normalizeDefinition(contract, undefined, 'producer');
  assert.equal(left.identity, right.identity);
  assert.equal(left.canonical, right.canonical);
  assert.equal(left.handlers.execute, handlers.execute);
  invalid(() => normalizeDefinition(contract, { execute: handlers.execute }, 'all'));
  invalid(() => normalizeDefinition(contract, { ...handlers, onFailure() {} }, 'all'));
  invalid(() => normalizeDefinition({ ...contract, events: ['success', 'success'] }, handlers, 'all'));
  invalid(() => normalizeDefinition({ ...contract, typo: true }, handlers, 'all'));
  invalid(() => normalizeDefinition({ ...contract, events: [['success']] }, handlers, 'all'));
  assert.throws(() => normalizeDefinition(contract, handlers, 'producer'), { code: 'MODE_OPERATION_NOT_ALLOWED' });
});

test('explicit null never substitutes for omitted config defaults', () => {
  for (const key of ['mode', 'concurrency', 'callbackConcurrency', 'closeGraceMs']) {
    invalid(() => normalizeOptions(options({ runtime: { [key]: null } })));
  }
  const sections = { lease: ['leaseMs', 'heartbeatMs', 'pollMs', 'recoveryLimit'],
    retention: ['runMs', 'deliveredEventMs', 'deadLetterMs'],
    limits: ['memberMax', 'totalBytes'], maintenance: ['batchSize', 'maxBatchesPerTick', 'timeBudgetMs'] };
  for (const [section, keys] of Object.entries(sections)) {
    for (const key of keys) invalid(() => normalizeOptions(options({ protocol: { [section]: { [key]: null } } })));
  }
  invalid(() => normalizeOptions(options({ redis: { mode: 'direct', host: 'x', port: 1, database: null } })));
});

test('attempt controls preserve/clear state, reject forgery/cross-attempt and poison second construction', () => {
  const first = createAttemptControl('{"cursor":1}');
  assert.deepEqual(first.consume(first.next()), { kind: 'next', stateCanonical: '{"cursor":1}' });
  const second = createAttemptControl('{"cursor":1}');
  assert.deepEqual(second.consume(second.next(null)), { kind: 'next', stateCanonical: 'null' });
  const third = createAttemptControl('null');
  invalid(() => third.consume({ kind: 'end' }));
  const fourth = createAttemptControl('null');
  const token = fourth.end();
  try { fourth.next(); } catch {}
  invalid(() => fourth.consume(token));
  const fifth = createAttemptControl('null');
  invalid(() => fifth.consume(createAttemptControl('null').end()));
  const sixth = createAttemptControl('null');
  invalid(() => sixth.next(new Date()));
  invalid(() => sixth.consume(sixth.end()));
});
