import { QueuebitError } from '../api/errors.js';
import type { BatchQueueOptions, CapacityLimits, ExecutionPolicy, PolicyInput, Protocol, QueueMode, RedisOptions, TlsOptions } from '../api/types.js';
import { canonicalJson, frozenJson } from './json.js';

const DAY = 86400000;
const KiB = 1024;
export const SLOTS = Object.freeze({ member: 96 * KiB, definition: 16 * KiB, run: 96 * KiB,
  event: 128 * KiB, claim: 384 * KiB });
const LIMITS: CapacityLimits = { nonterminalRunMax: 10000, runMax: 20000, objectMax: 100000,
  unfinishedEventMax: 20000, definitionMax: 10000, memberMax: 1000, totalBytes: 512 * 1024 * KiB };
const DEFAULT_POLICY: ExecutionPolicy = { attempts: 3, timeoutMs: 30000, backoff: { baseMs: 1000, maxMs: 30000, jitter: 'full' } };

export interface NormalizedOptions {
  readonly namespace: string; readonly redis: RedisOptions;
  readonly runtime: { readonly mode: QueueMode; readonly concurrency: number; readonly callbackConcurrency: number; readonly closeGraceMs: number };
  readonly defaults: ExecutionPolicy; readonly protocol: Protocol; readonly protocolCanonical: string;
  readonly telemetry?: BatchQueueOptions['telemetry'];
}

export function configError(message: string): never { throw new QueuebitError('CONFIG_INVALID', message); }

/** Read validated own data properties only, including explicit undefined and unknown-key rejection. */
export function objectFields(value: unknown, allowed: readonly string[], label: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object') configError(`${label} must be a plain object`);
  const proto: unknown = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) configError(`${label} must be a plain object`);
  const result: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
  const descriptors = Object.getOwnPropertyDescriptors(value);
  for (const key of Reflect.ownKeys(descriptors)) {
    if (typeof key !== 'string' || !allowed.includes(key)) configError(`Unknown ${label} field`);
    const descriptor = descriptors[key];
    if (!descriptor || !Object.hasOwn(descriptor, 'value') || descriptor.value === undefined || descriptor.value === null) configError(`${label}.${key} must be a non-null data value`);
    result[key] = descriptor.value;
  }
  return result;
}

function optionalObject(value: unknown, allowed: readonly string[], label: string): Record<string, unknown> {
  return value === undefined ? {} : objectFields(value, allowed, label);
}
export function integer(value: unknown, min: number, max: number, label: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max) configError(`${label} must be an integer in ${min}..${max}`);
  return value;
}
function string(value: unknown, label: string, maxBytes = 65536, empty = false): string {
  if (typeof value !== 'string' || (!empty && !value.length) || Buffer.byteLength(value) > maxBytes) configError(`Invalid ${label}`);
  return value;
}
export function safeName(value: unknown, label: string): string {
  const name = string(value, label, 128);
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(name)) configError(`${label} must use safe ASCII identifier characters`);
  return name;
}

export function policyInput(value: unknown): PolicyInput {
  const fields = objectFields(value, ['attempts', 'timeoutMs', 'backoff'], 'policy');
  const result: PolicyInput = {};
  if (fields.attempts !== undefined) result.attempts = integer(fields.attempts, 1, 1000, 'attempts');
  if (fields.timeoutMs !== undefined) result.timeoutMs = integer(fields.timeoutMs, 1, DAY, 'timeoutMs');
  if (fields.backoff !== undefined) {
    const backoff = objectFields(fields.backoff, ['baseMs', 'maxMs', 'jitter'], 'backoff');
    result.backoff = {};
    if (backoff.baseMs !== undefined) result.backoff.baseMs = integer(backoff.baseMs, 1, 3600000, 'baseMs');
    if (backoff.maxMs !== undefined) result.backoff.maxMs = integer(backoff.maxMs, 1, DAY, 'maxMs');
    if (backoff.jitter !== undefined) {
      if (backoff.jitter !== 'full') configError('backoff.jitter must be full');
      result.backoff.jitter = 'full';
    }
  }
  return result;
}

/** Resolve nested policy fields at the consumer boundary, not when an incomplete override is stored. */
export function mergePolicy(base: ExecutionPolicy = DEFAULT_POLICY, input?: unknown): ExecutionPolicy {
  const partial = input === undefined ? {} : policyInput(input);
  const result = { ...base, ...partial, backoff: { ...base.backoff, ...partial.backoff } };
  if (result.backoff.maxMs < result.backoff.baseMs) configError('backoff.maxMs must not be below baseMs');
  Object.freeze(result.backoff);
  return Object.freeze(result);
}

/** Saturate before exponentiation. The caller freezes this draw across transport retries. */
export function backoffDelay(policy: ExecutionPolicy, failures: number, random: number): number {
  integer(failures, 1, Number.MAX_SAFE_INTEGER, 'failures');
  if (!Number.isFinite(random) || random < 0 || random >= 1) configError('random must be in [0,1)');
  const { baseMs, maxMs } = policy.backoff;
  const exponent = Math.min(failures - 1, Math.ceil(Math.log2(maxMs / baseMs)));
  const ceiling = Math.min(maxMs, baseMs * 2 ** exponent);
  return Math.floor(random * (ceiling + 1));
}

function tls(input: unknown): TlsOptions {
  const fields = objectFields(input, ['ca', 'cert', 'key', 'servername'], 'tls');
  const result: TlsOptions = {};
  for (const key of ['ca', 'cert', 'key', 'servername'] as const) {
    if (fields[key] !== undefined) result[key] = string(fields[key], `tls.${key}`);
  }
  return result;
}
function auth(input: unknown): { username?: string; password?: string } {
  const fields = objectFields(input, ['username', 'password'], 'auth');
  const result: { username?: string; password?: string } = {};
  if (fields.username !== undefined) result.username = string(fields.username, 'username', 65536, true);
  if (fields.password !== undefined) result.password = string(fields.password, 'password', 65536, true);
  return result;
}
function address(input: unknown): { host: string; port: number } {
  const fields = objectFields(input, ['host', 'port'], 'address');
  return { host: string(fields.host, 'host', 1024), port: integer(fields.port, 1, 65535, 'port') };
}

function redisOptions(input: unknown): RedisOptions {
  const initial = objectFields(input, ['mode', 'url', 'tls', 'host', 'port', 'username', 'password',
    'database', 'name', 'seeds', 'nodeAuth', 'sentinelAuth', 'nodeTls', 'sentinelTls', 'addressMap'], 'redis');
  if (initial.mode === 'url') {
    const fields = objectFields(input, ['mode', 'url', 'tls'], 'redis.url');
    const raw = string(fields.url, 'redis.url');
    let parsed: URL;
    try { parsed = new URL(raw); } catch { return configError('Invalid Redis URL'); }
    if (!['redis:', 'rediss:'].includes(parsed.protocol) || !parsed.hostname || parsed.search || parsed.hash) configError('Invalid Redis URL');
    if (parsed.port) integer(Number(parsed.port), 1, 65535, 'port');
    if (parsed.pathname && parsed.pathname !== '/') {
      if (!/^\/\d+$/.test(parsed.pathname)) configError('Invalid Redis URL database');
      integer(Number(parsed.pathname.slice(1)), 0, Number.MAX_SAFE_INTEGER, 'database');
    }
    try { decodeURIComponent(parsed.username); decodeURIComponent(parsed.password); } catch { configError('Invalid Redis URL credentials'); }
    if (fields.tls !== undefined && parsed.protocol !== 'rediss:') configError('tls requires rediss URL');
    return { mode: 'url', url: raw, ...(fields.tls === undefined ? {} : { tls: tls(fields.tls) }) };
  }
  if (initial.mode === 'direct') {
    const fields = objectFields(input, ['mode', 'host', 'port', 'username', 'password', 'database', 'tls'], 'redis.direct');
    return { mode: 'direct', host: string(fields.host, 'host', 1024), port: integer(fields.port, 1, 65535, 'port'),
      database: integer(fields.database ?? 0, 0, Number.MAX_SAFE_INTEGER, 'database'),
      ...(fields.username === undefined ? {} : { username: string(fields.username, 'username', 65536, true) }),
      ...(fields.password === undefined ? {} : { password: string(fields.password, 'password', 65536, true) }),
      ...(fields.tls === undefined ? {} : { tls: tls(fields.tls) }) };
  }
  if (initial.mode === 'sentinel') {
    const fields = objectFields(input, ['mode', 'name', 'seeds', 'nodeAuth', 'sentinelAuth', 'nodeTls', 'sentinelTls', 'database', 'addressMap'], 'redis.sentinel');
    // The strict snapshot also rejects accessor and sparse seed arrays without executing hooks.
    const seeds: unknown = JSON.parse(canonicalJson(fields.seeds));
    if (!Array.isArray(seeds) || seeds.length < 1 || seeds.length > 32) configError('seeds must contain 1..32 addresses');
    const result: Extract<RedisOptions, { mode: 'sentinel' }> = { mode: 'sentinel', name: string(fields.name, 'sentinel.name', 128),
      seeds: seeds.map(address), database: integer(fields.database ?? 0, 0, Number.MAX_SAFE_INTEGER, 'database') };
    if (fields.nodeAuth !== undefined) result.nodeAuth = auth(fields.nodeAuth);
    if (fields.sentinelAuth !== undefined) result.sentinelAuth = auth(fields.sentinelAuth);
    if (fields.nodeTls !== undefined) result.nodeTls = tls(fields.nodeTls);
    if (fields.sentinelTls !== undefined) result.sentinelTls = tls(fields.sentinelTls);
    if (fields.addressMap !== undefined) {
      const snapshot: unknown = JSON.parse(canonicalJson(fields.addressMap));
      if (!snapshot || typeof snapshot !== 'object' || Array.isArray(snapshot)) configError('addressMap must be an object');
      const entries = Object.entries(snapshot);
      if (entries.length > 256) configError('addressMap exceeds 256 entries');
      result.addressMap = Object.fromEntries(entries.map(([key, value]) => [string(key, 'addressMap key', 1024), address(value)]));
    }
    return result;
  }
  return configError('redis.mode must be url, direct or sentinel');
}

function protocolOptions(input: unknown): Protocol {
  const fields = optionalObject(input, ['callback', 'lease', 'retention', 'limits', 'maintenance'], 'protocol');
  const lease = optionalObject(fields.lease, ['leaseMs', 'heartbeatMs', 'pollMs', 'recoveryLimit'], 'lease');
  const leaseMs = integer(lease.leaseMs ?? 30000, 3000, DAY, 'leaseMs');
  const retention = optionalObject(fields.retention, ['runMs', 'deliveredEventMs', 'deadLetterMs'], 'retention');
  const limitsInput = optionalObject(fields.limits, Object.keys(LIMITS), 'limits');
  const limits = { ...LIMITS };
  for (const key of Object.keys(LIMITS) as (keyof CapacityLimits)[]) {
    limits[key] = integer(limitsInput[key] ?? LIMITS[key], key === 'memberMax' ? 1 : key === 'objectMax' ? 3 : 2, LIMITS[key], key);
    if (key !== 'memberMax' && key !== 'totalBytes' && Math.floor(limits[key] * 0.8) >= Math.floor(limits[key] * 0.9)) configError(`Indistinguishable high/low thresholds for ${key}`);
  }
  if (limits.nonterminalRunMax > limits.runMax || limits.runMax > limits.objectMax) configError('Run/object limits are inconsistent');
  const minimum = limits.memberMax * SLOTS.member + SLOTS.definition + SLOTS.run + 256 * KiB + 8 + SLOTS.claim;
  if (limits.totalBytes < minimum) configError('totalBytes cannot fund members and a maximum initial Run claim');
  const maintenance = optionalObject(fields.maintenance, ['batchSize', 'maxBatchesPerTick', 'timeBudgetMs'], 'maintenance');
  const result: Protocol = {
    callback: mergePolicy({ ...DEFAULT_POLICY, attempts: 10, backoff: { ...DEFAULT_POLICY.backoff, maxMs: 60000 } }, fields.callback),
    lease: { leaseMs, heartbeatMs: integer(lease.heartbeatMs ?? 10000, 1000, Math.floor(leaseMs / 3), 'heartbeatMs'),
      pollMs: integer(lease.pollMs ?? 1000, 1, 1000, 'pollMs'), recoveryLimit: integer(lease.recoveryLimit ?? 20, 1, 1000, 'recoveryLimit') },
    retention: { runMs: integer(retention.runMs ?? 7 * DAY, 7 * DAY, 365 * DAY, 'runMs'),
      deliveredEventMs: integer(retention.deliveredEventMs ?? 7 * DAY, 7 * DAY, 365 * DAY, 'deliveredEventMs'),
      deadLetterMs: integer(retention.deadLetterMs ?? 30 * DAY, 30 * DAY, 30 * DAY, 'deadLetterMs') },
    limits,
    maintenance: { batchSize: integer(maintenance.batchSize ?? 100, 1, 100, 'batchSize'),
      maxBatchesPerTick: integer(maintenance.maxBatchesPerTick ?? 10, 1, 10, 'maxBatchesPerTick'),
      timeBudgetMs: integer(maintenance.timeBudgetMs ?? 50, 1, 50, 'timeBudgetMs') }
  };
  return frozenJson<Protocol>(canonicalJson(result)) as Protocol;
}

/** Pure synchronous configuration normalization. Does not inspect env, package identity or the network. */
export function normalizeOptions(input: unknown): NormalizedOptions {
  try {
    const fields = objectFields(input, ['namespace', 'redis', 'runtime', 'defaults', 'protocol', 'telemetry'], 'queue');
    const runtime = optionalObject(fields.runtime, ['mode', 'concurrency', 'callbackConcurrency', 'closeGraceMs'], 'runtime');
    const mode = runtime.mode ?? 'all';
    if (mode !== 'all' && mode !== 'producer' && mode !== 'consumer') configError('Invalid runtime.mode');
    const protocol = protocolOptions(fields.protocol);
    const result: NormalizedOptions = { namespace: safeName(fields.namespace, 'namespace'),
      redis: frozenJson<RedisOptions>(canonicalJson(redisOptions(fields.redis))) as RedisOptions,
      runtime: Object.freeze({ mode, concurrency: integer(runtime.concurrency ?? 4, 1, 1024, 'concurrency'),
        callbackConcurrency: integer(runtime.callbackConcurrency ?? 4, 1, 1024, 'callbackConcurrency'),
        closeGraceMs: integer(runtime.closeGraceMs ?? 30000, 0, DAY, 'closeGraceMs') }),
      defaults: mergePolicy(DEFAULT_POLICY, fields.defaults), protocol, protocolCanonical: canonicalJson(protocol),
      ...(fields.telemetry === undefined ? {} : { telemetry: normalizeTelemetry(fields.telemetry) }) };
    return Object.freeze(result);
  } catch (error) {
    if (error instanceof QueuebitError && error.code === 'CONFIG_INVALID') throw error;
    return configError('Invalid queue configuration');
  }
}
function normalizeTelemetry(input: unknown): NonNullable<BatchQueueOptions['telemetry']> {
  const fields = objectFields(input, ['sink'], 'telemetry');
  if (typeof fields.sink !== 'function') configError('telemetry.sink must be a function');
  return Object.freeze({ sink: fields.sink as NonNullable<BatchQueueOptions['telemetry']>['sink'] });
}
