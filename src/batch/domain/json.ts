import { createHash } from 'node:crypto';
import { QueuebitError } from '../api/errors.js';
import type { DeepReadonly, ErrorEnvelope } from '../api/types.js';

export const JSON_LIMITS = Object.freeze({ query: 256 * 1024, state: 64 * 1024, error: 32 * 1024 });

/** Descriptor-based canonical encoding does not invoke getters or toJSON. Root depth is zero. */
export function canonicalJson(value: unknown, maxBytes = JSON_LIMITS.query): string {
  let nodes = 0;
  let bytes = 0;
  const ancestors = new Set<object>();
  const chunks: string[] = [];
  const invalid = (): never => { throw new QueuebitError('JSON_INVALID', 'Expected bounded strict JSON data'); };
  const emit = (chunk: string): void => {
    bytes += Buffer.byteLength(chunk, 'utf8');
    if (bytes > maxBytes) throw new QueuebitError('PAYLOAD_TOO_LARGE', 'Canonical JSON exceeds the byte limit');
    chunks.push(chunk);
  };
  const visit = (item: unknown, depth: number): void => {
    if (++nodes > 10000 || depth > 64) invalid();
    if (item === null || typeof item === 'boolean' || typeof item === 'string') { emit(JSON.stringify(item)); return; }
    if (typeof item === 'number') {
      if (!Number.isFinite(item)) invalid();
      emit(JSON.stringify(item)); return;
    }
    if (typeof item !== 'object') invalid();
    const object = item as object;
    if (ancestors.has(object)) invalid();
    const array = Array.isArray(object);
    const proto: unknown = Object.getPrototypeOf(object);
    if (array ? proto !== Array.prototype : proto !== Object.prototype && proto !== null) invalid();
    const descriptors = Object.getOwnPropertyDescriptors(object);
    const keys = Reflect.ownKeys(descriptors);
    if (keys.some(key => typeof key !== 'string')) invalid();
    for (const key of keys) {
      const descriptor = descriptors[key as string];
      if (!descriptor || !Object.hasOwn(descriptor, 'value')) invalid();
    }
    ancestors.add(object);
    if (array) {
      const length = descriptors.length?.value as number;
      if (!Number.isSafeInteger(length) || length < 0 || length > 10000 || keys.length !== length + 1) invalid();
      emit('[');
      for (let i = 0; i < length; i++) {
        if (!Object.hasOwn(descriptors, String(i))) invalid();
        if (i) emit(',');
        visit(descriptors[String(i)]!.value, depth + 1);
      }
      emit(']');
    } else {
      emit('{');
      const names = (keys as string[]).sort();
      names.forEach((key, index) => {
        if (index) emit(',');
        emit(JSON.stringify(key)); emit(':');
        visit(descriptors[key]!.value, depth + 1);
      });
      emit('}');
    }
    ancestors.delete(object);
  };
  try { visit(value, 0); } catch (error) {
    if (error instanceof QueuebitError) throw error;
    invalid();
  }
  return chunks.join('');
}

/** Parse into independent ordinary JSON objects, then freeze every nested value. */
export function frozenJson<T>(canonical: string): DeepReadonly<T> {
  const value: unknown = JSON.parse(canonical);
  const freeze = (item: unknown): void => {
    if (item !== null && typeof item === 'object') {
      for (const child of Object.values(item)) freeze(child);
      Object.freeze(item);
    }
  };
  freeze(value);
  return value as DeepReadonly<T>;
}

export function digest(canonical: string): string {
  return createHash('sha256').update(canonical, 'utf8').digest('hex');
}

/** Raw business-key encoding deliberately differs from escaped JSON string encoding. */
export function encodeIdempotencyKey(key: unknown): string {
  if (typeof key !== 'string' || Buffer.byteLength(key, 'utf8') > 256) {
    throw new QueuebitError('CONFIG_INVALID', 'idempotencyKey must be at most 256 UTF-8 bytes');
  }
  for (let i = 0; i < key.length; i++) {
    const code = key.charCodeAt(i);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = key.charCodeAt(++i);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new QueuebitError('CONFIG_INVALID', 'Unpaired surrogate in idempotencyKey');
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw new QueuebitError('CONFIG_INVALID', 'Unpaired surrogate in idempotencyKey');
    }
  }
  return Buffer.from(key, 'utf8').toString('base64url');
}

/** Capture only own data fields of thrown values; untrusted formatting hooks are never used. */
export function errorEnvelope(error: unknown): ErrorEnvelope {
  const fallback: ErrorEnvelope = { code: 'BUSINESS_ERROR', name: 'Error', message: 'Unrecognized thrown value', truncated: false };
  try {
    if (!error || typeof error !== 'object') return Object.freeze(fallback);
    const descriptors = Object.getOwnPropertyDescriptors(error);
    const read = (name: string): string | undefined => {
      const descriptor = descriptors[name];
      return descriptor && Object.hasOwn(descriptor, 'value') && typeof descriptor.value === 'string' ? descriptor.value : undefined;
    };
    const code = read('code');
    const name = read('name');
    let message = read('message') ?? fallback.message;
    let stack = read('stack');
    let truncated = false;
    // Header fields must not consume the payload budget before message/stack trimming.
    const safeCode = code && Buffer.byteLength(code) <= 256 ? code : fallback.code;
    const safeName = name && Buffer.byteLength(name) <= 256 ? name : fallback.name;
    if ((code !== undefined && code !== safeCode) || (name !== undefined && name !== safeName)) truncated = true;
    for (;;) {
      const result: ErrorEnvelope = { code: safeCode, name: safeName, message, truncated, ...(stack === undefined ? {} : { stack }) };
      try { canonicalJson(result, JSON_LIMITS.error); return Object.freeze(result); }
      catch (failure) { if (!(failure instanceof QueuebitError) || failure.code !== 'PAYLOAD_TOO_LARGE') return Object.freeze(fallback); }
      truncated = true;
      if (stack && stack.length >= message.length) stack = stack.slice(0, Math.floor(stack.length / 2));
      else if (message.length) message = message.slice(0, Math.floor(message.length / 2));
      else return Object.freeze({ ...fallback, truncated: true });
    }
  } catch { return Object.freeze(fallback); }
}
