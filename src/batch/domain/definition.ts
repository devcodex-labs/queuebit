import { QueuebitError } from '../api/errors.js';
import type { EventKind, PolicyInput, QueueMode, TaskHandlers } from '../api/types.js';
import { canonicalJson, digest, frozenJson } from './json.js';
import { configError, objectFields, policyInput, safeName } from './config.js';

const HANDLERS = { batchSettled: 'onBatchSettled', success: 'onSuccess', failure: 'onFailure' } as const;
export interface Definition<Q, S> {
  readonly name: string; readonly version: string; readonly events: readonly EventKind[];
  readonly policy?: PolicyInput; readonly canonical: string; readonly identity: string;
  readonly handlers?: TaskHandlers<Q, S>;
}

/** Definition identity includes explicit policy only; worker defaults and function identity are local. */
export function normalizeDefinition<Q, S>(input: unknown, handlers: unknown, mode: QueueMode): Definition<Q, S> {
  const fields = objectFields(input, ['name', 'version', 'events', 'policy'], 'definition');
  const name = safeName(fields.name, 'task.name');
  const version = safeName(fields.version, 'task.version');
  const events: unknown = JSON.parse(canonicalJson(fields.events, 8192));
  if (!Array.isArray(events) || events.some(event => typeof event !== 'string' || !Object.hasOwn(HANDLERS, event)) || new Set(events).size !== events.length) configError('events must be a unique list of batchSettled/success/failure');
  const declaration = { name, version, events: (events as EventKind[]).sort(),
    ...(fields.policy === undefined ? {} : { policy: policyInput(fields.policy) }) };
  const canonical = canonicalJson(declaration, 8192);
  const frozen = frozenJson<typeof declaration>(canonical);
  const base = { ...frozen, canonical, identity: digest(canonical) };
  if (mode === 'producer') {
    if (handlers !== undefined) throw new QueuebitError('MODE_OPERATION_NOT_ALLOWED', 'Producer definitions must not provide handlers');
    return Object.freeze(base) as Definition<Q, S>;
  }
  const required = ['execute', ...declaration.events.map(event => HANDLERS[event])];
  const callbacks = objectFields(handlers, required, 'handlers');
  if (required.some(key => typeof callbacks[key] !== 'function')) configError('Every declared handler must be a function');
  return Object.freeze({ ...base, handlers: Object.freeze(callbacks) as unknown as TaskHandlers<Q, S> }) as Definition<Q, S>;
}
