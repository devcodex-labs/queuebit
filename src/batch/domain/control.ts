import { QueuebitError } from '../api/errors.js';
import type { BatchControl } from '../api/types.js';
import { canonicalJson, JSON_LIMITS } from './json.js';

export interface ControlIntent { kind: 'next' | 'end'; stateCanonical: string }

/** Tokens are recognized by object identity inside exactly one invocation, never by their shape. */
export function createAttemptControl<S>(currentState: string): {
  next: (state?: S | null) => BatchControl; end: () => BatchControl;
  consume: (value: unknown) => ControlIntent; readonly poisoned: boolean;
} {
  let constructed = false;
  let poisoned = false;
  let consumed = false;
  let token: BatchControl | undefined;
  let intent: ControlIntent | undefined;
  const invalid = (): never => { poisoned = true; throw new QueuebitError('HANDLER_CONTRACT_INVALID', 'Return exactly one control created by this attempt'); };
  const construct = (kind: 'next' | 'end', state?: S | null): BatchControl => {
    if (constructed || poisoned || consumed) return invalid();
    constructed = true;
    try {
      const stateCanonical = kind === 'end' || state === undefined ? currentState : canonicalJson(state, JSON_LIMITS.state);
      token = Object.freeze(Object.create(null)) as BatchControl;
      intent = { kind, stateCanonical };
      return token;
    } catch { return invalid(); }
  };
  return Object.freeze({
    next: (state?: S | null) => construct('next', state), end: () => construct('end'),
    consume(value: unknown): ControlIntent {
      if (poisoned || consumed || !constructed || value !== token || !intent) return invalid();
      consumed = true;
      return intent;
    },
    get poisoned(): boolean { return poisoned; }
  });
}
