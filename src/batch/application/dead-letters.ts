import type { DeadLettersOperator, DeadLetterListInput, EventReplayInput } from '../api/types.js';
import type { BatchRedisStore } from '../storage/redis/store.js';
import type { Telemetry } from '../runtime/telemetry.js';

/** Metadata-only administration uses the same Event CAS and parent Run receipt ring as runtime delivery. */
export function deadLettersOperator(store: BatchRedisStore, guard: () => void, telemetry: Telemetry): DeadLettersOperator {
  return Object.freeze({
    async get(eventId: string) { guard(); const result = await store.getDeadLetter(eventId); telemetry.emit('dead_letter_read'); return result; },
    async list(input?: DeadLetterListInput) { guard(); const result = await store.listDeadLetters(input); telemetry.emit('dead_letters_listed'); return result; },
    async replay(input: EventReplayInput) { guard(); const result = await store.replayEvent(input); telemetry.emit('event_replayed'); return result; }
  });
}
