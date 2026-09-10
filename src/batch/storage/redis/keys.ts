import { safeName } from '../../domain/config.js';
import { QueuebitError } from '../../api/errors.js';
import type { EventKind, RunStatus } from '../../api/types.js';
import { RUN_STATUSES } from '../../domain/operator.js';
import { eventIdentifier } from '../../domain/events.js';
export { RUN_STATUSES } from '../../domain/operator.js';

export function identifier(value: unknown): string {
  if (typeof value !== 'string' || !/^[a-f0-9]{32}$/.test(value)) throw new QueuebitError('CONFIG_INVALID', 'Expected a 128-bit lowercase hexadecimal identifier');
  return value;
}
function definitionId(value: string): string {
  if (!/^[a-f0-9]{64}$/.test(value)) throw new QueuebitError('CONFIG_INVALID', 'Invalid definition identity');
  return value;
}

/** Only this module constructs persistent keys. Every Lua key is passed explicitly in KEYS. */
export class BatchKeys {
  readonly prefix: string;
  constructor(namespace: string) { this.prefix = `qb:batch:v1:{${safeName(namespace, 'namespace')}}:`; }
  get meta(): string { return this.prefix + 'meta'; }
  get capacity(): string { return this.prefix + 'capacity'; }
  get definitions(): string { return this.prefix + 'idx:definitions'; }
  get gcDefinitions(): string { return this.prefix + 'gc:definitions'; }
  get leases(): string { return this.prefix + 'expiry:leases'; }
  get members(): string { return this.prefix + 'expiry:members'; }
  get gcRuns(): string { return this.prefix + 'gc:runs'; }
  get gcEvents(): string { return this.prefix + 'gc:events'; }
  get dueEvents(): string { return this.prefix + 'due:event'; }
  get dueReplays(): string { return this.prefix + 'due:replay'; }
  deadLetters(taskName?: string): string { return this.prefix + 'idx:dead' + (taskName === undefined ? '' : `:task:${safeName(taskName, 'taskName')}`); }
  eventById(value: string): string { return this.prefix + `event:${eventIdentifier(value).eventId}`; }
  definition(id: string): string { return this.prefix + `definition:${definitionId(id)}`; }
  run(id: string): string { return this.prefix + `run:${identifier(id)}`; }
  runtime(id: string): string { return this.prefix + `runtime:${identifier(id)}`; }
  due(id: string): string { return this.prefix + `due:run:${definitionId(id)}`; }
  blocked(id: string): string { return this.prefix + `blocked:${definitionId(id)}`; }
  definitionMembers(id: string): string { return this.prefix + `members:${definitionId(id)}`; }
  events(id: string): string { return this.prefix + `events:${identifier(id)}`; }
  eventLock(id: string): string { return this.prefix + `event-lock:${identifier(id)}`; }
  event(id: string, page: number, kind: EventKind): string {
    if (!Number.isSafeInteger(page) || page < 1 || !['batchSettled', 'success', 'failure'].includes(kind)) throw new QueuebitError('CONFIG_INVALID', 'Invalid event identity');
    return this.prefix + `event:${identifier(id)}:${page}:${kind}`;
  }
  idempotency(taskName: string, encoded: string): string {
    if (!/^[A-Za-z0-9_-]*$/.test(encoded) || encoded.length > 342) throw new QueuebitError('CONFIG_INVALID', 'Invalid encoded business key');
    return this.prefix + `idem:${safeName(taskName, 'taskName')}:${encoded}`;
  }
  runs(taskName?: string, status?: RunStatus): string {
    if (status !== undefined && !RUN_STATUSES.includes(status)) throw new QueuebitError('CONFIG_INVALID', 'Invalid status');
    return this.prefix + 'idx:runs' + (taskName === undefined ? '' : `:task:${safeName(taskName, 'taskName')}`)
      + (status === undefined ? '' : `:status:${status}`);
  }
}
