import type { BatchControl, BatchTask, CallbackContext, CancelResult, ExecuteContext, RedisOptions } from '../../src/batch/api/types.js';
import { createBatchQueue } from '../../src/batch/index.js';
import type { RunControlInput, RunControlResult, RunListInput, RunMetadata } from '../../src/batch/api/types.js';
import type { EventReplayInput, EventReplayResult, DeadLetterMetadata } from '../../src/batch/api/types.js';

const replayInput: EventReplayInput = { eventId: 'event', expectedRevision: 1, reason: 'retry', commandId: 'id' };
// @ts-expect-error Event replay cannot carry the Run target variant
const replayRun: EventReplayInput = { ...replayInput, runId: 'run' };
// @ts-expect-error applied replay requires eventId and replayGeneration in addition to the base control result
const incompleteReplay: EventReplayResult = { kind: 'applied', id: 'event', revision: 2, status: 'pending', changed: true };
// @ts-expect-error not_found cannot carry any success revision
const absentReplay: EventReplayResult = { kind: 'not_found', id: 'event', revision: 1 };
declare const deadLetter: DeadLetterMetadata;
// @ts-expect-error metadata does not expose callback snapshots
void deadLetter.state;
void [replayInput, replayRun, incompleteReplay, absentReplay];

const controlInput: RunControlInput = { runId: 'a', expectedRevision: 1, commandId: 'command', reason: 'pause' };
// @ts-expect-error Run control cannot carry Event variant fields
const mixedControl: RunControlInput = { ...controlInput, eventId: 'event' };
// @ts-expect-error operator control always requires CAS
const missingRevision: RunControlInput = { runId: 'a', commandId: 'command', reason: 'pause' };
// @ts-expect-error not_found never carries revision
const mixedResult: RunControlResult = { kind: 'not_found', id: 'a', revision: 1 };
// @ts-expect-error applied requires revision/status/changed
const missingResult: RunControlResult = { kind: 'applied', id: 'a' };
// @ts-expect-error filter is a single status
const multipleStatus: RunListInput = { status: ['pending', 'paused'] };
declare const metadata: RunMetadata;
// @ts-expect-error metadata never contains query payload
void metadata.query;
void [controlInput, mixedControl, missingRevision, mixedResult, missingResult, multipleStatus];

interface Query { after: number; filter: { state: string } }
interface State { cursor: number }
const queue = createBatchQueue({ namespace: 'typed', redis: { mode: 'direct', host: '127.0.0.1', port: 6379 } });
void queue.operator.runs.pause(controlInput);
void queue.operator.runs.list({ status: 'paused', limit: 5 });
void queue.operator.deadLetters.get('event');
void queue.operator.deadLetters.list({ taskName: 'typed', limit: 5 });
void queue.operator.deadLetters.replay(replayInput).then(result => {
  if (result.kind === 'applied') { const generation: number = result.replayGeneration; const id: string = result.eventId; void [generation, id]; }
});
// @ts-expect-error replay CAS is mandatory
void queue.operator.deadLetters.replay({ eventId: 'event', reason: 'again', commandId: 'command' });
// @ts-expect-error dead-letter lists cannot filter Run statuses
void queue.operator.deadLetters.list({ status: 'failed' });
void queue.operator.health.snapshot().then(value => { const origin: 'redis' | 'local' = value.origin; void origin; });
void queue.operator.capacity.snapshot().then(value => { const runs: number = value.counts.runs; void runs; });
const metrics = queue.operator.metrics.snapshot();
// @ts-expect-error local snapshots are readonly
metrics.counters.run_created = 1;
// @ts-expect-error observations cannot accept a caller-supplied namespace or arbitrary filter
void queue.operator.capacity.snapshot({ namespace: 'other' });
// @ts-expect-error local metrics never masquerade as Redis-wide origin
const globalMetrics: 'redis' = metrics.origin;
void globalMetrics;
// @ts-expect-error public operator cannot omit expectedRevision
void queue.operator.runs.cancel({ runId: 'a', commandId: 'command', reason: 'cancel' });
// @ts-expect-error no public get payload alias in operator
void queue.operator.runs.get('a');
const publicTask = queue.define<Query, State>({ name: 'typed', version: '1', events: [] }, {
  execute(ctx) {
    const after: number = ctx.query.after;
    // @ts-expect-error state shape must match the second explicit generic
    ctx.next({ after });
    return ctx.next({ cursor: after });
  }
});
const typedTask: BatchTask<Query, State> = publicTask;
void typedTask;
// @ts-expect-error define exposes exactly two type parameters, not three
queue.define<Query, State, string>({ name: 'extra', version: '1', events: [] }, { execute: ctx => ctx.end() });
declare const task: BatchTask<Query, State>;
declare const context: ExecuteContext<Query, State>;
declare const callback: CallbackContext<Query, State>;
void task.start({ query: { after: 0, filter: { state: 'ready' } } });
context.next({ cursor: 3 });
context.next(null);
context.next();
context.end();
// @ts-expect-error query nested snapshot is readonly
context.query.filter.state = 'changed';
// @ts-expect-error state contract is not the query contract
context.next({ after: 2 });
// @ts-expect-error callback snapshot is readonly
callback.state!.cursor = 4;
// @ts-expect-error mandatory query field
void task.start({ query: { after: 0 } });
// @ts-expect-error opaque control cannot be forged
const forged: BatchControl = {};
// @ts-expect-error sibling variant field
const redis: RedisOptions = { mode: 'url', url: 'redis://localhost', host: 'localhost' };
// @ts-expect-error not-found variant cannot carry a revision
const cancel: CancelResult = { found: false, runId: 'a', revision: 1 };
// @ts-expect-error found variant requires terminal status/revision/changed
const incomplete: CancelResult = { found: true, runId: 'a' };
void [forged, redis, cancel, incomplete];
