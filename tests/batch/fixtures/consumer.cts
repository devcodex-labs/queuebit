import queuebit = require('queuebit');
const queue = queuebit.createBatchQueue({ namespace: 'cjs-types', redis: { mode: 'url', url: 'redis://localhost:6379' } });
const task = queue.define<null, number>({ name: 'cjs', version: '1', events: [] }, { execute(ctx) { return ctx.end(); } });
// @ts-expect-error result discriminants cannot be mixed
const invalid: queuebit.EventReplayResult = { kind: 'not_found', id: 'none', revision: 1 };
// @ts-expect-error invalid query
void task.start({ query: 'wrong' });
// @ts-expect-error legacy worker is no longer exported
queuebit.createQueuebitWorker;
void invalid;
