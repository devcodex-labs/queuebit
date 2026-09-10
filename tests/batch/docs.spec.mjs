import assert from 'node:assert/strict';
import { test } from 'node:test';
import { copyFile, readFile, readdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { withRedis } from './helpers.mjs';
import { repo, installCandidate, command } from './package-harness.mjs';

test('docs: bilingual new API parity, actual example fresh compilation and post-write-failure replay against Redis', { timeout: 240000 }, async () => {
  const pages = await Promise.all(['en', 'zh'].map(lang => readFile(resolve(repo, `docs/v01/${lang}/batch-v2.md`), 'utf8')));
  for (const page of pages) {
    assert.deepEqual([...page.matchAll(/^## (\d+)\./gm)].map(match => Number(match[1])), [1,2,3,4,5,6,7,8,9,10]);
    for (const required of ['createBatchQueue', 'QueuebitError', 'OUTCOME_UNKNOWN', 'replayDrainDeadline', 'nodeAuth', 'sentinelAuth', '512MiB']) assert(page.includes(required), required);
    assert(!/CP[123]|DevCodex|MCP|@rocky|createQueuebitClient/.test(page), 'Internal workflow or old recommended API leaked into user docs');
  }
  const english = (await readdir(resolve(repo, 'docs/v01/en'))).filter(name => name.endsWith('.md')).sort();
  const chinese = (await readdir(resolve(repo, 'docs/v01/zh'))).filter(name => name.endsWith('.md')).sort();
  assert.equal(english.length, 24); assert.deepEqual(chinese, english);
  for (const lang of ['en', 'zh']) for (const name of english) {
    const page = await readFile(resolve(repo, 'docs/v01', lang, name), 'utf8');
    assert(!/createQueuebitClient|defineQueuebitConfig|jobs\\.add|createQueuebitWorker|CP[123]|DevCodex|@rocky/.test(page), `${lang}/${name}: stale API or internal workflow`);
  }
  const candidate = await installCandidate();
  assert.equal(candidate.source, 'actual-root-npm-pack');
  const reference = await readFile(resolve(repo, 'docs/v01/en/target-api.md'), 'utf8');
  const translatedReference = await readFile(resolve(repo, 'docs/v01/zh/target-api.md'), 'utf8');
  const apiCode = source => [...source.matchAll(/```ts\r?\n([\s\S]*?)```/g)].map(match => match[1]).join('\n');
  assert.equal(apiCode(reference), apiCode(translatedReference), 'Bilingual API example must have identical types');
  assert(apiCode(reference).includes('EventReplayInput'));
  await writeFile(resolve(candidate.consumer, 'manual-api.ts'), apiCode(reference));
  await command(process.execPath, [resolve(candidate.consumer, 'node_modules/typescript/bin/tsc'), '--noEmit', '--strict', '--exactOptionalPropertyTypes', '--noUncheckedIndexedAccess',
    '--target', 'ES2023', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', 'manual-api.ts'], { cwd: candidate.consumer });
  await copyFile(resolve(repo, 'examples/batch-v2/receipt-task.ts'), resolve(candidate.consumer, 'receipt-task.ts'));
  await command(process.execPath, [resolve(candidate.consumer, 'node_modules/typescript/bin/tsc'), '--strict', '--exactOptionalPropertyTypes', '--noUncheckedIndexedAccess',
    '--target', 'ES2023', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', '--outDir', 'build', 'receipt-task.ts'], { cwd: candidate.consumer });
  await withRedis(async target => {
    const driver = `
      import assert from 'node:assert/strict'; import {setTimeout as delay} from 'node:timers/promises';
      import {createBatchQueue} from 'queuebit'; import {defineReceiptTask} from './build/receipt-task.js';
      const config=JSON.parse(process.env.QUEUEBIT_CONSUMER_CONFIG);
      const queue=createBatchQueue({...config,defaults:{attempts:3,backoff:{baseMs:1,maxMs:1}},protocol:{lease:{pollMs:10}}});
      const rows=Object.freeze(Array.from({length:203},(_,i)=>Object.freeze({id:i+1,payload:{id:i+1}})));
      const written=new Map(), completed=new Set(); let injected=false,calls=0,pages=0;
      const task=defineReceiptTask(queue,{async readPage(snapshot,after,limit,signal){signal.throwIfAborted();assert.equal(snapshot,'fixed-snapshot');assert.equal(limit,100);pages++;return rows.filter(r=>r.id>after).slice(0,limit);}},
        {async putOnce(key,payload,signal){signal.throwIfAborted();calls++;if(!written.has(key))written.set(key,payload);if(!injected){injected=true;throw Error('Applied externally but reply lost');}},
         async completeOnce(eventId,snapshot,signal){signal.throwIfAborted();assert.equal(snapshot,'fixed-snapshot');completed.add(eventId);}});
      try {await queue.ready();const {runId}=await task.start({query:{snapshotId:'fixed-snapshot'},idempotencyKey:'one-snapshot'});
        let run;const deadline=Date.now()+15000;do{run=await task.get(runId);if(run?.callbacks.delivered===1)break;await delay(20);}while(Date.now()<deadline);
        assert.equal(run.status,'success');assert.equal(run.businessFailures,1);assert.equal(written.size,203);assert.equal(calls,204);assert.equal(pages,5);assert.equal(completed.size,1);assert.equal(run.callbacks.delivered,1);
      }finally{const closed=await queue.close();assert.equal(closed.remainingExecutions+closed.remainingCallbacks,0);}
      console.log(JSON.stringify({status:'PASS',records:written.size,calls,pages,completed:completed.size,node:process.version}));`;
    const output = await command(process.execPath, ['--input-type=module', '-e', driver], { cwd: candidate.consumer,
      env: { QUEUEBIT_CONSUMER_CONFIG: JSON.stringify({ namespace: target.namespace, redis: target.redis }) }, timeout: 30000 });
    await writeFile(resolve(candidate.directory, 'example-evidence.json'), JSON.stringify({ output, source: 'examples/batch-v2/receipt-task.ts',
      publicApiSource: 'docs/v01/en/target-api.md + zh/target-api.md', publicApiCompilationExitCode: 0, sha256: candidate.sha256, node: process.version }, null, 2));
  });
});
