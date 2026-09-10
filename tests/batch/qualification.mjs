import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readFile, readdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { repo } from './package-harness.mjs';

assert([22, 24].includes(Number(process.versions.node.split('.')[0])), 'Qualification requires Node22/24');
const scratch = resolve(repo, '../.devcodex/queuebit/.tmp'); await mkdir(scratch, { recursive: true });
const directory = await mkdtemp(resolve(scratch, 'batch-qualification-'));
const npmCli = process.env.npm_execpath ?? resolve(dirname(process.execPath), 'node_modules/npm/bin/npm-cli.js');
const evidence = { node: process.version, startedAt: new Date().toISOString(), directory, steps: [], status: 'UNVERIFIED' };
const save = () => writeFile(resolve(directory, 'qualification.json'), JSON.stringify(evidence, null, 2)); await save();
async function run(name, args, { tests, timeout = 360000 } = {}) {
  const logFile = resolve(directory, `${name}.log`), startedAt = new Date().toISOString();
  const child = spawn(process.execPath, args, { cwd: repo, windowsHide: true, env: { ...process.env, NODE_PATH: '' }, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = ''; const append = data => { output += data.toString(); }; child.stdout.on('data', append); child.stderr.on('data', append);
  const timer = setTimeout(() => child.kill(), timeout);
  const exitCode = await new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', resolve); }).finally(() => clearTimeout(timer));
  await writeFile(logFile, output); const step = { name, command: process.execPath, args, cwd: repo, pid: child.pid, startedAt,
    finishedAt: new Date().toISOString(), exitCode, logFile, sha256: createHash('sha256').update(output).digest('hex') };
  evidence.steps.push(step); await save();
  console.log(JSON.stringify(step));
  assert.equal(exitCode, 0, `${name} failed; full log ${logFile}\n${output.slice(-5000)}`);
  if (tests !== undefined) {
    assert(new RegExp(`tests ${tests}\\r?\\n`).test(output), `${name}: expected exactly ${tests} required tests`);
    for (const field of ['fail', 'cancelled', 'skipped']) assert(new RegExp(`${field} 0\\r?\\n`).test(output), `${name}: ${field} must be zero`);
  }
}
try {
  await run('core', [npmCli, 'run', 'test:batch'], { tests: 110 });
  await run('typecheck', [npmCli, 'run', 'typecheck']);
  await run('package-build', [npmCli, 'run', 'build']);
  await run('production-dependencies', [npmCli, 'ls', '--omit=dev', '--all']);
  await run('fresh-topologies-docs', ['--test', '--test-reporter=spec', '--test-concurrency=1', 'tests/batch/consumers.spec.mjs',
    'tests/batch/tls.spec.mjs', 'tests/batch/sentinel.spec.mjs', 'tests/batch/docs.spec.mjs'], { tests: 4, timeout: 480000 });
  await run('site', [npmCli, 'run', 'docs:validate']);
  let services = 0;
  for (const entry of await readdir(scratch, { withFileTypes: true })) {
    if (!entry.isDirectory() || !/^batch-(redis|topology)-/.test(entry.name)) continue;
    const dir = resolve(scratch, entry.name);
    for (const name of (await readdir(dir)).filter(name => name === 'service.json' || name === 'topology-evidence.json' || /^(worker-|proxy-).+\.json$/.test(name))) {
      const receipt = JSON.parse(await readFile(resolve(dir, name), 'utf8'));
      if (receipt.startedAt < evidence.startedAt) continue; // Historical failed fixtures keep their original receipts.
      if (name === 'topology-evidence.json') {
        assert.equal(receipt.cleanupVerified, true, dir);
        for (const service of receipt.services) { assert(service.stopped && service.portReleased, `${dir}/${service.name}`); services++; }
      } else {
        assert.equal(receipt.stopped, true, `${dir}/${name}`);
        const unowned = receipt.portOwnership === 'not-acquired' && receipt.failure?.code === 'TEST_REDIS_PORT_COLLISION' && receipt.portReleased === null;
        assert(name.startsWith('worker-') || receipt.portReleased === true || unowned, `${dir}/${name}`); services++;
      }
    }
  }
  evidence.servicesVerified = services; evidence.status = 'PASS'; evidence.finishedAt = new Date().toISOString(); await save();
  console.log(JSON.stringify({ status: 'PASS', node: evidence.node, tests: 114, servicesVerified: services, evidence: resolve(directory, 'qualification.json') }));
} catch (error) { evidence.status = 'BLOCK'; evidence.failure = { name: error.name, message: error.message }; await save(); throw error; }
