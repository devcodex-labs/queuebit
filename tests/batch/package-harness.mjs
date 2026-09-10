import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { copyFile, mkdir, mkdtemp, readFile, readdir, realpath, stat, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const repo = fileURLToPath(new URL('../../', import.meta.url));
const scratch = resolve(repo, '../.devcodex/queuebit/.tmp');
const npmCli = process.env.npm_execpath ?? resolve(dirname(process.execPath), 'node_modules/npm/bin/npm-cli.js');
export async function command(binary, args, { cwd, env = {}, timeout = 120000 } = {}) {
  const child = spawn(binary, args, { cwd, windowsHide: true, env: { ...process.env, NODE_PATH: '', ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = ''; child.stdout.on('data', data => { output += data; }); child.stderr.on('data', data => { output += data; });
  let timedOut = false;
  const timer = setTimeout(() => { timedOut = true; child.kill(); }, timeout);
  try {
    const code = await new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', resolve); });
    assert.equal(code, 0, `${binary} ${args.join(' ')}: ${timedOut ? 'timeout' : ''}\n${output}`);
    return output;
  } finally { clearTimeout(timer); }
}
export async function installCandidate() {
  await mkdir(scratch, { recursive: true });
  const directory = await mkdtemp(resolve(scratch, 'batch-package-'));
  const stage = resolve(directory, 'stage'), consumer = resolve(directory, 'consumer');
  await mkdir(resolve(stage, 'dist'), { recursive: true }); await mkdir(consumer);
  // tsup removes stale output files but can leave empty directories; npm does not pack empty directories.
  const files = (await readdir(resolve(repo, 'dist'), { recursive: true, withFileTypes: true }))
    .filter(entry => entry.isFile()).map(entry => resolve(entry.parentPath, entry.name).slice(resolve(repo, 'dist').length + 1).replaceAll('\\', '/'));
  assert.deepEqual(files.sort(), ['index.cjs', 'index.d.cts', 'index.d.ts', 'index.js']);
  const manifest = JSON.parse(await readFile(resolve(repo, 'package.json'), 'utf8'));
  assert.equal(manifest.name, 'queuebit'); assert.equal(manifest.private, true);
  assert.equal(manifest.engines.node, '>=22'); assert.equal(manifest.bin, undefined);
  assert.deepEqual(manifest.dependencies, { '@redis/client': '6.1.0' });
  assert.equal(manifest.peerDependencies, undefined);
  assert.equal(manifest.exports['.'].import.types, './dist/index.d.ts');
  assert.equal(manifest.exports['.'].require.types, './dist/index.d.cts');
  assert.deepEqual(Object.keys(manifest.exports).sort(), ['.', './package.json']);
  // All test entry points build first. Pack the real root, including its prepack hook; never synthesize metadata.
  const packOutput = await command(process.execPath, [npmCli, 'pack', '--json', '--pack-destination', directory], { cwd: repo });
  await writeFile(resolve(directory, 'npm-pack.log'), packOutput);
  // npm includes lifecycle stdout before its JSON result. Keep the real prepack output as evidence.
  const jsonStart = packOutput.search(/^\[\s*\{\s*"id":/m);
  assert(jsonStart >= 0, 'npm pack must return its JSON package receipt');
  const packed = JSON.parse(packOutput.slice(jsonStart))[0];
  assert.deepEqual(packed.files.map(file => file.path).sort(), ['LICENSE', 'README.md', 'dist/index.cjs', 'dist/index.d.cts', 'dist/index.d.ts', 'dist/index.js', 'package.json']);
  const tarball = resolve(directory, packed.filename);
  const sha256 = createHash('sha256').update(await readFile(tarball)).digest('hex');
  await writeFile(resolve(consumer, 'package.json'), JSON.stringify({ name: 'isolated-consumer', version: '0.0.0', private: true, type: 'module' }));
  await command(process.execPath, [npmCli, 'install', '--ignore-scripts', '--no-audit', '--no-fund', '--save-exact', tarball], { cwd: consumer });
  // Compiler dependencies live in this fresh consumer, never a repository tsconfig or path alias.
  await command(process.execPath, [npmCli, 'install', '--ignore-scripts', '--no-audit', '--no-fund', '--save-dev', '--save-exact', 'typescript@5.8.3', '@types/node@20.19.43'], { cwd: consumer });
  const installed = resolve(consumer, 'node_modules/queuebit');
  assert.equal(await realpath(installed), installed); assert((await stat(installed)).isDirectory());
  assert.deepEqual(JSON.parse(await readFile(resolve(installed, 'package.json'), 'utf8')), manifest);
  for (const name of ['consumer.mjs', 'consumer.cjs', 'consumer.mts', 'consumer.cts', 'consumer.ts']) await copyFile(new URL(`./fixtures/${name}`, import.meta.url), resolve(consumer, name));
  const evidence = { source: 'actual-root-npm-pack', packageRoot: repo, node: process.version, directory, consumer, tarball, sha256, integrity: packed.integrity, files: packed.files, manifest, results: [] };
  await writeFile(resolve(directory, 'package-evidence.json'), JSON.stringify(evidence, null, 2));
  return evidence;
}
export async function runConsumer(candidate, file, config) {
  const output = await command(process.execPath, [file], { cwd: candidate.consumer, timeout: 30000,
    env: { QUEUEBIT_CONSUMER_CONFIG: JSON.stringify(config) } });
  candidate.results.push({ file, exitCode: 0, output });
  await writeFile(resolve(candidate.directory, 'package-evidence.json'), JSON.stringify(candidate, null, 2));
}
export async function verifyTypes(candidate) {
  for (const [mode, files] of [['NodeNext', ['consumer.mts', 'consumer.cts']], ['Bundler', ['consumer.ts']]]) {
    const args = [resolve(candidate.consumer, 'node_modules/typescript/bin/tsc'), '--noEmit', '--strict', '--exactOptionalPropertyTypes', '--noUncheckedIndexedAccess',
      '--target', 'ES2023', '--module', mode === 'Bundler' ? 'ESNext' : mode, '--moduleResolution', mode, ...files];
    const output = await command(process.execPath, args, { cwd: candidate.consumer });
    candidate.results.push({ mode, files, exitCode: 0, output });
  }
  await writeFile(resolve(candidate.directory, 'package-evidence.json'), JSON.stringify(candidate, null, 2));
}
