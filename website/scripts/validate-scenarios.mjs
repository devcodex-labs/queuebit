import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const websiteDir = path.resolve(scriptDir, '..');
const repoDir = path.resolve(websiteDir, '..');
const docsRoot = path.join(repoDir, 'docs');
const docsDir = path.join(docsRoot, 'v01');
const manifest = JSON.parse(
  await readFile(path.join(websiteDir, 'scenarios.json'), 'utf8')
);

const errors = [];
const sources = new Map();

async function readRepoFile(relativePath) {
  return readFile(path.join(repoDir, relativePath), 'utf8');
}

function assertContainsAll(source, label, tokens) {
  for (const token of tokens) {
    if (!source.includes(token)) errors.push(`${label}: missing ${token}`);
  }
}

function assertContainsNone(source, label, tokens) {
  for (const token of tokens) {
    if (source.includes(token)) errors.push(`${label}: forbidden stale token ${token}`);
  }
}

function extractStringUnion(source, typeName) {
  const match = source.match(new RegExp(`type ${typeName} =([\\s\\S]*?);`));
  return match ? [...match[1].matchAll(/'([^']+)'/g)].map(item => item[1]) : [];
}

function sameMembers(actual, expected) {
  return actual.length === expected.length
    && actual.every((value, index) => value === expected[index]);
}

async function readDoc(language, file) {
  const key = `${language}/${file}`;
  if (!sources.has(key)) {
    sources.set(key, await readFile(path.join(docsDir, language, file), 'utf8'));
  }
  return sources.get(key);
}


function publicErrors(source) {
  const issues = [];
  for (const token of ['createBatchQueue', 'BatchQueue', 'BatchTask', 'RunControlInput', 'EventReplayInput', 'QueuebitError']) {
    if (!source.includes(token)) issues.push('missing API ' + token);
  }
  if (/createQueuebitClient|defineQueuebitConfig|jobs\.add|createQueuebitWorker|npx queuebit/.test(source)) issues.push('legacy API');
  return issues;
}
function readmeErrors(source) {
  const issues = [];
  const links = [...source.matchAll(/\]\(([^)]*docs\/v01\/[^)]+)\)/g)].map(match => match[1]);
  if (!links.length) issues.push('no manual links');
  for (const link of links) {
    if (!link.startsWith('https://github.com/devcodex-labs/queuebit/blob/main/')) issues.push('non-publishable docs link');
  }
  for (const token of ['Node.js 22+', 'Redis 7.2+', 'npm pack', 'createBatchQueue', '127.0.0.1:4180', '127.0.0.1:4181', '127.0.0.1:4182']) {
    if (!source.includes(token)) issues.push('missing README ' + token);
  }
  return issues;
}
if (manifest.version !== 3 || manifest.contractStatus !== 'confirmed' || manifest.evidenceLevel !== 'implemented-unreleased') {
  errors.push('scenario manifest must describe the current implemented, unreleased BatchQueue contract');
}
const expectedFiles = manifest.requiredPages.map(page => page.file).sort();
if (new Set(expectedFiles).size !== 24 || expectedFiles.length !== 24) errors.push('expected 24 distinct page roles per language');
const implementedCodes = new Set(extractStringUnion(await readRepoFile('src/batch/api/errors.ts'), 'QueuebitErrorCode'));
const config = await readRepoFile('website/rspress.config.ts');
for (const language of ['en', 'zh']) {
  const actual = (await readdir(path.join(docsDir, language))).filter(file => file.endsWith('.md')).sort();
  if (!sameMembers(actual, expectedFiles)) errors.push(language + ': page inventory drift');
  for (const page of manifest.requiredPages) {
    for (const field of ['role', 'audience', 'sidebarGroup', 'userPath']) if (page[field] === undefined) errors.push(page.file + ': missing role ' + field);
    const source = await readDoc(language, page.file);
    assertContainsAll(source, language + '/' + page.file, ['manual-label']);
    assertContainsNone(source, language + '/' + page.file, ['createQueuebitClient', 'defineQueuebitConfig', 'jobs.add', 'createQueuebitWorker', '0.0.0-staging', 'target-contract skeleton', 'DevCodex', '@rocky']);
    if (/\bQB_[A-Z_]+\b/.test(source)) errors.push(language + '/' + page.file + ': legacy error code');
    const codes = source.match(/\b(?:CONFIG_|JSON_|PAYLOAD_|QUEUE_|RESOURCE_|MODE_|TASK_|IDEMPOTENCY_|DEFINITION_|COMMAND_|REVISION_|CONTROL_|CAPACITY_|LEASE_|SCHEMA_|OUTCOME_|STORAGE_|CURSOR_|INDEX_|READ_|SEQUENCE_|NAMESPACE_|HANDLER_|CONNECTION_)[A-Z_]+\b/g) ?? [];
    for (const code of codes) if (!implementedCodes.has(code)) errors.push(language + '/' + page.file + ': unknown error ' + code);
    if (page.sidebar !== false) {
      const route = '/' + (language === 'zh' ? 'zh/' : '') + page.file.replace(/\.md$/, '');
      if (!config.includes("link: '" + route + "'")) errors.push(route + ': absent from sidebar');
      const group = language === 'zh' ? page.sidebarGroupZh : page.sidebarGroup;
      const groupStart = config.indexOf("text: '" + group + "', items:");
      const groupEnd = config.indexOf('] }', groupStart);
      if (groupStart < 0 || !config.slice(groupStart, groupEnd).includes("link: '" + route + "'")) errors.push(route + ': wrong sidebar group');
    }
  }
  const quick = await readDoc(language, 'quick-start.md');
  assertContainsAll(quick, language + '/quick-start', ['npm pack', 'createBatchQueue', 'defineReceiptTask', 'ready()', 'task.start', 'task.get', 'queue.close', 'receipt-task.ts']);
  const batch = await readDoc(language, 'batch-runs.md');
  assertContainsAll(batch, language + '/batch-runs', ['ctx.next', 'ctx.end', 'putOnce', 'snapshotId', 'class="qb-canonical-flow"', 'role="img"']);
  const api = await readDoc(language, 'target-api.md');
  errors.push(...publicErrors(api).map(issue => language + '/target-api: ' + issue));
  const contract = await readDoc(language, 'batch-v2.md');
  assertContainsAll(contract, language + '/batch-v2', ['512MiB', 'nodeAuth', 'sentinelAuth', 'OUTCOME_UNKNOWN', 'replayDrainDeadline', '30', '10000']);
  const fences = source => [...source.matchAll(/```ts\r?\n([\s\S]*?)```/g)].map(match => match[1]).join('\n');
  if (language === 'zh' && fences(api) !== fences(await readDoc('en', 'target-api.md'))) errors.push('bilingual public API examples differ');
}
for (const scenario of manifest.scenarios) {
  for (const field of ['inScope', 'trigger', 'config', 'execute', 'expectedState', 'failure', 'recovery', 'observe', 'executableEvidence', 'status']) {
    if (scenario[field] === undefined || scenario[field] === '') errors.push(scenario.id + ': missing ' + field);
  }
  if (scenario.status !== 'documented-implemented') errors.push(scenario.id + ': stale scenario status');
  await readRepoFile(scenario.executableEvidence);
  for (const language of ['en', 'zh']) {
    assertContainsAll(await readDoc(language, scenario.file), scenario.id + '/' + language, ['<span id="' + scenario.marker + '"></span>']);
  }
}
const api = await readDoc('en', 'target-api.md');
if (!publicErrors(api.replaceAll('EventReplayInput', '')).some(issue => issue.includes('EventReplayInput'))) errors.push('negative replay API probe failed');
if (!publicErrors(api + '\ncreateQueuebitClient').includes('legacy API')) errors.push('negative legacy API probe failed');
const readme = await readRepoFile('README.md');
errors.push(...readmeErrors(readme));
if (!readmeErrors(readme.replace('https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/quick-start.md', 'docs/v01/en/quick-start.md')).includes('non-publishable docs link')) errors.push('negative README link probe failed');
// GitHub source links point to this candidate's files, not an assumption that unpublished changes exist remotely.
for (const match of readme.matchAll(/https:\/\/github\.com\/devcodex-labs\/queuebit\/blob\/main\/([^\s)]+)/g)) await readRepoFile(match[1]);
const accessibilitySource = await readFile(
  path.join(websiteDir, 'components', 'A11yLabels.tsx'),
  'utf8'
);
assertContainsAll(accessibilitySource, 'A11yLabels.tsx release banner', [
  'useLang()',
  "title: 'Batch v2 · 未发布'",
  "title: 'Batch v2 · Unreleased'",
  'className="qb-release-banner"',
  'role="status"',
  'aria-live="polite"'
]);
assertContainsAll(accessibilitySource, 'A11yLabels.tsx mobile documentation sidebar', [
  'useEffect(',
  "window.matchMedia('(max-width: 768px)')",
  "sidebarSelector = '.rp-doc-layout__sidebar'",
  "openClass = 'rp-doc-layout__sidebar--open'",
  "event.target.closest('.rp-sidebar-menu__left')",
  "document.addEventListener('click', handleDocumentClick, true)",
  "menuButton.setAttribute('aria-controls', sidebar.id)",
  "menuButton.setAttribute('aria-expanded', String(mobileSidebarOpen))",
  'className="qb-mobile-sidebar-mask"',
  'aria-label={copy.closeMenu}'
]);
assertContainsNone(accessibilitySource, 'A11yLabels.tsx unsupported global mutation', [
  'MutationObserver',
  'document.body'
]);

const styles = await readFile(path.join(websiteDir, 'styles', 'queuebit.css'), 'utf8');
for (const token of [
  '.qb-canonical-flow',
  '.qb-flow-stage',
  '.qb-flow-arrow',
  '.qb-mobile-sidebar-mask',
  '.rp-doc-layout__sidebar.rp-doc-layout__sidebar--open'
]) {
  if (!styles.includes(token)) {
    errors.push(`queuebit.css: missing canonical flow style ${token}`);
  }
}

const packageMetadata = JSON.parse(
  await readFile(path.join(repoDir, 'package.json'), 'utf8')
);
const websitePackageMetadata = JSON.parse(
  await readFile(path.join(websiteDir, 'package.json'), 'utf8')
);
const packageFiles = [...(packageMetadata.files ?? [])].sort();
if (!sameMembers(packageFiles, ['LICENSE', 'README.md', 'dist'])) {
  errors.push(`package.json: expected files LICENSE|README.md|dist, received ${packageFiles.join('|')}`);
}
if (packageMetadata.scripts?.['docs:preview']
  !== 'npm run docs:build && npm --prefix website run preview') {
  errors.push('package.json: docs:preview must build then serve the fixed preview port');
}
if (packageMetadata.scripts?.['docs:dev']
  !== 'npm run docs:build && npm --prefix website run dev') {
  errors.push('package.json: docs:dev must build then delegate to the fixed 4181 generated preview script');
}
if (packageMetadata.scripts?.['docs:edit'] !== 'npm --prefix website run edit') {
  errors.push('package.json: docs:edit must delegate to the fixed 4182 hot-edit script');
}
if (websitePackageMetadata.scripts?.preview
  !== 'rspress preview --port 4180 --host 127.0.0.1') {
  errors.push('website/package.json: preview must pin 127.0.0.1:4180');
}
if (websitePackageMetadata.scripts?.dev
  !== 'rspress preview --port 4181 --host 127.0.0.1') {
  errors.push('website/package.json: dev must pin generated preview to 127.0.0.1:4181');
}
if (websitePackageMetadata.scripts?.edit
  !== 'rspress dev --port 4182 --host 127.0.0.1') {
  errors.push('website/package.json: edit must pin hot dev to 127.0.0.1:4182');
}


if (packageMetadata.engines.node !== '>=22' || packageMetadata.private !== true || packageMetadata.bin || packageMetadata.peerDependencies) errors.push('root package boundary drift');
if (JSON.stringify(packageMetadata.dependencies) !== JSON.stringify({ '@redis/client': '6.1.0' })) errors.push('runtime dependency drift');
if (packageMetadata.exports['.'].require.types !== './dist/index.d.cts') errors.push('CommonJS declaration mapping drift');
for (const script of ['test:batch:consumers', 'test:batch:tls', 'test:batch:sentinel', 'test:batch:docs']) {
  if (!packageMetadata.scripts[script]?.includes('build:batch:package')) errors.push(script + ': missing public package build');
}
const workflow = await readRepoFile('.github/workflows/publish.yml');
assertContainsNone(workflow, 'tag qualification', ['npm publish', 'id-token: write', '.github/redis/']);
assertContainsAll(workflow, 'tag qualification', ['uses: ./.github/workflows/batch-core.yml']);
const example = await readRepoFile('examples/batch-v2/receipt-task.ts');
assertContainsAll(example, 'receipt task', ['readPage', '100', 'putOnce', 'ctx.next', 'ctx.end', 'completeOnce', 'signal']);
if (errors.length) {
  console.error('Scenario validation failed with ' + errors.length + ' error(s):');
  for (const error of errors) console.error('- ' + error);
  process.exitCode = 1;
} else {
  console.log('Validated ' + manifest.scenarios.length + ' current scenarios, 48 bilingual pages, package/README/CI boundaries and negative semantic probes; runtime qualification is separate.');
}
