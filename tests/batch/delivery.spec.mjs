import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readFile, access } from 'node:fs/promises';

test('delivery: tag qualification has a reusable target and never enables publication', async () => {
  const root = new URL('../../', import.meta.url);
  const workflow = await readFile(new URL('.github/workflows/batch-core.yml', root), 'utf8');
  const tag = await readFile(new URL('.github/workflows/publish.yml', root), 'utf8');
  const triggers = /^on:\s*\r?\n([\s\S]*?)(?=^\S)/m.exec(workflow)?.[1];
  assert.match(triggers ?? '', /^  workflow_call:\s*$/m);
  // runner context exists in step.env, not in job.env during workflow planning.
  assert.doesNotMatch(workflow.split(/^    steps:\s*$/m)[0], /\$\{\{\s*runner\./);
  const redisStepEnvs = workflow.match(/^        env:\s*\r?\n          QUEUEBIT_BATCH_REDIS_BINARY: \$\{\{ runner\.temp \}\}\/queuebit-redis\/root\/usr\/bin\/redis-server\s*$/gm);
  assert.equal(redisStepEnvs?.length, 2, 'Redis extraction and qualification both need the step-scoped binary');
  assert.match(tag, /uses: \.\/\.github\/workflows\/batch-core\.yml/);
  for (const file of [workflow, tag]) {
    assert.match(file, /contents: read/);
    assert.doesNotMatch(file, /npm publish|id-token: write|contents: write/);
  }
  assert.equal(JSON.parse(await readFile(new URL('package.json', root), 'utf8')).private, true);
});

test('delivery: the current changelog is the only unreleased version entry point', async () => {
  const root = new URL('../../', import.meta.url);
  await assert.rejects(access(new URL('changelogs/unreleased.md', root)), { code: 'ENOENT' });
  assert.match(await readFile(new URL('CHANGELOG.md', root), 'utf8'), /Unreleased — BatchQueue v2/);
  const website = JSON.parse(await readFile(new URL('website/package.json', root), 'utf8'));
  assert.equal(website.devDependencies['rspress-plugin-mermaid'], undefined);
  assert.doesNotMatch(await readFile(new URL('website/rspress.config.ts', root), 'utf8'), /rspress-plugin-mermaid/);
  const lock = JSON.parse(await readFile(new URL('website/package-lock.json', root), 'utf8'));
  for (const name of ['rspress-plugin-mermaid', 'mermaid', 'dompurify', 'brace-expansion']) {
    assert.equal(lock.packages[`node_modules/${name}`], undefined, `retired dependency ${name}`);
  }
});
