# Development contract and acceptance route

<span class="manual-label">Maintainer · qualification, not an integration prerequisite</span>

## Source-of-truth order

The public BatchQueue types and [complete behavior contract](batch-v2.md) define the consumer boundary. The root entry is the only package API. Internal module builds exist for tests, not as supported package imports. Do not reintroduce removed CLI or framework adapters through incidental exports.

## Local qualification

Use Node22 or Node24 and Redis7.2 with OpenSSL. On Linux the harness uses `redis-server` by default; `QUEUEBIT_BATCH_REDIS_BINARY` selects an explicit fixture binary. On Windows the checked-in harness uses WSL Ubuntu-24.04 and its isolated Redis7.2.16 fixture location; inspect the harness default or provide the binary override before running. Tests own random local ports and temporary data directories and fail if prerequisites are missing.

```bash
npm ci
npm --prefix website ci
npm run typecheck
npm test
```

`npm test` runs the full required core, real-root fresh install, TLS, three-Sentinel failover, application example and documentation qualification. A missing fixture or skipped required case is not success. Run independently with `test:batch:consumers`, `test:batch:tls`, `test:batch:sentinel` or `test:batch:docs`; each includes its build prerequisite. Root prepack also builds before packing.

## Evidence that matters

Capture commands, exit codes, Node/Redis versions and actual tarball hash. Fresh consumers install without symlinks or repository path aliases, validate ESM/CJS and NodeNext/Bundler types, and reject removed subpaths. The example compiles the actual task module and tests 203 records with a post-write failure. Old-key isolation uses an owned Redis instance, seeded legacy keys, full command observation and new-key positive control.

## Local documentation site

`npm run docs:validate` checks scenarios, builds and validates local links. Generated preview uses `npm run docs:preview` at127.0.0.1:4180; `docs:dev` uses4181, `docs:edit` uses4182 for hot editing. Check English/Chinese navigation and mobile accessibility on rendered pages, not only Markdown.

## Rejected substitutes

No synthetic staging manifest, silent environment skip, mock-only failover, source alias consumer or weakened package-content assertion can stand in for the real package. Same-machine Sentinel tests do not establish fault-domain HA or disk-persistence durability. Do not stop existing user services to satisfy fixture prerequisites.

## Release closure

CI on pull requests, main and tags qualifies the current root package. It does not publish. The working tree keeps private=true and historical0.0.5 metadata until a release version is selected. Git operations, tag creation and npm publication require the release owner's decision. Local green tests are not proof that remote CI ran or a version was released.
