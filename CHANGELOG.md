# Changelog

## Unreleased — BatchQueue v2

### Breaking changes

- Replace the previous root API with `createBatchQueue`, `QueuebitError` and explicit public types. Remove the CLI, framework adapter, old Worker/Coordinator/jobs interfaces and internal package subpaths.
- Require Node.js 22+ and Redis 7.2+ with a writable single primary and noeviction. No legacy API compatibility or Redis-data migration is provided.
- Retire old source/tests/example/topology files and duplicate documentation mirrors. Existing `docs/v01` page URLs now describe BatchQueue only.

### Added

- Durable immutable Run queries, serial page checkpoints, finite retries, lease fencing, bounded local concurrency and cooperative shutdown.
- Durable ordered callbacks, dead-letter inspection and replay with a fixed first-dead expiry, plus revision-checked operator controls and bounded live lists.
- Namespace protocol/capacity/retention limits, explicit unknown write outcomes, TLS and Sentinel connections, and local telemetry/health/capacity observations.
- Actual-root tarball qualification with fresh ESM/CJS and TypeScript consumers, isolated Redis faults, TLS/Sentinel failover, a 203-record paging/idempotency example and bilingual user documentation.

### Release boundary

This is unreleased source, not a newly published version. The retained package version `0.0.5` is historical metadata and `private: true` prevents accidental publication. A new version, tag and npm publication require a separate release decision. Local tests do not establish remote CI execution or production durability across independent failure domains.

### Review fixes

- Verify Redis noeviction during readiness before namespace writes; missing policy information is rejected without changing server configuration.
- Reject orphaned execution-lease indexes instead of repeatedly treating them as successful no-op recovery.
- Enable reuse of the shared qualification workflow by tag checks, and remove the obsolete competing unreleased changelog.
- Remove the unused legacy Mermaid documentation plugin and refresh affected routing dependencies within the existing major version.
