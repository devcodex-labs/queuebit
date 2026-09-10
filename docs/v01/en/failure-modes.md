# How to read states and errors

<span class="manual-label">Reference · separate execution, delivery and transport outcomes</span>

## Run states

`pending` waits for execution; `running` has current leased work; `retrying` waits for a business retry; `blocked` cannot currently progress; `pausing` has a pause request with in-flight work; `paused` admits no next page. Terminal outcomes are `success`, `failed` and `cancelled`. Null from get means no retained matching Run, not a fabricated successful state.

## Event states

`pending`, `retrying`, `delivering`, `delivered` and `dead_letter` describe callback delivery. Run success does not mean callbacks have all been delivered. A callback failure does not redo execution. Replay uses a new delivery generation on the same immutable Event and does not extend its first-dead expiry.

## Error shape

`QueuebitError` exposes code, operation, optional runId/eventId/commandId, retryable and outcomeKnown. Handler failures are stored as bounded error envelopes. Unknown transport outcome is independent of retryability.

## Find by symptom

| Code or group | Safe first action |
|---|---|
| CONFIG_INVALID / JSON_INVALID / PAYLOAD_TOO_LARGE | Correct the input; inspect documented bounds |
| QUEUE_NOT_READY / QUEUE_CLOSED / MODE_OPERATION_NOT_ALLOWED | Use a ready Queue in the required mode |
| TASK_IDENTITY_MISMATCH / DEFINITION_HASH_CONFLICT | Deploy the matching immutable definition |
| IDEMPOTENCY_CONFLICT / COMMAND_CONFLICT | Compare the full original request; do not reuse an ID for a different action |
| REVISION_CONFLICT | Read fresh metadata and reconsider the action |
| OUTCOME_UNKNOWN / CONNECTION_UNAVAILABLE | Preserve identity and reconcile possible writes |
| CAPACITY_EXCEEDED | Inspect backlog, members and retained capacity; allow bounded draining |
| CURSOR_INVALID / CURSOR_EXPIRED | Restart the bounded live listing with the intended filter |
| INDEX_INCONSISTENT / STORAGE_INCONSISTENT / SCHEMA_MISMATCH / NAMESPACE_ORPHANED | Stop unsafe work and investigate; no manual reset |
| LEASE_LOST / HANDLER_CONTRACT_INVALID | Check stale attempts or invalid controls and correct the handler |
| RESOURCE_CLEANUP_FAILED | Inspect owned connection cleanup and process state |

This table groups common actions; the installed TypeScript `QueuebitErrorCode` union is the exhaustive code surface.

## Next

[Recovery runbooks](failure-runbooks.md) · [Operator control](operations.md)
