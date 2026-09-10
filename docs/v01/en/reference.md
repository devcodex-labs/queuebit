# Reference

<span class="manual-label">Reference · exact lookup after the first successful task</span>

## Find by task

| Need | Read |
|---|---|
| First real business batch | [Quick start](quick-start.md) |
| Public method/type lookup | [API lookup](target-api.md) |
| Full behavior and bounds | [Complete Batch contract](batch-v2.md) |
| Configuration defaults | [Field dictionary](cli-and-config.md) |
| Supported environments | [Compatibility](compatibility.md) |
| Interpret execution/delivery failures | [States and errors](failure-modes.md) |
| Control or replay work | [Operations](operations.md) |
| Host inside a framework | [Framework hosting](vext-integration.md) |
| Old CLI link | [Operator SDK, no CLI](cli-reference.md) |

## Public naming overview

`createBatchQueue` constructs a Queue; `define` returns a Task; `start` admits a Run; `ctx.next/end` settle a Batch; declared handlers deliver Events. `QueuebitError` carries stable error/outcome fields. Root exports and package metadata are the only import paths.

## Version status

The current manual describes the unreleased source tree. Historical npm releases do not match it. Existing v01 URLs are retained for discoverability, not compatibility with removed APIs or data.

## Maintainer entry

[Architecture](architecture.md) · [Redis model](redis-model.md) · [Lifecycle](worker-lifecycle.md) · [Qualification](development-contract.md)
