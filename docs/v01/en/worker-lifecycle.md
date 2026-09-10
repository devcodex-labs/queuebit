# Runtime lifecycle and ownership

<span class="manual-label">Maintainer · leases, physical slots and cleanup</span>

## Common lifecycle

Import/construct/define perform no network I/O. Ready owns connection setup, protocol checks, definition/member registration and mode-specific loops. Repeated ready/close share in-flight results; a closed Queue cannot be reused. No global signal handlers or worker processes are created on import.

## Execution and callback ownership

Each attempt owns a fenced lease and a physical local slot. Logical lease loss revokes commit authority, while an unresolved JavaScript handler continues occupying its physical slot. Redis fencing prevents stale state commits, not external side effects. Callbacks have separate slots/budgets and use immutable Event snapshots.

## Time advancement

Bounded polling and maintenance promote eligible work, recover expired authority and reclaim eligible retained objects. Producers also participate in maintenance but do not execute business handlers. There is no separate public scheduler/coordinator process.

## Replay boundaries

Normal callbacks advance their sequence on delivered/dead_letter. Late replay never rewinds it and shares the parent Event lock. The first eligible lease crossing the fixed expiry freezes replayDrainDeadline; at/after expiry only that already-valid attempt may settle within its frozen bounds.

## Connection policy

Commands share bounded deadlines; offline queues are disabled. Unknown write outcomes remain explicit. Cleanup stops only connections/processes owned by the participant or test harness. Close stops new claims and drains within closeGraceMs before revoking remaining authority; its result reports residual executions/callbacks.

## Required fault windows

Exercise process death, uncertain claim/settlement replies, event-loop stalls, non-cooperative handlers, close during ready, stale token races, callback retries/replay/expiry and fixture cleanup. Use the real package for distribution checks, not internal test builds.

## Next

[Storage model](redis-model.md) · [Qualification](development-contract.md)
