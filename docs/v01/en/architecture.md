# Architecture

<span class="manual-label">Maintainer · not an integration prerequisite</span>

Read the [user guide](quick-start.md) before extending the library. BatchQueue has one public entry and one Redis state machine. It does not expose its storage or runtime classes as package subpaths.

## Module boundaries

| Module | Responsibility |
|---|---|
| Root and Batch API | Explicit public exports, types and stable errors |
| Domain | Strict JSON/configuration, task identity, context-owned controls and cursors |
| Application | Admission and query/control orchestration |
| Runtime | Local physical slots, leases, cooperative execution, callbacks and shutdown |
| Redis storage | Explicit key plans, static Lua operations, budgets and bounded indexes |

The only runtime dependency is `@redis/client@6.1.0`. Application repositories, providers and framework hosting remain outside the package.

## Atomic boundaries

Every state-changing settlement checks the current lease/token and revision in the Redis operation. Stale execution cannot advance a Run, but fencing cannot retract an external side effect. Prepaid capacity covers settlement and Event creation; GC returns charges only when dependencies are no longer protected.

## Lifecycle

Import, construction and definition are pure with respect to network I/O. `ready()` owns connections and starts the selected mode. `close()` stops admission/claiming and drains within the local grace period; unresolved handlers continue occupying physical slots until they exit.

## Qualification

Use the [qualification guide](development-contract.md). Internal tests may build isolated modules, while distribution tests must pack and install the actual root package with no repository aliases.
