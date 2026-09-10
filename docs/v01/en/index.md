---
pageType: home
hero:
  name: queuebit
  text: Durable batch processing for Node.js
  tagline: Process a fixed business snapshot in bounded pages, recover progress in Redis, and deliver callbacks safely.
  actions:
    - theme: brand
      text: Start your first batch
      link: /quick-start.html
    - theme: alt
      text: Process business records
      link: /batch-runs.html
    - theme: alt
      text: Read the learning path
      link: /concepts.html
features:
  - title: Start with a real snapshot
    details: Install the local root package, connect Redis, register the receipt task, supply durable adapters and observe its Run.
    link: /quick-start.html
  - title: Bounded work uses one task
    details: Admit a finite query and let a managed consumer advance each page from committed state.
    link: /job-recipes.html
  - title: Learn as your task grows
    details: Understand finite retries, cooperative timeouts, business idempotency and operator controls.
    link: /concepts.html
  - title: Keep pages small and recoverable
    details: Freeze input membership and payload, write idempotently and commit a keyset cursor after each page.
    link: /batch-runs.html
  - title: Scale independent consumers
    details: Share namespace and immutable contracts, then tune local execution and callback slots to downstream capacity.
    link: /distributed-workers.html
  - title: Keep your framework in charge
    details: Your application owns authentication, lifecycle and hosting; no framework adapter is required.
    link: /vext-integration.html
  - title: Operations stay out of the first path
    details: Production deployment, capacity, alerts, and recovery live under Production, not in the first integration path.
    link: /failure-runbooks.html
---

<span class="manual-label">Home · BatchQueue user manual</span>

> **Release status:** This manual matches the unreleased BatchQueue source tree. Install the local root tarball; historical npm releases do not implement this API. The v01 page URLs are retained, not legacy compatibility.
