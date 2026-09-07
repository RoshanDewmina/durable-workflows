# Interview guide

This guide explains the engineering story; it does not assert personal mastery. Practice the exercises and explain the tradeoffs in your own words before using draft resume wording.

## The two-minute story

Relay accepts a bounded synthetic data import and persists it with an idempotency key before a worker sees it. Multiple workers compete for jobs through SQLite transactions. Each claim gets an expiring lease and a higher fencing token, so a paused worker cannot later overwrite the result committed by its replacement. Failures become retry events with a bounded attempt count, and every transition creates an immutable event envelope. The browser makes the operational state visible instead of treating the queue as an implementation detail.

## Decisions to defend

**Why SQLite WAL?** It creates a reproducible local product with real transactions and process-level concurrency, no external account, and minimal setup. WAL improves reader/writer coexistence, while `BEGIN IMMEDIATE` deliberately serializes claims. PostgreSQL with `FOR UPDATE SKIP LOCKED` is the likely multi-host step, but adding it here would weaken the zero-infrastructure demonstration.

**Why both leases and fencing tokens?** A lease allows abandoned work to become eligible again. Time alone cannot stop an old worker from waking up after expiry. The monotonic token lets the database reject that stale completion after a new claim.

**Why at least once?** A process can stop after an external effect but before recording success. The project only atomically deduplicates its local summary because the summary and terminal state share one SQLite transaction. A real remote sink needs its own idempotency contract or an outbox/reconciliation design.

**Why a request hash under the idempotency key?** Returning an existing job is safe only when the canonical payload is identical. A reused key with changed records gets HTTP 409, preserving the original request identity instead of silently substituting data.

**Why return 404 across owners?** It enforces the same owner check while avoiding confirmation that a guessed job ID belongs to somebody else. Authentication resolves the principal from a server-side bearer-token map; the client never submits a tenant ID.

## Failure sequence

1. API transaction inserts the job, owner-scoped idempotency key, and sequence-1 queued event.
2. A worker transaction claims it, increments the attempt and fencing token, and writes a running event.
3. If the dependency misses its deadline, the worker records `retry_wait` and a bounded next-run time. The final allowed attempt records immutable `failed`.
4. If the worker process exits, the running lease stays visible. After expiry, a recovery transaction emits `retry_wait`; the replacement claim gets a higher fence.
5. The old worker's completion cannot match current owner plus fence and is rejected before the side effect transaction.

## Alternatives

- PostgreSQL and a dedicated queue would support broader deployment and operational tooling, at the cost of local setup and another failure domain.
- An in-memory task runner would be smaller, but it could not prove restart recovery.
- A global idempotency key would incorrectly collide across owners; the schema keys it by `(tenant_id, idempotency_key)`.
- Killing timeout threads would require a process boundary or cooperative dependency API. The current deadline bounds the workflow wait, not the dependency thread lifetime.

## Hands-on exercises

1. Run `make demo`, identify the queued/running/succeeded event sequences, and explain why the sequence equals the job revision.
2. Run `DW_DEPENDENCY_MODE=timeout make serve`, submit one import, and explain the state history after three attempts.
3. Read `test_process_kill_then_restart_recovers_expired_lease`, then change the lease duration and predict the observed recovery time before running it.
4. Use Alpha to create a job and Beta to request its UUID. Explain the 404 response and find the owner predicate in the database query.
5. Run `make benchmark` twice. Compare the receipt conditions and explain why a local synthetic rate cannot become a production claim.
6. Sketch the changes for a remote payment or webhook effect: provider idempotency key, transactional outbox, delivery attempts, acknowledgement, and reconciliation.

## Likely follow-up questions

**What happens when the database is busy?** Connections wait up to five seconds through SQLite's busy timeout. A claim transaction remains short; processing happens outside it. Sustained write contention is a signal to move to a server database, not increase the lock duration indefinitely.

**Can two workers both execute one job?** A transaction ensures one current claim. A lease expiry can lead to overlapping execution if the original worker is merely slow. That is why execution is described as at least once and commits require fencing.

**Are events private?** Event payloads include state, kind, attempt, and record count, never record values. The API exports only the authenticated owner's envelopes.

**What would you monitor?** Job counts by state, event volume, committed results, retry causes, oldest queued age, lease expiry count, and processing latency. This first version exposes the first three and structured transition logs; histograms and alert thresholds require a real operating environment.

## Mastery checklist

- Explain idempotency versus deduplication without notes.
- Draw the stale-worker timeline and show where the fence is checked.
- Reproduce the killed-process recovery test.
- Explain one case the local guarantee does not cover.
- Read and approve any resume wording yourself.

Personal mastery status: **pending**.

