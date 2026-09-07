# Durable workflow contract

This repository implements the `durable-workflows` producer portion of portfolio contracts v1.

- `GET /health` returns `{"status":"ok","service":"durable-workflows","version":"0.1.0"}` while SQLite is reachable.
- `POST /jobs` requires server-side bearer authentication, an `Idempotency-Key`, and strict JSON shaped as `{"kind":"data_import","records":[{"value":1}]}`.
- `GET /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/events`, and `GET /events` are scoped to the authenticated synthetic principal.
- States are `queued`, `running`, `retry_wait`, `succeeded`, `failed`, and `cancelled`. Revisions increase with each state transition and terminal states are immutable.
- Every immutable event uses schema version 1, a unique UUID event ID, source `durable-workflows`, event type `job.state_changed`, an RFC 3339 UTC timestamp, UUID job ID, synthetic tenant ID, positive per-job sequence, and a content-free state payload.
- Reusing one owner's idempotency key with a different canonical request hash returns HTTP 409. The existing identity is never replaced.
- Default service port is 8111; default binding is `127.0.0.1`. `HOST` and `PORT` explicitly override deployment configuration.

This local copy is intentionally limited to the applicable service contract. The portfolio parent document remains the frozen authority for cross-project integration.

