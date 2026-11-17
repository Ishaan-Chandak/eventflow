# Phase 1 decisions

- Use Spring JDBC and explicit SQL for transactional event, job, and outbox writes. PostgreSQL remains the source of truth.
- Keep the receiver on the Compose network and restrict endpoint URLs to `mock-receiver` by default.
- Phase 1 implements successful delivery and persists attempt history. Failure retries, leases, Redis pending recovery, and UI belong to later phases.
- HTTP delivery is at least once; the stable delivery ID header lets receivers deduplicate.

## Phase 2

- `attempt_count` is lifetime history. `cycle_attempt_count` resets to zero on manual retry, so each manual retry gets up to five further attempts while attempt numbers remain unique and increasing.
- HTTP 408, 429, 5xx, timeout, and network errors are transient. Other 4xx are permanent. Retry delay doubles from five seconds and caps at 300 seconds; tests use a one-second base for speed.
- PostgreSQL `next_attempt_at` is authoritative. Redis Sorted Set speeds due lookup; the scheduler also scans PostgreSQL, so missing index entries do not strand a scheduled retry.

## Phase 3

- A PostgreSQL status transition and attempt insertion share one transaction. Each in-progress job has a 15-second lease, longer than the 5-second HTTP deadline. The HTTP request runs after the transaction commits; completion is conditional on the lease owner.
- Expired leases mark unfinished attempts `UNKNOWN` and schedule a new attempt or mark the job `DEAD` when its cycle budget is exhausted.
- Redis `XAUTOCLAIM` reassigns pending messages after 20 seconds. A message for an active lease stays pending; it is acknowledged only after a durable terminal/retry state is observed or a delivery commits.
- PostgreSQL scans recover retries missing from the Redis sorted set and old queued jobs whose stream entries were lost after publication. This can create duplicates, which the database claim prevents from being sent concurrently while a lease is active.

## Phase 4

- Serve a small React/Vite dashboard through nginx and proxy API and local demo controls on the same origin. This avoids browser CORS configuration for the local stack.
- Keep page navigation in the URL hash so the demo needs no server-side route handling or router dependency.
- Poll delivery data every four seconds. WebSockets would add complexity without improving this local demo.
- Use TypeScript 6.0.3 because the maintained `typescript-eslint` release currently declares support below 6.1; the frontend lockfile records exact resolved dependency versions.
