# EventFlow — Codex Build Specification

**Objective:** Implement a working, testable, portfolio-quality webhook delivery platform with **Java/Spring Boot, PostgreSQL, Redis Streams, Redis Sorted Sets, React/TypeScript, Docker, and CI**. This is an implementation request, **not a request to generate only a plan or skeleton**.

## 0. Instructions for Codex

- Inspect the current repository; if empty, scaffold it. Respect existing work and do not overwrite unrelated files.
- Implement in phases below, running code/tests after each phase. Do not claim a test passed unless you executed it.
- Prioritize a working end-to-end system over adding buzzword features. If time runs short, finish backend persistence, dispatch, retries, a basic UI, tests, and README before adding optional polish.
- Use compatible maintained versions: Java 21, Spring Boot, Maven, TypeScript, React + Vite, PostgreSQL, Redis, Docker Compose. Document exact versions.
- Make reasonable choices without repeatedly asking questions; record meaningful tradeoffs in `docs/decisions.md`.
- If you can commit code, make logical commits after passing milestones. Report work performed, commands run, test results, and known limitations.

## 1. Product requirements

A developer can register webhook endpoints subscribing to event types, publish JSON events, and see the delivery lifecycle in a dashboard. For each subscribed endpoint, create an independent delivery job. Background workers send HTTP POST requests. Transient failures trigger retry with exponential backoff; permanent failures become DEAD. Users can manually retry dead jobs. All durable state lives in PostgreSQL; Redis provides a queue and retry index.

**Demo path:** Start services -> register local mock receiver for `order.created` -> publish event -> see HTTP 200/SUCCEEDED -> switch mock receiver to HTTP 500 -> publish -> see retries and attempts -> switch receiver back to 200 -> observe success -> demonstrate DEAD and manual retry.

### Stack
- Backend: Spring Boot Web MVC, Validation, Spring Data JPA or JDBC, Spring Data Redis, Flyway, Actuator.
- Database: PostgreSQL (JSONB for payload, indexed job statuses and due times).
- Queue: Redis Streams, consumer groups via `XREADGROUP`; acknowledge via `XACK`; reclaim stale pending entries via `XAUTOCLAIM`.
- Retry index: Redis Sorted Sets (`ZADD`, `ZRANGE ... BYSCORE` or `ZRANGEBYSCORE`, `ZREM`). **PostgreSQL is authoritative for retry times**.
- Frontend: React/TypeScript/Vite; simple responsive UI, no elaborate design system.
- Tests: JUnit 5, Spring Boot Test, Testcontainers Postgres/Redis, WireMock or equivalent mock HTTP service.
- Infra: Docker Compose; GitHub Actions running backend tests and frontend build/typecheck.

## 2. Architecture

```text
React dashboard -> Spring Boot REST API -> PostgreSQL events/jobs/outbox
                                             |
                                      Outbox publisher
                                             |
                                   Redis Stream deliveries
                                             |
                                   Spring Boot worker(s)
                                             |
                                  HTTP webhook receiver
                                             |
                              PostgreSQL attempt/job updates
                                             |
                        transient error -> PostgreSQL retry due
                                             |
                             Redis Sorted Set retry index
                                             |
                    Retry scheduler -> DB job transition + outbox
```

### Reliability rules (non-negotiable)

1. API inserts event, matching delivery jobs, and outbox rows **in one PostgreSQL transaction**. Never rely on a Redis publish that happens only inside an uncommitted request.
2. Outbox publisher claims unpublished rows safely (e.g. `FOR UPDATE SKIP LOCKED`/DB lease), sends `XADD`, then marks published. Crash after `XADD` but before marking may republish a duplicate: workers handle it.
3. Redis Stream carries job IDs, **not** the sole copy of payload/delivery state. Workers load job/event/endpoint from PostgreSQL.
4. Worker atomically claims a job using a PostgreSQL lease and status conditional; do **not** hold a DB transaction open during outbound HTTP. Concurrent workers must not process a currently leased job.
5. Only `XACK` after durable PostgreSQL state is committed (or duplicate/terminal entry is confirmed harmless). Use `XAUTOCLAIM` to recover stale pending stream messages; periodically reconcile expired DB leases.
6. System provides **at-least-once delivery, not exactly-once**. HTTP may reach receiver just before a worker crashes. Include stable `X-EventFlow-Delivery-Id` header so receivers can deduplicate. Never claim duplicates are impossible.
7. On transient error, **commit** `RETRY_SCHEDULED` + `next_attempt_at` in PostgreSQL before best-effort `ZADD` to Redis. Redis index is rebuildable via periodic DB scan.
8. Scheduler consults Redis due entries and the PostgreSQL fallback scan; atomically transitions only due `RETRY_SCHEDULED` jobs to `QUEUED` and creates new outbox records. Remove stale ZSET entries only after DB commit. Two schedulers must not double-schedule.
9. Redis outage must not lose an accepted event: PostgreSQL persists outbox/jobs and publisher/scheduler resume on recovery. Add a reconciliation path for missed queue publications and stale job leases.
10. Set bounded HTTP timeouts, payload sizes, worker concurrency, and safe error/logging practices. Do not log secrets or complete payloads by default.

## 3. Flyway schema

Use UUID IDs, UTC instants, foreign keys, useful indexes, check constraints.

- `webhook_endpoints`: `id`, `name`, `url`, `enabled`, `event_types` (TEXT[] or join table), `created_at`, `updated_at`.
- `events`: `id`, `event_type`, `payload JSONB`, `idempotency_key` (unique, nullable), `payload_sha256`, `created_at`. Replaying the same key+canonical event returns same ID/jobs; same key with different content returns 409.
- `delivery_jobs`: `id`, `event_id`, `endpoint_id`, `status` (`QUEUED`, `IN_PROGRESS`, `RETRY_SCHEDULED`, `SUCCEEDED`, `DEAD`), `attempt_count`, `max_attempts` (default 5), `next_attempt_at`, `lease_owner`, `lease_expires_at`, `last_http_status`, `last_error_code`, `created_at`, `updated_at`; unique `(event_id, endpoint_id)`; index `(status, next_attempt_at)`.
- `delivery_attempts`: `id`, `delivery_job_id`, `attempt_number`, `started_at`, `finished_at`, `outcome` (`SUCCESS`, `TRANSIENT_FAILURE`, `PERMANENT_FAILURE`, `UNKNOWN`), `http_status`, `duration_ms`, `error_code`, `error_summary`. If an attempt crashes mid-flight, recover its state as UNKNOWN.
- `outbox_messages`: `id`, `delivery_job_id`, `created_at`, `published_at`, `lease_owner`, `lease_expires_at`, `publish_attempts`. Multiple outbox rows for different attempts are allowed; workers deduplicate by DB job state.

## 4. REST APIs (`/api/v1`)

All list APIs should paginate and order newest-first. Use consistent JSON error structure, validation 400, missing 404, duplicate-key conflict 409, invalid retry state 409.

- `POST /endpoints`: `{ "name":"Demo receiver", "url":"http://mock-receiver:8082/webhook", "eventTypes":["order.created"], "enabled":true }` -> 201.
- `GET /endpoints`: list.
- `PATCH /endpoints/{id}`: update name/subscriptions/enabled state.
- `POST /events` with `Idempotency-Key` header and body `{ "eventType":"order.created", "payload":{"orderId":"o-123","total":42.5} }` -> 202, event ID, created job IDs.
- `GET /events`: recent events.
- `GET /deliveries?status=RETRY_SCHEDULED&page=0&size=20`: jobs and summary.
- `GET /deliveries/{id}`: full job + attempt history.
- `POST /deliveries/{id}/retry`: manual retry DEAD job, idempotent/consistent state transition, return 202; document whether attempt counter starts a new retry cycle or resets.
- `GET /actuator/health`: health endpoint.
- Mock receiver includes **local-only** test controls: choose HTTP 200, HTTP 500, HTTP 400 or artificial timeout; record incoming request IDs for debugging.

## 5. Dispatch, retries, and recovery

- Create consumer group `delivery-workers` on Redis stream `eventflow:deliveries`; handle group-already-exists.
- Worker receives job ID; atomically claim eligible job using lease (duration > HTTP timeout); increment attempt counter and create attempt record, commit before making network request.
- Use outbound HTTP POST with bounded connect/read timeout and no redirects. Include `X-EventFlow-Delivery-Id`, `X-EventFlow-Event-Id`, `X-EventFlow-Attempt`.
- 2xx -> SUCCEEDED; 408/429/5xx/timeouts/network errors -> retryable; other 4xx -> DEAD. Max 5 attempts per cycle.
- Exponential backoff configurable (e.g. 5,10,20,40 seconds plus bounded jitter). Do not sleep workers to implement retries.
- Failed retryable attempt -> persist attempt outcome and job `RETRY_SCHEDULED`/due timestamp, then best-effort index in Redis Sorted Set `eventflow:retry-due`; `XACK` only after DB commit.
- Retry scheduler reads Redis due indices and additionally scans PostgreSQL for due entries missing from Redis; creates outbox entries through atomic state transitions.
- Recovery uses `XAUTOCLAIM` for idle pending stream messages and a periodic scan for expired `IN_PROGRESS` leases, marking unfinished attempts UNKNOWN and making jobs eligible for retry according to policy. Tests should demonstrate that no job remains permanently stranded.

## 6. Security scope

**Local-only MVP.** Do not implement open internet-facing webhook registration without SSRF protections. Use a configurable URL allowlist to permit only the Docker Compose `mock-receiver` by default. Bind exposed ports to `127.0.0.1`. Limit body sizes; disallow redirects; avoid leaking stack traces or secrets. Authentication, multitenancy, secrets management, robust public destination URL validation and hosted deployment are **future work**, not implied production readiness. Optional after core: outbound HMAC SHA-256 signing and receiver verification with tested byte-level signature handling.

## 7. React UI

- Dashboard status tiles (QUEUED/IN_PROGRESS/RETRY_SCHEDULED/SUCCEEDED/DEAD), recent deliveries with status filter.
- Endpoint create/list/edit form.
- Publish event page with event type, JSON payload validator, optional idempotency key.
- Delivery detail page with attempts, response codes, errors, durations, next due timestamp.
- Retry button only for DEAD jobs.
- Optional local demo controls for mock receiver; clear error/loading/empty states.
- Simple polling every 3–5 sec; WebSockets are unnecessary for MVP.

## 8. Repository layout

```text
eventflow/
  backend/
    pom.xml
    src/main/java/.../{api,domain,repository,service,worker,scheduler,config}/
    src/main/resources/application.yml
    src/main/resources/db/migration/
    src/test/java/...
  frontend/
    package.json
    src/{api,components,pages,types}/
  mock-receiver/
  docs/{architecture.md,decisions.md,failure-scenarios.md}
  .github/workflows/ci.yml
  docker-compose.yml
  .env.example
  README.md
```

Provide working Dockerfiles and Compose services for postgres, redis, backend, frontend, mock-receiver. Include health checks, safe demo defaults, `.gitignore`, and no committed real secrets.

## 9. Required tests

**Unit:** input validation, idempotency comparison, retry classification/backoff, URL allowlist, state machine and max attempts.

**Integration using PostgreSQL/Redis Testcontainers and an HTTP test server:**
1. One event creates exactly one job per matched endpoint plus outbox.
2. Duplicate same idempotency key/payload reuses event; conflicting payload gives 409.
3. `POST /events` -> stream -> receiver HTTP 200 -> DB SUCCEEDED + attempt history.
4. HTTP 500 -> RETRY_SCHEDULED -> scheduler redispatch -> eventual success or DEAD.
5. HTTP 400 -> DEAD without automated retries.
6. Manual retry on DEAD succeeds, history remains understandable.
7. Worker crash/expired job lease and stale pending messages are recovered.
8. Redis is temporarily unavailable: API persists event/outbox, then eventual delivery after recovery.
9. Duplicate stream messages do not cause concurrent delivery with an active lease.
10. Two scheduler instances cannot create duplicate due transitions.
11. Clearing Redis retry sorted set does not lose scheduled retries because PostgreSQL scan rebuilds/reprocesses them.

**Build:** backend tests, frontend typecheck/lint/build, `docker compose up --build`, manual smoke test, CI workflow.

## 10. Execution milestones (work through in order)

**Phase 1 — Working vertical slice:** scaffold infra and migrations; endpoint/event APIs; transactional outbox; publish to Redis Stream; worker sends to local receiver; successful result persisted. Add happy-path integration test. Confirm system runs locally.

**Phase 2 — Failure lifecycle:** attempt logging, HTTP status classification, exponential retry schedule indexed in Redis Sorted Set, DEAD status, manual retry, deterministic tests.

**Phase 3 — Recovery and correctness:** job leases, `XAUTOCLAIM`, PostgreSQL fallback retry scan, Redis outage/restart recovery, concurrency tests, no false exactly-once claims.

**Phase 4 — UI and delivery:** dashboard/forms/detail pages, Compose one-command startup, README, architecture diagram, CI checks, final smoke test.

**Optional after acceptance:** HMAC signatures, metrics dashboard, load testing, per-endpoint rate limiting, proper public URL security, deployment with authentication. Do NOT block MVP on Kafka, Kubernetes, OAuth, or microservice splitting.

## 11. Definition of done

- `docker compose up --build` provides a functional local app and test receiver.
- The core demo workflow is repeatable: successful delivery, automatic retry, DEAD and manual retry.
- Accepted events survive Redis downtime; at-least-once semantics and known duplicate window are documented.
- Users can inspect each delivery and attempt in the dashboard.
- Tests and CI are present and actually run; README contains commands, API curl examples, architecture diagram, and documented limitations.
- Deliver **implementation and files**, not just scaffolding or an explanation.

## 12. Final Codex report

When finished, report: key files/architecture, commands to run, checks executed and results, known missing items, and three highest-value future improvements. Never invent test outcomes, deployment status, benchmark claims or resume metrics.

## Official reference documentation

- Redis Streams: https://redis.io/docs/latest/develop/data-types/streams/
- Redis `XREADGROUP`: https://redis.io/docs/latest/commands/xreadgroup/
- Redis `XAUTOCLAIM`: https://redis.io/docs/latest/commands/xautoclaim/
- Redis sorted sets: https://redis.io/docs/latest/develop/data-types/sorted-sets/
- Spring Data Redis Streams: https://docs.spring.io/spring-data/redis/reference/redis/redis-streams.html
- PostgreSQL `SKIP LOCKED`: https://www.postgresql.org/docs/current/sql-select.html