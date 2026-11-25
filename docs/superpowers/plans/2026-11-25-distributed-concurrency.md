# Distributed Concurrency and Endpoint Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add lease-based Redis admission that atomically enforces global and per-endpoint delivery concurrency across worker replicas without weakening EventFlow's durable delivery guarantees.

**Architecture:** A `DistributedAdmissionController` executes cluster-slot-compatible Lua scripts over global and endpoint lease sorted sets. `DeliveryPipeline` resolves endpoint identity and obtains distributed admission before executor submission or PostgreSQL attempt claim; saturation and Redis failures leave entries in the PEL for delayed `XAUTOCLAIM` recovery.

**Tech Stack:** Java 21, Spring Boot 3.4.5, Spring Data Redis/Lettuce, Redis 7.4 Lua, PostgreSQL/JDBC, Micrometer, JUnit 5, Testcontainers.

**Spec:** `docs/superpowers/specs/2026-11-25-distributed-concurrency-design.md`

## Global Constraints

- PostgreSQL remains authoritative for delivery state.
- Acquire global and endpoint permits atomically before executor submission and HTTP delivery.
- Never wait for distributed capacity on an executor thread.
- Admission denial or Redis failure consumes no delivery attempt and does not ACK.
- Every permit is token-owned, finite, renewable, and idempotently releasable.
- ACK remains after durable PostgreSQL finalization.
- Metric labels remain bounded and contain no endpoint, job, token, worker, or URL identifiers.
- Do not add circuit breakers, rate limiting, fairness, priority queues, or unrelated refactoring.

## Review Focus

- Partial or stale global/endpoint ownership during acquire, renew, release, and expiry cleanup.
- Permit loss between executor submission, HTTP start, finalization, ACK, and cleanup.
- Saturated or unavailable Redis accidentally creating a PostgreSQL attempt or hot reclaim loop.
- Process pause, partition, or failover allowing lease overlap while stale PostgreSQL completion remains fenced.
- Endpoint isolation when one endpoint saturates and other stream entries continue progressing.

---

### Task 1: Configuration and admission metrics

**Files:**
- Modify: `backend/src/main/java/dev/eventflow/worker/DeliveryProperties.java`
- Modify: `backend/src/main/java/dev/eventflow/worker/DeliveryMetrics.java`
- Modify: `backend/src/test/java/dev/eventflow/worker/DeliveryPropertiesTest.java`
- Modify: `backend/src/test/java/dev/eventflow/worker/DeliveryMetricsTest.java`

**Interfaces:**
- Produces: global/per-endpoint limits, permit lease, renewal interval, deferral interval, and bounded admission metric methods.

- [ ] Write failing validation and metric-cardinality tests.
- [ ] Run focused tests and verify expected failures.
- [ ] Add validated configuration and failure-isolated metric methods.
- [ ] Run focused tests and the existing backend unit suite.

### Task 2: Atomic Redis admission coordinator

**Files:**
- Create: `backend/src/main/java/dev/eventflow/worker/DistributedAdmissionController.java`
- Create: `backend/src/test/java/dev/eventflow/worker/DistributedAdmissionControllerTest.java`
- Create: `backend/src/test/java/dev/eventflow/worker/DistributedAdmissionRedisIntegrationTest.java`

**Interfaces:**
- Produces: `AdmissionResult tryAcquire(UUID endpointId)`, `Permit.close()`, scheduled ownership-aware renewal, and bounded rejection reasons.
- Consumes: Task 1 configuration and metrics.

- [ ] Write failing unit tests for result mapping, fail-closed errors, duplicate release, stale release, renewal loss, and scheduler cleanup.
- [ ] Write failing Testcontainers tests for atomic acquisition, global limit, endpoint limit, independent endpoints, expiry recovery, renewal, and multiple controller instances.
- [ ] Implement server-time Lua acquire/renew/release with a shared Cluster hash tag and unique tokens.
- [ ] Run focused unit and Redis integration tests.

### Task 3: Pipeline admission and cleanup lifecycle

**Files:**
- Modify: `backend/src/main/java/dev/eventflow/worker/DeliveryPipeline.java`
- Modify: `backend/src/main/java/dev/eventflow/worker/LocalDeliveryExecutor.java`
- Modify: `backend/src/test/java/dev/eventflow/worker/DeliveryPipelineConcurrencyTest.java`
- Modify: `backend/src/test/java/dev/eventflow/worker/LocalDeliveryExecutorTest.java`

**Interfaces:**
- Consumes: Task 2 `tryAcquire` and permit lifecycle.
- Produces: endpoint lookup before admission, no executor wait, PEL deferral, and release on rejection/completion/exception/shutdown.

- [ ] Write failing tests proving denial/unavailability does not submit, ACK, claim, or consume an attempt; executor rejection and delivery exceptions release both permits.
- [ ] Refactor local submission to expose deterministic rejection cleanup without weakening existing ownership.
- [ ] Integrate endpoint lookup and distributed acquisition into both new-consume and reclaim paths.
- [ ] Run pipeline, executor, and backend unit tests.

### Task 4: End-to-end failure and recovery coverage

**Files:**
- Create: `backend/src/test/java/dev/eventflow/DistributedConcurrencyIntegrationTest.java`
- Modify: `backend/src/test/java/dev/eventflow/HappyPathIntegrationTest.java`
- Modify: `backend/pom.xml`

**Interfaces:**
- Verifies Tasks 1-3 against real PostgreSQL, Redis, and controlled HTTP receivers.

- [ ] Add deterministic synchronized tests for multi-replica global enforcement, per-endpoint enforcement, independent progress, long requests and renewal, crash expiry, Redis outage/recovery, delivery failure, executor rejection, no attempt consumption during saturation, and accepted-job recovery.
- [ ] Run each focused integration class and address only reproduced failures with a failing regression test.
- [ ] Run the complete backend test suite.

### Task 5: Configuration and architecture documentation

**Files:**
- Modify: `backend/src/main/resources/application.yml`
- Modify: `.env.example`
- Modify: `docker-compose.yml`
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/failure-scenarios.md`
- Modify: `docs/decisions.md`

**Interfaces:**
- Documents the exact configuration and behavior implemented by Tasks 1-4.

- [ ] Add operator-facing defaults and Compose propagation.
- [ ] Document keys, scripts, lifecycle, failure cases, single-slot throughput tradeoff, lease limitations, and code-reading order.
- [ ] Run configuration binding tests and inspect the complete diff for scope and consistency.

### Task 6: Final verification

**Files:**
- Verify all changed files.

**Interfaces:**
- Produces exact test evidence and limitation report.

- [ ] Run backend unit and Testcontainers integration tests with exact command and results captured.
- [ ] Run benchmark tests if configuration propagation touches benchmark files.
- [ ] Review ACK ordering, attempt accounting, ownership fencing, metric cardinality, and every terminal cleanup path.
- [ ] Record branch/tooling limitation if Git remains unavailable; do not claim branch creation.
