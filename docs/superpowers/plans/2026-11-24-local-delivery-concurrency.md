# Local Delivery Concurrency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add safe, configurable, bounded local webhook delivery concurrency with backpressure, shutdown, and Micrometer observability.

**Architecture:** A validated configuration bean feeds a Spring-managed fixed `ThreadPoolExecutor` and shared semaphore. Both Redis intake paths reserve capacity before fetching and transfer permits to async tasks that ACK only after durable completion.

**Tech Stack:** Java 21, Spring Boot 3.4, JDBC/PostgreSQL, Spring Data Redis/Lettuce, Micrometer, JUnit 5, Testcontainers.

**Spec:** `docs/superpowers/specs/2026-11-24-local-delivery-concurrency-design.md`

## Global Constraints

- Default local concurrency is 1; execution capacity is bounded.
- HTTP timeout is less than the job lease, which is less than pending idle.
- Redis ACK happens only after durable handling.
- No identifier or URL metric labels; metrics cannot change business outcomes.
- No Phase 2 distributed coordination or unrelated refactors.

## Review Focus

- Reservation ownership across empty reads, short reads, exceptions, rejection, cancellation, and shutdown.
- ACK ordering relative to final PostgreSQL commit.
- Lease expiry or `XAUTOCLAIM` racing an active HTTP request.
- Executor shutdown returning queued wrappers and avoiding permit leaks.
- Metric exceptions or unexpected result classification changing delivery outcomes.

---

### Task 1: Validated configuration and bounded executor

**Files:**
- Create: `backend/src/main/java/dev/eventflow/worker/DeliveryProperties.java`
- Create: `backend/src/main/java/dev/eventflow/worker/LocalDeliveryExecutor.java`
- Create: `backend/src/test/java/dev/eventflow/worker/LocalDeliveryExecutorTest.java`
- Modify: `backend/src/main/java/dev/eventflow/EventFlowApplication.java`

**Interfaces:**
- Produces: `LocalDeliveryExecutor.reserve(int)`, reservation submission/release, lifecycle shutdown, capacity/active state.

- [ ] Write tests for concurrency bound, exhausted admission, rejection cleanup, task exception cleanup, cancellation, and bounded graceful shutdown.
- [ ] Run the focused tests and verify they fail because the new types do not exist.
- [ ] Implement validated properties and the minimal executor/admission lifecycle.
- [ ] Run focused tests and the backend unit suite.

### Task 2: Async Redis intake and durable ACK ordering

**Files:**
- Modify: `backend/src/main/java/dev/eventflow/worker/DeliveryPipeline.java`
- Create: `backend/src/test/java/dev/eventflow/worker/DeliveryPipelineConcurrencyTest.java`
- Modify: `backend/src/test/java/dev/eventflow/HappyPathIntegrationTest.java`

**Interfaces:**
- Consumes: Task 1 admission/submission API.
- Produces: non-blocking `consume()` and `reclaimPending()` sharing local capacity.

- [ ] Write failing tests proving no read at zero capacity, read count equals reservation, rejection/no-finalization does not ACK, completion ACKs, and permits return after failures.
- [ ] Implement capacity-aware reads, async task wrappers, structured failure logs, and ACK-after-commit behavior.
- [ ] Add/adjust integration coverage for concurrency, timeout, pending recovery, and lease expiry.
- [ ] Run focused and integration tests.

### Task 3: Micrometer observability

**Files:**
- Create: `backend/src/main/java/dev/eventflow/worker/DeliveryMetrics.java`
- Create: `backend/src/test/java/dev/eventflow/worker/DeliveryMetricsTest.java`
- Modify: `backend/src/main/java/dev/eventflow/worker/DeliveryPipeline.java`
- Modify: `backend/src/main/java/dev/eventflow/worker/LocalDeliveryExecutor.java`
- Modify: `backend/src/main/resources/application.yml`

**Interfaces:**
- Produces: bounded-tag HTTP timing/outcome counters and executor gauges/rejection counter.

- [ ] Write failing tests for bounded result tags and metric-failure isolation.
- [ ] Implement metrics and instrument executor and durable outcomes.
- [ ] Run metrics, worker, and backend tests.

### Task 4: Operator documentation and final verification

**Files:**
- Modify: `backend/src/main/resources/application.yml`
- Modify: `.env.example`
- Modify: `README.md`
- Modify: `benchmarks/docker-compose.yml`
- Modify: `benchmarks/run.ps1`
- Modify: `benchmarks/README.md`
- Modify: `benchmarks/tests/test_compose.py`

**Interfaces:**
- Consumes: configuration names from Task 1.

- [ ] Add behavior tests for benchmark propagation and watch them fail.
- [ ] Document defaults and pass local concurrency into benchmark worker containers/commands.
- [ ] Run backend tests, benchmark tests, and available integration tests.
- [ ] Review the complete diff for races, leaks, ACK ordering, and scope.
