# EventFlow Performance Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Docker-reproducible benchmark harness that independently measures EventFlow ingestion and durable webhook delivery without changing delivery semantics.

**Architecture:** A standalone benchmark Compose stack runs an API-only backend, scalable worker replicas, PostgreSQL, Redis, and a concurrent benchmark receiver. A Python runner generates phased load, correlates responses with PostgreSQL state, and writes reports; host scripts capture machine metadata and orchestrate isolated projects.

**Tech Stack:** Python 3.13, standard-library HTTP/concurrency/testing, Psycopg 3, Matplotlib, Docker Compose, PostgreSQL 16, Redis 7, existing Spring Boot backend.

**Spec:** `docs/superpowers/specs/2026-11-18-performance-benchmark-design.md`

## Global Constraints

- Do not modify EventFlow delivery classification, retries, leases, stream acknowledgement, idempotency, or persistence semantics.
- Every run uses unique identifiers and the default host workflow uses fresh Compose-project volumes.
- Only measured traffic contributes to reported performance metrics.
- Durable completion and end-to-end latency come from PostgreSQL, never receiver arrival alone.
- Do not include or claim invented performance results.
- Receiver latency is 0–4900 ms and worker count is at least one.

## Review Focus

- Partial publication failures must remain represented in artifacts without corrupting rate denominators; covered in Task 2 aggregation tests.
- Warm-up timeout must prevent measured traffic; covered in Task 3 orchestration tests.
- Drain timeout must preserve partial results, mark the run incomplete, and exit nonzero; covered in Task 3 orchestration tests.
- Payload boundary values must produce exact payload bytes while keeping the full request below 1 MiB; covered in Task 1 tests.
- Concurrent receiver recording must not lose delivery IDs; covered in Task 4 tests.

---

### Task 1: Benchmark Model and Metrics

**Files:**
- Create: `benchmarks/eventflow_bench/__init__.py`
- Create: `benchmarks/eventflow_bench/config.py`
- Create: `benchmarks/eventflow_bench/payloads.py`
- Create: `benchmarks/eventflow_bench/metrics.py`
- Test: `benchmarks/tests/test_config.py`
- Test: `benchmarks/tests/test_payloads.py`
- Test: `benchmarks/tests/test_metrics.py`

**Interfaces:**
- Produces: `BenchmarkConfig`, `RunIdentity`, `build_payload(target_bytes, run_id, phase, sequence)`, `nearest_rank(values, percentile)`, and summary aggregation functions used by the runner and reporter.

- [ ] Write failing unit tests for validation, unique naming, exact serialized payload size, nearest-rank boundaries, ingestion windows, completion windows, incomplete records, and phase filtering.
- [ ] Run `python -m unittest discover -s benchmarks/tests -p "test_*.py" -v` and verify failures are missing modules/functions.
- [ ] Implement the minimal model, payload, and metrics modules with typed dataclasses and UTC/monotonic timestamp separation.
- [ ] Re-run the unit suite and verify Task 1 tests pass.

### Task 2: PostgreSQL Collection and Artifact Reporting

**Files:**
- Create: `benchmarks/eventflow_bench/database.py`
- Create: `benchmarks/eventflow_bench/reporting.py`
- Create: `benchmarks/eventflow_bench/compare.py`
- Test: `benchmarks/tests/test_database.py`
- Test: `benchmarks/tests/test_reporting.py`
- Test: `benchmarks/tests/test_compare.py`

**Interfaces:**
- Consumes: Task 1 configuration and metrics.
- Produces: `PostgresCollector`, `write_run_artifacts(...)`, and comparison CLI functions that emit JSON, CSV, and PNG files.

- [ ] Write failing tests for parameterized run-scoped SQL, row normalization, complete and partial artifact schemas, CSV rows, comparison tables, and graph file generation.
- [ ] Run the focused tests and verify expected failures.
- [ ] Implement read-only PostgreSQL polling/query helpers, deterministic JSON/CSV writers, and Matplotlib comparison graphs.
- [ ] Re-run the focused tests and verify they pass.

### Task 3: Load Generation and Phase Orchestration

**Files:**
- Create: `benchmarks/eventflow_bench/http_client.py`
- Create: `benchmarks/eventflow_bench/runner.py`
- Create: `benchmarks/eventflow_bench/__main__.py`
- Test: `benchmarks/tests/test_http_client.py`
- Test: `benchmarks/tests/test_runner.py`

**Interfaces:**
- Consumes: Task 1 configuration/payload/metrics and Task 2 collector/reporting.
- Produces: `python -m eventflow_bench run ...` and `python -m eventflow_bench compare ...`.

- [ ] Write failing tests using fake HTTP and database adapters for endpoint registration, bounded concurrent publishing, unique idempotency keys, warm-up drain, steady-state collection, successful drain, and timeout partial-result behavior.
- [ ] Run the focused tests and verify expected failures.
- [ ] Implement injectable HTTP/database adapters and the phase state machine with guaranteed artifact writing.
- [ ] Re-run the focused tests and verify they pass.

### Task 4: Concurrent Benchmark Receiver

**Files:**
- Create: `benchmarks/receiver/server.py`
- Create: `benchmarks/receiver/Dockerfile`
- Test: `benchmarks/tests/test_receiver.py`

**Interfaces:**
- Produces: concurrent `/webhook`, `/health`, `/control`, `/received`, and `/reset` behavior for benchmark-only use.

- [ ] Write failing tests for concurrent delivery recording, lock-protected counts, reset, health, validation, and configurable 0–4900 ms delay.
- [ ] Run the focused receiver tests and verify expected failures.
- [ ] Implement a bounded threaded HTTP server and benchmark control surface.
- [ ] Re-run receiver tests and verify they pass.

### Task 5: Docker Orchestration, Metadata, and Documentation

**Files:**
- Create: `benchmarks/Dockerfile`
- Create: `benchmarks/requirements.txt`
- Create: `benchmarks/docker-compose.yml`
- Create: `benchmarks/run.ps1`
- Create: `benchmarks/capture-machine.ps1`
- Create: `benchmarks/README.md`
- Create: `benchmarks/tests/test_compose.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: all prior tasks.
- Produces: isolated one-command benchmark runs, configurable worker scaling, host/container metadata, cleanup, and documented comparison workflow.

- [ ] Write static failing tests that parse the Compose configuration and scripts to verify API workers are disabled, worker replicas have no host port, benchmark receiver is used, results are bind-mounted, project names are unique, and cleanup is narrowly scoped.
- [ ] Run the full unit suite and verify the new static tests fail.
- [ ] Implement pinned benchmark images, standalone Compose services, PowerShell orchestration and metadata capture, plus reproducible documentation.
- [ ] Run the full unit suite and verify all tests pass.
- [ ] Run `docker compose -f benchmarks/docker-compose.yml config` and verify the standalone stack resolves.
- [ ] Build the benchmark receiver and runner images.
- [ ] Run a small isolated smoke benchmark without performance assertions; verify JSON/CSV/PNG artifact schemas and successful job correlation.
- [ ] Run existing backend tests if the environment supports Docker/Testcontainers; otherwise report the exact limitation.

## Self-review

- Requirements 1–10 map to the current-system section in the design and Tasks 1–5.
- Interfaces use consistent names across tasks.
- Tests cover the five review-focus failure modes.
- No task changes application delivery code or asserts a performance threshold.
