# EventFlow Performance Benchmark Design

## Intent

Build a reproducible performance evaluation framework that measures EventFlow's HTTP ingestion separately from durable webhook delivery. The framework is for architecture study and comparative experiments, not for publishing fabricated or environment-independent performance claims.

## Goals

- Generate concurrent event publications with configurable event count, publisher concurrency, payload size, worker count, and receiver latency.
- Separate warm-up traffic, a measured steady-state window, and queue drain.
- Report ingestion requests per second and API p50/p95/p99 latency from client observations.
- Report durable completed-delivery throughput and end-to-end p50/p95/p99 latency from PostgreSQL timestamps.
- Give every run a unique run ID, event type, endpoint, and idempotency-key namespace.
- Provide optional physical isolation through a unique Docker Compose project and fresh volumes.
- Export raw and summarized data as JSON and CSV, then generate comparison graphs from saved summaries.
- Capture host, Docker, container image, and benchmark configuration metadata.
- Test orchestration-independent calculations and benchmark receiver behavior without changing EventFlow delivery semantics.

## Non-goals and anti-goals

- Do not change delivery classification, retry behavior, leases, stream acknowledgement, idempotency, or persistence semantics.
- Do not add benchmark-only endpoints to the EventFlow API.
- Do not report benchmark results that were not produced by an actual run.
- Do not treat receiver arrival as durable delivery completion.
- Do not silently delete or reuse application data or named volumes.
- Do not turn the harness into a production observability system or a general load-testing platform.

## Current-system constraints

- `POST /api/v1/events` transactionally writes the event, matching jobs, and outbox rows to PostgreSQL.
- The outbox publisher claims at most 20 rows per scheduled invocation and runs every second.
- Each backend process reads at most 10 Redis Stream records every 500 ms and sends webhooks synchronously.
- The backend's `WORKER_ENABLED` switch controls the scheduled pipeline as a whole, including outbox publication and consumption.
- Multiple worker containers are safe because PostgreSQL claims use conditional state changes and outbox selection uses `FOR UPDATE SKIP LOCKED`.
- Redis carries job IDs; PostgreSQL supplies durable event, job, and attempt timestamps.
- The existing mock receiver uses single-threaded `HTTPServer`, so it cannot represent a neutral concurrent sink.
- The API caps delivery list pages at 100, making per-job API polling an intrusive and inefficient measurement source.

## Approaches considered

### Selected: external Python harness with a Docker Compose overlay

Run a benchmark-only receiver, harness, and configurable worker replicas alongside the normal PostgreSQL and Redis services. Keep the API instance's workers disabled and run worker containers without published host ports. Query PostgreSQL read-only after publication to determine queue drain, durable successes, and end-to-end latency.

This cleanly separates client-side ingestion observations from server-side completion facts and keeps benchmark code out of production paths. It requires a small Python dependency set for PostgreSQL access and plotting.

### Rejected: add benchmark controls and metrics to application services

Extending the API and existing mock receiver would reduce the number of benchmark components, but benchmark-only code would share production execution paths and could change the behavior being measured.

### Rejected: combine a generic load generator with independent correlation scripts

A tool such as k6 could produce HTTP load well, but correlating phase membership, event/job IDs, database completion, machine metadata, and comparison outputs would require a second orchestration layer. The split would make a run harder to reproduce and validate.

## Architecture

The benchmark will use a standalone Compose file rather than additively merging with the development stack; this prevents the normal worker-enabled backend, frontend, and single-threaded mock receiver from starting accidentally. It will reuse the existing backend build context and define:

- `benchmark-api`: one backend instance exposed on the normal API port with `WORKER_ENABLED=false`.
- `benchmark-worker`: the same backend image with workers enabled, no published ports, and a configurable replica count supplied by the runner.
- `benchmark-receiver`: a concurrent HTTP receiver that accepts EventFlow webhooks, applies configurable latency, and exposes only benchmark control/health functions.
- `benchmark-runner`: a Python CLI container with access to the API, receiver, and PostgreSQL networks.

The host entrypoint will create a unique run ID and Compose project name, start fresh project-scoped volumes, scale `benchmark-worker`, wait for health, execute the runner, and leave results in a bind-mounted run directory. Cleanup will target only that exact Compose project. A documented option will preserve the stack for diagnosis. Worker count must be at least one because the API instance has workers disabled.

## Run lifecycle

1. Validate configuration and create a unique run identifier.
2. Capture host and Docker metadata plus resolved benchmark settings from the host wrapper; the container runner separately records its cgroup/container view without requiring a Docker socket mount.
3. Start an isolated Compose project with fresh PostgreSQL and Redis volumes.
4. Register one unique endpoint subscribed to the run's unique event type.
5. Warm up with a configured number or duration of unmeasured events, using unique idempotency keys.
6. Wait for every warm-up job to succeed before steady-state measurement; abort if the warm-up drain timeout expires.
7. Publish the configured measured event count at fixed publisher concurrency and record monotonic request start/end times, UTC wall-clock start/end times, response status, event ID, and job IDs.
8. Stop publishing and enter queue drain, polling PostgreSQL until all measured jobs are terminal or the drain timeout expires.
9. Query measured events, jobs, and attempts by the unique event type and run namespace.
10. Compute reports, write artifacts, and optionally tear down the exact project and its volumes. A drain timeout still writes partial artifacts marked incomplete and returns a nonzero exit code.

Warm-up and measured traffic use distinct phase markers in both idempotency keys and payload metadata. Only measured traffic contributes to reported metrics.

## Measurement definitions

### Ingestion

- Successful ingestion: `POST /api/v1/events` returns HTTP 202 and exactly one job ID for the run's endpoint.
- API latency: client-observed monotonic elapsed time for each measured POST.
- Ingestion RPS: successful measured requests divided by the interval from the first measured request start to the last measured successful response completion.
- API percentiles: nearest-rank p50, p95, and p99 over successful measured request latencies, with the percentile algorithm identified in output metadata.

### Delivery

- Completed delivery: a measured job whose PostgreSQL status is `SUCCEEDED` with a successful, finished attempt.
- Delivery throughput: successful measured jobs divided by the interval from the earliest measured event creation time to the latest successful attempt finish time. The report will also include steady-state completion throughput: successful attempt completions whose `finished_at` falls between the first measured request's UTC start and the last measured response's UTC completion, divided by that exact wall-clock interval. This prevents comparisons from hiding post-publication drain work.
- End-to-end latency: successful attempt `finished_at` minus the corresponding event `created_at`.
- End-to-end percentiles: nearest-rank p50, p95, and p99 across successful measured jobs.
- Queue drain duration: time from the final measured publish completion until all measured jobs are terminal, or until timeout.

Incomplete, rejected, dead, retried, duplicate, and timed-out records remain visible in counts and raw CSV data; they are not silently folded into successful throughput.

## Configuration

The CLI and environment will support:

- measured event count;
- warm-up event count;
- publisher concurrency;
- worker container count;
- payload byte target;
- receiver latency in milliseconds;
- drain timeout and polling interval;
- API, receiver, and PostgreSQL connection settings;
- output directory and optional human label.

Payload size means the UTF-8 serialized byte length of the `payload` JSON value, excluding the API's `eventType` envelope and HTTP headers. Reports also record the actual full request-body byte length. Payload generation will target the requested payload size deterministically and reject values too small for its fixed payload structure or whose full request body exceeds EventFlow's one-megabyte request limit.

Receiver latency for successful-throughput runs is restricted to 0–4900 milliseconds because EventFlow has a fixed five-second outbound request timeout. Timeout and retry experiments at or above five seconds are outside this successful-throughput harness rather than being silently reported as capacity results.

## Isolation and identity

Each run uses a cryptographically random or UUID-based run ID. Event types, endpoint names, idempotency keys, payload phase markers, Compose project names, and result directories derive from that ID. The normal path starts fresh project-scoped database and Redis volumes. Direct runner invocation against an existing stack remains safe through logical isolation but will be documented as less suitable for clean comparisons.

## Output artifacts

Each run directory will contain:

- `config.json`: validated inputs and run identity;
- `machine.json`: operating system, CPU, memory, Docker engine, Compose, and container/image metadata available to the runner script;
- `summary.json`: definitions, counts, rates, percentiles, timestamps, and phase durations;
- `requests.csv`: one row per measured API request;
- `deliveries.csv`: one row per measured job with durable state and latency;
- `errors.json`: structured publication, drain, or query failures, when present.

The comparison command will read one or more `summary.json` files and emit a comparison CSV plus PNG graphs for throughput and latency percentiles. Graphs will always label run configuration and units.

The benchmark receiver will use bounded concurrent request handling and lock-protected receipt state. Its configured concurrency and receipt counts will be recorded so receiver saturation remains visible as a possible limiting factor rather than being mistaken for EventFlow capacity.

## Testing strategy

- Unit tests cover configuration validation, exact payload sizing, percentile boundaries, rate windows, run-scoped naming, phase filtering, delivery aggregation, and timeout/incomplete cases.
- Receiver tests cover concurrent requests, configured delay, delivery ID recording, and reset/isolation behavior.
- A lightweight integration/smoke test runs a small Docker benchmark and asserts artifact schemas and successful correlation when Docker is available; it will not assert performance thresholds.
- Existing backend tests remain unchanged and must continue to pass.

## Documentation and reproducibility

`benchmarks/README.md` will document prerequisites, the one-command isolated run, parameter overrides, comparison graph generation, artifact interpretation, measurement definitions, cleanup behavior, and known sources of variance. Examples will use placeholders or clearly marked sample paths, never invented measurements.

## Builder choices left open

- Exact Python module filenames may follow a small package layout under `benchmarks/`, provided orchestration, measurement, reporting, receiver, and tests remain separately testable.
- Plotting may use Matplotlib; PostgreSQL access may use Psycopg 3. Dependency versions must be pinned in the benchmark image.
- The host wrapper may be PowerShell-first for this repository's documented Windows workflow, with a portable direct `docker compose` command documented alongside it.
