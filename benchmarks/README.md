# EventFlow performance benchmarks

This directory contains an isolated load generator for measuring EventFlow ingestion and durable webhook delivery. It does not change the API, worker, retry, or delivery semantics, and it contains no precomputed performance claims.

## What the benchmark measures

- **Ingestion RPS**: successful measured `POST /api/v1/events` responses divided by the interval from the first successful request start to the last successful response completion.
- **API latency p50/p95/p99**: client-observed elapsed time for successful measured requests, using nearest-rank percentiles.
- **Drained delivery throughput**: successful measured jobs divided by the interval from the earliest measured PostgreSQL event creation to the latest successful attempt completion.
- **Steady-state delivery throughput**: successful attempt completions during the measured publication wall-clock window divided by that window.
- **End-to-end latency p50/p95/p99**: PostgreSQL successful attempt `finished_at` minus event `created_at`.
- **Queue drain**: the phase after publication in which the runner waits for all accepted measured jobs to reach `SUCCEEDED` or `DEAD`.

Receiver arrival timestamps are diagnostic only. A delivery counts as complete only after PostgreSQL records `SUCCEEDED` and a finished successful attempt.

## Current capacity boundaries

The application currently publishes up to 20 outbox rows per scheduled one-second invocation per backend process. Each backend process reads up to 10 Redis Stream messages per 500 ms invocation and performs outbound HTTP synchronously. Worker count therefore means backend worker containers, not threads inside one process. PostgreSQL conditional claims and `FOR UPDATE SKIP LOCKED` make replica scaling safe. The development mock receiver is deliberately not used because its `HTTPServer` handles one request at a time; the benchmark receiver defaults to 128 bounded concurrent requests.

## Reproducible Docker run

Prerequisites: Docker Desktop with Compose, PowerShell 7 or Windows PowerShell 5.1, and enough local resources for PostgreSQL, Redis, the API, and the requested worker count.

From the repository root:

```powershell
.\benchmarks\run.ps1 -EventCount 10000 -WarmupCount 1000 -PublisherConcurrency 32 -WorkerCount 4 -PayloadBytes 1024 -ReceiverLatencyMs 10
```

Every invocation generates a new UUID, Compose project, event type, endpoint, idempotency-key namespace, result directory, PostgreSQL volume, and Redis instance. The exact project and its volumes are removed after the run unless `-KeepStack` is supplied. A warm-up failure aborts measured traffic. A drain timeout writes partial artifacts, marks the summary incomplete, and exits nonzero.

Important parameters:

| Parameter | Default | Constraint |
|---|---:|---|
| `EventCount` | 1000 | at least 1 |
| `WarmupCount` | 100 | at least 0 |
| `PublisherConcurrency` | 10 | at least 1 |
| `WorkerCount` | 1 | at least 1 |
| `PayloadBytes` | 1024 | serialized UTF-8 `payload` JSON bytes, excluding the API envelope |
| `ReceiverLatencyMs` | 0 | 0–4900; EventFlow's outbound timeout is five seconds |
| `ReceiverMaxConcurrency` | 128 | bounded receiver request capacity |
| `DrainTimeoutSeconds` | 120 | at least 1 |

The actual full request-body byte size is recorded alongside the requested payload size.

## Artifacts

Runs are written beneath `benchmarks/results/<run-id>/`:

- `config.json`: validated settings;
- `machine.json`: host CPU, memory, OS, Docker engine, and Docker resource limits;
- `summary.json`: run identity, metric definitions, counts, throughput, percentiles, and completion state;
- `requests.csv`: measured and warm-up API observations with phase markers;
- `deliveries.csv`: durable measured-job states and end-to-end latencies;
- `errors.json`: structured failures or an empty array.

Do not compare runs from materially different machine allocations, thermal states, image versions, or background workloads.

## Comparison graphs

Build the runner once, then compare any saved summaries:

```powershell
$env:BENCHMARK_RESULTS_DIR = (Resolve-Path .\benchmarks\results).Path
docker compose -f .\benchmarks\docker-compose.yml build benchmark-runner
docker compose -f .\benchmarks\docker-compose.yml run --rm --no-deps benchmark-runner compare --output-dir /results/comparison /results/<run-a>/summary.json /results/<run-b>/summary.json
```

This creates `comparison.csv`, `throughput.png`, and `latency-percentiles.png`. Labels and units come from the saved summaries; no results are synthesized.

## Direct Compose operation

The Compose file is standalone and intentionally omits the frontend and development mock receiver. Use a unique project name and result directory when invoking it manually:

```powershell
$env:BENCHMARK_RESULTS_DIR = (New-Item -ItemType Directory -Force .\benchmarks\results\manual).FullName
docker compose -p eventflow-bench-manual -f .\benchmarks\docker-compose.yml up -d --build --scale benchmark-worker=2 postgres redis benchmark-receiver benchmark-api benchmark-worker
```

Direct runs against an existing application stack retain logical isolation through the unique event type and idempotency keys but do not provide the clean database and Redis state used by `run.ps1`.

## Tests

The runner image contains pinned Python dependencies. After building it:

```powershell
docker build -t eventflow-benchmark-tests .\benchmarks
docker run --rm --mount type=bind,source=${PWD},target=/app -w /app -e PYTHONPATH=benchmarks --entrypoint python eventflow-benchmark-tests -m unittest discover -s benchmarks/tests -p 'test_*.py' -v
```

Tests assert calculations, isolation, phase behavior, artifact schemas, receiver concurrency, and Compose roles. They never assert a performance threshold.
