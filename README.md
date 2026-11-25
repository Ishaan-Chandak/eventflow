# EventFlow

Local webhook delivery platform built with Java 21, Spring Boot 3.4.5, Maven 3.9.9, PostgreSQL 16.9, Redis 7.4.2, React 19.3.1, Vite 8.3.8, TypeScript 6.0.3, Node 24.22.1, nginx 1.30.6, and Python 3.13. Frontend dependency versions are locked in `frontend/package-lock.json`.

Start Docker Desktop, then run `docker compose up -d --build` from this directory. Open the dashboard at **http://localhost:3000**. The API is at `http://localhost:8080` and health at `http://localhost:8080/actuator/health`. Stop with `docker compose down`.

In the dashboard, register an endpoint using `http://mock-receiver:8082/webhook` and `order.created`, publish a JSON event, then open its delivery to see attempts. The Deliveries page has local mock receiver controls for HTTP 200, 500, 400, and timeout. Set 500, publish another event, then set 200 to watch retry recovery. Set 400 to create a DEAD job and use its Retry button after restoring 200.

Register a receiver:

```sh
curl -X POST http://localhost:8080/api/v1/endpoints -H 'Content-Type: application/json' -d '{"name":"Demo","url":"http://mock-receiver:8082/webhook","eventTypes":["order.created"],"enabled":true}'
```

Publish an event:

```sh
curl -X POST http://localhost:8080/api/v1/events -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' -d '{"eventType":"order.created","payload":{"orderId":"o-123"}}'
```

Use the returned job ID with `GET /api/v1/deliveries/{id}`. The mock receiver exposes received delivery IDs at `http://localhost:8082/received`.

To demo failures, set the local receiver to HTTP 500, 400, 200, or `timeout`:

```sh
curl -X POST http://localhost:8082/control -H 'Content-Type: application/json' -d '{"status":500}'
```

HTTP 500, 408, 429, timeout, and network errors retry with exponential backoff. Other 4xx become `DEAD`. Read a delivery's `attempts` and `next_attempt_at` using `GET /api/v1/deliveries/{id}`. Retry a `DEAD` delivery with `POST /api/v1/deliveries/{id}/retry`. This starts a new five-attempt cycle while keeping its lifetime attempt history.

Run backend tests with `cd backend && mvn test`. If Java/Maven are not installed on the host, run from the repository root on Windows:

```bat
docker run --rm --mount type=bind,source=D:\Projects\eventflow\backend,target=/app --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock -w /app -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal maven:3.9.9-eclipse-temurin-21 mvn test
```

Replace the `source=` path if the checkout is elsewhere. For frontend development, run `cd frontend && npm ci && npm run dev` with the Compose backend running; Vite serves at `http://localhost:5173`. Frontend checks are `npm run typecheck`, `npm run lint`, and `npm run build`. GitHub Actions runs backend tests and all three frontend checks.

Workers use PostgreSQL leases and reclaim stale Redis pending messages. An expired lease records an `UNKNOWN` attempt and retries within the remaining cycle budget. PostgreSQL scans republish old queued jobs and find due retries even if Redis loses its stream or sorted-set index. Delivery remains **at least once**: the receiver may see a request just before a worker crashes. Use `X-EventFlow-Delivery-Id` to deduplicate at the receiver. See [architecture](docs/architecture.md), [failure scenarios](docs/failure-scenarios.md), and [decisions](docs/decisions.md).

Local outbound delivery concurrency is bounded per backend process. `LOCAL_DELIVERY_CONCURRENCY` defaults to `1`; raise it only with enough PostgreSQL pool capacity and after measuring the workload. `HTTP_REQUEST_TIMEOUT` defaults to `5s`, `LEASE_SECONDS` to `15`, `PENDING_IDLE_SECONDS` to `20`, and `DELIVERY_SHUTDOWN_GRACE` to `5s`. Startup validation requires the HTTP timeout to be shorter than the lease and pending idle to exceed the lease. Actuator exposes low-cardinality delivery metrics at `/actuator/metrics`.

Distributed admission is mandatory for new HTTP deliveries and fails closed when Redis is unavailable. `GLOBAL_DELIVERY_CONCURRENCY` and `ENDPOINT_DELIVERY_CONCURRENCY` both default to `1`. `DISTRIBUTED_PERMIT_LEASE` defaults to `30s` and `DISTRIBUTED_PERMIT_RENEWAL` to `10s`; renewal must be no more than one third of the lease. `CAPACITY_DEFERRAL` defaults to `20s` and must be at least `PENDING_IDLE_SECONDS`. Admission atomically reserves global and endpoint capacity before executor submission. Saturated entries remain pending, record only a PostgreSQL deferral deadline, and consume no delivery attempt until `XAUTOCLAIM` makes them eligible again.

This is a local-only demo. Ports bind to `127.0.0.1`, and webhook destinations are limited to `mock-receiver` by default. It has no authentication, multitenancy, hosted deployment, or public destination SSRF protection. Do not expose it to the internet without adding those controls.

For reproducible ingestion and durable-delivery performance evaluation, including configurable load, worker replicas, receiver latency, JSON/CSV output, and comparison graphs, see [benchmarks/README.md](benchmarks/README.md). The benchmark stack is isolated from the development stack and does not include invented results.
