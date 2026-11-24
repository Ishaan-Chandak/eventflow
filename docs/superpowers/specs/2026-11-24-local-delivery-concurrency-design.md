# Local Delivery Concurrency Design

## Intent

Phase 1 adds bounded, configurable concurrency inside one EventFlow worker process without changing its durable PostgreSQL/outbox model or at-least-once delivery contract. The default remains one local delivery at a time; operators can raise it for measured workloads.

## Goals and constraints

- Reserve local capacity before `XREADGROUP` or `XAUTOCLAIM` and share that capacity across both paths.
- Never hold more admitted messages than configured local concurrency, and never use an unbounded executor queue.
- Keep Redis ACK after a durable PostgreSQL terminal or retry-scheduled transition.
- Let rejected, cancelled, interrupted, crashed, or unfinalized work remain recoverable from the PEL and existing lease recovery.
- Shut down within a configured bounded grace period without false ACKs.
- Add low-cardinality Micrometer timing, utilization, activity, rejection, and outcome metrics.
- Preserve API, headers, retry behavior, transactional outbox, and conditional PostgreSQL claims.

Non-goals are distributed semaphores, circuit breakers, fairness, rate limiting, and publisher/consumer separation.

## Approaches considered

1. **Bounded executor queue as admission control.** Simple, but it cannot reserve capacity before Redis fetches and makes unused-read and shutdown permit ownership indirect.
2. **Shared semaphore plus a fixed executor (selected).** A local capacity component reserves permits before Redis operations and transfers one permit to each submitted task. A fixed-size executor with no backlog beyond admitted capacity provides explicit ownership and deterministic cleanup.
3. **Virtual threads with a semaphore.** Feasible on Java 21, but adds no value for the conservative Phase 1 bound and makes executor saturation/rejection behavior less explicit.

## Components and flow

`DeliveryProperties` binds and validates concurrency, executor capacity, shutdown grace, HTTP timeout, lease, and pending-idle timing. `LocalDeliveryExecutor` owns the `ThreadPoolExecutor`, shared capacity semaphore, lifecycle, and executor gauges/counters. `DeliveryMetrics` owns failure-isolated Micrometer recording with bounded result tags. `DeliveryPipeline` remains responsible for Redis polling, PostgreSQL claim/finalize transactions, outbound HTTP, and ACK ordering.

For each poll, the pipeline reserves up to the configured free capacity, asks Redis for no more than that reservation, releases reservations not matched by returned messages, and submits each returned message with exactly one reservation. The task calls `deliver`; only `true` triggers ACK. Its permit is released exactly once in `finally`. Rejection releases immediately and does not ACK.

Each PostgreSQL claim uses a unique per-attempt lease token, so a late response from an expired attempt cannot finalize a newer attempt in the same process. Delivery completion immediately refills available capacity without waiting for the scheduled idle poll. Because EventFlow uses only the response status, the Java HTTP response stream is closed as soon as headers arrive rather than waiting indefinitely for a response body.

## Failure and timing behavior

- Empty/short Redis reads and Redis exceptions release all unused reservations.
- Invalid messages, submission rejection, task exceptions, interruption, cancellation, or failed durable finalization do not ACK.
- A duplicate message whose job is already durably outside `IN_PROGRESS` may be ACKed; an actively leased job returns false and remains pending.
- HTTP timeout must remain below the job lease, and pending idle must remain above the lease. These validated inequalities prevent normal `XAUTOCLAIM` from stealing an active task before its lease should expire.
- `LeaseRecovery` remains authoritative after process loss. Conditional finalization by `lease_owner` prevents a stale task from overwriting recovered state.
- Shutdown stops admission, waits only for the configured grace, then cancels queued/running work. Unstarted wrappers release their permits; no cancellation path ACKs.

## Observability

Metrics use only bounded labels such as `success`, `transient_failure`, `permanent_failure`, `timeout`, `network_error`, and `unfinalized`. Identifiers and URLs remain in structured logs, never metric tags. Metric recording is guarded so registry/instrument failures cannot alter delivery outcomes.

## Documentation and benchmark

`application.yml`, `.env.example`, and benchmark documentation expose the local concurrency setting and defaults. Benchmark runs pass the value to worker containers so local concurrency can be measured independently from worker replica count; no throughput claim is made without results.
