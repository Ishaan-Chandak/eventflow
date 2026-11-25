# Distributed Concurrency and Endpoint Isolation Design

## Intent

Phase 2 adds Redis-backed delivery admission shared by all EventFlow worker replicas. A delivery must own both a global permit and a permit for its destination endpoint before it can enter the local executor or claim a PostgreSQL delivery attempt. PostgreSQL remains authoritative for delivery state, and the existing transactional outbox, at-least-once delivery, conditional delivery leases, pending-entry recovery, and reconciliation paths remain intact.

## Goals

- Enforce configurable global and per-endpoint concurrency during healthy, timely Redis operation across multiple replicas.
- Acquire the global and endpoint permit atomically, without making executor threads wait.
- Identify every acquisition with a cryptographically strong unique token; process or worker identity is metadata, not ownership.
- Make release and renewal ownership-aware and idempotent, with finite lease expiry so process loss cannot leak capacity forever.
- Defer saturated deliveries without consuming a delivery attempt, marking a job failed, or creating a tight retry loop.
- Fail closed for new HTTP deliveries when Redis admission is unavailable while allowing already admitted requests to finish normally.
- Keep Redis Stream ACK after the relevant durable PostgreSQL transition.
- Expose bounded-cardinality Micrometer telemetry for admission, utilization, recovery, renewal, and deferral.

## Non-goals

Circuit breakers, request-rate limiting, priority queues, advanced fairness, and unrelated worker refactoring are not part of Phase 2.

## Approaches considered

1. **Redis Stream PEL deferral (selected).** A denied or unavailable admission remains unacknowledged in the consumer group's pending-entry list. `XAUTOCLAIM` makes it eligible again only after the configured pending idle interval. The PostgreSQL job stays `QUEUED`, no attempt is created, and old queued-job reconciliation remains the backstop if Redis loses the stream or PEL.
2. **PostgreSQL `CAPACITY_DEFERRED` state.** This would ACK after a new database transition and later recreate an outbox message. It provides database-visible scheduling, but adds a delivery state, scheduler path, and sustained PostgreSQL write amplification during saturation.
3. **Dedicated Redis deferral sorted set.** This avoids PEL growth but creates another non-authoritative Redis queue whose loss and reconciliation would require new machinery.

PEL deferral changes the fewest durable-state invariants and directly reuses Phase 1 recovery. A `capacity_deferred_until` timestamp on the still-`QUEUED` PostgreSQL job prevents reconciliation from repeatedly publishing the same saturated job; it is scheduling metadata, not an attempt or failure state. The approach is deliberately coarse: endpoint fairness is not guaranteed, but new stream entries continue to be consumed and other endpoints can progress while saturated entries wait in the PEL.

## Redis key model and Cluster compatibility

All keys touched by one Lua invocation contain the same Redis Cluster hash tag, `{eventflow:admission}`, so they occupy one slot:

- `eventflow:{eventflow:admission}:global` — sorted set of acquisition token to lease-expiry epoch milliseconds.
- `eventflow:{eventflow:admission}:endpoint:<endpoint-uuid>` — sorted set of the same token and expiry for one endpoint.

The token is a fresh UUID for each acquisition. No non-expiring counter is maintained. Sorted-set cardinality after removal of expired members is the source of truth for utilization. Endpoint UUIDs appear in Redis keys and structured logs but never as metric tags.

The fixed hash tag intentionally concentrates admission data in one Redis Cluster slot. This is required for atomic global-plus-endpoint Lua operations and is an explicit scalability tradeoff.

## Lua operations

### Acquire

The acquire script receives the global key and selected endpoint key, the unique token, lease duration, global limit, and endpoint limit. It obtains Redis server time, removes expired members from both sets, checks both cardinalities, and inserts the token with one expiry into both sets only when both limits have room. It returns a bounded reason (`acquired`, `global_saturated`, or `endpoint_saturated`) plus utilization counts and the number of expired members recovered. Redis server time prevents worker clock skew from changing lease validity.

Acquisition is all-or-nothing. Redis errors and unexpected script results are classified as `redis_unavailable` and fail closed.

### Renew

The renewal script uses Redis server time and extends the expiry only if the same token exists in both the global and endpoint sets. If either membership is missing, it removes the token from both sets and reports ownership lost. A stale task therefore cannot recreate or renew an expired/reassigned permit.

### Release

The release script removes the supplied token from both sets and returns whether a live ownership pair was removed. Repeated release is harmless. A stale owner cannot release a newer acquisition because every acquisition has a different token.

## Admission and delivery lifecycle

1. `DeliveryPipeline.consume` or `reclaimPending` reserves local capacity before reading, as in Phase 1.
2. For every returned entry, the polling thread validates the job ID and reads the endpoint ID from PostgreSQL without changing delivery state.
3. The polling thread attempts the atomic distributed acquisition. It never waits for capacity.
4. On saturation, Redis unavailability, invalid/non-queued state, or executor rejection, local capacity is released immediately. Saturation/unavailability leaves the stream entry unacknowledged; it becomes eligible through `XAUTOCLAIM` after the configured idle interval. Executor rejection also releases the distributed token.
5. Only an admitted task enters the executor. It starts a renewal schedule, then performs the existing conditional PostgreSQL claim and HTTP delivery.
6. If the PostgreSQL claim finds a durably terminal/non-active job, the existing duplicate handling may ACK it. If another live claim owns it, it remains pending.
7. Successful, retry-scheduled, and dead outcomes are committed in PostgreSQL before ACK. Failure to finalize does not ACK.
8. Renewal is cancelled and distributed release runs in `finally` on every path, including HTTP exceptions, interruption, failed finalization, and cancellation. Rejection releases before returning from submission.

The permit abstraction owns token, endpoint key, expiry state, renewal, and an atomic local `closed` flag so release happens at most once from the process while Redis release itself remains idempotent.

## Lease duration and renewal

The distributed permit lease and renewal interval are configurable. Validation requires a positive lease, a positive renewal interval, and renewal comfortably before expiry (renewal interval at most one third of the lease). The default lease is longer than the normal HTTP timeout, but renewal still protects configured long-running deliveries and process pauses shorter than the remaining lease.

Renewal executes on a dedicated bounded scheduler, never on delivery executor threads. A renewal Redis error increments a failure metric but does not abort an in-flight HTTP request. Once the lease expires, EventFlow no longer claims strict distributed exclusion for that request.

## Failure behavior and guarantees

- **Redis unavailable before acquisition:** new deliveries fail closed, remain `QUEUED` and pending, consume no attempt, and are reclaimed after the pending-idle delay. Local reservations are returned immediately.
- **Redis recovers:** later `XAUTOCLAIM` calls reacquire capacity and resume delivery. PostgreSQL reconciliation recreates missing queue work if Redis lost it.
- **Process crash:** permit leases expire. A later acquire removes expired members and records recovered permits. PostgreSQL delivery-lease recovery independently marks an unfinished attempt `UNKNOWN` and schedules retry.
- **Process pause or network partition:** renewal may miss its deadline. Redis can expire the permit and admit another delivery while the original HTTP request is still running. PostgreSQL's conditional lease owner prevents stale database finalization, but cannot retract an already sent HTTP request. Therefore the mechanism provides bounded lease-based concurrency, not strict global concurrency under arbitrary failures.
- **Redis failover:** completed Lua calls are atomic on the active primary, but replication/failover may lose recent admission state depending on Redis durability settings. New admissions fail closed while unavailable; after failover, temporary over-admission is possible if lease state was lost.
- **In-flight Redis outage:** admitted HTTP work continues. Release/renew may fail, but expiry prevents permanent leaks. The PostgreSQL outcome still controls ACK and retry state.
- **Long HTTP request:** timely renewals retain ownership. Renewal loss does not cancel the request; it is reported and the lease-based limitation applies.
- **Executor rejection or shutdown:** distributed and local permits are released, no HTTP call begins, no attempt is consumed, and the entry is not ACKed.
- **Delivery exception or durable-finalization failure:** both permits release in `finally`; ACK behavior remains unchanged.

## Deferral and hot-loop control

Capacity denial updates only `capacity_deferred_until` and `updated_at`; it does not change job status, create a `delivery_attempts` row, increment attempt counters, or ACK the entry. The timestamp suppresses reconciliation duplicates while the entry waits in the PEL until `XAUTOCLAIM`'s minimum idle time elapses. The configurable capacity-deferral interval is validated to be at least the pending idle interval. New entries can still be read from the stream, so one saturated endpoint does not stop independent endpoints; advanced fairness is intentionally deferred.

## Configuration

Phase 2 adds configuration under `eventflow` for:

- global concurrency limit;
- default per-endpoint concurrency limit;
- permit lease duration;
- renewal interval;
- capacity-deferral/pending reclaim idle duration.

Defaults will preserve a conservative single-delivery posture unless explicitly raised. Existing local concurrency remains a per-replica upper bound and should normally be no greater than the global limit, though validation will not require that because replicas can use different local sizing during rollout.

## Metrics

Metrics use bounded tags only:

- global utilization gauge and endpoint-utilization distribution summary without endpoint tags;
- admission acquisition latency timer tagged by bounded result;
- admission rejection counter tagged by `global_saturated`, `endpoint_saturated`, `redis_unavailable`, `ownership_lost`, or `executor_rejected`;
- permit recovery counter by `global` or `endpoint` scope;
- renewal failure counter by bounded reason;
- deferral counter by bounded reason;
- active distributed-permit gauge local to the process.

No endpoint ID, job ID, token, URL, or worker ID is a metric label.

## Verification strategy

Deterministic unit tests cover script result mapping, configuration validation, unique ownership, duplicate/stale release, renewal ownership loss, every cleanup path, metrics cardinality, and no attempt consumption on denial. Redis Testcontainers tests execute the real Lua scripts for atomic paired acquisition, global and endpoint enforcement across independent coordinator instances, endpoint isolation, expiry recovery, renewal, outages, and recovery. Pipeline integration tests use latches/barriers and controlled executors to cover multi-replica delivery bounds, long-running requests, executor rejection, delivery failure, ACK ordering, accepted-job recovery, and saturation without arbitrary timing sleeps where synchronization is possible.

## Tradeoffs and limitations

- Lease expiry makes crash recovery automatic but prevents a claim of strict concurrency during long pauses, partitions, failover state loss, or renewal failure.
- One Cluster slot is a deliberate atomicity bottleneck.
- PEL deferral is durable within Redis and recoverable through PostgreSQL reconciliation, but is not a PostgreSQL-visible scheduled state.
- Per-endpoint limits use one Redis key per active endpoint; keys become empty after all members expire or release but may remain as empty key metadata until Redis deletes the empty sorted set.
- The design provides endpoint isolation, not fairness or priority.
