# Failure scenarios

| Scenario | Recovery path |
| --- | --- |
| Redis is down during event creation | The API commits event, jobs, and outbox in PostgreSQL. The publisher resumes when Redis returns. |
| Publisher crashes after `XADD` | The outbox lease expires and the job ID may be published again. Database job claims suppress concurrent sends. |
| Worker crashes before HTTP | Its lease expires; the unfinished attempt becomes `UNKNOWN` and a retry is scheduled. |
| Worker crashes after receiver accepts HTTP | The job may retry and the receiver may see a duplicate. The delivery ID header is stable. |
| Redis loses a published Stream entry | A PostgreSQL scan creates a new outbox row for an old queued job. |
| Redis loses the retry sorted set | The PostgreSQL due scan still queues scheduled retries. |
| Scheduler instances race | A conditional database transition lets only one instance create the next outbox row. |
| Redis is unavailable before distributed admission | The worker fails closed, releases local capacity, leaves the job `QUEUED` and the entry pending, and consumes no attempt. `XAUTOCLAIM` resumes after Redis recovery. |
| Worker crashes while holding distributed permits | Redis lease expiry recovers admission capacity; PostgreSQL delivery-lease recovery independently records the unfinished attempt as `UNKNOWN`. |
| Permit renewal fails during HTTP | The HTTP request is not cancelled. Lease expiry may allow temporary concurrency overlap, while conditional PostgreSQL finalization fences stale delivery owners. |
| Redis failover loses recent admission keys | New work fails closed during unavailability, but temporary over-admission is possible after failover; the design does not claim strict limits under admission-state loss. |
| Local executor rejects admitted work | Both distributed and local permits are released, the entry is not ACKed, and no attempt is consumed. |

These paths are designed for a local MVP. Public webhook destinations, authentication, secrets, and hosted deployment remain outside this scope.
