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

These paths are designed for a local MVP. Public webhook destinations, authentication, secrets, and hosted deployment remain outside this scope.
