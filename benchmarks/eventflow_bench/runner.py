from __future__ import annotations

from pathlib import Path
from typing import Any
import time

from .config import BenchmarkConfig, RunIdentity
from .metrics import summarize_delivery, summarize_ingestion
from .reporting import write_run_artifacts
from .payloads import validate_payload_size


class WarmupFailed(RuntimeError):
    pass


class BenchmarkRunner:
    def __init__(self, config: BenchmarkConfig, identity: RunIdentity, http: Any, database: Any, machine: dict[str, Any]):
        self.config = config
        self.identity = identity
        self.http = http
        self.database = database
        self.machine = machine
        self._requests: list[dict[str, Any]] = []
        self._endpoint_id = ""

    def _write(self, summary: dict[str, Any], requests: list[dict[str, Any]], deliveries: list[dict[str, Any]], errors: list[dict[str, Any]]) -> None:
        write_run_artifacts(Path(self.config.output_dir), self.config.to_public_dict(), self.machine, summary, requests, deliveries, errors)

    def run(self) -> int:
        try:
            return self._execute()
        except WarmupFailed:
            raise
        except Exception as error:
            summary = self._base_summary(self._endpoint_id, False)
            if self._requests:
                summary["ingestion"] = summarize_ingestion(self._requests)
            self._write(summary, self._requests, [], [{"code": "runner_exception", "error": f"{type(error).__name__}: {error}"}])
            return 4

    def _execute(self) -> int:
        validate_payload_size(
            self.config.payload_bytes, self.identity, self.config.event_count, self.config.warmup_count
        )
        if hasattr(self.http, "wait_until_ready"):
            self.http.wait_until_ready(self.config.drain_timeout_seconds, self.config.poll_interval_seconds)
        self.http.configure_receiver(self.config.receiver_latency_ms)
        endpoint_id = self.http.register_endpoint(self.identity, self.config.receiver_url)
        self._endpoint_id = endpoint_id
        if self.config.warmup_count:
            warmup = self.http.publish_many(self.identity, "warmup", self.config.warmup_count, self.config.publisher_concurrency, self.config.payload_bytes)
            self._requests.extend(warmup)
            accepted = sum(1 for row in warmup if row["success"])
            warmup_job_ids = [str(row["job_id"]) for row in warmup if row.get("job_id")]
            warmup_ok, counts = self.database.wait_for_terminal(
                self.identity.event_type, "warmup", accepted, self.config.drain_timeout_seconds,
                self.config.poll_interval_seconds, job_ids=warmup_job_ids,
            )
            if accepted != self.config.warmup_count or not warmup_ok:
                errors = [{"code": "warmup_failed", "accepted": accepted, "expected": self.config.warmup_count, "statuses": counts}]
                summary = self._base_summary(endpoint_id, False)
                self._write(summary, self._requests, [], errors)
                raise WarmupFailed("warm-up jobs did not all succeed before the timeout")

        steady = self.http.publish_many(
            self.identity, "steady", self.config.event_count, self.config.publisher_concurrency, self.config.payload_bytes
        )
        self._requests.extend(steady)
        accepted = sum(1 for row in steady if row["success"])
        job_ids = [str(row["job_id"]) for row in steady if row.get("job_id")]
        drain_started = time.perf_counter()
        drained, counts = self.database.wait_for_terminal(
            self.identity.event_type, "steady", accepted, self.config.drain_timeout_seconds,
            self.config.poll_interval_seconds, job_ids=job_ids,
        )
        drain_seconds = time.perf_counter() - drain_started
        deliveries = self.database.deliveries(self.identity.event_type, "steady")
        if steady:
            publication_start = min(str(row["start_utc"]) for row in steady)
            publication_end = max(str(row["end_utc"]) for row in steady)
        else:
            publication_start = publication_end = "1970-01-01T00:00:00+00:00"
        complete = drained and accepted == self.config.event_count and len(deliveries) == accepted
        errors: list[dict[str, Any]] = []
        if not complete:
            terminal_count = counts.get("SUCCEEDED", 0) + counts.get("DEAD", 0)
            code = "drain_timeout" if terminal_count < accepted else "terminal_failure" if counts.get("DEAD", 0) else "publication_or_correlation_incomplete"
            errors.append({
                "code": code,
                "accepted": accepted, "expected": self.config.event_count, "correlated": len(deliveries), "statuses": counts,
            })
        summary = self._base_summary(endpoint_id, complete)
        summary["queue_drain_seconds"] = drain_seconds
        if hasattr(self.http, "receiver_diagnostics"):
            summary["receiver"] = self.http.receiver_diagnostics()
        summary["publication_window"] = {"start_utc": publication_start, "end_utc": publication_end}
        summary["ingestion"] = summarize_ingestion(self._requests)
        summary["delivery"] = summarize_delivery(deliveries, publication_start, publication_end)
        self._write(summary, self._requests, deliveries, errors)
        return 0 if complete else 2

    def _base_summary(self, endpoint_id: str, complete: bool) -> dict[str, Any]:
        return {
            "schema_version": 1, "run_id": self.identity.run_id, "label": self.identity.label,
            "event_type": self.identity.event_type, "endpoint_id": endpoint_id,
            "complete": complete, "config": self.config.to_public_dict(),
            "percentile_method": "nearest-rank",
        }
