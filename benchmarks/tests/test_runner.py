import json
import tempfile
import unittest
from pathlib import Path

from eventflow_bench.config import BenchmarkConfig, RunIdentity
from eventflow_bench.runner import BenchmarkRunner, WarmupFailed


class FakeHttp:
    def __init__(self):
        self.phases = []

    def configure_receiver(self, latency_ms):
        self.latency_ms = latency_ms

    def register_endpoint(self, identity, receiver_url):
        return "endpoint"

    def publish_many(self, identity, phase, count, concurrency, payload_bytes):
        self.phases.append(phase)
        return [{
            "phase": phase, "sequence": index, "success": True, "status": 202,
            "start_monotonic": 10 + index * 0.1, "end_monotonic": 10.05 + index * 0.1,
            "start_utc": f"2026-01-01T00:00:0{index}+00:00", "end_utc": f"2026-01-01T00:00:0{index}.050000+00:00",
            "latency_ms": 50, "event_id": f"event-{index}", "job_id": f"job-{phase}-{index}",
        } for index in range(count)]

    def receiver_diagnostics(self):
        return {"max_concurrency": 128, "receipt_count": 2}


class FakeDatabase:
    def __init__(self, outcomes):
        self.outcomes = iter(outcomes)

    def wait_for_terminal(self, event_type, phase, expected, timeout_seconds, poll_interval_seconds, job_ids=None):
        self.job_ids = job_ids
        return next(self.outcomes)

    def deliveries(self, event_type, phase):
        return [{
            "job_id": "job-steady-0", "status": "SUCCEEDED",
            "event_created_at": "2026-01-01T00:00:00+00:00",
            "finished_at": "2026-01-01T00:00:00.500000+00:00", "e2e_latency_ms": 500,
        }]


class RunnerTests(unittest.TestCase):
    def config(self, directory, warmup=1):
        return BenchmarkConfig(event_count=1, warmup_count=warmup, payload_bytes=256, output_dir=directory)

    def test_successful_run_writes_correlated_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            http = FakeHttp()
            runner = BenchmarkRunner(self.config(directory), RunIdentity.create("test", "b" * 32), http, FakeDatabase([(True, {"SUCCEEDED": 1}), (True, {"SUCCEEDED": 1})]), {"host": {}})
            code = runner.run()
            summary = json.loads((Path(directory) / "summary.json").read_text())
            self.assertEqual(0, code)
            self.assertTrue(summary["complete"])
            self.assertEqual(1, summary["ingestion"]["successful"])
            self.assertEqual(1, summary["delivery"]["successful"])
            self.assertIn("queue_drain_seconds", summary)
            self.assertEqual(128, summary["receiver"]["max_concurrency"])
            self.assertNotIn("postgres_dsn", summary["config"])
            self.assertEqual(["job-steady-0"], runner.database.job_ids)

    def test_warmup_timeout_aborts_before_steady_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            http = FakeHttp()
            runner = BenchmarkRunner(self.config(directory), RunIdentity.create("test", "c" * 32), http, FakeDatabase([(False, {"QUEUED": 1})]), {})
            with self.assertRaises(WarmupFailed):
                runner.run()
            self.assertEqual(["warmup"], http.phases)

    def test_drain_timeout_writes_partial_artifacts_and_returns_nonzero(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = BenchmarkRunner(self.config(directory, warmup=0), RunIdentity.create("test", "d" * 32), FakeHttp(), FakeDatabase([(False, {"IN_PROGRESS": 1})]), {})
            self.assertEqual(2, runner.run())
            summary = json.loads((Path(directory) / "summary.json").read_text())
            self.assertFalse(summary["complete"])
            self.assertEqual("drain_timeout", json.loads((Path(directory) / "errors.json").read_text())[0]["code"])

    def test_terminal_dead_is_not_mislabeled_as_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = BenchmarkRunner(self.config(directory, warmup=0), RunIdentity.create("test", "e" * 32), FakeHttp(), FakeDatabase([(False, {"DEAD": 1})]), {})
            self.assertEqual(2, runner.run())
            self.assertEqual("terminal_failure", json.loads((Path(directory) / "errors.json").read_text())[0]["code"])

    def test_database_exception_preserves_request_artifacts(self):
        class BrokenDatabase(FakeDatabase):
            def wait_for_terminal(self, *args, **kwargs):
                raise RuntimeError("database unavailable")
        with tempfile.TemporaryDirectory() as directory:
            runner = BenchmarkRunner(self.config(directory, warmup=0), RunIdentity.create("test", "f" * 32), FakeHttp(), BrokenDatabase([]), {})
            self.assertEqual(4, runner.run())
            self.assertTrue((Path(directory) / "requests.csv").read_text().strip())
            self.assertEqual("runner_exception", json.loads((Path(directory) / "errors.json").read_text())[0]["code"])


if __name__ == "__main__":
    unittest.main()
