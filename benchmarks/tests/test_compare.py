import csv
import json
import tempfile
import unittest
from pathlib import Path

from eventflow_bench.compare import write_comparison


class CompareTests(unittest.TestCase):
    def test_writes_comparison_csv_and_graphs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summaries = []
            for index, rps in enumerate((10.0, 20.0)):
                path = root / f"run-{index}.json"
                path.write_text(json.dumps({
                    "run_id": str(index), "label": f"run-{index}",
                    "config": {"worker_count": index + 1, "publisher_concurrency": 4, "payload_bytes": 100, "receiver_latency_ms": 0},
                    "ingestion": {"rps": rps, "latency_ms": {"p50": 1, "p95": 2, "p99": 3}},
                    "delivery": {"drained_rps": rps - 1, "steady_state_rps": rps - 2, "e2e_latency_ms": {"p50": 4, "p95": 5, "p99": 6}},
                }))
                summaries.append(path)
            outputs = write_comparison(summaries, root / "comparison")
            self.assertTrue(outputs["csv"].exists())
            self.assertTrue(outputs["throughput_graph"].exists())
            self.assertTrue(outputs["latency_graph"].exists())
            with outputs["csv"].open(newline="") as handle:
                self.assertEqual(["10.0", "20.0"], [row["ingestion_rps"] for row in csv.DictReader(handle)])

    def test_incomplete_unmeasured_summary_is_included_without_crashing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "incomplete.json"
            path.write_text(json.dumps({"run_id": "x", "label": "benchmark", "complete": False, "config": {"worker_count": 1, "publisher_concurrency": 1, "payload_bytes": 100, "receiver_latency_ms": 0}}))
            outputs = write_comparison([path], root / "out")
            with outputs["csv"].open(newline="") as handle:
                row = next(csv.DictReader(handle))
            self.assertEqual("False", row["complete"])
            self.assertEqual("", row["ingestion_rps"])


if __name__ == "__main__":
    unittest.main()
