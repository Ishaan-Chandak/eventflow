import csv
import json
import tempfile
import unittest
from pathlib import Path

from eventflow_bench.reporting import write_run_artifacts


class ReportingTests(unittest.TestCase):
    def test_writes_complete_json_and_csv_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            result = write_run_artifacts(
                Path(directory),
                config={"event_count": 1},
                machine={"host": {"os": "test"}},
                summary={"run_id": "abc", "complete": True},
                requests=[{"sequence": 0, "success": True}],
                deliveries=[{"job_id": "job", "status": "SUCCEEDED"}],
                errors=[],
            )
            self.assertEqual(True, json.loads((result / "summary.json").read_text())["complete"])
            with (result / "requests.csv").open(newline="") as handle:
                self.assertEqual("0", list(csv.DictReader(handle))[0]["sequence"])
            self.assertTrue((result / "deliveries.csv").exists())

    def test_partial_run_always_writes_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            result = write_run_artifacts(Path(directory), {}, {}, {"complete": False}, [], [], [{"message": "timeout"}])
            self.assertEqual([{"message": "timeout"}], json.loads((result / "errors.json").read_text()))
            self.assertIn("sequence", (result / "requests.csv").read_text().splitlines()[0])
            self.assertIn("job_id", (result / "deliveries.csv").read_text().splitlines()[0])


if __name__ == "__main__":
    unittest.main()
