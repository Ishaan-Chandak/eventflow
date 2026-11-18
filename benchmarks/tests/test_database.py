import unittest
from datetime import datetime, timezone

from eventflow_bench.database import normalize_delivery_row


class DatabaseTests(unittest.TestCase):
    def test_normalizes_postgres_timestamps_and_latency(self):
        row = {
            "job_id": "job-1",
            "status": "SUCCEEDED",
            "event_created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 1, 1, 0, 0, 1, 250000, tzinfo=timezone.utc),
            "attempt_count": 1,
        }
        normalized = normalize_delivery_row(row)
        self.assertEqual(1250.0, normalized["e2e_latency_ms"])
        self.assertEqual("2026-01-01T00:00:00+00:00", normalized["event_created_at"])


if __name__ == "__main__":
    unittest.main()
