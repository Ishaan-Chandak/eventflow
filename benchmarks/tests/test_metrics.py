import unittest

from eventflow_bench.metrics import nearest_rank, summarize_ingestion, summarize_delivery


class MetricsTests(unittest.TestCase):
    def test_nearest_rank_boundaries(self):
        values = [1.0, 2.0, 3.0, 100.0]
        self.assertEqual(2.0, nearest_rank(values, 50))
        self.assertEqual(100.0, nearest_rank(values, 95))
        self.assertIsNone(nearest_rank([], 50))

    def test_ingestion_uses_successful_response_window(self):
        requests = [
            {"phase": "steady", "success": True, "start_monotonic": 10.0, "end_monotonic": 10.1, "latency_ms": 100},
            {"phase": "steady", "success": True, "start_monotonic": 10.2, "end_monotonic": 11.0, "latency_ms": 800},
            {"phase": "steady", "success": False, "start_monotonic": 10.3, "end_monotonic": 12.0, "latency_ms": 1700},
            {"phase": "warmup", "success": True, "start_monotonic": 1.0, "end_monotonic": 2.0, "latency_ms": 1000},
        ]
        result = summarize_ingestion(requests)
        self.assertEqual(2, result["successful"])
        self.assertAlmostEqual(2.0, result["rps"])
        self.assertEqual(100.0, result["latency_ms"]["p50"])
        self.assertEqual(800.0, result["latency_ms"]["p95"])

    def test_ingestion_window_includes_early_failed_attempt(self):
        requests = [
            {"phase": "steady", "success": False, "start_monotonic": 9.0, "end_monotonic": 9.1, "latency_ms": 100},
            {"phase": "steady", "success": True, "start_monotonic": 10.0, "end_monotonic": 11.0, "latency_ms": 1000},
        ]
        result = summarize_ingestion(requests)
        self.assertEqual(0.5, result["rps"])
        self.assertEqual(2.0, result["window_seconds"])

    def test_delivery_reports_drain_and_steady_windows(self):
        rows = [
            {"status": "SUCCEEDED", "event_created_at": "2026-01-01T00:00:00+00:00", "finished_at": "2026-01-01T00:00:01+00:00", "e2e_latency_ms": 1000},
            {"status": "SUCCEEDED", "event_created_at": "2026-01-01T00:00:00.500000+00:00", "finished_at": "2026-01-01T00:00:03+00:00", "e2e_latency_ms": 2500},
            {"status": "DEAD", "event_created_at": "2026-01-01T00:00:00.700000+00:00", "finished_at": None, "e2e_latency_ms": None},
        ]
        result = summarize_delivery(rows, "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:02+00:00")
        self.assertEqual(2, result["successful"])
        self.assertEqual(1, result["steady_state_completions"])
        self.assertEqual(0.5, result["steady_state_rps"])
        self.assertAlmostEqual(2 / 3, result["drained_rps"])
        self.assertEqual(2500.0, result["e2e_latency_ms"]["p95"])

    def test_drained_window_starts_at_earliest_measured_event_even_if_incomplete(self):
        rows = [
            {"status": "IN_PROGRESS", "event_created_at": "2026-01-01T00:00:00+00:00", "finished_at": None, "e2e_latency_ms": None},
            {"status": "SUCCEEDED", "event_created_at": "2026-01-01T00:00:01+00:00", "finished_at": "2026-01-01T00:00:03+00:00", "e2e_latency_ms": 2000},
        ]
        result = summarize_delivery(rows, "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:02+00:00")
        self.assertAlmostEqual(1 / 3, result["drained_rps"])


if __name__ == "__main__":
    unittest.main()
