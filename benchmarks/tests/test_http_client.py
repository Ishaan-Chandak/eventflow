import threading
import time
import unittest

from eventflow_bench.config import RunIdentity
from eventflow_bench.http_client import EventFlowHttpClient


class RecordingTransport:
    def __init__(self):
        self.keys = []
        self.lock = threading.Lock()

    def post_json(self, path, body, headers=None):
        with self.lock:
            self.keys.append((headers or {}).get("Idempotency-Key"))
        time.sleep(0.005)
        return 202, {"eventId": "event", "jobIds": [f"job-{body['payload']['benchmark']['sequence']}"]}


class HttpClientTests(unittest.TestCase):
    def test_concurrent_publication_uses_unique_keys(self):
        transport = RecordingTransport()
        client = EventFlowHttpClient(transport)
        identity = RunIdentity.create("test", "a" * 32)
        rows = client.publish_many(identity, "steady", 12, 4, 256)
        self.assertEqual(12, len(rows))
        self.assertEqual(12, len(set(transport.keys)))
        self.assertTrue(all(row["success"] for row in rows))
        self.assertTrue(all(row["latency_ms"] > 0 for row in rows))

    def test_wait_until_ready_retries_transient_failures(self):
        class ReadyTransport:
            def __init__(self):
                self.calls = 0
            def get_json(self, path):
                self.calls += 1
                if self.calls < 3:
                    raise OSError("starting")
                return 200, {"status": "UP"}
        transport = ReadyTransport()
        client = EventFlowHttpClient(transport, transport)
        client.wait_until_ready(timeout_seconds=1, poll_interval_seconds=0.001)
        self.assertEqual(4, transport.calls)


if __name__ == "__main__":
    unittest.main()
