import json
import threading
import time
import unittest
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from receiver.server import create_server


def request(port, path, method="GET", body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method, headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=3) as response:
        return response.status, json.loads(response.read()) if response.headers.get("Content-Length") != "0" and response.status != 204 else None


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.server = create_server("127.0.0.1", 0, max_concurrency=8)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_records_concurrent_delivery_ids_without_loss(self):
        def send(index):
            return request(self.port, "/webhook", "POST", {"value": index}, {"X-EventFlow-Delivery-Id": f"delivery-{index}"})[0]
        with ThreadPoolExecutor(max_workers=20) as pool:
            statuses = list(pool.map(send, range(40)))
        status, body = request(self.port, "/received")
        self.assertEqual([200] * 40, statuses)
        self.assertEqual(40, body["count"])
        self.assertEqual(40, len({row["deliveryId"] for row in body["receipts"]}))

    def test_control_applies_latency_and_reset_clears_receipts(self):
        status, _ = request(self.port, "/control", "POST", {"latencyMs": 40})
        self.assertEqual(204, status)
        started = time.perf_counter()
        request(self.port, "/webhook", "POST", {}, {"X-EventFlow-Delivery-Id": "one"})
        self.assertGreaterEqual(time.perf_counter() - started, 0.035)
        request(self.port, "/reset", "POST", {})
        self.assertEqual(0, request(self.port, "/received")[1]["count"])

    def test_health_exposes_capacity(self):
        status, body = request(self.port, "/health")
        self.assertEqual(200, status)
        self.assertEqual(8, body["maxConcurrency"])


if __name__ == "__main__":
    unittest.main()
