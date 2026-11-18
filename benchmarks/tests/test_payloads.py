import json
import unittest

from eventflow_bench.config import RunIdentity
from eventflow_bench.payloads import build_payload, validate_payload_size


class PayloadTests(unittest.TestCase):
    def test_builds_exact_utf8_payload_size(self):
        payload = build_payload(256, "run", "steady", 3)
        encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
        self.assertEqual(256, len(encoded))
        self.assertEqual("steady", payload["benchmark"]["phase"])

    def test_rejects_too_small_target(self):
        with self.assertRaises(ValueError):
            build_payload(10, "run", "steady", 1)

    def test_validates_largest_sequence_and_full_request_envelope(self):
        identity = RunIdentity.create("test", "a" * 32)
        with self.assertRaisesRegex(ValueError, "largest sequence"):
            validate_payload_size(101, identity, event_count=11, warmup_count=0)
        with self.assertRaisesRegex(ValueError, "1 MiB"):
            validate_payload_size(1_048_550, identity, event_count=1, warmup_count=0)


if __name__ == "__main__":
    unittest.main()
