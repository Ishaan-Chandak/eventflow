import unittest

from eventflow_bench.config import BenchmarkConfig, RunIdentity


class ConfigTests(unittest.TestCase):
    def test_rejects_invalid_capacity_settings(self):
        for values in ({"worker_count": 0}, {"publisher_concurrency": 0}, {"receiver_latency_ms": 5000}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                BenchmarkConfig(**values)

    def test_run_identity_is_unique_and_safe(self):
        first = RunIdentity.create("baseline")
        second = RunIdentity.create("baseline")
        self.assertNotEqual(first.run_id, second.run_id)
        self.assertRegex(first.event_type, r"^benchmark\.[a-f0-9]{32}$")
        self.assertTrue(first.idempotency_key("steady", 7).endswith("-steady-7"))

    def test_public_config_omits_database_credentials(self):
        config = BenchmarkConfig(postgres_dsn="postgresql://secret-user:secret-password@db/eventflow")
        serialized = str(config.to_public_dict())
        self.assertNotIn("secret-user", serialized)
        self.assertNotIn("secret-password", serialized)


if __name__ == "__main__":
    unittest.main()
