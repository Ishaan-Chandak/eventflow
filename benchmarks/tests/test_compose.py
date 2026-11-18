import unittest
from pathlib import Path

import yaml


class ComposeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = yaml.safe_load(Path("benchmarks/docker-compose.yml").read_text())
        cls.services = cls.document["services"]

    def test_api_and_workers_have_separate_scheduling_roles(self):
        self.assertEqual("false", self.services["benchmark-api"]["environment"]["WORKER_ENABLED"])
        self.assertEqual("true", self.services["benchmark-worker"]["environment"]["WORKER_ENABLED"])
        self.assertNotIn("ports", self.services["benchmark-worker"])

    def test_stack_uses_benchmark_receiver_and_project_scoped_volumes(self):
        self.assertEqual("./receiver", self.services["benchmark-receiver"]["build"])
        self.assertIn("benchmark-receiver", self.services["benchmark-worker"]["environment"]["ALLOWED_WEBHOOK_HOSTS"])
        self.assertEqual({"postgres-data": None}, self.document["volumes"])

    def test_runner_binds_results_and_uses_internal_services(self):
        runner = self.services["benchmark-runner"]
        self.assertIn("${BENCHMARK_RESULTS_DIR:-./results}:/results", runner["volumes"])
        self.assertEqual("http://benchmark-api:8080", runner["environment"]["BENCHMARK_API_URL"])
        self.assertEqual("http://benchmark-receiver:8082", runner["environment"]["BENCHMARK_RECEIVER_URL"])


if __name__ == "__main__":
    unittest.main()
