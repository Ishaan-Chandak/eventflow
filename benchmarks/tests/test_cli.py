import tempfile
import unittest
from pathlib import Path

from eventflow_bench.__main__ import capture_container_environment, load_machine


class CliTests(unittest.TestCase):
    def test_load_machine_accepts_windows_powershell_utf8_bom(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "machine.json"
            path.write_bytes(b"\xef\xbb\xbf{\"host\":{\"os\":\"Windows\"}}")
            self.assertEqual("Windows", load_machine(str(path))["host"]["os"])

    def test_capture_container_environment_reads_cgroup_limits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "cpu.max").write_text("200000 100000\n")
            (root / "memory.max").write_text("1073741824\n")
            result = capture_container_environment(root)
            self.assertEqual(2.0, result["cpu_limit"])
            self.assertEqual(1073741824, result["memory_limit_bytes"])


if __name__ == "__main__":
    unittest.main()
