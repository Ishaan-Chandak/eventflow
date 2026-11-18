from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .compare import write_comparison
from .config import BenchmarkConfig, RunIdentity
from .database import PostgresCollector
from .http_client import EventFlowHttpClient, JsonTransport
from .runner import BenchmarkRunner, WarmupFailed


def load_machine(path: str | None) -> dict[str, object]:
    if not path or not Path(path).exists():
        return {}
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def capture_container_environment(cgroup_root: Path = Path("/sys/fs/cgroup")) -> dict[str, object]:
    result: dict[str, object] = {"logical_cpus_visible": os.cpu_count()}
    cpu_file = cgroup_root / "cpu.max"
    if cpu_file.exists():
        quota, period = cpu_file.read_text().strip().split()
        result["cpu_limit"] = None if quota == "max" else int(quota) / int(period)
    memory_file = cgroup_root / "memory.max"
    if memory_file.exists():
        value = memory_file.read_text().strip()
        result["memory_limit_bytes"] = None if value == "max" else int(value)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eventflow-bench")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run one benchmark")
    run.add_argument("--run-id", required=True)
    run.add_argument("--label", default="benchmark")
    run.add_argument("--event-count", type=int, default=1000)
    run.add_argument("--warmup-count", type=int, default=100)
    run.add_argument("--publisher-concurrency", type=int, default=10)
    run.add_argument("--worker-count", type=int, default=1)
    run.add_argument("--payload-bytes", type=int, default=1024)
    run.add_argument("--receiver-latency-ms", type=int, default=0)
    run.add_argument("--drain-timeout-seconds", type=float, default=120)
    run.add_argument("--poll-interval-seconds", type=float, default=0.25)
    run.add_argument("--output-dir", required=True)
    compare = commands.add_parser("compare", help="compare saved summary files")
    compare.add_argument("--output-dir", required=True)
    compare.add_argument("summaries", nargs="+")
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    if arguments.command == "compare":
        write_comparison([Path(path) for path in arguments.summaries], Path(arguments.output_dir))
        return 0
    config = BenchmarkConfig(
        event_count=arguments.event_count, warmup_count=arguments.warmup_count,
        publisher_concurrency=arguments.publisher_concurrency, worker_count=arguments.worker_count,
        payload_bytes=arguments.payload_bytes, receiver_latency_ms=arguments.receiver_latency_ms,
        drain_timeout_seconds=arguments.drain_timeout_seconds, poll_interval_seconds=arguments.poll_interval_seconds,
        api_url=os.getenv("BENCHMARK_API_URL", "http://benchmark-api:8080"),
        receiver_url=os.getenv("BENCHMARK_RECEIVER_URL", "http://benchmark-receiver:8082"),
        postgres_dsn=os.getenv("BENCHMARK_POSTGRES_DSN", "postgresql://eventflow:eventflow@postgres:5432/eventflow"),
        output_dir=arguments.output_dir, label=arguments.label,
    )
    machine_path = os.getenv("BENCHMARK_MACHINE_JSON")
    machine = load_machine(machine_path)
    machine["runner_container"] = capture_container_environment()
    http = EventFlowHttpClient(JsonTransport(config.api_url), JsonTransport(config.receiver_url))
    runner = BenchmarkRunner(config, RunIdentity.create(arguments.label, arguments.run_id), http, PostgresCollector(config.postgres_dsn), machine)
    try:
        return runner.run()
    except WarmupFailed as error:
        print(str(error))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
