from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _flat(summary: dict[str, Any]) -> dict[str, Any]:
    config = summary["config"]
    ingestion = summary.get("ingestion", {})
    delivery = summary.get("delivery", {})
    api_latency = ingestion.get("latency_ms", {})
    e2e_latency = delivery.get("e2e_latency_ms", {})
    row: dict[str, Any] = {
        "run_id": summary["run_id"], "label": summary.get("label", summary["run_id"]),
        "complete": summary.get("complete", False),
        "worker_count": config["worker_count"], "publisher_concurrency": config["publisher_concurrency"],
        "payload_bytes": config["payload_bytes"], "receiver_latency_ms": config["receiver_latency_ms"],
        "ingestion_rps": ingestion.get("rps"), "delivery_drained_rps": delivery.get("drained_rps"),
        "delivery_steady_state_rps": delivery.get("steady_state_rps"),
    }
    for percentile in ("p50", "p95", "p99"):
        row[f"api_{percentile}_ms"] = api_latency.get(percentile)
        row[f"e2e_{percentile}_ms"] = e2e_latency.get(percentile)
    return row


def write_comparison(summary_paths: list[Path], output_dir: Path) -> dict[str, Path]:
    if not summary_paths:
        raise ValueError("at least one summary path is required")
    rows = [_flat(_read(path)) for path in summary_paths]
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "comparison.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = [f"{row['label']}\n{str(row['run_id'])[:8]}" for row in rows]
    throughput = output_dir / "throughput.png"
    figure, axis = plt.subplots(figsize=(max(7, len(rows) * 1.5), 4.5))
    positions = range(len(rows))
    axis.bar([x - 0.2 for x in positions], [row["ingestion_rps"] or 0 for row in rows], width=0.4, label="Ingestion RPS")
    axis.bar([x + 0.2 for x in positions], [row["delivery_drained_rps"] or 0 for row in rows], width=0.4, label="Delivered RPS")
    axis.set_xticks(list(positions), labels, rotation=20, ha="right")
    axis.set_ylabel("requests/jobs per second")
    axis.legend()
    figure.tight_layout()
    figure.savefig(throughput, dpi=150)
    plt.close(figure)

    latency = output_dir / "latency-percentiles.png"
    figure, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    for percentile in ("p50", "p95", "p99"):
        axes[0].plot(list(positions), [row[f"api_{percentile}_ms"] if row[f"api_{percentile}_ms"] is not None else float("nan") for row in rows], marker="o", label=percentile)
        axes[1].plot(list(positions), [row[f"e2e_{percentile}_ms"] if row[f"e2e_{percentile}_ms"] is not None else float("nan") for row in rows], marker="o", label=percentile)
    axes[0].set_title("API latency"); axes[1].set_title("End-to-end latency")
    for axis in axes:
        axis.set_ylabel("milliseconds"); axis.set_xticks(list(positions), labels, rotation=20, ha="right"); axis.legend()
    figure.tight_layout()
    figure.savefig(latency, dpi=150)
    plt.close(figure)
    return {"csv": csv_path, "throughput_graph": throughput, "latency_graph": latency}
