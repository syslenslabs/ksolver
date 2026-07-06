#!/usr/bin/env python3
"""Export collected VRAM probe JSONL rows to a flat CSV training table."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "data" / "results.jsonl"
OUT = ROOT / "data" / "training_rows.csv"


FIELDS = [
    "schema_version",
    "scenario",
    "ok",
    "oom",
    "error",
    "framework",
    "framework_version",
    "trainer_style",
    "verified_real_framework",
    "customer_workload_fingerprint",
    "family",
    "model_arch",
    "precision",
    "precision_bytes",
    "batch_size",
    "seq_len",
    "image_size",
    "hidden_size",
    "layers",
    "heads",
    "optimizer",
    "activation_checkpointing",
    "gradient_accumulation_steps",
    "requested_gpu_type",
    "requested_gpu_count",
    "input_pipeline",
    "dataloader_sleep_ms",
    "sample_interval_seconds",
    "reserve_extra_mib",
    "param_count",
    "param_count_m",
    "activation_units_m",
    "tokens_or_pixels",
    "steps_requested",
    "steps_completed",
    "elapsed_seconds",
    "samples_per_second",
    "gpu_sku_label",
    "gpu_name",
    "gpu_total_mib",
    "gpu_total_gib",
    "nvidia_smi_peak_used_mib",
    "peak_vram_fraction",
    "oom_risk_label",
    "torch_peak_allocated_mib",
    "torch_peak_reserved_mib",
    "max_gpu_util_pct",
    "max_power_w",
    "max_temp_c",
    "sample_count",
    "image",
    "image_digest",
    "command_hash",
    "manifest_hash",
    "kubernetes_namespace",
    "kubernetes_job",
    "kubernetes_pod",
    "raw_log",
    "collected_at_unix",
]


def precision_bytes(value: str | None) -> float:
    return {
        "fp32": 4.0,
        "float32": 4.0,
        "fp16": 2.0,
        "float16": 2.0,
        "bf16": 2.0,
        "bfloat16": 2.0,
        "int8": 1.0,
    }.get(str(value or "fp32").lower(), 4.0)


def activation_units(row: dict[str, Any]) -> float:
    batch = float(row.get("batch_size") or 0)
    layers = float(row.get("layers") or 0)
    family = row.get("family")
    if family == "cnn":
        image_size = float(row.get("image_size") or 0)
        return batch * image_size * image_size * layers
    seq_len = float(row.get("seq_len") or 0)
    hidden = float(row.get("hidden_size") or 0)
    return batch * seq_len * hidden * layers


def tokens_or_pixels(row: dict[str, Any]) -> float:
    batch = float(row.get("batch_size") or 0)
    if row.get("family") == "cnn":
        image_size = float(row.get("image_size") or 0)
        return batch * image_size * image_size
    return batch * float(row.get("seq_len") or 0)


def flatten(row: dict[str, Any]) -> dict[str, Any]:
    samples = row.get("nvidia_smi_samples") or []
    flat = {field: row.get(field) for field in FIELDS}
    flat["precision_bytes"] = precision_bytes(row.get("precision"))
    flat["param_count_m"] = float(row.get("param_count") or 0) / 1_000_000.0
    flat["activation_units_m"] = activation_units(row) / 1_000_000.0
    flat["tokens_or_pixels"] = tokens_or_pixels(row)
    peak = float(row.get("nvidia_smi_peak_used_mib") or 0)
    total = float(row.get("gpu_total_mib") or 0)
    flat["peak_vram_fraction"] = (peak / total) if total else None
    flat["gpu_sku_label"] = row.get("gpu_sku_label") or (
        "rtx-4090" if row.get("gpu_name") == "NVIDIA GeForce RTX 4090" else row.get("requested_gpu_type")
    )
    flat["gpu_total_gib"] = row.get("gpu_total_gib") or (round(total / 1024.0, 2) if total else None)
    flat["oom_risk_label"] = bool(row.get("oom")) or (bool(total) and peak >= 0.90 * total)
    flat["max_gpu_util_pct"] = max((s.get("gpu_util_pct") or 0 for s in samples), default=None)
    flat["max_power_w"] = max((s.get("power_w") or 0 for s in samples), default=None)
    flat["max_temp_c"] = max((s.get("temp_c") or 0 for s in samples), default=None)
    flat["sample_count"] = len(samples)
    return flat


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text().splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=RESULTS)
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()

    rows = load_rows(args.input)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(flatten(row))
    print(f"wrote {args.out} with {len(rows)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
