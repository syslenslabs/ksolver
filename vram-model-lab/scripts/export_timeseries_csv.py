#!/usr/bin/env python3
"""Export per-sample GPU memory telemetry from VRAM probe JSONL."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "data" / "results.jsonl"
OUT = ROOT / "data" / "memory_timeseries.csv"


FIELDS = [
    "scenario",
    "family",
    "model_arch",
    "trainer_style",
    "precision",
    "optimizer",
    "batch_size",
    "seq_len",
    "image_size",
    "hidden_size",
    "layers",
    "heads",
    "activation_checkpointing",
    "gradient_accumulation_steps",
    "input_pipeline",
    "dataloader_sleep_ms",
    "reserve_extra_mib",
    "param_count",
    "gpu_sku_label",
    "gpu_name",
    "gpu_total_mib",
    "gpu_total_gib",
    "kubernetes_namespace",
    "kubernetes_job",
    "kubernetes_pod",
    "image_digest",
    "manifest_hash",
    "command_hash",
    "collected_at_unix",
    "sample_index",
    "sample_elapsed_seconds",
    "sample_timestamp",
    "memory_used_mib",
    "memory_used_fraction",
    "gpu_util_pct",
    "power_w",
    "temp_c",
]


def load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def sample_rows(row: dict[str, Any]) -> list[dict[str, Any]]:
    samples = row.get("nvidia_smi_samples") or []
    out = []
    total = float(row.get("gpu_total_mib") or 0)
    for idx, sample in enumerate(samples):
        memory = sample.get("memory_used_mib")
        out.append({
            "scenario": row.get("scenario"),
            "family": row.get("family"),
            "model_arch": row.get("model_arch"),
            "trainer_style": row.get("trainer_style"),
            "precision": row.get("precision"),
            "optimizer": row.get("optimizer"),
            "batch_size": row.get("batch_size"),
            "seq_len": row.get("seq_len"),
            "image_size": row.get("image_size"),
            "hidden_size": row.get("hidden_size"),
            "layers": row.get("layers"),
            "heads": row.get("heads"),
            "activation_checkpointing": row.get("activation_checkpointing"),
            "gradient_accumulation_steps": row.get("gradient_accumulation_steps"),
            "input_pipeline": row.get("input_pipeline"),
            "dataloader_sleep_ms": row.get("dataloader_sleep_ms"),
            "reserve_extra_mib": row.get("reserve_extra_mib"),
            "param_count": row.get("param_count"),
            "gpu_sku_label": row.get("gpu_sku_label") or (
                "rtx-4090" if row.get("gpu_name") == "NVIDIA GeForce RTX 4090" else row.get("requested_gpu_type")
            ),
            "gpu_name": row.get("gpu_name"),
            "gpu_total_mib": row.get("gpu_total_mib"),
            "gpu_total_gib": row.get("gpu_total_gib") or (round(total / 1024.0, 2) if total else None),
            "kubernetes_namespace": row.get("kubernetes_namespace"),
            "kubernetes_job": row.get("kubernetes_job"),
            "kubernetes_pod": row.get("kubernetes_pod"),
            "image_digest": row.get("image_digest"),
            "manifest_hash": row.get("manifest_hash"),
            "command_hash": row.get("command_hash"),
            "collected_at_unix": row.get("collected_at_unix"),
            "sample_index": idx,
            "sample_elapsed_seconds": sample.get("elapsed_seconds"),
            "sample_timestamp": sample.get("ts"),
            "memory_used_mib": memory,
            "memory_used_fraction": (float(memory) / total) if memory is not None and total else None,
            "gpu_util_pct": sample.get("gpu_util_pct"),
            "power_w": sample.get("power_w"),
            "temp_c": sample.get("temp_c"),
        })
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=RESULTS)
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()

    rows = load_rows(args.input)
    flat = []
    for row in rows:
        flat.extend(sample_rows(row))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flat)
    print(f"wrote {args.out} with {len(flat)} samples from {len(rows)} runs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
