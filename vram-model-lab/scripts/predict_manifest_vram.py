#!/usr/bin/env python3
"""Predict VRAM for Kubernetes manifests using ksolver.ai annotations or env hints."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from fingerprint_manifest import fingerprint, pod_templates
from fit_peak_vram_model import features


ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "data" / "models" / "peak_vram_linear.json"

ANNOTATION_PREFIX = "ksolver.ai/vram-"
ENV_PREFIX = "KSOLVER_VRAM_"


def as_bool(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "yes", "on"}


def as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_key(key: str) -> str:
    key = key.lower().replace("-", "_")
    if key.startswith("vram_"):
        key = key[5:]
    return key


def object_annotations_by_doc(path: Path) -> dict[int, dict[str, str]]:
    docs = [doc for doc in yaml.safe_load_all(path.read_text()) if doc]
    out = {}
    for idx, doc in enumerate(docs):
        metadata = doc.get("metadata") or {}
        annotations = metadata.get("annotations") or {}
        out[idx] = {k: str(v) for k, v in annotations.items()}
    return out


def template_annotations(path: Path) -> dict[tuple[int, int], dict[str, str]]:
    docs = [doc for doc in yaml.safe_load_all(path.read_text()) if doc]
    out = {}
    for doc_idx, doc in enumerate(docs):
        for template_idx, template in enumerate(pod_templates(doc)):
            metadata = template.get("metadata") or {}
            annotations = metadata.get("annotations") or {}
            out[(doc_idx, template_idx)] = {k: str(v) for k, v in annotations.items()}
    return out


def parse_cli_tokens(tokens: list[str]) -> dict[str, Any]:
    hints: dict[str, Any] = {}
    joined = " ".join(tokens)
    patterns = {
        "batch_size": r"--(?:batch-size|batch_size|per-device-train-batch-size|per_device_train_batch_size)[=\s]+(\d+)",
        "seq_len": r"--(?:seq-len|seq_len|max-seq-len|max_seq_len|sequence-length|sequence_length|max-position-embeddings)[=\s]+(\d+)",
        "image_size": r"--(?:image-size|image_size|resolution)[=\s]+(\d+)",
        "precision": r"--(?:precision|dtype|torch-dtype|torch_dtype)[=\s]+([A-Za-z0-9_]+)",
        "optimizer": r"--(?:optimizer|optim)[=\s]+([A-Za-z0-9_]+)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, joined)
        if match:
            hints[key] = match.group(1)
    return hints


def hints_from_row(row: dict[str, Any], object_annotations: dict[str, str], pod_annotations: dict[str, str]) -> dict[str, Any]:
    hints: dict[str, Any] = {}
    for annotations in [object_annotations, pod_annotations]:
        for key, value in annotations.items():
            if key.startswith(ANNOTATION_PREFIX):
                hints[normalize_key(key[len(ANNOTATION_PREFIX):])] = value
    for key, value in (row.get("env") or {}).items():
        if key.startswith(ENV_PREFIX):
            hints[normalize_key(key[len(ENV_PREFIX):])] = value
    hints.update(parse_cli_tokens((row.get("command") or []) + (row.get("args") or [])))
    return hints


def row_from_hints(hints: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    missing = []
    family = hints.get("family")
    if family not in {"transformer", "cnn", "mlp"}:
        missing.append("family")
    precision = str(hints.get("precision") or "fp16").lower()
    precision = {"float16": "fp16", "float32": "fp32", "bfloat16": "bf16"}.get(precision, precision)
    batch_size = as_int(hints.get("batch_size"))
    hidden_size = as_int(hints.get("hidden_size"))
    layers = as_int(hints.get("layers"))
    if batch_size is None:
        missing.append("batch_size")
    if hidden_size is None:
        missing.append("hidden_size")
    if layers is None:
        missing.append("layers")
    seq_len = as_int(hints.get("seq_len") or hints.get("sequence_length"))
    image_size = as_int(hints.get("image_size"))
    if family in {"transformer", "mlp"} and seq_len is None:
        missing.append("seq_len")
    if family == "cnn" and image_size is None:
        missing.append("image_size")
    param_count = as_int(hints.get("param_count"))
    # The fitted models include parameter count as a material driver. Treating an
    # absent value as zero produces a plausible-looking but dangerously low estimate.
    if param_count is None or param_count <= 0:
        missing.append("param_count")
    if missing:
        return None, sorted(set(missing))
    return {
        "family": family,
        "precision": precision,
        "batch_size": batch_size,
        "seq_len": seq_len,
        "image_size": image_size,
        "hidden_size": hidden_size,
        "layers": layers,
        "heads": as_int(hints.get("heads")),
        "optimizer": str(hints.get("optimizer") or "adamw").lower(),
        "activation_checkpointing": as_bool(hints.get("activation_checkpointing")),
        "param_count": param_count,
        "reserve_extra_mib": as_int(hints.get("reserve_extra_mib")) or 0,
    }, []


def predict(job: dict[str, Any], artifact: dict[str, Any], gpu_total_mib: int) -> dict[str, Any]:
    family_model = (artifact.get("family_models") or {}).get(job["family"])
    global_model = artifact.get("global") or artifact
    selected = family_model if family_model and family_model.get("usable_for_prediction") else global_model
    x = np.array(features(job, mode=selected.get("feature_mode", "interactions")), dtype=float)
    point = float(x @ np.array(selected["coefficients"], dtype=float))
    safety = max(1024.0, float(selected.get("leave_one_out_abs_error_p95_mib") or 0.0))
    conservative = point + safety
    return {
        "input": job,
        "selected_model": selected.get("name", "global"),
        "selected_model_rows": selected.get("training_rows"),
        "point_estimate_mib": round(point, 1),
        "safety_margin_mib": round(safety, 1),
        "conservative_estimate_mib": round(conservative, 1),
        "gpu_total_mib": gpu_total_mib,
        "decision": "fits" if conservative < gpu_total_mib else "risk_or_oom",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--model", type=Path, default=MODEL)
    parser.add_argument("--gpu-total-mib", type=int, default=24563)
    args = parser.parse_args()
    artifact = json.loads(args.model.read_text())
    object_annotations = object_annotations_by_doc(args.manifest)
    pod_annotations = template_annotations(args.manifest)
    outputs = []
    for row in fingerprint(args.manifest):
        hints = hints_from_row(
            row,
            object_annotations.get(row["document_index"], {}),
            pod_annotations.get((row["document_index"], row["template_index"]), {}),
        )
        job, missing = row_from_hints(hints)
        output = {
            "fingerprint": row,
            "hints": hints,
            "status": "predicted" if job else "missing_hints",
            "missing_hints": missing,
        }
        if job:
            output["prediction"] = predict(job, artifact, args.gpu_total_mib)
        outputs.append(output)
    print(json.dumps(outputs, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
