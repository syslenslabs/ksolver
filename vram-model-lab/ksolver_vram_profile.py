"""Tiny opt-in metadata emitter for future framework-aware VRAM prediction.

This is the SDK shape we want users to adopt inside training containers. A
sidecar can receive this file over an emptyDir volume or HTTP later, but the app
process has the semantic truth: model params, precision, batch shape, optimizer,
and distributed strategy.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT = "/ksolver/profile/vram-profile.json"


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def count_parameters(model: Any) -> int | None:
    parameters = getattr(model, "parameters", None)
    if parameters is None:
        return None
    total = 0
    try:
        for param in parameters():
            total += int(param.numel())
        return total
    except Exception:
        return None


def command_hash() -> str:
    payload = "\0".join([os.environ.get("_", ""), *os.sys.argv])
    return hashlib.sha256(payload.encode()).hexdigest()


def report_training_job(
    *,
    model: Any | None = None,
    framework: str = "pytorch",
    model_name: str | None = None,
    precision: str | None = None,
    batch_size: int | None = None,
    sequence_length: int | None = None,
    image_size: int | None = None,
    optimizer: str | None = None,
    distributed_strategy: str | None = None,
    output_path: str = DEFAULT_OUTPUT,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = {
        "schema_version": 1,
        "emitted_at_unix": int(time.time()),
        "framework": framework,
        "model_name": model_name,
        "parameter_count": count_parameters(model) if model is not None else None,
        "precision": precision,
        "batch_size": _safe_int(batch_size),
        "sequence_length": _safe_int(sequence_length),
        "image_size": _safe_int(image_size),
        "optimizer": optimizer,
        "distributed_strategy": distributed_strategy,
        "command_hash": command_hash(),
        "pod_name": os.environ.get("HOSTNAME"),
        "namespace": Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read_text().strip()
        if Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace").exists()
        else None,
        "extra": extra or {},
    }
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(profile, indent=2, sort_keys=True) + "\n")
    return profile
