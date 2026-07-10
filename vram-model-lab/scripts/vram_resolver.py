#!/usr/bin/env python3
"""Confidence-ranked VRAM resolution cascade for a Kubernetes Pod.

resolve(pod) -> {vram_mib, vram_gib, source, confidence, missing, fingerprint, hard}

Runtime tier order (highest-confidence available wins):
  1. explicit annotation  ksolver.dev/predicted-peak-vram-{bytes,gib}   -> authoritative (hard)
  4. historical fingerprint (observed p95 peak)                          -> high (hard)   [phase B]
  3. referenced config read via k8s API (deepspeed/accelerate)           -> high (hard)   [phase C]
  2. static spec sniff (annotations/env/CLI) -> linear model             -> high (hard)
  -. none                                                                -> advisory (no hard constraint)

Only authoritative/high tiers earn a hard constraint; everything else is advisory so a
mis-sniff can never strand a workload. This module is the single source of the model math
(reuses predict_manifest_vram); the Rust admission webhook is thin glue that calls it.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from predict_manifest_vram import as_int, hints_from_row, predict, row_from_hints
from fingerprint_manifest import sha256_json

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL = ROOT / "data" / "models" / "peak_vram_linear.json"
DEFAULT_GPU_TOTAL_MIB = 24563  # a nominal 24 GiB card; only affects the fits/oom decision

EXPLICIT_BYTES = "ksolver.dev/predicted-peak-vram-bytes"
EXPLICIT_GIB = "ksolver.dev/predicted-peak-vram-gib"
MIB = 1024.0 * 1024.0

# Tier 4: how many prior observations of the same workload fingerprint we require before we
# trust the measured peak as a hard constraint (a promotion threshold).
FINGERPRINT_MIN_SAMPLES = 3


def fingerprint_key(fp: dict[str, Any]) -> str:
    return f"{fp.get('image')}|{fp.get('command_hash')}"


def _p95(values: list[float]) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = 0.95 * (len(ordered) - 1)
    lo = int(rank)
    frac = rank - lo
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + frac * (ordered[hi] - ordered[lo])


def record_observation(store_path: str | Path, pod: dict[str, Any], peak_mib: float) -> None:
    """Append one measured peak-VRAM observation for a pod's workload fingerprint.

    This is how the tier-4 store gets populated going forward: when a run completes, record the
    pod it ran and its measured peak (e.g. from ksolver's completed-job observations / nvidia-smi).
    Store format is JSONL of {"image", "command_hash", "peak_mib"} keyed by pod_fingerprint.
    """
    fp = pod_fingerprint(pod)
    row = {"image": fp.get("image"), "command_hash": fp.get("command_hash"), "peak_mib": float(peak_mib)}
    with Path(store_path).open("a") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")


def index_observation(observations: dict[str, list[float]], pod: dict[str, Any], peak_mib: float) -> str:
    """Add an observation to an in-memory store index (so a running predictor sees it immediately).
    Returns the fingerprint key. Pair with record_observation() to also persist it."""
    key = fingerprint_key(pod_fingerprint(pod))
    observations.setdefault(key, []).append(float(peak_mib))
    return key


def load_observations(path: str | Path) -> dict[str, list[float]]:
    """Index observed peak VRAM (MiB) by workload fingerprint key.

    Store format: JSONL rows of {"image", "command_hash", "peak_mib"} — one per observed run.
    """
    store: dict[str, list[float]] = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        peak = row.get("peak_mib")
        if peak is None:
            continue
        key = f"{row.get('image')}|{row.get('command_hash')}"
        store.setdefault(key, []).append(float(peak))
    return store


def _gpu_container(pod: dict[str, Any]) -> dict[str, Any]:
    """The container that requests a GPU, else the first container."""
    containers = ((pod.get("spec") or {}).get("containers")) or []
    for c in containers:
        resources = c.get("resources") or {}
        for bucket in ("requests", "limits"):
            for key in (resources.get(bucket) or {}):
                if "gpu" in key.lower():
                    return c
    return containers[0] if containers else {}


def _pod_annotations(pod: dict[str, Any]) -> dict[str, str]:
    return {k: str(v) for k, v in ((pod.get("metadata") or {}).get("annotations") or {}).items()}


def _row_from_pod(pod: dict[str, Any]) -> dict[str, Any]:
    c = _gpu_container(pod)
    env = {e["name"]: e.get("value", "") for e in (c.get("env") or []) if e.get("name")}
    return {
        "image": c.get("image"),
        "command": c.get("command") or [],
        "args": c.get("args") or [],
        "env": env,
    }


def pod_fingerprint(pod: dict[str, Any]) -> dict[str, Any]:
    """Stable identity for a pod's workload: image + hash of command/args/env."""
    c = _gpu_container(pod)
    env = {e.get("name"): e.get("value") for e in (c.get("env") or []) if e.get("name")}
    return {
        "image": c.get("image"),
        "command_hash": sha256_json(
            {"command": c.get("command") or [], "args": c.get("args") or [], "env": env}
        ),
    }


# Tier 3: map common training-config keys (DeepSpeed / HF TrainingArguments / accelerate) to
# our hint keys. Matched case-insensitively against a flattened config dict.
_CONFIG_KEY_MAP = {
    "batch_size": (
        "train_micro_batch_size_per_gpu",
        "per_device_train_batch_size",
        "micro_batch_size",
        "train_batch_size",
        "batch_size",
    ),
    "seq_len": (
        "max_seq_length",
        "max_position_embeddings",
        "sequence_length",
        "block_size",
        "seq_len",
    ),
    "image_size": ("image_size", "resolution"),
    "hidden_size": ("hidden_size", "n_embd", "d_model"),
    "layers": ("num_hidden_layers", "num_layers", "n_layer", "layers"),
    "heads": ("num_attention_heads", "n_head", "heads"),
    "family": ("family", "model_family"),
}


def _flatten(doc: Any, out: dict[str, Any]) -> None:
    if isinstance(doc, dict):
        for k, v in doc.items():
            if isinstance(v, (dict, list)):
                _flatten(v, out)
            else:
                out.setdefault(str(k).lower(), v)
    elif isinstance(doc, list):
        for item in doc:
            _flatten(item, out)


def hints_from_config_docs(docs: list[Any]) -> dict[str, Any]:
    """Extract model hints from referenced training-config documents (already fetched)."""
    flat: dict[str, Any] = {}
    for doc in docs or []:
        _flatten(doc, flat)
    hints: dict[str, Any] = {}
    for hint_key, candidates in _CONFIG_KEY_MAP.items():
        for cand in candidates:
            if cand in flat and flat[cand] not in (None, ""):
                hints[hint_key] = flat[cand]
                break
    precision = _detect_precision(docs)
    if precision:
        hints["precision"] = precision
    return hints


def _detect_precision(docs: list[Any]) -> str | None:
    """Find a precision from configs: explicit dtype/precision wins over fp16/bf16 flags.

    Handles HF top-level booleans (`fp16: true`) and DeepSpeed nests (`fp16: {enabled: true}`).
    """
    explicit: str | None = None
    flag: str | None = None

    def truthy(v: Any) -> bool:
        if isinstance(v, dict):
            return str(v.get("enabled")).lower() in {"true", "1"}
        return str(v).lower() in {"true", "1"}

    def walk(node: Any) -> None:
        nonlocal explicit, flag
        if isinstance(node, dict):
            for k, v in node.items():
                kl = str(k).lower()
                if kl in {"torch_dtype", "dtype", "precision"} and v and explicit is None:
                    explicit = str(v)
                if kl in {"fp16", "bf16"} and truthy(v) and flag is None:
                    flag = kl
                if isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    for doc in docs or []:
        walk(doc)
    return explicit or flag


def _explicit_estimate_mib(ann: dict[str, str]) -> float | None:
    if EXPLICIT_BYTES in ann:
        b = as_int(ann[EXPLICIT_BYTES])
        if b:
            return b / MIB
    if EXPLICIT_GIB in ann:
        try:
            return float(ann[EXPLICIT_GIB]) * 1024.0
        except (TypeError, ValueError):
            return None
    return None


def _result(
    mib: float | None,
    source: str,
    confidence: str,
    missing: list[str],
    fingerprint: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out = {
        "vram_mib": round(mib, 1) if mib is not None else None,
        "vram_gib": round(mib / 1024.0, 2) if mib is not None else None,
        "source": source,
        "confidence": confidence,
        "missing": missing,
        "fingerprint": fingerprint,
        # Only high-confidence tiers earn a hard constraint; advisory tiers annotate only.
        "hard": confidence in ("authoritative", "high"),
    }
    if extra:
        out.update(extra)
    return out


def resolve(
    pod: dict[str, Any],
    artifact: dict[str, Any] | None = None,
    gpu_total_mib: int = DEFAULT_GPU_TOTAL_MIB,
    observations: dict[str, list[float]] | None = None,
    fingerprint_min_samples: int = FINGERPRINT_MIN_SAMPLES,
    config_docs: list[Any] | None = None,
) -> dict[str, Any]:
    """Resolve a pod's expected peak VRAM through the confidence cascade (tiers 1, 4, 3, 2)."""
    if artifact is None:
        artifact = json.loads(DEFAULT_MODEL.read_text())
    ann = _pod_annotations(pod)
    fingerprint = pod_fingerprint(pod)

    # Tier 1 — explicit annotation (authoritative).
    explicit = _explicit_estimate_mib(ann)
    if explicit is not None:
        return _result(explicit, "explicit-annotation", "authoritative", [], fingerprint)

    # Tier 4 — historical observation by workload fingerprint (measured beats sniffed).
    if observations:
        samples = observations.get(fingerprint_key(fingerprint), [])
        if len(samples) >= fingerprint_min_samples:
            return _result(
                _p95(samples),
                "historical-fingerprint",
                "high",
                [],
                fingerprint,
                extra={"observation_samples": len(samples)},
            )

    row = _row_from_pod(pod)
    sniff_hints = hints_from_row(row, ann, ann)

    # Tier 3 — referenced training config (deepspeed/accelerate/HF) fetched via the k8s API,
    # merged over the sniffed hints (config fills gaps CLI/env can't).
    if config_docs:
        combined = dict(sniff_hints)
        combined.update({k: v for k, v in hints_from_config_docs(config_docs).items() if v not in (None, "")})
        job, _missing = row_from_hints(combined)
        if job:
            pred = predict(job, artifact, gpu_total_mib)
            return _result(
                pred["conservative_estimate_mib"],
                "config+model",
                "high",
                [],
                fingerprint,
                extra={
                    "point_estimate_mib": pred["point_estimate_mib"],
                    "selected_model": pred["selected_model"],
                },
            )

    # Tier 2 — static spec sniff (annotations/env/CLI) -> linear model.
    job, missing = row_from_hints(sniff_hints)
    if job:
        pred = predict(job, artifact, gpu_total_mib)
        return _result(
            pred["conservative_estimate_mib"],
            "static-sniff+model",
            "high",
            [],
            fingerprint,
            extra={
                "point_estimate_mib": pred["point_estimate_mib"],
                "selected_model": pred["selected_model"],
            },
        )

    # Nothing resolvable -> advisory only (never hard-admit on a guess).
    return _result(None, "unknown", "advisory", missing, fingerprint)


if __name__ == "__main__":
    import sys

    pod = json.load(sys.stdin)
    print(json.dumps(resolve(pod), indent=2, sort_keys=True))
