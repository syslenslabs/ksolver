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
# Inline training config (DeepSpeed / HF TrainingArguments / accelerate JSON) — a zero-infra tier-3
# path, so the config tier fires without a k8s ConfigMap fetch. Merges with any passed config_docs.
INLINE_CONFIG_ANNOTATION = "ksolver.ai/vram-config"
MIB = 1024.0 * 1024.0

# A model *prediction* above the largest plausible single-GPU VRAM (or <=0) is almost certainly a
# bad extrapolation. It must NOT become a hard node constraint — that would strand the job on zero
# feasible nodes. ~144 GiB covers today's biggest single GPU (H200 141 GB) with headroom.
MAX_PLAUSIBLE_SINGLE_GPU_MIB = 144 * 1024

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


# Starter table of well-known model architectures, so a pod that only references a model name
# still yields the family/hidden/layers the linear model needs. Approximate; extend as needed.
KNOWN_MODELS: dict[str, dict[str, Any]] = {
    "gpt2": {"family": "transformer", "hidden_size": 768, "layers": 12, "heads": 12, "param_count": 124_000_000},
    "gpt2-medium": {"family": "transformer", "hidden_size": 1024, "layers": 24, "heads": 16, "param_count": 355_000_000},
    "gpt2-large": {"family": "transformer", "hidden_size": 1280, "layers": 36, "heads": 20, "param_count": 774_000_000},
    "bert-base-uncased": {"family": "transformer", "hidden_size": 768, "layers": 12, "heads": 12, "param_count": 110_000_000},
    "bert-large-uncased": {"family": "transformer", "hidden_size": 1024, "layers": 24, "heads": 16, "param_count": 340_000_000},
    "llama-2-7b": {"family": "transformer", "hidden_size": 4096, "layers": 32, "heads": 32, "param_count": 6_700_000_000},
    "llama-2-13b": {"family": "transformer", "hidden_size": 5120, "layers": 40, "heads": 40, "param_count": 13_000_000_000},
    "resnet50": {"family": "cnn", "hidden_size": 2048, "layers": 50, "param_count": 25_000_000},
    "vit-b-16": {"family": "cnn", "hidden_size": 768, "layers": 12, "param_count": 86_000_000},
}

MODEL_NAME_ANNOTATION = "ksolver.ai/vram-model"
_MODEL_ENV_KEYS = {"MODEL_NAME", "MODEL", "HF_MODEL", "MODEL_NAME_OR_PATH", "PRETRAINED_MODEL_NAME_OR_PATH"}


def _normalize_model_name(raw: str) -> str:
    name = raw.strip().strip("'\"").split("/")[-1].lower()  # drop org prefix (meta-llama/Llama-2-7b)
    return name.replace("_", "-")


def infer_model_hints(command: list[str], args: list[str], env: dict[str, Any], ann: dict[str, str]) -> dict[str, Any]:
    """If a well-known model name appears in the pod, return its architecture hints (else {})."""
    candidates: list[str] = []
    if ann.get(MODEL_NAME_ANNOTATION):
        candidates.append(ann[MODEL_NAME_ANNOTATION])
    for key, value in (env or {}).items():
        if key.upper() in _MODEL_ENV_KEYS and value:
            candidates.append(str(value))
    tokens = list(command or []) + list(args or [])
    for i, tok in enumerate(tokens):
        if tok in ("--model", "--model-name", "--model_name_or_path", "--pretrained", "--model-id") and i + 1 < len(tokens):
            candidates.append(tokens[i + 1])
        if "=" in tok:
            candidates.append(tok.split("=", 1)[1])
        candidates.append(tok)
    for cand in candidates:
        arch = KNOWN_MODELS.get(_normalize_model_name(cand))
        if arch:
            return dict(arch)
    return {}


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
        return _result(
            explicit, "explicit-annotation", "authoritative", [], fingerprint,
            extra={"explanation": f"operator-declared peak VRAM ({round(explicit / 1024.0, 2)} GiB)"},
        )

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
                extra={
                    "observation_samples": len(samples),
                    "explanation": f"measured p95 of {len(samples)} prior run(s) of this workload",
                },
            )

    row = _row_from_pod(pod)
    sniff_hints = hints_from_row(row, ann, ann)
    # Fill architecture gaps (family/hidden/layers/param) from a referenced known model name, so a
    # pod that only says e.g. `--model gpt2-large --batch-size 4 --seq-len 1024` still predicts.
    for key, value in infer_model_hints(
        row.get("command") or [], row.get("args") or [], row.get("env") or {}, ann
    ).items():
        sniff_hints.setdefault(key, value)

    # Tier 3 — referenced training config (deepspeed/accelerate/HF), merged over the sniffed hints
    # (config fills gaps CLI/env can't). Sources: config_docs passed in (e.g. fetched ConfigMaps)
    # plus an inline `ksolver.ai/vram-config` annotation (zero-infra path).
    effective_docs = list(config_docs) if config_docs else []
    inline = ann.get(INLINE_CONFIG_ANNOTATION)
    if inline:
        try:
            effective_docs.append(json.loads(inline))
        except (ValueError, TypeError):
            pass
    if effective_docs:
        combined = dict(sniff_hints)
        combined.update({k: v for k, v in hints_from_config_docs(effective_docs).items() if v not in (None, "")})
        job, _missing = row_from_hints(combined)
        if job:
            return _model_result(predict(job, artifact, gpu_total_mib), "config+model", fingerprint)

    # Tier 2 — static spec sniff (annotations/env/CLI) -> linear model.
    job, missing = row_from_hints(sniff_hints)
    if job:
        return _model_result(predict(job, artifact, gpu_total_mib), "static-sniff+model", fingerprint)

    # Nothing resolvable -> advisory only (never hard-admit on a guess).
    missing_txt = ", ".join(missing) if missing else "no usable hints"
    return _result(
        None, "unknown", "advisory", missing, fingerprint,
        extra={"explanation": f"no VRAM signal ({missing_txt}); advisory only"},
    )


def _model_result(pred: dict[str, Any], source: str, fingerprint: dict[str, Any]) -> dict[str, Any]:
    """Build a tier-2/tier-3 model result, downgrading implausible extrapolations to advisory so a
    bad prediction can never become a hard constraint that strands the job."""
    mib = pred.get("conservative_estimate_mib")
    job = pred.get("input") or {}
    shape = f"seq {job.get('seq_len')}" if job.get("seq_len") else f"image {job.get('image_size')}"
    explanation = (
        f"predicted {round((mib or 0) / 1024.0, 2)} GiB from {job.get('family')} "
        f"(hidden {job.get('hidden_size')}, {job.get('layers')} layers) at batch "
        f"{job.get('batch_size')}, {shape}, {job.get('precision')}"
    )
    extra = {
        "point_estimate_mib": pred.get("point_estimate_mib"),
        "selected_model": pred.get("selected_model"),
        "explanation": explanation,
    }
    if mib is None or mib <= 0 or mib > MAX_PLAUSIBLE_SINGLE_GPU_MIB:
        extra["guard"] = "prediction outside plausible single-GPU VRAM range; advisory only"
        extra["explanation"] = explanation + "; exceeds plausible single-GPU VRAM, advisory only"
        keep = mib if (mib is not None and mib > 0) else None
        return _result(keep, source, "advisory", [], fingerprint, extra=extra)
    return _result(mib, source, "high", [], fingerprint, extra=extra)


if __name__ == "__main__":
    import sys

    pod = json.load(sys.stdin)
    print(json.dumps(resolve(pod), indent=2, sort_keys=True))
