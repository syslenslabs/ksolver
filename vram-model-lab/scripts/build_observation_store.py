#!/usr/bin/env python3
"""Seed a tier-4 VRAM observation store from measured lab runs (data/results.jsonl).

Reconstructs each run's pod fingerprint (image + command/env hash) exactly as the probe
submitted it, then records its measured peak via vram_resolver.record_observation.

NOTE: tier 4 only fires for a fingerprint seen >= FINGERPRINT_MIN_SAMPLES times, so this batch
seed is mainly useful for workloads that recur. The forward path — record_observation() on each
completed run (e.g. from ksolver's job observations) — is the primary populator.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import vram_resolver as vr
from run_k8s_probe import DEFAULT_IMAGE, WORKLOAD, scenario_env

# Scenario keys the probe reads (subset present in a results row); scenario_env fills the rest
# with the same defaults the probe used, so the reconstructed env hashes to the original.
SCEN_KEYS = [
    "name", "family", "model_arch", "trainer_style", "precision", "batch_size", "seq_len",
    "image_size", "hidden_size", "layers", "heads", "steps", "optimizer",
    "activation_checkpointing", "gradient_accumulation_steps", "reserve_extra_mib",
    "input_pipeline", "sample_interval_seconds", "dataloader_sleep_ms",
    "requested_gpu_type", "requested_gpu_count",
]


def pod_from_row(row: dict) -> dict:
    scenario = {k: row[k] for k in SCEN_KEYS if k in row and row[k] is not None}
    scenario.setdefault("name", row.get("scenario") or row.get("name") or "obs")
    env = scenario_env(scenario)  # already k8s-format list of {name, value}
    return {
        "spec": {
            "containers": [
                {
                    "name": "probe",
                    "image": row.get("image") or DEFAULT_IMAGE,
                    "command": ["python", "-u", "-c", WORKLOAD],
                    "args": [],
                    "env": env,
                    "resources": {"limits": {"nvidia.com/gpu": "1"}},
                }
            ]
        }
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=str(vr.ROOT / "data" / "results.jsonl"))
    ap.add_argument("--out", default=str(vr.ROOT / "data" / "observations.jsonl"))
    args = ap.parse_args()

    Path(args.out).write_text("")  # truncate
    recorded = 0
    for line in Path(args.results).read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        peak = row.get("nvidia_smi_peak_used_mib")
        if not row.get("ok") or not peak:
            continue
        vr.record_observation(args.out, pod_from_row(row), peak)
        recorded += 1

    store = vr.load_observations(args.out)
    recurring = sum(1 for v in store.values() if len(v) >= vr.FINGERPRINT_MIN_SAMPLES)
    print(
        f"recorded {recorded} observations -> {len(store)} distinct fingerprints; "
        f"{recurring} recur >= {vr.FINGERPRINT_MIN_SAMPLES} (tier-4-eligible). wrote {args.out}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
