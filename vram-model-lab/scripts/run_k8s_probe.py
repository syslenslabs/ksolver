#!/usr/bin/env python3
"""Generate and run single-GPU Kubernetes VRAM probes.

The runner submits one Kubernetes Job at a time, waits for completion, stores
pod logs, and extracts the KSOLVER_VRAM_RESULT JSON line emitted by the probe.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
SCENARIOS = ROOT / "scenarios.yaml"
DATA = ROOT / "data"
RAW = DATA / "raw"
RESULTS = DATA / "results.jsonl"
DEFAULT_IMAGE = "pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime"


WORKLOAD = r"""
import hashlib
import json
import math
import os
import subprocess
import sys
import threading
import time

import torch
import torch.nn as nn
import torch.nn.functional as F


def env_int(name, default):
    return int(os.environ.get(name, str(default)))


def env_bool(name, default=False):
    return os.environ.get(name, str(default)).lower() in ("1", "true", "yes", "on")


FAMILY = os.environ.get("KSOLVER_FAMILY", "mlp")
MODEL_ARCH = os.environ.get("KSOLVER_MODEL_ARCH", FAMILY)
TRAINER_STYLE = os.environ.get("KSOLVER_TRAINER_STYLE", "synthetic")
# Reality-only: this flag is NEVER trusted from the scenario label. It is set True solely by
# build_real_torchvision_model() when a genuine published architecture is actually instantiated,
# so a synthetic run can never masquerade as real-framework data.
VERIFIED_REAL_FRAMEWORK = False
REAL_FRAMEWORK_NAME = None
CUSTOMER_WORKLOAD_FINGERPRINT = env_bool("KSOLVER_CUSTOMER_WORKLOAD_FINGERPRINT")
PRECISION = os.environ.get("KSOLVER_PRECISION", "fp32")
BATCH_SIZE = env_int("KSOLVER_BATCH_SIZE", 8)
SEQ_LEN = env_int("KSOLVER_SEQ_LEN", 512)
IMAGE_SIZE = env_int("KSOLVER_IMAGE_SIZE", 224)
HIDDEN_SIZE = env_int("KSOLVER_HIDDEN_SIZE", 768)
LAYERS = env_int("KSOLVER_LAYERS", 4)
HEADS = env_int("KSOLVER_HEADS", 8)
STEPS = env_int("KSOLVER_STEPS", 5)
OPTIMIZER = os.environ.get("KSOLVER_OPTIMIZER", "adamw")
ACTIVATION_CHECKPOINTING = env_bool("KSOLVER_ACTIVATION_CHECKPOINTING")
GRADIENT_ACCUMULATION_STEPS = env_int("KSOLVER_GRADIENT_ACCUMULATION_STEPS", 1)
REQUESTED_GPU_TYPE = os.environ.get("KSOLVER_REQUESTED_GPU_TYPE", "unknown")
REQUESTED_GPU_COUNT = env_int("KSOLVER_REQUESTED_GPU_COUNT", 1)
SCENARIO = os.environ.get("KSOLVER_SCENARIO", "manual")
RESERVE_EXTRA_MIB = env_int("KSOLVER_RESERVE_EXTRA_MIB", 0)
SAMPLE_INTERVAL_SECONDS = float(os.environ.get("KSOLVER_SAMPLE_INTERVAL_SECONDS", "0.2"))
INPUT_PIPELINE = os.environ.get("KSOLVER_INPUT_PIPELINE", "gpu_synthetic")
DATALOADER_SLEEP_MS = env_int("KSOLVER_DATALOADER_SLEEP_MS", 0)


if not torch.cuda.is_available():
    print("KSOLVER_VRAM_RESULT " + json.dumps({
        "scenario": SCENARIO,
        "ok": False,
        "oom": False,
        "error": "torch.cuda.is_available() is false",
    }), flush=True)
    sys.exit(2)


device = torch.device("cuda")
gpu_name = torch.cuda.get_device_name(0)
gpu_props = torch.cuda.get_device_properties(0)
gpu_total_mib = int(gpu_props.total_memory / 1024 / 1024)


def dtype_for_precision():
    if PRECISION == "fp16":
        return torch.float16
    if PRECISION == "bf16":
        return torch.bfloat16
    return torch.float32


class TinyMLP(nn.Module):
    def __init__(self):
        super().__init__()
        mods = [nn.Linear(SEQ_LEN, HIDDEN_SIZE), nn.GELU()]
        for _ in range(max(0, LAYERS - 2)):
            mods += [nn.Linear(HIDDEN_SIZE, HIDDEN_SIZE), nn.GELU()]
        mods.append(nn.Linear(HIDDEN_SIZE, SEQ_LEN))
        self.net = nn.Sequential(*mods)

    def forward(self, x):
        return self.net(x)


class TinyCNN(nn.Module):
    def __init__(self):
        super().__init__()
        channels = max(16, HIDDEN_SIZE // 8)
        mods = [nn.Conv2d(3, channels, 3, padding=1), nn.ReLU()]
        for _ in range(max(1, LAYERS - 1)):
            mods += [nn.Conv2d(channels, channels, 3, padding=1), nn.ReLU()]
        self.features = nn.Sequential(*mods)
        self.head = nn.Linear(channels, 1000)

    def forward(self, x):
        y = self.features(x)
        y = y.mean(dim=(2, 3))
        return self.head(y)


class ResidualBlock(nn.Module):
    def __init__(self, channels):
        super().__init__()
        self.net = nn.Sequential(
            nn.Conv2d(channels, channels, 3, padding=1),
            nn.BatchNorm2d(channels),
            nn.ReLU(),
            nn.Conv2d(channels, channels, 3, padding=1),
            nn.BatchNorm2d(channels),
        )

    def forward(self, x):
        return F.relu(x + self.net(x))


class ResNetish(nn.Module):
    def __init__(self):
        super().__init__()
        channels = max(16, HIDDEN_SIZE // 8)
        self.stem = nn.Sequential(nn.Conv2d(3, channels, 7, stride=2, padding=3), nn.BatchNorm2d(channels), nn.ReLU())
        self.blocks = nn.Sequential(*[ResidualBlock(channels) for _ in range(max(1, LAYERS))])
        self.head = nn.Linear(channels, 1000)

    def forward(self, x):
        y = self.stem(x)
        y = self.blocks(y)
        y = y.mean(dim=(2, 3))
        return self.head(y)


class DepthwiseBlock(nn.Module):
    def __init__(self, channels):
        super().__init__()
        self.net = nn.Sequential(
            nn.Conv2d(channels, channels, 3, padding=1, groups=channels),
            nn.Conv2d(channels, channels * 4, 1),
            nn.SiLU(),
            nn.Conv2d(channels * 4, channels, 1),
        )

    def forward(self, x):
        return x + self.net(x)


class EfficientNetish(nn.Module):
    def __init__(self):
        super().__init__()
        channels = max(16, HIDDEN_SIZE // 8)
        self.stem = nn.Sequential(nn.Conv2d(3, channels, 3, stride=2, padding=1), nn.SiLU())
        self.blocks = nn.Sequential(*[DepthwiseBlock(channels) for _ in range(max(1, LAYERS))])
        self.head = nn.Linear(channels, 1000)

    def forward(self, x):
        y = self.stem(x)
        y = self.blocks(y)
        y = y.mean(dim=(2, 3))
        return self.head(y)


class ConvNeXtBlock(nn.Module):
    def __init__(self, channels):
        super().__init__()
        self.dw = nn.Conv2d(channels, channels, 7, padding=3, groups=channels)
        self.pw1 = nn.Conv2d(channels, channels * 4, 1)
        self.pw2 = nn.Conv2d(channels * 4, channels, 1)

    def forward(self, x):
        y = self.dw(x)
        y = F.gelu(self.pw1(y))
        return x + self.pw2(y)


class ConvNeXtish(nn.Module):
    def __init__(self):
        super().__init__()
        channels = max(16, HIDDEN_SIZE // 8)
        self.stem = nn.Conv2d(3, channels, 4, stride=4)
        self.blocks = nn.Sequential(*[ConvNeXtBlock(channels) for _ in range(max(1, LAYERS))])
        self.head = nn.Linear(channels, 1000)

    def forward(self, x):
        y = self.stem(x)
        y = self.blocks(y)
        y = y.mean(dim=(2, 3))
        return self.head(y)


class TinyTransformer(nn.Module):
    def __init__(self):
        super().__init__()
        causal = MODEL_ARCH in ("gpt", "gpt-style", "causal-lm")
        layer = nn.TransformerEncoderLayer(
            d_model=HIDDEN_SIZE,
            nhead=HEADS,
            dim_feedforward=HIDDEN_SIZE * 4,
            dropout=0.0,
            batch_first=True,
            activation="gelu",
        )
        self.embed = nn.Embedding(32000, HIDDEN_SIZE)
        self.encoder = nn.TransformerEncoder(layer, num_layers=LAYERS)
        self.head = nn.Linear(HIDDEN_SIZE, 32000)
        self.causal = causal

    def forward(self, x):
        y = self.embed(x)
        mask = None
        if self.causal:
            mask = torch.triu(torch.full((x.shape[1], x.shape[1]), float("-inf"), device=x.device, dtype=y.dtype), diagonal=1)
        if ACTIVATION_CHECKPOINTING:
            import torch.utils.checkpoint as ckpt
            for layer in self.encoder.layers:
                if mask is None:
                    y = ckpt.checkpoint(layer, y, use_reentrant=False)
                else:
                    y = ckpt.checkpoint(lambda inp: layer(inp, src_mask=mask), y, use_reentrant=False)
        else:
            y = self.encoder(y, mask=mask)
        return self.head(y)


def build_real_torchvision_model():
    # Real published architecture from torchvision (bundled in the pytorch/pytorch image), NOT the
    # synthetic harness. This is the only path that may mark a sample verified_real_framework=True.
    global VERIFIED_REAL_FRAMEWORK, REAL_FRAMEWORK_NAME
    import torchvision.models as tvm

    synthetic_aliases = ("cnn", "mlp", "transformer", "resnet-style", "efficientnet-style", "convnext-style")
    name = "resnet50" if MODEL_ARCH in synthetic_aliases else MODEL_ARCH
    factory = getattr(tvm, name, None)
    if not callable(factory):
        raise ValueError("unknown torchvision model: " + str(name))
    model = factory(weights=None)
    VERIFIED_REAL_FRAMEWORK = True
    REAL_FRAMEWORK_NAME = "torchvision:" + name
    return model


def build_model():
    if TRAINER_STYLE == "torchvision":
        if FAMILY != "cnn":
            raise ValueError("trainer_style=torchvision requires family=cnn (image input)")
        return build_real_torchvision_model()
    if FAMILY == "transformer":
        return TinyTransformer()
    if FAMILY == "cnn":
        if MODEL_ARCH in ("resnet", "resnet-style"):
            return ResNetish()
        if MODEL_ARCH in ("efficientnet", "efficientnet-style"):
            return EfficientNetish()
        if MODEL_ARCH in ("convnext", "convnext-style"):
            return ConvNeXtish()
        return TinyCNN()
    return TinyMLP()


def make_batch():
    target_device = torch.device("cpu") if INPUT_PIPELINE == "cpu_to_gpu" else device
    if FAMILY == "transformer":
        x = torch.randint(0, 32000, (BATCH_SIZE, SEQ_LEN), device=target_device)
        y = torch.randint(0, 32000, (BATCH_SIZE, SEQ_LEN), device=target_device)
        return x, y
    if FAMILY == "cnn":
        x = torch.randn(BATCH_SIZE, 3, IMAGE_SIZE, IMAGE_SIZE, device=target_device)
        y = torch.randint(0, 1000, (BATCH_SIZE,), device=target_device)
        return x, y
    x = torch.randn(BATCH_SIZE, SEQ_LEN, device=target_device)
    y = torch.randn(BATCH_SIZE, SEQ_LEN, device=target_device)
    return x, y


nvml_samples = []
stop_sampling = False


def sample_nvidia_smi():
    global stop_sampling
    sampler_started = time.time()
    while not stop_sampling:
        try:
            sample_time = time.time()
            out = subprocess.check_output([
                "nvidia-smi",
                "--query-gpu=timestamp,memory.used,utilization.gpu,power.draw,temperature.gpu",
                "--format=csv,noheader,nounits",
            ], text=True, timeout=2).strip()
            if out:
                parts = [p.strip() for p in out.split(",")]
                nvml_samples.append({
                    "elapsed_seconds": sample_time - sampler_started,
                    "ts": parts[0],
                    "memory_used_mib": int(float(parts[1])),
                    "gpu_util_pct": int(float(parts[2])),
                    "power_w": float(parts[3]) if parts[3] != "[Not Supported]" else None,
                    "temp_c": int(float(parts[4])),
                })
        except Exception:
            pass
        time.sleep(SAMPLE_INTERVAL_SECONDS)


sampler = threading.Thread(target=sample_nvidia_smi, daemon=True)
sampler.start()
torch.cuda.empty_cache()
torch.cuda.reset_peak_memory_stats()
started = time.time()
ok = True
oom = False
error = None
steps_completed = 0
extra_reservation = None

try:
    if RESERVE_EXTRA_MIB > 0:
        reserve_elems = int(RESERVE_EXTRA_MIB * 1024 * 1024 / 2)
        extra_reservation = torch.empty(reserve_elems, dtype=torch.float16, device=device)
        extra_reservation.fill_(0)
        torch.cuda.synchronize()

    model = build_model().to(device=device, dtype=dtype_for_precision())
    param_count = sum(p.numel() for p in model.parameters())
    if OPTIMIZER == "sgd":
        opt = torch.optim.SGD(model.parameters(), lr=1e-3)
    else:
        opt = torch.optim.AdamW(model.parameters(), lr=1e-4)

    for step in range(STEPS):
        opt.zero_grad(set_to_none=True)
        loss = None
        for _accum in range(GRADIENT_ACCUMULATION_STEPS):
            if DATALOADER_SLEEP_MS > 0:
                time.sleep(DATALOADER_SLEEP_MS / 1000.0)
            x, y = make_batch()
            if INPUT_PIPELINE == "cpu_to_gpu":
                x = x.to(device=device, non_blocking=False)
                y = y.to(device=device, non_blocking=False)
            if x.is_floating_point():
                x = x.to(dtype=dtype_for_precision())
            out = model(x)
            if FAMILY == "transformer":
                step_loss = F.cross_entropy(out.reshape(-1, out.shape[-1]), y.reshape(-1))
            elif FAMILY == "cnn":
                step_loss = F.cross_entropy(out, y)
            else:
                step_loss = F.mse_loss(out.float(), y.float())
            (step_loss / GRADIENT_ACCUMULATION_STEPS).backward()
            loss = step_loss
        opt.step()
        torch.cuda.synchronize()
        steps_completed += 1
        print("KSOLVER_VRAM_SAMPLE " + json.dumps({
            "scenario": SCENARIO,
            "step": step + 1,
            "allocated_mib": int(torch.cuda.memory_allocated() / 1024 / 1024),
            "reserved_mib": int(torch.cuda.memory_reserved() / 1024 / 1024),
            "max_allocated_mib": int(torch.cuda.max_memory_allocated() / 1024 / 1024),
            "max_reserved_mib": int(torch.cuda.max_memory_reserved() / 1024 / 1024),
        }), flush=True)
except torch.cuda.OutOfMemoryError as exc:
    ok = False
    oom = True
    error = str(exc)
    param_count = locals().get("param_count", None)
except Exception as exc:
    ok = False
    error = repr(exc)
    param_count = locals().get("param_count", None)
finally:
    stop_sampling = True
    sampler.join(timeout=2)

elapsed = time.time() - started
max_nvml_mib = max([s["memory_used_mib"] for s in nvml_samples], default=None)
result = {
    "schema_version": 1,
    "scenario": SCENARIO,
    "ok": ok,
    "oom": oom,
    "error": error,
    "framework": "pytorch",
    "framework_version": torch.__version__,
    "real_framework": REAL_FRAMEWORK_NAME,
    "trainer_style": TRAINER_STYLE,
    "verified_real_framework": VERIFIED_REAL_FRAMEWORK,
    "customer_workload_fingerprint": CUSTOMER_WORKLOAD_FINGERPRINT,
    "family": FAMILY,
    "model_arch": MODEL_ARCH,
    "precision": PRECISION,
    "batch_size": BATCH_SIZE,
    "seq_len": SEQ_LEN if FAMILY != "cnn" else None,
    "image_size": IMAGE_SIZE if FAMILY == "cnn" else None,
    "hidden_size": HIDDEN_SIZE,
    "layers": LAYERS,
    "heads": HEADS if FAMILY == "transformer" else None,
    "optimizer": OPTIMIZER,
    "activation_checkpointing": ACTIVATION_CHECKPOINTING,
    "gradient_accumulation_steps": GRADIENT_ACCUMULATION_STEPS,
    "requested_gpu_type": REQUESTED_GPU_TYPE,
    "requested_gpu_count": REQUESTED_GPU_COUNT,
    "sample_interval_seconds": SAMPLE_INTERVAL_SECONDS,
    "input_pipeline": INPUT_PIPELINE,
    "dataloader_sleep_ms": DATALOADER_SLEEP_MS,
    "reserve_extra_mib": RESERVE_EXTRA_MIB,
    "param_count": param_count,
    "steps_requested": STEPS,
    "steps_completed": steps_completed,
    "elapsed_seconds": elapsed,
    "samples_per_second": (BATCH_SIZE * steps_completed / elapsed) if elapsed > 0 else None,
    "gpu_name": gpu_name,
    "gpu_total_mib": gpu_total_mib,
    "torch_peak_allocated_mib": int(torch.cuda.max_memory_allocated() / 1024 / 1024),
    "torch_peak_reserved_mib": int(torch.cuda.max_memory_reserved() / 1024 / 1024),
    "nvidia_smi_peak_used_mib": max_nvml_mib,
    "nvidia_smi_samples": nvml_samples,
}
print("KSOLVER_VRAM_RESULT " + json.dumps(result, sort_keys=True), flush=True)
sys.exit(0 if ok or oom else 1)
"""


def run(cmd: list[str], *, input_text: str | None = None, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def slug(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9-]+", "-", value)
    return value.strip("-")[:45]


def load_scenarios(path: Path) -> list[dict[str, Any]]:
    with path.open() as f:
        doc = yaml.safe_load(f)
    return list(doc["scenarios"])


def existing_scenario_names() -> set[str]:
    if not RESULTS.exists():
        return set()
    names = set()
    for line in RESULTS.read_text().splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("scenario"):
            names.add(row["scenario"])
    return names


def scenario_env(scenario: dict[str, Any]) -> list[dict[str, str]]:
    mapping = {
        "KSOLVER_SCENARIO": scenario["name"],
        "KSOLVER_FAMILY": scenario.get("family", "mlp"),
        "KSOLVER_MODEL_ARCH": scenario.get("model_arch", scenario.get("family", "mlp")),
        "KSOLVER_TRAINER_STYLE": scenario.get("trainer_style", "synthetic"),
        "KSOLVER_VERIFIED_REAL_FRAMEWORK": str(scenario.get("verified_real_framework", False)).lower(),
        "KSOLVER_CUSTOMER_WORKLOAD_FINGERPRINT": str(scenario.get("customer_workload_fingerprint", False)).lower(),
        "KSOLVER_PRECISION": scenario.get("precision", "fp32"),
        "KSOLVER_BATCH_SIZE": scenario.get("batch_size", 8),
        "KSOLVER_SEQ_LEN": scenario.get("seq_len", 512),
        "KSOLVER_IMAGE_SIZE": scenario.get("image_size", 224),
        "KSOLVER_HIDDEN_SIZE": scenario.get("hidden_size", 768),
        "KSOLVER_LAYERS": scenario.get("layers", 4),
        "KSOLVER_HEADS": scenario.get("heads", 8),
        "KSOLVER_STEPS": scenario.get("steps", 5),
        "KSOLVER_OPTIMIZER": scenario.get("optimizer", "adamw"),
        "KSOLVER_ACTIVATION_CHECKPOINTING": str(scenario.get("activation_checkpointing", False)).lower(),
        "KSOLVER_GRADIENT_ACCUMULATION_STEPS": scenario.get("gradient_accumulation_steps", 1),
        "KSOLVER_REQUESTED_GPU_TYPE": scenario.get("requested_gpu_type", "unknown"),
        "KSOLVER_REQUESTED_GPU_COUNT": scenario.get("requested_gpu_count", 1),
        "KSOLVER_SAMPLE_INTERVAL_SECONDS": scenario.get("sample_interval_seconds", 0.2),
        "KSOLVER_INPUT_PIPELINE": scenario.get("input_pipeline", "gpu_synthetic"),
        "KSOLVER_DATALOADER_SLEEP_MS": scenario.get("dataloader_sleep_ms", 0),
        "KSOLVER_RESERVE_EXTRA_MIB": scenario.get("reserve_extra_mib", 0),
        "NVIDIA_VISIBLE_DEVICES": "all",
        "NVIDIA_DRIVER_CAPABILITIES": "compute,utility",
    }
    return [{"name": k, "value": str(v)} for k, v in mapping.items()]


def scenario_vram_annotations(scenario: dict[str, Any]) -> dict[str, str]:
    """Expose the declared pre-run fingerprint through the resolver's public contract."""
    fields = {
        "family": scenario.get("family"),
        "precision": scenario.get("precision"),
        "batch-size": scenario.get("batch_size"),
        "seq-len": scenario.get("seq_len"),
        "image-size": scenario.get("image_size"),
        "hidden-size": scenario.get("hidden_size"),
        "layers": scenario.get("layers"),
        "heads": scenario.get("heads"),
        "optimizer": scenario.get("optimizer"),
        "activation-checkpointing": scenario.get("activation_checkpointing"),
        "param-count": scenario.get("param_count"),
        "reserve-extra-mib": scenario.get("reserve_extra_mib"),
    }
    return {
        f"ksolver.ai/vram-{key}": str(value).lower() if isinstance(value, bool) else str(value)
        for key, value in fields.items()
        if value not in (None, "", 0)
    }


def build_manifest(
    scenario: dict[str, Any],
    image: str,
    namespace: str,
    node_selector: dict[str, str] | None = None,
    tolerate_gpu: bool = False,
) -> dict[str, Any]:
    job_name = "ksolver-vram-" + slug(scenario["name"])
    # Cross-SKU runs (roadmap F1): target a specific GPU node pool via nodeSelector. A per-scenario
    # `node_selector` in scenarios.yaml is the base; the CLI `--node-selector` overrides on conflict
    # so the same matrix can be re-run per SKU without editing the file. GPU pools are usually tainted
    # (nvidia.com/gpu:NoSchedule), so `--tolerate-gpu` adds the matching toleration or the probe pod
    # stays Pending even with the right nodeSelector.
    merged_selector: dict[str, str] = {}
    merged_selector.update(scenario.get("node_selector") or {})
    merged_selector.update(node_selector or {})
    requested_gpu_count = int(scenario.get("requested_gpu_count", 1))
    vram_annotations = scenario_vram_annotations(scenario)

    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "runtimeClassName": "nvidia",
        "containers": [{
            "name": "probe",
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            # Request the physical GPU from the device plugin. The NVIDIA runtime alone
            # exposes the device but does not make this a Kubernetes-scheduled allocation.
            "resources": {
                "requests": {"nvidia.com/gpu": str(requested_gpu_count)},
                "limits": {"nvidia.com/gpu": str(requested_gpu_count)},
            },
            "env": scenario_env(scenario),
            "command": ["python", "-u", "-c", WORKLOAD],
        }],
    }
    if merged_selector:
        pod_spec["nodeSelector"] = merged_selector
    if tolerate_gpu:
        pod_spec["tolerations"] = [{
            "key": "nvidia.com/gpu",
            "operator": "Exists",
            "effect": "NoSchedule",
        }]

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "ksolver-vram-probe",
                "ksolver.ai/vram-scenario": slug(scenario["name"]),
            },
        },
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 3600,
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/name": "ksolver-vram-probe",
                        "ksolver.ai/vram-scenario": slug(scenario["name"]),
                    },
                    "annotations": vram_annotations,
                },
                "spec": pod_spec,
            },
        },
    }


def manifest_hash(manifest: dict[str, Any]) -> str:
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def command_hash() -> str:
    return hashlib.sha256(WORKLOAD.encode()).hexdigest()


def wait_for_pod(job_name: str, namespace: str, timeout: int) -> str:
    deadline = time.time() + timeout
    selector = f"job-name={job_name}"
    while time.time() < deadline:
        got = run(["kubectl", "-n", namespace, "get", "pods", "-l", selector, "-o", "json"])
        if got.returncode == 0:
            items = json.loads(got.stdout).get("items", [])
            if items:
                return items[0]["metadata"]["name"]
        time.sleep(1)
    raise TimeoutError(f"timed out waiting for pod for {job_name}")


def wait_for_job(job_name: str, namespace: str, timeout: int) -> None:
    deadline = time.time() + timeout
    last_error = ""
    while time.time() < deadline:
        got = run(["kubectl", "-n", namespace, "get", "job", job_name, "-o", "json"], timeout=30)
        if got.returncode != 0:
            last_error = got.stderr.strip() or got.stdout.strip()
            time.sleep(1)
            continue
        job = json.loads(got.stdout)
        status = job.get("status", {})
        if status.get("succeeded", 0) > 0:
            return
        if status.get("failed", 0) > 0:
            return
        for condition in status.get("conditions", []):
            if condition.get("type") in ("Complete", "Failed") and condition.get("status") == "True":
                return
        time.sleep(1)
    raise RuntimeError(last_error or f"timed out waiting for job/{job_name}")


def image_digest_for_pod(pod_name: str, namespace: str) -> str | None:
    got = run(["kubectl", "-n", namespace, "get", "pod", pod_name, "-o", "json"])
    if got.returncode != 0:
        return None
    pod = json.loads(got.stdout)
    statuses = pod.get("status", {}).get("containerStatuses", [])
    if not statuses:
        return None
    return statuses[0].get("imageID")


def parse_result(log_text: str) -> dict[str, Any] | None:
    for line in reversed(log_text.splitlines()):
        if line.startswith("KSOLVER_VRAM_RESULT "):
            return json.loads(line.split(" ", 1)[1])
    return None


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")


def parse_key_value_pairs(pairs: list[str]) -> dict[str, str]:
    """Parse repeatable KEY=VALUE CLI args into a dict (later duplicates win)."""
    out: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"--node-selector expects KEY=VALUE, got {pair!r}")
        key, value = pair.split("=", 1)
        key = key.strip()
        if not key:
            raise SystemExit(f"--node-selector has an empty key: {pair!r}")
        out[key] = value.strip()
    return out


def run_scenario(
    scenario: dict[str, Any],
    image: str,
    namespace: str,
    timeout: int,
    keep_jobs: bool,
    node_selector: dict[str, str] | None = None,
    tolerate_gpu: bool = False,
) -> dict[str, Any]:
    manifest = build_manifest(scenario, image, namespace, node_selector, tolerate_gpu)
    job_name = manifest["metadata"]["name"]
    mh = manifest_hash(manifest)
    RAW.mkdir(parents=True, exist_ok=True)

    run(["kubectl", "-n", namespace, "delete", "job", job_name, "--ignore-not-found=true"], timeout=60)
    applied = run(["kubectl", "apply", "-f", "-"], input_text=yaml.safe_dump(manifest), timeout=60)
    if applied.returncode != 0:
        raise RuntimeError(applied.stderr or applied.stdout)

    pod_name = wait_for_pod(job_name, namespace, timeout)
    print(f"running {scenario['name']} in pod/{pod_name}", flush=True)
    try:
        wait_for_job(job_name, namespace, timeout)
    finally:
        logs = run(["kubectl", "-n", namespace, "logs", f"pod/{pod_name}", "--timestamps=false"], timeout=120)
        log_text = logs.stdout + logs.stderr
        log_path = RAW / f"{int(time.time())}-{slug(scenario['name'])}.log"
        log_path.write_text(log_text)

    result = parse_result(log_text)
    if result is None:
        describe = run(["kubectl", "-n", namespace, "describe", "pod", pod_name], timeout=60)
        raise RuntimeError(f"probe did not emit result; logs at {log_path}\n{describe.stdout[-3000:]}")

    result.update({
        "kubernetes_namespace": namespace,
        "kubernetes_job": job_name,
        "kubernetes_pod": pod_name,
        "image": image,
        "image_digest": image_digest_for_pod(pod_name, namespace),
        "command_hash": command_hash(),
        "manifest_hash": mh,
        "raw_log": str(log_path.relative_to(ROOT)),
        "collected_at_unix": int(time.time()),
    })
    append_jsonl(RESULTS, result)

    if not keep_jobs:
        run(["kubectl", "-n", namespace, "delete", "job", job_name, "--ignore-not-found=true"], timeout=60)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", help="scenario name from scenarios.yaml")
    parser.add_argument("--all", action="store_true", help="run all scenarios")
    parser.add_argument("--scenarios-file", type=Path, default=SCENARIOS)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--wait-timeout", type=int, default=1200)
    parser.add_argument("--keep-jobs", action="store_true")
    parser.add_argument("--print-manifest", action="store_true", help="print manifests instead of submitting jobs")
    parser.add_argument("--skip-existing", action="store_true", help="skip scenario names already present in results.jsonl")
    parser.add_argument(
        "--node-selector",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="nodeSelector label to target a SKU's node pool (repeatable); overrides scenario node_selector",
    )
    parser.add_argument(
        "--tolerate-gpu",
        action="store_true",
        help="add a toleration for the standard nvidia.com/gpu:NoSchedule taint on GPU node pools",
    )
    args = parser.parse_args()

    node_selector = parse_key_value_pairs(args.node_selector)

    if not os.environ.get("KUBECONFIG"):
        print("warning: KUBECONFIG is not set; expected ~/.kube/wsl for this lab", file=sys.stderr)

    scenarios = load_scenarios(args.scenarios_file)
    if args.all:
        selected = scenarios
    elif args.scenario:
        selected = [s for s in scenarios if s["name"] == args.scenario]
        if not selected:
            raise SystemExit(f"unknown scenario {args.scenario!r}")
    else:
        selected = [s for s in scenarios if s["name"] == "smoke-mlp"]

    if args.print_manifest:
        for scenario in selected:
            print("---")
            print(yaml.safe_dump(
                build_manifest(scenario, args.image, args.namespace, node_selector, args.tolerate_gpu),
                sort_keys=False,
            ))
        return 0

    if args.skip_existing:
        seen = existing_scenario_names()
        before = len(selected)
        selected = [s for s in selected if s["name"] not in seen]
        print(f"skip-existing: {before - len(selected)} skipped, {len(selected)} remaining", flush=True)

    for scenario in selected:
        result = run_scenario(
            scenario, args.image, args.namespace, args.wait_timeout, args.keep_jobs,
            node_selector, args.tolerate_gpu,
        )
        peak = result.get("nvidia_smi_peak_used_mib")
        status = "oom" if result.get("oom") else ("ok" if result.get("ok") else "failed")
        print(f"{scenario['name']}: {status}, peak_nvidia_smi={peak} MiB", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
