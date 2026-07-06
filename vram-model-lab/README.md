# VRAM Model Lab

This folder is a local dataset builder for predicting peak GPU VRAM before ksolver
places a training job. It is intentionally separate from the scheduler code.

The current target environment is the WSL kube context:

```sh
export KUBECONFIG=~/.kube/wsl
```

On this machine, the 4090 is exposed to Kubernetes through:

- `runtimeClassName: nvidia`
- `NVIDIA_VISIBLE_DEVICES=all`
- `NVIDIA_DRIVER_CAPABILITIES=compute,utility`

The node does not currently advertise `nvidia.com/gpu`, so the collector runs
jobs serially and assumes exclusive use of the single card.

## Decision

Start with a Python in-container profiler, not a pure sidecar and not only a
Docker/YAML hash.

Why:

- A sidecar cannot reliably inspect live Python model objects in a different
  container unless the application cooperates.
- Docker image SHA plus YAML hash is useful as a cache key, but it does not tell
  us model shape, precision, optimizer, sequence length, activation behavior, or
  framework allocation policy.
- A generated short training probe gives ground truth quickly: peak allocated
  memory, peak reserved memory, `nvidia-smi` used memory, throughput, and OOM.

The product path should become layered:

1. Observed-history predictor keyed by image digest, command hash, YAML hash,
   framework, GPU SKU, and run knobs.
2. Optional Python SDK/wrapper that emits semantic metadata for PyTorch,
   Hugging Face, DeepSpeed, FSDP, TensorFlow, JAX, and Ray/Kubeflow launches.
3. Scheduler admission that consumes a conservative p95/p99 VRAM upper bound.

## Common Kubernetes Training Entrypoints

The first probes use plain Kubernetes `Job` because it works everywhere. The
same fingerprint fields map to the common higher-level submission APIs:

- Plain `Pod`, `Job`, or `CronJob`
- Kubeflow `PyTorchJob`
- Kubeflow `TFJob`
- Kubeflow `MPIJob`
- Ray `RayJob`
- Volcano `Job` or gang-scheduled `PodGroup`
- Argo Workflows
- Airflow, Flyte, or Metaflow launching Kubernetes pods
- Helm charts wrapping one of the above
- Custom operators that ultimately create pods

## Usage

## Matrix Harness

Yes, the lab has a matrix harness:

1. Define scenarios in YAML with model family and parameters.
2. Generate a deterministic matrix with `generate_scenario_grid.py`, or add a
   focused grid YAML.
3. Run the matrix on Kubernetes with `run_k8s_probe.py`.
4. Store raw per-run results in `data/results.jsonl`.
5. Export flat training rows to `data/training_rows.csv`.
6. Export per-sample memory curves to `data/memory_timeseries.csv`.
7. Fit/evaluate the model from the collected rows.

Core commands:

```sh
export KUBECONFIG=~/.kube/wsl

python3 vram-model-lab/scripts/generate_scenario_grid.py
python3 vram-model-lab/scripts/run_k8s_probe.py \
  --all \
  --scenarios-file vram-model-lab/generated/scenario_grid.yaml \
  --skip-existing \
  --wait-timeout 1800

python3 vram-model-lab/scripts/export_training_csv.py
python3 vram-model-lab/scripts/export_timeseries_csv.py
python3 vram-model-lab/scripts/fit_peak_vram_model.py
python3 vram-model-lab/scripts/evaluate_model.py
```

Generate a larger 4090 overnight-style sweep:

```sh
python3 vram-model-lab/scripts/generate_overnight_4090_sweep.py
export KUBECONFIG=~/.kube/wsl
python3 vram-model-lab/scripts/run_k8s_probe.py \
  --all \
  --scenarios-file vram-model-lab/generated/overnight_4090_sweep.yaml \
  --wait-timeout 1800
```

Generate a more realistic 4090 sweep. This uses longer runs, CPU-to-GPU batch
staging, a small dataloader delay, faster memory sampling, gradient accumulation
variants, and architecture-style CNN/Transformer/MLP probes:

```sh
python3 vram-model-lab/scripts/generate_realistic_4090_sweep.py --steps 30
export KUBECONFIG=~/.kube/wsl
python3 vram-model-lab/scripts/run_k8s_probe.py \
  --all \
  --scenarios-file vram-model-lab/generated/realistic_4090_sweep.yaml \
  --wait-timeout 1800
```

For a bounded daytime run:

```sh
python3 vram-model-lab/scripts/generate_realistic_4090_sweep.py --steps 30 --limit 6
python3 vram-model-lab/scripts/run_k8s_probe.py \
  --all \
  --scenarios-file vram-model-lab/generated/realistic_4090_sweep.yaml \
  --wait-timeout 1800
```

Generate a targeted follow-up iteration after the main sweep. This is for the
next most important gaps: repeatability, longer sequence lengths, and
near-capacity pressure rows:

```sh
python3 vram-model-lab/scripts/generate_iteration_4090_sweep.py --iteration 2
export KUBECONFIG=~/.kube/wsl
python3 vram-model-lab/scripts/run_k8s_probe.py \
  --all \
  --scenarios-file vram-model-lab/generated/iteration_4090_sweep.yaml \
  --wait-timeout 1800
```

`reserve_extra_mib` is the synthetic VRAM headroom probe knob: rows with this
value greater than zero intentionally allocate synthetic VRAM padding so the
probe can learn near-OOM behavior and scheduler headroom. Treat it as a
stress-test signal, not as organic model memory demand.

The CSV includes the target columns:

- `nvidia_smi_peak_used_mib`
- `torch_peak_allocated_mib`
- `torch_peak_reserved_mib`

It also includes feature columns such as:

- `family`, `precision`, `precision_bytes`
- `batch_size`, `seq_len`, `image_size`, `hidden_size`, `layers`, `heads`
- `optimizer`, `activation_checkpointing`, `reserve_extra_mib`
- `verified_real_framework`, `customer_workload_fingerprint`
- `param_count`, `param_count_m`, `activation_units_m`, `tokens_or_pixels`
- `gpu_name`, `gpu_total_mib`, image/command/manifest hashes

Use `verified_real_framework: true` only for a probe that runs a real training
entrypoint, such as Hugging Face Trainer, torchvision/timm, DeepSpeed, FSDP,
Accelerate, TensorFlow, or JAX, instead of the synthetic architecture harness.
Use `customer_workload_fingerprint: true` only when the row is attached to a
production/customer-style fingerprint such as image digest, command hash,
framework profile, GPU SKU, and completed-job outcome history.

To inspect the Kubernetes env that will feed those labels into the probe result
without creating a Job:

```sh
python3 vram-model-lab/scripts/run_k8s_probe.py \
  --print-manifest \
  --scenarios-file vram-model-lab/examples/evidence-gate-scenarios.yaml \
  --scenario verified-hf-trainer-fingerprint-smoke
```

To verify every evidence-gate example preserves those labels through manifest
generation:

```sh
python3 vram-model-lab/scripts/verify_evidence_gate_manifest.py
```

The lab fits two model types:

- VRAM regression: `fit_peak_vram_model.py`
- OOM/near-capacity risk classifier: `fit_oom_classifier.py`

The safe pipeline command also exports the CSV:

```sh
python3 vram-model-lab/scripts/run_pipeline.py
```

The safe pipeline also runs `verify_evidence_gate_manifest.py` and records
`evidence_gate_verifier_ok` in `data/models/scheduler_report.json`, so broken
verified-framework/customer-fingerprint manifest plumbing fails the scheduler
demo readiness gate before any hard-admission claims are made.

Run the safe no-new-probes pipeline:

```sh
export KUBECONFIG=~/.kube/wsl
python3 vram-model-lab/scripts/run_pipeline.py
```

This refits/evaluates the current dataset, predicts the example manifest, checks
for leftover probe resources, and writes:

- `vram-model-lab/data/models/scheduler_report.json`

Run a small smoke probe:

```sh
export KUBECONFIG=~/.kube/wsl
python3 vram-model-lab/scripts/run_k8s_probe.py --scenario smoke-mlp --wait-timeout 900
```

Run the default scenario grid:

```sh
export KUBECONFIG=~/.kube/wsl
python3 vram-model-lab/scripts/run_k8s_probe.py --all --wait-timeout 1800
```

Generate and run a deterministic sweep:

```sh
python3 vram-model-lab/scripts/generate_scenario_grid.py --limit 10
export KUBECONFIG=~/.kube/wsl
python3 vram-model-lab/scripts/run_k8s_probe.py \
  --all \
  --scenarios-file vram-model-lab/generated/scenario_grid.yaml \
  --skip-existing \
  --wait-timeout 1800
```

Run the pipeline and opt into probe collection:

```sh
export KUBECONFIG=~/.kube/wsl
python3 vram-model-lab/scripts/run_pipeline.py --run-smoke
python3 vram-model-lab/scripts/run_pipeline.py --run-grid
```

Fit the first simple model from collected results:

```sh
python3 vram-model-lab/scripts/export_training_csv.py
python3 vram-model-lab/scripts/fit_peak_vram_model.py
python3 vram-model-lab/scripts/evaluate_model.py
```

Predict a proposed job with a conservative safety margin:

```sh
python3 vram-model-lab/scripts/predict_peak_vram.py \
  --family transformer \
  --precision fp16 \
  --batch-size 8 \
  --seq-len 2048 \
  --hidden-size 1024 \
  --layers 8 \
  --heads 16 \
  --optimizer adamw \
  --param-count 166337792
```

Predict directly from Kubernetes manifests with annotations or env hints:

```sh
python3 vram-model-lab/scripts/predict_manifest_vram.py \
  vram-model-lab/examples/annotated-training-manifests.yaml
```

Explore the manifest suite for common submitters:

```sh
for f in vram-model-lab/manifests/*.yaml; do
  echo "$f"
  python3 vram-model-lab/scripts/predict_manifest_vram.py "$f"
done
```

See [manifests/README.md](manifests/README.md) for what each submission style
represents and which fields drive VRAM.

Data lands in:

- `vram-model-lab/data/raw/`: complete pod logs
- `vram-model-lab/data/results.jsonl`: one parsed result per run
- `vram-model-lab/data/training_rows.csv`: flat CSV for model training
- `vram-model-lab/data/memory_timeseries.csv`: one row per `nvidia-smi`
  interval sample
- `vram-model-lab/data/models/`: fitted model coefficients

## SDK / Sidecar Shape

The practical split is:

- Python SDK or wrapper emits semantic metadata from inside the training process.
- A future sidecar can collect that metadata from an `emptyDir` volume or local
  HTTP endpoint.
- A DaemonSet or probe runner collects ground-truth GPU telemetry.

Example SDK usage:

```python
import ksolver_vram_profile as ks

ks.report_training_job(
    model=model,
    framework="pytorch",
    model_name="llama-finetune",
    precision="bf16",
    batch_size=4,
    sequence_length=4096,
    optimizer="adamw",
    distributed_strategy="fsdp",
)
```

The sidecar-only approach is weaker because a sidecar cannot reliably inspect
Python objects in a different container. It can collect files, labels, env vars,
and logs, but the application process has to cooperate for high-quality
parameter count, precision, batch shape, optimizer, and distributed strategy.

Local SDK-to-sidecar validation without torch:

```sh
tmpdir=$(mktemp -d)
export PYTHONPATH=vram-model-lab
export KSOLVER_PROFILE_OUTPUT=$tmpdir/vram-profile.json
python3 vram-model-lab/examples/sdk_usage_no_torch.py
python3 vram-model-lab/ksolver_vram_sidecar.py \
  --profile $tmpdir/vram-profile.json \
  --once \
  --timeout-seconds 5
rm -rf "$tmpdir"
```

Kubernetes shape:

- [sidecar-profile-job.yaml](examples/sidecar-profile-job.yaml) shows a trainer
  container and `ksolver-vram-sidecar` sharing `/ksolver/profile` through an
  `emptyDir`.
- In production, the sidecar script should be baked into an image or mounted by
  ConfigMap, and the event sink should be stdout, HTTP, or a node-local
  collector.

## Manifest Hint Contract

`predict_manifest_vram.py` can make a scheduling estimate from Kubernetes YAML
when the manifest includes enough semantic hints. It reads:

- annotations named `ksolver.ai/vram-*` on the object or pod template,
- environment variables named `KSOLVER_VRAM_*`,
- common CLI flags such as `--batch-size`, `--seq-len`, `--image-size`,
  `--precision`, and `--optimizer`.

Minimum hints:

- all families: `family`, `batch_size`, `hidden_size`, `layers`
- transformer/MLP: `seq_len`
- CNN: `image_size`

Recommended hints:

- `precision`
- `optimizer`
- `param_count`
- `heads` for transformers
- `activation_checkpointing`
- `reserve_extra_mib` for explicit synthetic headroom/profiling probes

Example annotations:

```yaml
metadata:
  annotations:
    ksolver.ai/vram-family: transformer
    ksolver.ai/vram-hidden-size: "1024"
    ksolver.ai/vram-layers: "8"
    ksolver.ai/vram-heads: "16"
    ksolver.ai/vram-param-count: "166337792"
```

## Result Format

Each completed probe emits one `KSOLVER_VRAM_RESULT` JSON object with:

- framework, model family, precision, batch size, sequence length, hidden size
- layers, parameters estimate, optimizer, activation checkpointing flag
- GPU name, total VRAM, max `nvidia-smi` used memory
- PyTorch peak allocated and reserved memory
- elapsed seconds, samples per second, OOM status
- image, image digest if Kubernetes reports it, command hash, manifest hash
- verification labels: `verified_real_framework` and
  `customer_workload_fingerprint`

The fitted artifact is deliberately transparent at first. It contains:

- one global ridge model with interaction features,
- per-family ridge models for `transformer`, `cnn`, and `mlp` when enough rows
  exist,
- normalized model-driver impact rows so the scheduler, UI, and summaries can
  explain whether a prediction is being driven by parameter count, activation
  footprint, precision, optimizer/checkpointing choices, model family, or
  synthetic headroom probes,
- a quality gate that only allows a family model to serve predictions when
  leave-one-out error is within a sane scheduling bound.

The most important conceptual feature is not parameter count alone. For
training jobs, peak VRAM is roughly a combination of weights, gradients,
optimizer state, activations, framework overhead, and allocator headroom. The
lab therefore records both `param_count_m` and an activation proxy:

- transformer/MLP activation proxy: `batch_size * seq_len * hidden_size * layers`
- CNN activation proxy: `batch_size * image_size * image_size * layers`

Precision changes the byte multiplier, optimizer controls state overhead, and
activation checkpointing changes how many intermediate tensors must remain live
through backward. The model artifact reports top drivers using coefficient
times observed feature standard deviation, rather than raw coefficients, so
features with different units can be compared directionally.

The predictor reports both a point estimate and a conservative scheduling
estimate using leave-one-out p95 absolute error as the default safety margin.
The immediate goal is not a perfect predictor; it is to produce enough
controlled measurements to prove whether VRAM-aware scheduling can avoid CUDA
OOM and preserve scarce high-VRAM nodes.

## Current Limitations

- The WSL NVIDIA runtime did not produce hard OOM labels even for synthetic
  reservations at the reported memory limit. Near-capacity rows are useful risk
  data, but real OOM calibration should be repeated on cloud or bare-metal nodes
  with hard device memory behavior.
- The current model is linear and intentionally transparent. It is good enough
  to drive a conservative local demo, but arbitrary-job prediction will need a
  larger dataset and likely a nonlinear model or stronger per-family models. As
  of the current local dataset, the transformer, CNN, and MLP family models pass
  the quality gate, but they are still synthetic-family predictors rather than
  validated customer workload predictors.
- Semantic metadata still depends on opt-in SDK/wrapper reporting. Image digest
  and manifest hashes are stable cache keys, not sufficient predictors by
  themselves.
