# Small Training Submission Manifests

This directory contains small training-job YAMLs for the common ways GPU work
shows up in Kubernetes. They are meant for understanding and prediction, not as
production-ready manifests.

Some manifests are directly runnable on the local WSL/k3s cluster if the image
is available and the cluster supports `runtimeClassName: nvidia`. Operator CRDs
such as Kubeflow, Ray, Volcano, MPI Operator, and Argo require those operators to
be installed before `kubectl apply` will work.

Validate the whole suite without applying anything:

```sh
for f in vram-model-lab/manifests/*.yaml; do
  echo "$f"
  python3 vram-model-lab/scripts/fingerprint_manifest.py "$f"
  python3 vram-model-lab/scripts/predict_manifest_vram.py "$f"
done
```

## What Each File Represents

- `01-plain-pod-pytorch-mlp.yaml`: a raw `Pod`; this is the lowest-level shape
  every higher-level submitter eventually creates.
- `02-batch-job-pytorch-transformer.yaml`: a Kubernetes `Job`; the most common
  plain batch submission style.
- `03-cronjob-pytorch-cnn.yaml`: a Kubernetes `CronJob`; same pod shape, but
  time-triggered.
- `04-kubeflow-pytorchjob-transformer.yaml`: Kubeflow `PyTorchJob`; common for
  distributed PyTorch.
- `05-kubeflow-tfjob-cnn.yaml`: Kubeflow `TFJob`; common for TensorFlow.
- `06-rayjob-transformer.yaml`: Ray `RayJob`; common for Ray Train or custom
  distributed Python training.
- `07-volcano-job-transformer.yaml`: Volcano gang-scheduled `Job`; common where
  all workers must start together.
- `08-mpi-operator-job.yaml`: MPI Operator `MPIJob`; common for MPI-style
  distributed launch.
- `09-argo-workflow-cnn.yaml`: Argo `Workflow`; common for pipeline-driven
  training.
- `10-airflow-kubernetespodoperator-pod.yaml`: the pod shape emitted by Airflow
  `KubernetesPodOperator`.

## Fields That Drive VRAM

Your intuition is directionally right: **parameter count and number of layers
are major drivers**, but they are not the whole story. For training, the largest
drivers are usually:

1. **Parameter count**
   Model weights scale with parameter count and precision.

2. **Optimizer state**
   Adam/AdamW commonly adds extra state per parameter. SGD is usually cheaper.

3. **Activations**
   Activations scale with `batch_size * sequence_length * hidden_size * layers`
   for transformers/MLPs, or `batch_size * image_size * image_size * channels *
   layers` for CNNs. This is why a smaller model with a long sequence length or
   large batch can use more VRAM than a larger model with a smaller input.

4. **Precision**
   `fp32` usually costs more than `fp16`/`bf16`, but framework kernels,
   optimizer states, and master weights can make the ratio non-exact.

5. **Distributed strategy**
   FSDP, ZeRO, tensor parallelism, and pipeline parallelism can reduce per-GPU
   parameter/optimizer memory while sometimes increasing communication buffers.

6. **Framework allocator behavior**
   PyTorch reserved memory and `nvidia-smi` process memory can differ. The lab
   records both because scheduler admission cares about what the device reports.

## Why The Annotations Exist

Kubernetes YAML usually has image, command, args, env vars, resources, labels,
and scheduler constraints. It usually **does not** reliably contain model
parameter count, true number of layers, dtype, optimizer, sequence length, image
resolution, or distributed strategy.

The `ksolver.ai/vram-*` annotations are the explicit semantic contract:

```yaml
metadata:
  annotations:
    ksolver.ai/vram-family: transformer
    ksolver.ai/vram-batch-size: "6"
    ksolver.ai/vram-seq-len: "1536"
    ksolver.ai/vram-hidden-size: "1024"
    ksolver.ai/vram-layers: "8"
    ksolver.ai/vram-heads: "16"
    ksolver.ai/vram-param-count: "166337792"
    ksolver.ai/vram-precision: fp16
    ksolver.ai/vram-optimizer: adamw
```

The predictor can infer some fields from common CLI flags, but annotations or an
SDK/sidecar profile are more reliable.
