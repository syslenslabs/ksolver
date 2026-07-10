# VRAM → DRA wedge

Predict a pod's peak GPU VRAM and inject it as a Kubernetes-native resource request, so GPU
memory stops being a user guess. ksolver becomes the *predictive, provably-safe planning layer*
in front of any binder (KAI or plain kube); DRA is the interface.

Design spec: `docs/superpowers/specs/2026-07-08-vram-dra-wedge-design.md`.

## How it fits together

```
 pod ──▶ admission webhook (ksolver, Rust) ──▶ predictor /predict (Python) ──▶ resolution
                     │                                                              │
                     └── merge JSONPatch: annotations + nodeAffinity + (DRA claim) ◀┘
```

- **Resolver** (`vram_resolver.py`) — confidence-ranked cascade, highest available tier wins:
  1. **explicit** annotation `ksolver.dev/predicted-peak-vram-{gib,bytes}` → authoritative
  4. **historical fingerprint** (image + command/env hash) → observed p95 peak → high *(measured beats sniffed)*
  3. **referenced config** (DeepSpeed/HF/accelerate — via passed `config_docs` or an inline
     `ksolver.ai/vram-config` JSON annotation) → model → high
  2. **static sniff** (annotations/env/CLI flags; a known model name like `--model gpt2-large`
     auto-fills family/hidden/layers) → model → high
  - else → **advisory** only (never a hard constraint on a guess)

  **Promotion gate:** model-predicted tiers (2/3) are **advisory by default** (soft — annotated but
  no hard node constraint), because the model isn't calibration-gated yet. Set
  `KSOLVER_VRAM_HARD_ADMIT=true` to let them hard-constrain. **Explicit** (tier 1) and **measured**
  (tier 4) always hard-constrain. This mirrors the roadmap's "advisory until calibration" principle
  and the dashboard's `hard-admit=blocked` state.
- **Delivery** (`vram_admission.py`) — turns a resolution into a JSONPatch: annotate the estimate;
  at high/authoritative confidence add `nodeAffinity ksolver.dev/gpu-vram-gib Gt floor(est)-1`
  (enforceable today, no DRA driver needed) plus a DRA consumable-capacity `ResourceClaimTemplate`
  (`build_resource_claim_template`, live-ready once a GPU DRA driver publishes devices).
- **Rust relay** (`ksolver … admission.rs` / `shadow.rs`) — the real ksolver webhook calls the
  predictor and merges the VRAM patch with its schedulerName patch. **Fails open** on any error.

## Run it

```bash
# one-command local demo across all tiers (starts predictor, drives /admit)
PY=/path/to/venv/python vram-model-lab/scripts/wedge_demo.sh

# or manually:
python vram-model-lab/scripts/vram_admission_service.py --port 8091 \
    --observations vram-model-lab/data/observations.jsonl        # predictor + tier-4 store
KUBECONFIG=... KSOLVER_VRAM_PREDICTOR_URL=http://127.0.0.1:8091 \
    ksolver shadow                                                # webhook relays to it
```

Deps: `numpy`, `pyyaml`. `KSOLVER_VRAM_PREDICTOR_URL` unset ⇒ VRAM injection disabled (no
behavior change).

## Using the DRA path

`POST /claim {pod}` returns `{claim: <ResourceClaimTemplate sized to predicted VRAM>, pod_patch:
<JSONPatch wiring the pod to the claim>, resolution: {...}}`. GitOps flow: apply the RCT, then
apply the pod with the patch (adds `spec.resourceClaims` + the GPU container's `resources.claims`).
A GPU DRA driver then allocates the claim's consumable-capacity memory at schedule time. A
validated worked example is in `examples/dra-bundle.yaml`.

**k8s 1.31–1.35 compatibility.** `resource.k8s.io` changes group-version across this range
(`v1alpha3`→`v1beta1`→`v1beta2`→`v1` GA), so ksolver is version-adaptive on both sides:
- **Read (demand modeling):** the scheduler discovers the served version and lists DRA objects as
  `DynamicObject`, parsing shape-tolerantly (`ksolver/src/dra.rs`, `collector.rs`) — one binary,
  no dependency bump.
- **Emit (`/claim`):** consumable-capacity memory claims are GA-only. Pass the cluster's served
  version via the `ksolver.ai/dra-api-version` annotation; on a non-GA cluster `/claim` returns
  `claim: null` (with a reason) and you rely on the **node-affinity feasibility patch**, which keeps
  the pod off too-small GPUs on every version.

## Populating the tier-4 store

- Forward (primary): `vram_resolver.record_observation(store, pod, peak_mib)` on each completed
  run. Tier 4 fires once a fingerprint recurs ≥ `FINGERPRINT_MIN_SAMPLES` (3).
- Batch seed from measured lab runs: `python vram-model-lab/scripts/build_observation_store.py`.

## Status

Built + unit-tested (18 Python + 3 Rust) and CI-covered; proven live locally (merged webhook
patch; tier-4 firing on real data; DRA claim accepted by k8s 1.36 server-side dry-run).

Remaining / not done:
- In-cluster `MutatingWebhookConfiguration` + TLS (run against a live API server).
- Auto-populate the tier-4 store from ksolver's own completed-job observations. UNBLOCKED: the
  byte-identical Rust fingerprint now exists (`ksolver/src/scheduler/vram_store.rs`
  `workload_command_hash`, cross-checked against the Python hash in a unit test), plus
  `observations_from_pods` (reads the `ksolver.dev/observed-peak-vram-mib` annotation on
  Succeeded pods) and `append_observations` (writes the same JSONL the resolver reads).
  Remaining wiring: (1) collect completed `corev1::Pod`s with their raw spec in the shadow
  loop and feed them to `observations_from_pods` -> `append_observations`; (2) a VRAM-metrics
  source (DCGM / probe sidecar) that sets `ksolver.dev/observed-peak-vram-mib` — ksolver does
  not measure VRAM itself.
- Full DRA allocation loop (needs a GPU DRA driver; node-affinity is the enforceable fallback).
- **DRA API version split — RESOLVED (2026-07-10) via version-adaptive handling** (see the
  compatibility note above). ksolver reads DRA dynamically (`DynamicObject` + discovery, shape-
  tolerant parsing) and emits GA-only, degrading to node-affinity on pre-GA clusters — one binary
  across k8s 1.31–1.35 with **no dependency bump**. This supersedes the earlier "coupled
  `kube 0.98→4.0` + `k8s-openapi 0.24→0.28` migration" (branch `worktree-agent-aac1cc84…`), which is
  no longer needed for compatibility (kept only as reference for the typed v1 field mapping).
- Model breadth: single-SKU (4090), no true CUDA-OOM labels yet.
