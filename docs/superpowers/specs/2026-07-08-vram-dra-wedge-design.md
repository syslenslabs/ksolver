# Design: VRAM → DRA wedge (predictive VRAM as a Dynamic Resource Allocation claim)

Date: 2026-07-08
Status: approved-scope (all tiers, priority A → B → C)

## Goal

Make ksolver's peak-VRAM estimate *actionable* by turning it into a Kubernetes-native
resource request that a production binder (KAI or plain kube) can place safely. A mutating
admission webhook resolves a pod's expected peak VRAM through a confidence-ranked cascade,
then injects that number as a **DRA consumable-capacity `ResourceClaim`** (and/or a node
feasibility constraint), so GPU memory stops being a user guess.

The wedge answers the question DRA/KAI make users answer by hand today: *"how much GPU
memory does this workload actually need?"*

## Non-goals

- Not building a DRA driver, and not requiring a live allocation loop for the first proof
  (there is no NVIDIA DRA driver on the dev cluster; only KWOK fake nodes). Proof standard
  is **injection + feasibility + server-side dry-run** (option B), escalatable later.
- Not doing container-image filesystem introspection. Runtime params (batch/seq) are
  generally not baked into images and pulling images is expensive + a security surface.
- Not replacing KAI/kube binding. ksolver stays the *planning/estimation* layer.

## Architecture

Two cooperating components (KAI-style microservice split):

1. **Predictor service (Python, reuses existing lab tooling).** Owns the resolution cascade
   and the model math (single source of truth, so predictions never drift from training).
   HTTP `POST /predict` takes a pod (or AdmissionReview pod spec) and returns:
   `{ vram_gib, source, confidence, missing[], fingerprint }`.
   - Reuses `predict_manifest_vram.py` (annotation/env/CLI sniff + linear model from
     `peak_vram_linear.json`) and `fingerprint_manifest.py`.
   - Loads the model once at startup.

2. **Admission webhook (Rust, extend `scheduler_admission_handler` in `shadow.rs`).** Thin
   glue: on a GPU pod, call the predictor, then mutate the pod per the confidence gate:
   - inject annotation `ksolver.dev/predicted-peak-vram-gib` (+ `-source`, `-confidence`),
   - build a DRA `ResourceClaimTemplate` requesting GPU memory as **consumable capacity**
     sized to the estimate, and reference it from the pod, **and/or**
   - add a `nodeAffinity` excluding nodes whose `ksolver.dev/gpu-vram-gib` < estimate.
   - Returns a JSONPatch in the AdmissionReview response.

Data flow: `API server → AdmissionReview → Rust webhook → POST /predict (Python) →
{vram,source,confidence} → Rust builds patch → AdmissionReview response`.

## Resolution cascade (confidence-ranked)

Highest-confidence available tier wins. The DRA claim carries the source + confidence, and
**only high-confidence tiers earn a hard constraint**; lower tiers annotate advisory-only
(matches the roadmap promotion gate).

| Tier | Source | Confidence | Ships in |
|------|--------|-----------|----------|
| 1 | Explicit annotation `ksolver.dev/predicted-peak-vram-*` | authoritative (hard) | A |
| 2 | Static spec sniff (annotations/env/CLI) → linear model | high if no `missing[]`, else advisory | A |
| 4 | Historical observation by fingerprint (image_digest + command_hash + shape) → observed p95 peak | high (measured) | B |
| 3 | Referenced ConfigMap/Secret read via k8s API (deepspeed/accelerate) → features → model | high if complete | C |
| — | none | unknown → advisory only, never hard-admit | A (fallback) |

Priority to implement: **A (tiers 1–2 + gate + DRA injection) → B (tier 4 fingerprint) →
C (tier 3 API config)**. Cascade order at *runtime* is 1 → 4 → 3 → 2 → unknown (measured
beats sniffed), even though tier 4/3 are built after A.

## Confidence gate → constraint strength

- **authoritative / high** → hard: inject the DRA memory claim AND node feasibility.
- **advisory** → annotate `ksolver.dev/predicted-peak-vram-gib` + `...-advisory=true`, no
  hard constraint (so a mis-sniff can't strand a job).
- **unknown** → annotate `...-source=unknown`; do not constrain.

## DRA claim shape

A `ResourceClaimTemplate` in the pod's namespace, referenced from
`pod.spec.resourceClaims`, requesting a `deviceClassName: gpu.ksolver` device with a
consumable-capacity request of `memory: <estimate>Gi`. When no DRA driver is present, the
node-affinity path (`ksolver.dev/gpu-vram-gib >= estimate`) is the enforceable fallback;
the ResourceClaim is still emitted (and dry-run validated) to prove the wedge and to be
live-ready once a driver exists.

## Error handling

- Predictor unreachable / errors / times out → webhook **fails open** (no mutation, pod
  admitted unchanged) and logs; never block workloads on the estimator.
- Prediction `missing[]` non-empty → advisory tier (annotate, don't constrain).
- Malformed pod / non-GPU pod → no-op passthrough.
- Webhook must respond well under the admission timeout; predictor call has a short budget
  (e.g. 2s) with fail-open on exceed.

## Testing / proof standard

- **Unit (Python):** cascade tier selection + confidence for representative pods (explicit,
  sniffable, missing-feature, fingerprint-hit, configmap). Model prediction sanity vs known
  rows.
- **Unit (Rust):** given a predictor response, the webhook emits the correct JSONPatch
  (annotations + ResourceClaim + affinity) for each confidence level; fail-open on error.
- **Integration:** POST a real AdmissionReview to the webhook → assert patched pod; then
  `kubectl apply --dry-run=server` the patched pod + generated ResourceClaim to prove the
  API accepts them (DRA API is GA on the cluster).
- **Live demo (best-effort, flaky cluster):** on the KWOK cluster, submit an unlabeled GPU
  pod, show the webhook injects the estimate + node constraint, and the pod lands only on
  adequate-VRAM nodes (via node-affinity fallback).

## Success criteria (definition of done for the wedge)

An unlabeled GPU training pod, admitted through the webhook, comes out with a
prediction-sourced VRAM estimate, a valid DRA ResourceClaim sized to it, and — at high
confidence — a node constraint that keeps it off too-small GPUs; all verified by unit tests
+ server-side dry-run, with fail-open behavior when the estimator is unavailable.
