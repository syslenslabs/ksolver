# Fractional / Shared GPU Support — Design Spec

**Status:** Draft for review (design pass; no implementation yet).
**Author:** autonomous design pass (2026-07-01), pending user review.
**Related:** `docs/superpowers/specs/2026-06-30-gpu-scheduler-design.md` (overall architecture).

## Problem

Today the scheduler treats GPUs as **whole integer units**: `pod_filter::parse_gpu_quantity` floors any non-integer to 0, `effective_gpu_request` counts only exact `gpu_resource_names` (default `nvidia.com/gpu`), and the per-namespace quota `GPU_RESOURCE` const is hardwired to `nvidia.com/gpu`. But a large share of the "$$$ savings" story is **sharing a physical GPU** across workloads:

- **MIG (Multi-Instance GPU)** — an A100/H100 is partitioned into fixed slices (e.g. `1g.5gb`, `2g.10gb`, `3g.20gb`) exposed as distinct extended resources like `nvidia.com/mig-1g.5gb`.
- **Time-slicing / fractional shares** — a GPU advertised as N replicas (`nvidia.com/gpu: 4` via the device-plugin's `replicas`) or a fractional request (`0.5`).
- **DRA (Dynamic Resource Allocation)** — `ResourceClaim`/`DeviceClass` (k8s 1.31+ beta) instead of extended resources.

Not modeling these means we either ignore packable capacity (undercount savings) or mis-place pods (a pod needing a `1g.5gb` slice "fits" a node we counted as having 8 whole GPUs).

## Key existing capability (important)

The **solver core already supports arbitrary integer extended resources generically**: `cpsat_rust::solve` iterates `node.extended_resources` and constrains `Σ per_replica_scalar_requests[res] · x ≤ capacity · y` for EACH resource name — nothing there is GPU-specific. So a MIG slice modeled as its own integer resource (`nvidia.com/mig-1g.5gb`) already flows through placement, gang scaling, and quota-style constraints **without solver changes**. The gaps are all in the *input/collection/config* layers and in the non-integer cases.

## Approaches

### A. MIG as distinct integer resources (recommended first)
Treat each MIG profile as its own extended resource. Nodes advertise `nvidia.com/mig-1g.5gb: 7` etc.; pods request them. **~0 solver work** (generic scalar path). Work is: (1) collector already reads all extended resources into `NormalizedNode.extended_resources` — verify MIG names pass through; (2) shadow `gpu_resource_names` config must recognize MIG resource names (glob/prefix `nvidia.com/mig-*`) so MIG pods are IN-SCOPE and their requests counted; (3) quota should sum across a configurable GPU-resource *set*, not a single `GPU_RESOURCE` const; (4) decision-trace `gpu_request` should report the slice profile.
- **Pros:** minimal, exact, reuses verified machinery; covers the most common real fractional deployment (MIG).
- **Cons:** doesn't model the *equivalence* between a whole GPU and its slices (a node with a free whole GPU can't auto-satisfy a MIG request unless it's partitioned) — but that mirrors real k8s behavior (MIG geometry is fixed at node config), so it's correct, not a limitation.

### B. Fractional shares (scaled integers)
Model a shared GPU as a scaled integer budget: advertise capacity `1000` milli-GPU per physical GPU, requests in milli-GPU (`0.5` → `500`). Mirror the existing millicpu pattern. Requires: parse fractional GPU quantities (don't floor), scale node capacity + requests by 1000, keep quota in milli-GPU.
- **Pros:** models time-slicing/fractional; small, self-contained (parsing + scaling).
- **Cons:** time-slicing has no memory isolation — "fits" ≠ "performs"; must disclose (caveat) that fractional packing is best-effort on non-MIG GPUs.

### C. DRA (ResourceClaim / DeviceClass)
Model `ResourceClaim`s referenced by pods and device availability per node from `ResourceSlice`s. Substantial: new collection (ResourceClaim/ResourceClaimTemplate/DeviceClass/ResourceSlice), a new feasibility predicate (claim satisfiable on node), and solver variables for claim→device assignment.
- **Pros:** the forward-looking k8s standard; where the ecosystem is heading.
- **Cons:** large; beta API churn; most fleets today still use MIG/device-plugin. **Defer** until A/B land and DRA adoption warrants it.

## Recommendation & phasing

1. **Phase F1 — MIG (Approach A).** Generalize the GPU-resource *set* (config: exact names + `nvidia.com/mig-*` prefix), make quota sum over that set, ensure MIG pods are in-scope and counted, surface the profile in the trace. Almost entirely input/config/observability; solver untouched. Highest value/effort ratio.
2. **Phase F2 — Fractional shares (Approach B).** Milli-GPU scaling for time-sliced/fractional requests, with a "no memory isolation" caveat. Self-contained parsing/scaling change.
3. **Phase F3 — DRA (Approach C).** Only after F1/F2 and once DRA adoption justifies the collection + solver work. Its own spec.

## Data model / component impact (F1)

- `ShadowConfig.gpu_resource_names` → a matcher supporting exact names + prefix globs (e.g. `nvidia.com/mig-*`); env `KSOLVER_SHADOW_GPU_RESOURCES` keeps CSV, add glob support. A pod is GPU-in-scope if any requested extended resource matches.
- `pod_filter::effective_gpu_request` → sum over ALL matching GPU-resource names (MIG slices included), not just the exact list.
- Quota: replace the single `GPU_RESOURCE` const with the configured GPU-resource set; `build_pending_input_diagnosed` sums running + emits quota groups per matching resource (or an aggregate "GPU units" if profiles are fungible for quota — decision needed; default: per-exact-resource to stay precise).
- `collector`/`normalizer`: confirm MIG extended-resource names already flow into `NormalizedNode.extended_resources` and `NormalizedWorkload.extended_resource_requests` (they should — generic maps). Add a test with a MIG node/pod.
- Decision trace: `gpu_request` becomes per-resource (e.g. `mig-1g.5gb×1`) so the dashboard shows the profile.
- **Conformance (Phase 2)** already validates feasibility generically, so MIG feasibility is covered once the resources flow.

## Risks / open questions

- **Quota fungibility:** should a namespace GPU quota count MIG slices as fractions of a whole GPU, or cap each profile separately? (Proposed default: per-resource caps; a "GPU-equivalent" aggregate is a follow-up.)
- **Whole-GPU vs slice equivalence:** intentionally NOT modeled (matches fixed MIG geometry); document it.
- **Fractional memory isolation (F2):** disclose via caveat; never claim time-sliced packing is isolated.
- **Config ergonomics:** glob matching must not accidentally scope non-GPU extended resources.

## Testing strategy

- F1: unit tests for the GPU-resource matcher (exact + glob), `effective_gpu_request` summing MIG slices, quota over the set; a builder test with a MIG node + MIG pod placing correctly; a conformance bucket check (MIG pod is strict, not expected-divergence).
- F2: fractional parse/scale unit tests; a 0.5-GPU pair sharing one GPU; caveat presence.
- All shadow-only, binds nothing.

## Out of scope (this spec)
Real binding, preemption/priority, cross-node NVLink topology for slices, DRA implementation (F3 gets its own spec).
