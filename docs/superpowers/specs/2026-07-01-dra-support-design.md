# DRA (Dynamic Resource Allocation) Support — Design Spec

**Status:** Draft for review (design pass; no implementation). This is fractional-GPU **Phase F3**,
deferred from `docs/superpowers/specs/2026-07-01-fractional-gpu-design.md`.
**Author:** autonomous design pass (2026-07-01), pending user review.

## Problem

Extended resources (`nvidia.com/gpu`, `nvidia.com/mig-*`) are integer, node-advertised counts —
the whole current model. **DRA** (Dynamic Resource Allocation, stable in Kubernetes v1.35) is a
different mechanism: a pod references **ResourceClaims**; devices are advertised per node via
**ResourceSlices**; the scheduler *allocates* specific devices to claims and records the result in
the claim's status. GPUs (and other accelerators) are increasingly exposed via DRA rather than the
device-plugin extended-resource path. A GPU scheduler that "accounts for everything" must model DRA
feasibility, or it will mis-decide on DRA-based fleets (treat a DRA GPU pod as needing no GPU, since
it has no `nvidia.com/gpu` request).

## Background: the DRA objects (what we must read)

- **`ResourceClaim`** (namespaced): a request for one or more devices, with a `spec.devices.requests`
  list; each request names a **DeviceClass** and a CEL/selector + count. Status carries the
  allocation (which devices on which node) once scheduled.
- **`ResourceClaimTemplate`** (namespaced): pods reference a template; the controller materializes a
  per-pod `ResourceClaim`. A pod's `spec.resourceClaims[]` maps a claim name → (claim | template).
- **`DeviceClass`** (cluster): defines a class of devices + selectors (CEL) + config.
- **`ResourceSlice`** (cluster, node-scoped): published by drivers; lists the **devices** available on
  a node (name, attributes, capacity), the driver, and the pool. This is the per-node inventory.
- A pod is DRA-feasible on a node iff each of its claims can be satisfied by *unallocated* devices in
  that node's ResourceSlices matching the claim's DeviceClass + selectors + count, jointly (a pod's
  multiple requests must be satisfiable simultaneously, and across pods competing for the same node).

## Why this is fundamentally harder than extended resources

Extended resources reduce to a scalar `Σ req·x ≤ capacity·y` per resource. DRA is a **matching /
assignment** problem: a claim request selects a *subset* of concrete devices by attributes; two
requests may or may not share devices; devices are individually consumable. So feasibility is not a
scalar comparison — it's "does a valid device assignment exist for all admitted pods on this node?"

## Approaches

### A. Scalar approximation (fast, lossy) — recommended FIRST
Collapse each `(node, DeviceClass)` into an integer count = number of matching **unallocated**
devices available on that node; collapse each pod claim request into a per-DeviceClass integer demand.
Then DRA rides the EXISTING generic extended-resource path (`Σ demand·x ≤ count·y`), exactly like MIG.

Three correctness details (codex, against the v1.35 API):
- **DeviceClass matching is NOT skipped (codex #1).** A `DeviceClass` selects devices via
  selectors/CEL over `ResourceSlice` device *attributes* — it is not a pre-existing label on the
  slice. F3a must therefore include a **limited DeviceClass-selector evaluator**: support classes
  whose selector reduces to simple attribute equality/`In` over device attributes (and bare classes);
  a class/claim whose selector needs full CEL beyond that is **not counted precisely** and its pods
  are caveated ("DRA: selector not fully evaluated") / bucketed as expected-divergence. So the
  narrower contract is explicit: attribute-equality selectors modeled, arbitrary CEL deferred (F3b).
- **Allocation source of truth = `ResourceClaim.status.allocation`, NOT ResourceSlices (codex #2).**
  ResourceSlices publish the device *inventory*; they do not mark which devices are already taken.
  Unallocated count = (devices in the node's slices matching the class) − (devices already allocated
  to that node/class per allocated `ResourceClaim` statuses). Compute availability by subtracting
  claim-status allocations from slice inventory — never infer "free" from slices alone (avoids
  double-counting).
- **ResourceSlice node scoping (codex #3).** v1 slices target `spec.nodeName`, `nodeSelector`,
  `allNodes`, or per-device node selection. F3a MVP supports **`nodeName`-scoped** slices (the common
  attached-GPU case); `nodeSelector`/`allNodes` are either expanded into per-node counts or deferred
  with a documented caveat.

Feasibility then becomes "enough matching devices of each class remain on the node."
- **Pros:** reuses all verified machinery (solver, residual, gang, quota); modest collection +
  a limited selector evaluator; correct for homogeneous GPU classes with attribute-equality selectors
  (the common case — one class = one GPU model).
- **Cons:** arbitrary CEL / heterogeneous intra-class selection is approximated ⇒ can be optimistic;
  mitigated by the caveat + conformance bucketing until F3b.

### B. Exact per-device assignment (full fidelity)
Add device-assignment variables `a[claim_request, device]` with per-device capacity and selector
feasibility, plus linking to placement `x`. Precise but a large solver extension (potentially many
variables; CEL evaluation for selectors) and a big collection layer.
- **Defer** until A is in use and real fleets need attribute-level precision; its own spec.

## Recommendation & phasing

1. **F3a — DRA scalar approximation (Approach A).** Collect ResourceSlices/DeviceClasses/Claims/
   Templates; per node, count unallocated matching devices per DeviceClass; per pod, sum claim demand
   per DeviceClass; feed as synthetic integer extended resources keyed like `dra/<deviceclass>` into
   the existing input. In-scope detection: a pod with any `resourceClaims` becomes GPU-in-scope.
   Caveat when selectors/CEL exceed bare-class. **No solver change** (generic scalar path).
2. **F3b — Exact assignment (Approach B).** Only if A proves insufficient.

## Data model / component impact (F3a)

- **Collector:** new reads — `ResourceClaim`, `ResourceClaimTemplate`, `DeviceClass` (cluster),
  `ResourceSlice` (cluster). Reuse the verifier's raw-collection pattern; feature/version-gate on the
  DRA API group (`resource.k8s.io`) being present (skip cleanly on older clusters).
- **Normalizer:** compute, per node, `available_by_deviceclass: map<class, i64>` =
  (devices in the node's `nodeName`-scoped ResourceSlices whose attributes satisfy the DeviceClass
  selector, via the limited evaluator) − (devices already allocated per `ResourceClaim.status.allocation`
  for that node/class). Add to `NormalizedNode` as synthetic extended resources `dra/<class>`. For each
  pod, resolve `resourceClaims[]` (claim or template) → per-class integer demand → add to
  `extended_resource_requests` as `dra/<class>`. Mark whether any claim/class used a selector the
  limited evaluator can't fully model (for the caveat).
- **pod_filter / scope:** a pod referencing any `resourceClaims` is GPU-in-scope even without an
  `nvidia.com/gpu` request. Add a `KSOLVER_SHADOW_DRA_ENABLED` (default on when the API group exists).
- **Solver/builder:** unchanged — `dra/<class>` are just more integer extended resources.
- **Residual:** running DRA pods are reflected by the availability computation itself — their devices
  are excluded because `available_by_deviceclass` subtracts allocated devices per
  `ResourceClaim.status.allocation` (the single source of truth; codex #2). Do NOT also subtract via
  the generic running-pod residual for `dra/<class>` (would double-count); DRA availability is
  authoritative from claim allocations.
- **Decision trace:** DRA pods report their per-class demand; caveat "DRA: DeviceClass-granularity
  (per-device selectors not modeled)" when applicable.
- **Conformance (Phase 2):** DRA pods are bucketed as expected-divergence until F3b, since Filter
  now includes the DRA plugin's exact device matching that F3a approximates.

## Risks / open questions

- **DeviceClass selector evaluation (codex #1)** — F3a needs a limited attribute-equality/`In`
  evaluator over device attributes; arbitrary CEL is deferred to F3b and caveated. The evaluator's
  supported subset must be defined precisely in the F3a plan.
- **Allocation source of truth (codex #2)** — availability = slice inventory − allocated devices from
  `ResourceClaim.status.allocation`; never infer "free" from ResourceSlices alone.
- **ResourceSlice node scoping (codex #3)** — F3a supports `nodeName`-scoped slices; `nodeSelector`/
  `allNodes`/per-device selection are expanded to per-node counts or deferred (documented caveat).
- **Templates vs claims** — a pod may reference a template (per-pod claim not yet materialized) or a
  shared claim; F3a must resolve both to per-class demand (shared claims are subtle — multiple pods
  may share one claim; count once). Document shared-claim handling explicitly.
- **API availability** — gate all DRA collection on the `resource.k8s.io` group; no-op on clusters
  without it (no regression to extended-resource fleets).
- **Structured Parameters / partitionable devices** — out of scope for F3a; note for F3b.

## Testing strategy

- F3a: unit tests for (1) per-node DeviceClass availability from synthetic ResourceSlices (incl.
  already-allocated devices excluded); (2) pod claim/template → per-class demand (incl. shared claim
  counted once); (3) selector-present ⇒ caveat; (4) a DRA pod placed via the generic path on a node
  with matching devices, and dropped when none remain. All shadow-only, binds nothing.

## Out of scope
Exact per-device assignment (F3b), CEL evaluation, partitionable/structured-parameter devices, real
binding of DRA allocations.
