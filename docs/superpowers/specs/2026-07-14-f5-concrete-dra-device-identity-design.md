# F5 — Concrete DRA device identity + topology (design DRAFT)

**Status: DRAFT for review + driver-env validation. NOT approved for implementation.** This is the
"autonomous prep" the frontier roadmap (`2026-07-13-frontier-roadmap.md`, F5) calls for: a design for
device-identity variables. F5 is ranked *high potential, high risk* — the model here MUST be validated
against a real GPU DRA driver + concrete device/topology inventory before any code lands, because a
wrong device/attribute/topology model is worse than the current honest scalar approximation. It builds
directly on the deferred "Approach B" in `2026-07-01-dra-support-design.md` §B.

## Problem: where the scalar approximation (F3a) is lossy

F3a collapses DRA to `Σ demand·x ≤ count·y` per `(node, DeviceClass)` — it answers "are there ≥N
matching devices free on this node?" It CANNOT answer the questions a real fleet needs:

1. **Attribute-level matching within a class.** A claim request selects a *subset* of a class's
   devices by attributes (e.g. `memory >= 40Gi`, `productName == "A100-SXM4-80GB"`). Two requests on
   one node may each be satisfiable alone but not jointly (they want the same device). Scalar counting
   over-admits. Today this is disclosed as an "approximate" caveat + Phase-2 expected-divergence.
2. **Consumable capacity / partitionable devices (DRA 1.31→1.34 GA).** A device can expose divisible
   capacity (e.g. MIG-like memory slices via `consumable-capacity`). Scalar device *count* ignores
   per-device capacity, so it can't model "3 claims of 20Gi share one 80Gi device."
3. **Topology / NVLink.** Gang co-location on an *NVLink island* (not just a node) is invisible to the
   scalar model — it's currently label-filter only. NVLink-optimal placement needs device-to-device
   locality, which lives below the node.

## Approach: device-assignment variables + topology objective

### Core variables (F5a — exact assignment)
Add booleans `a[r, d]` = "claim-request `r` is satisfied by concrete device `d`", where `d` ranges over
the *pre-filtered* set of devices whose attributes satisfy `r`'s selector (see scale mitigations).
Constraints:
- **Demand:** `Σ_d a[r,d] = count(r)` when `r`'s owning pod/gang is placed, else `0`
  (`Σ_d a[r,d] = count(r) · placed[w]`). Reuse the existing placement bool `x`/`placed[w]`.
- **Exclusivity:** `Σ_r a[r,d] ≤ 1` per non-partitionable device `d` (a device serves one request).
- **Node linkage:** `a[r,d]` may be 1 only if `d`'s node is the node the pod is placed on:
  `a[r,d] ≤ x[w, node(d)]`. This is the join that makes device assignment *consistent* with placement
  — the piece scalar counting lacks.
  - **Gang caveat (self-review):** for a multi-member gang spread across nodes, the *workload-level*
    presence bool `x[w, node(d)]` ("gang w has a member on node(d)") is NECESSARY but NOT SUFFICIENT —
    it would let member A's request bind a device on the node where member B sits. Exact multi-member
    device linkage needs a *per-member* node assignment bool `x[member, node]` and links each request
    to its own member: `a[r,d] ≤ x[member(r), node(d)]`. The current solver tracks per-member placement
    via `OptimizationWorkload.members` / `assignment_counts`, NOT per-member node booleans, so F5a MUST
    scope to (a) single-pod claims and (b) co-located gangs (all members on one node, where
    workload-level linkage IS exact). Cross-node gang per-member device linkage is a defined F5
    sub-item requiring the per-member node vars — deferred, not silently approximated.
- **Selector feasibility** is enforced at *variable creation* (only create `a[r,d]` for devices that
  pass `r`'s selector), reusing/extending `dra.rs::eval_selector`. Selectors that exceed the modeled
  CEL subset ⇒ do NOT create exact vars for that request; fall back to F3a scalar + keep the
  "approximate" caveat (never silently wrong).

### Consumable capacity (F5b — partitionable devices)
For devices exposing `consumable-capacity`, replace the per-device exclusivity bool with an integer
`cap[r,d]` (capacity units request `r` takes from `d`) and `Σ_r cap[r,d] ≤ capacity(d)`. Ties to the
VRAM→DRA wedge: the wedge already emits sized `consumable-capacity` memory claims
(`vram_admission.build_resource_claim_template`), so F5b is where ksolver would *verify/optimize* those
placements exactly rather than approximate.

### Topology / NVLink (F5c — locality objective)
Model device-to-device locality as a graph from the driver's topology inventory (NVLink domains /
`ResourceSlice` topology attributes). Reward a gang whose devices land in one locality domain:
- Per gang × domain, a bool `island[w, dom]` upper-bounded by the gang's device assignments in `dom`;
  add `+weight·island` to the existing **two-phase soft objective** (same machinery as soft affinity /
  co-placement — cost + admission pinned in phase 1, locality maximized in phase 2). Locality is a
  *preference*, never an admission/cost change — consistent with how all soft scoring works today.
- Hard "must be NVLink-colocated" is a future opt-in (a constraint, not a reward), deferred until the
  soft form is validated.

## Collection layer
Extend the F3a collector (`collector::augment_with_dra` + `dra.rs`) to retain, per device:
`(driver, pool, device, node, attributes map, capacity, topology/NVLink domain)` instead of collapsing
to a count. New pure structures in `dra.rs` (e.g. `DeviceInventory { devices: Vec<ConcreteDevice> }`)
alongside the existing `DraAvailability` (keep both — scalar path stays the fallback). Allocation
subtraction stays sourced from `ResourceClaim.status.allocation` device identities (unchanged from
F3a). Topology attributes vary by driver → this is the **highest-uncertainty** part and the reason for
driver-env validation.

## Honesty summary upgrade
The device-correctness summary flips a claim from `approximate`/`unsupported` to `exact` **only** when:
every request's selector was fully modeled (no CEL fallback), all its candidate devices were
enumerated with attributes, and (for F5c) topology was read from real inventory. Any fallback keeps the
existing honest caveat. Phase-2 conformance: DRA pods move OUT of the expected-divergence bucket into
strict only for fully-exact claims — validating that ksolver now matches the DRA scheduler plugin's
device matching.

## Scale / risk mitigations (the "high risk" the roadmap flags)
- **Variable explosion:** `a[r,d]` is O(requests × candidate-devices). Mitigate: pre-filter devices by
  selector (most requests match few devices); scope candidates to the pod's *feasible* nodes only
  (reuse `build_pending_input` feasible-node filtering); symmetry-break identical devices on a node
  (assign to the lowest-index free device — or keep them as a scalar sub-count when a request wants K
  interchangeable devices, a hybrid of A+B).
- **Wrong-model risk:** attribute keys, capacity semantics, and topology encoding differ across drivers
  and DRA versions (v1alpha3 → v1 GA). Do NOT infer them from docs — read them from a live driver.
- **Solver time:** device vars only added for pods with `resourceClaims` (small subset); gate the exact
  path behind a flag (`enable_exact_dra`, shadow-only, default off) so the scalar path remains the
  safe default, exactly as `enable_soft_affinity` gates two-phase.

## Phasing
- **F5a** exact single-device assignment (count=1, non-partitionable), node-linked, selector-gated,
  scalar fallback retained. Scoped to single-pod claims + co-located gangs (where workload-level node
  linkage is exact); cross-node gang per-member device linkage deferred to a sub-item (needs per-member
  node vars — see the Node-linkage gang caveat).
- **F5b** consumable-capacity / partitionable devices (`cap[r,d]`), ties to the VRAM wedge.
- **F5c** topology/NVLink soft-locality objective (two-phase), then optional hard co-location.

## Testing strategy
- **Pure (offline, unit):** device-assignment model builder + `dra.rs` inventory parsing over fixture
  ResourceSlice/Claim JSON (attributes, capacity, topology), incl. the joint-infeasibility case scalar
  gets wrong (two requests, one matching device → exactly one admitted). Solver tests: `a[r,d]`
  exclusivity, node linkage, capacity sharing, island reward never changes admission/cost.
- **Live (REQUIRED before promoting):** a real GPU DRA driver env (or a faithful fake driver publishing
  realistic ResourceSlices with attributes + NVLink topology). Validate attribute keys, capacity units,
  and topology encoding match assumptions; run `conform` and confirm exact-claim pods now agree with the
  DRA plugin. **This is the gate: no `exact` honesty claim until this passes.**

## Open questions (need driver-env answers)
1. Exact attribute-key namespacing across drivers (grouped-attr CEL → API key form) at GA vs v1alpha3.
2. Consumable-capacity representation in `ResourceSlice`/`allocation` at v1 GA.
3. How NVLink/topology is expressed — device attributes, a separate topology object, or driver-specific.
4. Whether structured-parameters / partitionable-device sub-devices need first-class modeling or ride
   consumable-capacity.

## Non-goals
Arbitrary CEL (keep the modeled subset + fallback), preemption of allocated devices, cross-node device
sharing, and implementing before driver-env validation.
