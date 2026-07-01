# GPU-Aware Kubernetes Scheduler on CP-SAT — Design Spec

**Date:** 2026-06-30
**Status:** Approved design; ready for implementation planning
**Branch:** `scheduler`

## 1. Summary

Build an **online, autonomous GPU scheduler** for Kubernetes — a secondary scheduler
(opt-in via `schedulerName: ksolver`) that places GPU pods on nodes using an optimization
model, aiming to save large organizations money on GPU fleets (10–20% efficiency gains) by
producing globally better placement than the greedy default scheduler.

The engine is Google OR-Tools **CP-SAT** (Rust crate `cp_sat`), which `ksolver` already uses
as an offline cost-minimizing planner. This project extends that investment into a live
scheduler while keeping the offline planner and the existing web simulator working on the
same solver code.

### North-star vision (not all v1)
A GPU scheduler that "accounts for everything": whole-GPU packing, topology awareness
(NVLink/NVSwitch/rack), fractional GPUs (MIG, time-slicing, DRA), priority-aware and gang
preemption, and multi-tenant quota with fair-share borrow/reclaim — all under a configurable
multi-objective. This spec lays out the full architecture so these compose cleanly, and
sequences the build so each phase ships working, safe software.

### Guiding principle (from design review)
The hard, risky part is **not** the CP-SAT math — it is the **transactional scheduling
contract with Kubernetes** (reservation, gang atomicity, feasibility conformance, stale-cache
invalidation). The rollout is therefore designed so the scheduler **proves its value in shadow
mode before it ever binds a production pod**, and so a safe capacity/quota ledger exists before
any online binding.

## 2. Requirements

### Functional
- Online secondary scheduler; autonomously binds pods to nodes; opt-in `schedulerName: ksolver`.
- **Fail-open**: if the scheduler is down or errs, pods simply pend; the cluster is never harmed.
  This also dissolves the bootstrap chicken-and-egg: the default scheduler places system pods
  and the ksolver pod itself; ksolver only touches opt-in pods.
- Supports **both** batch/gang training jobs (all-or-nothing placement) and inference/
  long-running services (single-pod, spread-preferring) in one unified solver formulation.
- **GPU fidelity levels** (phased): L1 whole-GPU; L2 topology (NVLink/rack); L3 fractional
  (MIG, time-slicing, DRA — each a *separate backend*, not one abstraction).
- **Configurable multi-objective** over: maximize GPU utilization, minimize fragmentation,
  topology quality, minimize cost, priority/fairness. Named preset profiles. v1 exposes a
  single conservative lexicographic profile; the configuration surface is designed in but not
  combinatorially exposed until later.
- **Priority-aware preemption**, including **gang preemption** (free a whole node for a big
  job), respecting `PriorityClass` and PodDisruptionBudgets, with anti-thrashing. (Post-v1.)
- **Multi-tenancy**: hard quota in early phases; fair-share borrow/reclaim as the final phase.

### Non-functional
- Scale target: **1000s of GPUs**.
- Latency budget: **~3 minutes** solve time is acceptable for batch/gang if it yields materially
  better placement. Latency-sensitive paths get a faster route (post-v1). The engine only makes
  variables out of **pending** pods; running pods are fixed context, so problem size scales with
  the arrival batch, not the whole cluster.
- Stack: **NVIDIA-first** (GPU Operator, device plugin, MIG, NFD/GFD topology labels).
  DRA and non-NVIDIA vendors are pluggable, not assumed. Target k8s **v1.31**.
- **Determinism**: the solver core is deterministic (no clock/random), so the simulator matches
  production and the model is unit-testable.

## 3. Architecture

Five components. `solver-core` is the shared heart; a **policy** layer owns all Kubernetes
semantics; a thin **controller** does plumbing. The offline planner and web simulator reuse the
same core, so they can never disagree with production decision logic.

```
 solver-core (pure Rust lib: NO k8s / NO I/O / NO clock / NO random)
   solve(SolverState, PlacementRequest, Config) -> SolverDecision
   • stable API; L1/L2/L3/preemption are INTERNAL backends (may differ in variable structure)
   • returns assignments + evictions PLUS infeasibility core / best-incumbent / objective
     breakdown / timeout status
   • deterministic -> golden fixture tests + scale benchmarks
        ▲                                              ▲
        │ same core                                    │ same core
 scheduler-policy (k8s semantics + transactional contract)      planner (offline)
   • k8s -> abstract translation (feasibility sets, spread         • snapshot -> plan
     domains/skew, remaining quota, residual capacity)             • existing analyzer,
   • single-writer reservation / quota LEDGER                        refactored onto core
   • gang reservation / permit / rollback
   • preemption candidate generation (bounded)
   • cache-freshness contract; bind-conflict revalidation
   • infeasible / timeout fallback policy
   • safety controls (dry-run, allowlist, caps, kill-switch)
        ▲
        │ validated decisions
 scheduler-controller (thin plumbing)
   • informers/watch pending ksolver pods
   • Binding API, Eviction API
   • events, metrics, traces, leader election, fail-open
        │ decision traces / snapshots
 simulator / UI (existing axum SPA, extended)
   • what-if solves through core; basic decision traces; scale-bench harness
```

### Component contracts

**solver-core** (`ksolver` lib, pure).
- *Input*: `SolverState` (abstract: nodes with residual CPU/mem/GPU-by-SKU, per-GPU
  free/used for topology, tenant ownership, price, DaemonSet overhead; running pods as fixed
  residual; **no raw k8s objects** — no taints/labels/PDBs), `PlacementRequest` (workloads with
  `group_size`, per-replica requests, feasibility sets, abstract spread domains+skew, tenant,
  priority, required/optional flag), `Config` (objective profile / lexicographic tiers, solve
  time limit, decomposition flag).
- *Output*: `SolverDecision` = assignments (`count[w,n]` / per-replica where expanded),
  proposed evictions, **plus** structured explanation: infeasibility core, best incumbent,
  objective breakdown, and status (optimal / feasible-incumbent / infeasible / timeout).
- *Rule*: no Kubernetes concepts, no I/O, no wall clock, no randomness. Fixed variable ordering.

**scheduler-policy** (new).
- Translates live informer state into abstract `SolverState`/`PlacementRequest`, computing each
  workload's **feasibility set** (taints/tolerations, nodeSelector/affinity, topology-spread,
  PVC zone/`WaitForFirstConsumer`, host ports, runtimeClass, extended resources) and reserving
  DaemonSet overhead on any node that could be newly activated.
- Owns the **single-writer reservation + quota ledger** (see §5) that both the batch path and
  any future fast path transact against.
- Owns gang reservation/permit/rollback, preemption candidate generation, cache-freshness,
  bind-conflict revalidation, and fallback policy.
- Owns all safety controls.

**scheduler-controller** (new, thin).
- Informer/watch for pending `schedulerName: ksolver` pods; leader election; executes validated
  decisions via Binding/Eviction APIs; emits events/metrics/traces; enforces fail-open.

**planner** (existing, refactored). The current offline analyzer is refactored to build a
`SolverState` and call `solver-core` instead of its own inline formulation. Snapshot → plan
workflow preserved for what-if and reporting.

**simulator/UI** (existing axum SPA, kept). Runs the identical `solver-core` for what-if solves,
renders objective breakdowns and infeasibility cores, and hosts the synthetic scale-benchmark
harness. Rich live observe/replay "cockpit" is deferred post-v1; v1 emits basic decision traces.

## 4. Solver-core formulation (L1: whole-GPU, non-preemptive)

Fully integer-linear. Extension backends (L2/L3/preemption) may introduce additional variable
structures behind the stable API; they are **not** required to preserve this exact model.

### Variables
```
count[w,n]  ∈ [0, group_size[w]]  integer   # replicas of workload w on node n (aggregated)
placed[w]   ∈ {0,1}                          # gang all-or-nothing latch
used_new[n] ∈ {0,1}                          # node newly activated by THIS solve
```
`node_active` is split: already-live/nonempty nodes are constant-active; `used_new[n]` is the
decision var for newly activated nodes, so cost/consolidation measures *incremental* scheduling
cost.

### Hard constraints
```
Σ_n count[w,n] = group_size[w] · placed[w]              # gang atomicity (math side)
count[w,n] = 0                       for n ∉ feasible[w] # feasibility (from policy layer)
Σ_w cpu[w]·count[w,n] ≤ residual_cpu[n]                 # capacity; running pods pre-subtracted;
Σ_w mem[w]·count[w,n] ≤ residual_mem[n]                 #   DaemonSet overhead reserved on nodes
Σ_w gpu[w]·count[w,n] ≤ residual_gpu[n]                 #   that could be newly activated
count[w,n] ≤ group_size[w] · used_new[n]                # activation link
Σ (gpu assigned to tenant t) ≤ remaining_quota[t]       # hard quota (remaining, not total)
placed[w] = 1                        for required workloads   # place-all-or-report-infeasible
+ abstract anti-affinity / topology-spread constraints (only where legal under aggregation)
```
GPU capacity is **per SKU** (A100/H100/L40S/…), not a single scalar; `residual_gpu[n]` and
`gpu[w]` are per-SKU-aware from the start (real clusters are heterogeneous).

### Objective — lexicographic tiers (correctness-critical order)
```
1. maximize  Σ admitted priority                         # admit required / high-priority first
2. minimize  Σ eviction_cost · evict[p]                  # preemption phase only
3. minimize  spread / topology violations                # L2
4. minimize  Σ price · used_new[n]  +  idle_gpu penalty  # cost + fragmentation
```
where `idle_gpu[n] = gpu_cap[n] − running_gpu[n] − assigned_gpu[n]`, bounded to active nodes.
No ratio/normalized terms (they are nonlinear and circular in `node_active`). A weighted-sum
mode with **bounded integer coefficients** is a designed-in alternative but v1 ships
lexicographic-only, with the invariant *"one unplaced high-priority gang must dominate the
maximum possible cost saving."*

### Modeling rules
- **Hybrid variable expansion**: aggregate ordinary workloads via `count[w,n]`; expand to
  per-replica and/or per-GPU variables **only** for topology-sensitive workloads (L2), per-pod
  PVC-zone cases, per-replica anti-affinity identity, or heterogeneous gang replicas.
  Aggregation is used only where constraints provably survive it.
- **Per-pool decomposition** is an optimization guarded by an **independence check** (no gang
  spans pools; no cross-pool topology/quota/global-cost/preemption coupling). Coupled workloads
  stay in a shared master problem.

## 5. The Kubernetes transactional contract (highest-risk area)

A secondary scheduler using the raw Binding API has **no** kube-scheduler Reserve/Permit/PreBind
lifecycle. We build the equivalent explicitly. This is the make-or-break subsystem.

### 5.1 Single-writer reservation + quota ledger
- One authoritative in-process ledger (guarded by leader election) tracks, per node,
  capacity **committed to not-yet-observed binds**, and per tenant, **quota committed** but not
  yet reflected in informer caches.
- **Every** admission path (batch solve today, fast path later) transacts against this ledger.
  No path may keep an independent capacity/quota view. Ledger entries are never allowed to drive
  a node's assigned GPUs above allocatable or a tenant above quota.
- Entries are reconciled/cleared as informers observe the resulting pods (or on TTL expiry).

### 5.2 Gang reservation / permit / rollback
- On a gang decision, create a **TTL'd reservation** in the ledger holding the chosen nodes'
  capacity for that gang, so concurrent solves and the default scheduler cannot double-book.
- **Bind all members**, revalidating each target node's `resourceVersion`/capacity immediately
  before each bind.
- If any member fails to bind (node changed, capacity gone, kubelet admission rejection),
  **roll back**: release the reservation and unbind/return already-bound members to pending.
  No partial gang is ever left running.
- TTL guards against controller crashes; on restart, reservations are rebuilt from
  bound-but-incomplete gang state.

### 5.3 Cache-freshness & bind-conflict revalidation
- Each solve records the informer `resourceVersion` it was built from.
- Before binding, if a target node has advanced past a safe delta, that node's decision is
  **re-validated or discarded**, never blindly applied. Defines max acceptable staleness and a
  re-list trigger.

### 5.4 Fallback policy (consumes solver explanation artifacts)
- Optimal or feasible incumbent within budget → execute.
- Timeout with feasible incumbent → execute incumbent (configurable) or defer.
- Infeasible → leave pods pending with an explainable event citing the infeasibility core.
  Never bind blindly.

### 5.5 Feasibility conformance
- A conformance suite compares ksolver's computed feasible-node set to **kube-scheduler's Filter
  result** across real pod specs (nodeSelector, affinity, taints, topology spread, PVC zones,
  host ports, init containers, extended resources, runtimeClass, PDB, priority, terminating
  pods). Reimplementing Filter is a large hidden surface and must be validated, not assumed.

## 6. Control loop & latency classing

```
watch pending pods (schedulerName=ksolver)
  → (v1) enqueue for the batch window
  → on window tick (configurable): snapshot state → policy builds SolverState
        → solver-core.solve() → policy validates + reserves (ledger, freshness)
        → controller binds (and, later, evicts)
  → emit events, metrics, decision trace
```
- **v1 uses a single batch/admission path.** A **fast path** (cheap heuristic placement for
  simple single-pod inference) is deferred until the shared ledger is proven; when added, it
  must transact against the *same* ledger (single writer) and share the *same* feasibility
  logic to avoid drift, double-booking, and priority inversion.
- **Leader election** ensures a single active binder (HA standby otherwise).
- **Fail-open** throughout.

## 7. Preemption (post-v1, bounded candidate generation)

Not "the solver may evict anything." Sequence:
1. Triggered only when a required workload is infeasible on residual capacity.
2. Policy layer generates a **bounded candidate set**: strictly lower priority, PDB
   `disruptionsAllowed > 0`, not reserved, within a max-evictions budget.
3. Candidates become `evict[p] ∈ {0,1}` in a **scoped second solve**; freeing a pod returns its
   capacity; objective tier 2 minimizes eviction cost.
4. Re-check PDBs at execution (state changes); evict via the graceful **Eviction API**;
   **anti-thrashing** cooldown; track evicted pods' replacements so they don't escape policy.
5. Preemption requires an explicit **"capacity expected but not yet available"** state — the
   ledger must model in-flight reclaim (eviction success ≠ immediate kubelet availability).
6. If PDBs block all candidates → report infeasible with reason; never force.

## 8. Multi-tenancy

- **Early phases — hard quota**: `remaining_quota[t]` as a hard constraint (§4).
- **Final phase — fair-share borrow/reclaim** (the biggest $ feature): each tenant has a
  guaranteed share and may borrow idle GPUs above it; on owner reclaim, over-guarantee borrowers
  become preferential preemption candidates (ties into §7). Modeled as objective terms rewarding
  owned capacity and penalizing borrowing, with a configurable fairness policy
  (e.g. dominant-resource). Sits on placement + preemption; does not reshape the core API.

## 9. Phasing (build order)

Resequenced so a safe capacity ledger and SKU awareness exist before any binding, and so the
solver proves its edge in shadow mode first.

1. **Shadow mode** — translate live state, compute decisions, **bind nothing**, emit traces.
   Establishes the state-translation, solver-core call path, and decision tracing with zero
   production risk.
2. **Feasibility conformance** — suite comparing ksolver's feasible set to kube-scheduler's
   Filter across real pod specs.
3. **Single-writer reservation/quota ledger** — the transactional foundation (§5.1).
4. **L1 online binding** — non-gang, non-preemptive, SKU-aware GPU pods bound through the ledger.
5. **Gang reservation/permit semantics** with **failure injection** (§5.2).
6. **Scale benchmark as a release gate** — synthetic 1k/5k/10k-GPU clusters with realistic
   queues, SKUs, quotas, affinity/spread, gangs; measure model-build time, solve time to first
   incumbent, memory, incumbent quality, and invalidation rate under simulated churn. Must pass
   before broad online rollout.
7. **Bounded preemption** with asynchronous capacity handling (§7).
8. **L2 topology** — per-GPU/per-replica expansion for topology-sensitive workloads; NVLink/rack.
9. **L3 backends** — MIG inventory (discrete resource classes) first, then time-slicing, then
   DRA (distinct claim lifecycle; treated as its own backend, not "just fractional").
10. **Fair-share borrow/reclaim.**

Throughout: the **heuristic baseline** runs alongside CP-SAT as the measured control group, so
the solver's 10–20% efficiency edge is *demonstrated* (in shadow mode and benchmarks) before and
as it graduates to binding. CP-SAT graduates for a workload class only where it beats the
baseline within the time budget.

## 10. Testing & safety

### Testing
- **Golden formulation tests**: tiny fixture clusters with hand-obvious optima → assert exact
  solver output (guards formulation regressions).
- **Property tests**: solver never violates hard constraints (capacity/quota/feasibility) on
  random inputs.
- **Determinism tests**: identical input → identical output.
- **Control-plane integration tests** (kind/envtest with failure injection): two schedulers
  competing for GPU-like extended resources; slow binds; controller restart; TTL expiry; pod
  deletion; kubelet admission rejection; gang rollback; preemption + PDB; fail-open; dry-run.
  Assert: no partial gang survives; ledgers never go negative; no node exceeds allocatable.
- **Feasibility conformance suite** (§5.5) as a first-class gate.
- **Scale/perf benchmarks** as CI release gates (phase 6).

### Safety controls (all in the policy layer)
Dry-run mode; namespace/label allowlist; max-evictions-per-window; max gang size; max concurrent
solves; global kill-switch; per-decision explainability; leader election; fail-open.

## 11. Explicitly deferred (in the design target, out of v1)

- Fast (heuristic) latency path — until the shared ledger is proven.
- Weighted-sum objective mode and combinatorial multi-objective configurability — v1 is
  lexicographic-only with one conservative profile.
- Rich simulator observe/replay "cockpit" — v1 emits basic traces only.
- Time-slicing and DRA backends, and L2/L3 hybrid expansion — roadmap, not v1.
- Fair-share borrow/reclaim — final phase, separate policy project.

## 12. Open questions for implementation planning

- Exact ledger persistence/recovery model (pure in-memory rebuilt-from-cluster on restart vs a
  lightweight persisted journal) and its interaction with leader failover.
- How much of kube-scheduler's Filter to reuse vs reimplement (conformance results will decide).
- Language boundary for the controller: stay in Rust (kube-rs) vs a thin Go controller — Rust is
  the default given the existing codebase; revisit only if kube-rs gaps appear.
- Decision-trace schema and how the simulator subscribes (extend the existing SSE channel).

## 13. Implementation status (as of 2026-07-01)

Shadow-mode scheduler (`ksolver shadow`, binds nothing) is implemented and verified. Delivered:

- **Solver foundation:** partial admission (all-or-nothing latch), pending-only residual solve,
  bounded solve (`KSOLVER_SHADOW_SOLVE_SECS`), multi-worker sizing fixed for the 100-node
  pending path (Phase 7b — scarce-900j went 336→800 optimal at the 10 s cap).
- **Gangs:** grouping by label, single-node co-location, all-or-nothing admission.
- **Per-namespace GPU quota** (Phase 11): `Σ total_gpu·placed ≤ limit`; `KSOLVER_SHADOW_QUOTAS`.
- **Pod anti-affinity** (Phases 5e–5h, 12, + matchExpressions): hostname (exact-node) and
  non-hostname topology (zone/rack domain) exclusion, pending-vs-running both directions,
  within-gang self-spread, cross-workload same-batch; full label selectors
  (matchLabels + matchExpressions In/NotIn/Exists/DoesNotExist). Unmodeled terms disclosed
  via a "pod anti-affinity" caveat (cross-namespace scoping, unsupported operators).
- **Node affinity:** kube-conformant OR-of-terms (matchExpressions) + matchFields (metadata.name).
- **Fractional GPU:** MIG mixed-strategy slices observed/placed (Phase F1); time-sliced-node
  disclosure caveat (Phase F2). DRA (F3) deferred — see the fractional-GPU spec.
- **Feasibility conformance** (Phase 2): `ksolver conform` compares our feasibility to
  kube-scheduler Filter via kube-scheduler-simulator (live run needs a simulator deployed).
- **Observability:** live dashboard + traces; specific per-pod unschedulability reasons
  (Phase 13); caveats for time-slicing and unmodeled constraints.

Still deferred / needs a decision or infrastructure: real binding + the single-writer
reservation ledger (§4/§10 — mutation, needs authorization); DRA (F3); MIG-aware per-resource
quota (units decision); soft/preferred affinity scoring; fair-share; cross-namespace anti-affinity.
