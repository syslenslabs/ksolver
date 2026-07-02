# KSolver

Kubernetes cluster cost optimizer. Connects to a live cluster (or a saved snapshot), collects every scheduling constraint, and uses [CP-SAT](https://developers.google.com/optimization/cp/cp_solver) constraint programming to find the cheapest node fleet that still satisfies all placement rules.

![KSolver Dashboard](docs/screenshots/dashboard.png)

## Why this exists

The default Kubernetes scheduler is excellent at general-purpose placement: it checks whether a
pod can run on a node, respects the core scheduling rules, and picks a node quickly. That is the
right default for most services.

KSolver exists because some workloads need a different optimization objective. GPU and other
scarce compute fleets are expensive, shape-sensitive, and easy to fragment. A locally reasonable
placement can still waste capacity: a 1-GPU job can strand an 8-GPU node, a low-priority job can
block a high-priority training gang, or a workload can request an H100 when a smaller GPU class
would have been enough.

For those cases, the question is not just "can this pod run?" It is:

- Will this placement leave enough contiguous GPU capacity for the next large job?
- Is this workload using the right GPU class, memory size, and node topology?
- Can a lower-urgency job finish by its deadline using fewer or cheaper resources?
- Which constraints are preventing consolidation, admission, or scale-down?
- What is the dollar and capacity cost of the current placement rules?

KSolver is built for SRE and platform teams who need that explanation and control. It uses the
same Kubernetes constraints as input, but evaluates placement globally so teams can reduce
fragmentation, expose why workloads are pending, quantify waste, and decide when a specialized
GPU scheduler is worth using instead of the default scheduler.

## What it does

KSolver answers one question: **how much are you overspending on compute, and what's blocking you from spending less?**

It collects nodes, pods, taints, affinities, anti-affinities, topology spread constraints, PDBs, node selectors, VPAs, and DaemonSets from your cluster. It feeds everything into a constraint solver that jointly optimizes placement and request levels, then shows you:

- **Dollar savings** broken down by placement consolidation, request rightsizing, and constraint relaxation
- **Ranked action items** with kubectl commands, effort badges, and risk levels
- **Constraint cost attribution** showing exactly how much each taint, affinity rule, or anti-affinity is costing you
- **Interactive constraint simulator** to toggle constraints on/off and watch nodes consolidate in real time
- **Per-node drainability** analysis showing which nodes can be emptied and what's pinned
- **Fleet recommendations** suggesting better instance types from the pricing catalog
- **VPA coverage** highlighting workloads missing VPA and estimating the waste

![Constraint Simulator](docs/screenshots/simulator.png)

## Quick start

```bash
# Build
cargo build --features rust-cp-sat

# Run against your current kubeconfig
./target/debug/ksolver serve 0.0.0.0:8080

# Open the dashboard
open http://localhost:8080
```

## Helm install

```bash
helm install ksolver oci://us-central1-docker.pkg.dev/syslens-dev/syslens/ksolver \
  --version 0.5.1 \
  --namespace ksolver --create-namespace
```

## Architecture

```
ksolver/src/
  collector.rs          Kubernetes API collection (nodes, pods, VPAs, PDBs, etc.)
  model.rs              Domain types — Node, Workload, Constraint, Solution
  normalizer.rs         Normalize collected state into solver input
  optimizer_input.rs    Build CP-SAT model variables and constraints
  cpsat_rust.rs         CP-SAT solver integration via or-tools bindings
  planner.rs            Post-solve: generate moves, actions, savings waterfall
  explainability.rs     Constraint cost attribution and blocker analysis
  pricing.rs            Cloud provider instance pricing catalog
  historical_usage.rs   Prometheus-based usage collection for rightsizing
  verifier.rs           Validate solutions against kube-scheduler-simulator
  server.rs             Axum HTTP server and SSE streaming
  service.rs            Orchestrate collect -> solve -> plan pipeline
  metrics.rs            Prometheus metrics exposition
  state_cache.rs        Snapshot persistence for offline analysis
```

The solver runs as a single binary serving both the API and the single-page dashboard at `/`.

## Configuration

The dashboard exposes all solver parameters through the Advanced Settings panel. Key options:

| Parameter | Default | Description |
|-----------|---------|-------------|
| CPU/Memory Headroom | 0% | Reserve capacity on every node |
| Overcommit Ratio | 1.0 | Allow packing beyond requests (1.0 = strict) |
| Ignore Taints | off | Treat taints as soft for upper-bound analysis |
| Relax Anti-Affinity | off | Allow tighter packing by softening anti-affinity |
| Joint Rightsizing | off | Co-optimize request sizes and placement |
| Usage-Adjusted Requests | off | Replace raw requests with Prometheus-based demand |

## Shadow-mode GPU scheduler

Observes pending pods with `schedulerName: ksolver` that request GPUs, computes
where they *would* be placed, records decision traces, and **binds nothing**:

    KUBECONFIG=~/.kube/config KSOLVER_SHADOW_BATCH_SECONDS=10 \
      cargo run --features rust-cp-sat -- shadow

Environment variables:

- `KSOLVER_SHADOW_SCHEDULER_NAME` (default `ksolver`) — pods whose `spec.schedulerName` matches are in scope.
- `KSOLVER_SHADOW_BATCH_SECONDS` (default `10`) — batch window between solves.
- `KSOLVER_SHADOW_NAMESPACES` — comma-separated namespace allowlist (empty = all).
- `KSOLVER_SHADOW_GPU_RESOURCES` (default `nvidia.com/gpu`) — exact resource names counted as GPUs.
- `KSOLVER_SHADOW_GPU_RESOURCE_PREFIXES` (default `nvidia.com/mig-`) — resource-name prefixes counted as GPUs, so **MIG (mixed strategy)** slices like `nvidia.com/mig-1g.5gb` are observed and placed (via the solver's generic extended-resource path). Whole-GPU (`nvidia.com/gpu`) matching is unchanged; the `single` MIG strategy (slices exposed as `nvidia.com/gpu`) already works via the whole-GPU path. Per-namespace quota still counts whole `nvidia.com/gpu` (MIG-aware quota is a follow-up).
- `KSOLVER_SHADOW_ADDR` (default `127.0.0.1:8090`) — serves a live dashboard at `/` plus `/api/scheduler/traces`, `/api/scheduler/binding-plan`, `/metrics`, `/healthz`, `/readyz`. Open `http://127.0.0.1:8090/` to watch shadow decisions (placements, gangs, caveats) update live. Unplaced pods report a specific reason — e.g. "no feasible node (insufficient residual capacity or excluded by anti-affinity)", "gang not admitted (insufficient capacity or quota)", or "gang members have heterogeneous requests" — so you can see *why* a pod would not schedule.
- `KSOLVER_SHADOW_SOLVE_SECS` (default `10`) — CP-SAT solve time budget. Shadow accepts the best incumbent within this budget rather than proving optimality; each trace shows `solve_core_millis` (solver-only time) and `solver_status` (Feasible vs Optimal). Effective cadence is roughly `batch window + snapshot collection + solve`.
- `KSOLVER_SHADOW_QUOTAS` (default none) — per-namespace GPU quotas as `ns=cap` pairs, e.g. `KSOLVER_SHADOW_QUOTAS="team-a=200,team-b=300"`. A namespace over its cap gets only as many pending pods admitted as fit under the remaining quota (`cap − GPUs already used by its running pods`, clamped ≥ 0); the rest are reported unplaced with a "capacity or quota" reason. Namespaces without a configured quota are unconstrained. Enforced only in the shadow (partial-admission) path. The quota counts **all** GPU resources — whole `nvidia.com/gpu` plus MIG slices (`nvidia.com/mig-*`) — with each unit counting as 1 toward the cap (a profile-weighted policy is a future refinement).

**Preferred (soft) affinity** (`preferredDuringScheduling`) is honored as a cost-tie-break: after the cost-optimal solve, among equally-optimal placements the scheduler prefers higher-scoring nodes. It runs only when the solve proves optimal and **never changes which pods are admitted or the total cost** (a two-phase pass that pins the cost optimum + admitted set, then maximizes soft score). Both dimensions contribute to a single per-node soft score:

- **Preferred node affinity** — a node earns each matching term's `weight`.
- **Preferred pod affinity / anti-affinity** — a node accumulates `+weight` (affinity) or `-weight` (anti-affinity) for **each** running pod that matches the term's label selector (with full namespace scoping) and shares the candidate node's topology domain (`node.labels[topologyKey]`, label-based for every key including `kubernetes.io/hostname`). This mirrors the upstream scheduler's per-pod score accumulation, in **both directions**: *forward* (the pending pod's own preferences vs running pods) and *symmetric* (a running pod's preferred terms steering the pending pod, when the term matches every gang member).
- **Co-placement of two pending pods** — when two *pending* pods/gangs express mutual (or one-directional) preferred pod **affinity**, the batch solver jointly rewards co-placing them in the same topology domain — going beyond the sequential kube-scheduler, which can only score against already-running pods. The reward is applied only in the cost-preserving second phase, so it never changes admission or cost. Soft anti-affinity between two *pending* pods is out of scope (its hard form is enforced by cross-workload anti-affinity above).

Placements on **time-sliced** GPU nodes (NVIDIA `nvidia.com/gpu.sharing-strategy=time-slicing`, or `nvidia.com/gpu.replicas>1` when no strategy label) are disclosed with a "time-sliced GPU: shared, no isolation" caveat — such GPUs are oversubscribed with no memory/fault isolation, so "fits" ≠ isolated performance. (MPS nodes are not flagged as time-sliced.)

Best-effort pod anti-affinity is modeled for `requiredDuringScheduling` terms with full label selectors — `matchLabels` and `matchExpressions` (`In`/`NotIn`/`Exists`/`DoesNotExist`), same namespace — across **any topology key**: `kubernetes.io/hostname` excludes the exact node; `topology.kubernetes.io/zone`, a rack label, etc. exclude the whole topology *domain* that already holds a matching pod (pending-vs-running, both directions). Cross-namespace terms are modeled via both an explicit `namespaces` list (empty ⇒ own namespace) and a `namespaceSelector` (empty `{}` = all namespaces; else namespaces whose labels match). Terms we still can't model (unsupported label-selector operators) are surfaced as a "pod anti-affinity" caveat on the decision.

**Dry-run binding plan.** `GET /api/scheduler/binding-plan` renders, from the latest decision, the exact `Binding` subresource payloads (`apiVersion: v1`, `kind: Binding`, `metadata`, `target: Node`) that a real binder *would* POST for each placed pod — one entry per placement, with `dry_run: true`, `trace_sequence`, and `solve_millis` so staleness is visible. Each entry also carries a **`readiness`** (`ready`, or `stale` with a reason) computed against the *latest* cluster snapshot — the stale/conflict guard a real binder must run before applying: it flags a vanished target node, a pod gone from the snapshot, a pod recreated under the same name (uid changed), or a pod already scheduled. (It is a stale/conflict check, not a full scheduler-predicate revalidation.) The dashboard shows the plan with a readiness column. This is the actionable, inspectable output of shadow mode and the groundwork for real binding (a separate, authorization-gated phase); it is **rendered, never applied** — shadow still issues only read/watch/list. A dedicated `no-mutation` test guards both the renderer (no API-call/kube-client symbols) and the shadow loop (no `Binding`/create/patch/delete/evict/replace calls).

**Real binding (Phase 3, opt-in — mutates the cluster).** By default ksolver binds nothing. Setting `KSOLVER_ENABLE_REAL_BINDING=true` arms the executor: after each solve it POSTs a `pods/binding` for every decision whose readiness re-check is `ready`, actually scheduling the pod onto the chosen node. Safety controls:

- `KSOLVER_ENABLE_REAL_BINDING` (default `false`) — master switch; when unset/false the scheduler is read-only and no mutation-capable client is even created.
- `KSOLVER_REAL_BINDING_DRY_RUN` (default `false`) — send bindings with server-side `dryRun=All`: the apiserver validates them but persists nothing. Use this first to confirm the path before going live.
- `KSOLVER_MAX_BINDS_PER_PASS` (default `10`) — throttle on bindings applied per solve.
- Every candidate gets a **final live re-check immediately before the POST** (`should_apply`): the pod must still exist, match uid, be owned by our scheduler, be un-terminating, unbound, and Pending — otherwise it is skipped. A post-response parse error is reconciled against live state so a bind that actually succeeded is never miscounted as failed. Per-pod errors never abort the pass. Metrics: `ksolver_shadow_bound_total`, `ksolver_shadow_bind_skipped_total`, `ksolver_shadow_bind_failed_total`.
- All mutation lives in one module (`scheduler/binder.rs`); a `no-mutation` test keeps `shadow.rs` and the plan renderer free of direct mutating calls.

When real binding is enabled the service account also needs `create` on the pod binding subresource:

```yaml
  - apiGroups: [""]
    resources: [pods/binding]
    verbs: [create]
```

Shadow mode (default) issues only read/watch/list. Minimal RBAC (read-only):

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ksolver-shadow-readonly
rules:
  - apiGroups: [""]
    resources: [pods, nodes, persistentvolumeclaims, persistentvolumes]
    verbs: [get, list, watch]
  - apiGroups: ["apps"]
    resources: [daemonsets, deployments]
    verbs: [get, list, watch]
  - apiGroups: ["storage.k8s.io"]
    resources: [storageclasses]
    verbs: [get, list, watch]
  - apiGroups: ["policy"]
    resources: [poddisruptionbudgets]
    verbs: [get, list, watch]
```

It grants no `create`/`update`/`patch`/`delete` and no `pods/binding` — shadow
mode cannot mutate the cluster even if a bug tried to.

## Feasibility conformance

`ksolver conform` checks that our node-feasibility logic agrees with the real kube-scheduler
Filter phase. For each pending pod and each (non-cordoned) node it gets two verdicts — ours
(`feasible_on_node`) and the scheduler's — and reports every disagreement:

    KSOLVER_SCHEDULER_SIMULATOR_URL=http://localhost:8080 \
      ksolver conform --sample 20 --cluster my-cluster

- The scheduler verdict comes from [kube-scheduler-simulator](https://github.com/kubernetes-sigs/kube-scheduler-simulator): we import a snapshot with exactly **one node** (empty of other pods) plus the pod; the pod binding to that node means Filter passed, unschedulable means it failed. One node isolates Filter from Score.
- Both sides test raw allocatable (empty node), so DaemonSet-reserve/overcommit/headroom — separate ksolver layers, not Filter predicates — don't skew the comparison.
- Pods carrying constructs we intentionally don't model (pod affinity/anti-affinity, `DoNotSchedule` topology spread, priority, `matchFields` node affinity) are bucketed as **expected divergence**; only plain pods must match exactly. `FALSE-POSITIVE` results (we say feasible, the scheduler rejects) are listed first — those are the dangerous ones.
- Read-only on the real cluster; only the simulator (a sandbox) is scheduled against. With no simulator URL configured, `conform` prints a skip notice and exits 0.

**Live-verified** (2026-07-01) against a self-built arm64 kube-scheduler-simulator (v0.4.0 publishes amd64-only images that crash under emulation on Apple Silicon — build them from source with `docker buildx --platform=linux/arm64`). `conform` ran end-to-end and produced a confusion matrix (agree / false-positive / false-negative) with **zero false-negatives**. Note: the single-node import path can yield spurious false-positives when the imported node isn't marked Ready inside the simulator (its own KWOK re-manages imported node status) — a harness-fidelity caveat, not a Filter-modeling gap.

## License

MIT
