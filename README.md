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

For the GPU scheduler shadow loop, deploy the same chart in observe-only mode:

```bash
helm install ksolver ./chart \
  --namespace ksolver --create-namespace \
  --set runtime.mode=shadow \
  --set scheduler.bindingRolloutMode=observe-only \
  --set scheduler.enableRealBinding=false \
  --set scheduler.bindingKillSwitch=true
```

The chart's default RBAC is read-only. It does not render `pods/binding` or Event write
permissions unless `rbac.allowBindingMutations=true` or `rbac.allowEventWrites=true` is set, and
those flags are guarded so they render only with the matching shadow-mode rollout switches.

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

Shadow mode watches pending GPU pods assigned to `schedulerName: ksolver`,
computes placements, and records decision traces. It is read-only by default:
no pod is bound unless an operator explicitly enables a binding rollout mode.

### Start shadow mode

```bash
KUBECONFIG=~/.kube/config \
KSOLVER_SHADOW_BATCH_SECONDS=10 \
cargo run --features rust-cp-sat -- shadow
```

Always include `--features rust-cp-sat` for local shadow and demo runs. Without
it, the process can start but the solver is unavailable, `/readyz` returns
`503`, and placement reports fail closed.

Open <http://127.0.0.1:8090/> to inspect placements, unplaced reasons, repair
advice, safety gates, and scenario evidence.

### Health and operator APIs

| Endpoint | Purpose |
| --- | --- |
| `/healthz` | Process liveness |
| `/readyz` | Kubernetes watch and solver readiness |
| `/api/scheduler/traces` | Recent placement decisions and caveats |
| `/api/scheduler/binding-plan` | Read-only Kubernetes `Binding` payload preview |
| `/api/scheduler/repair-plan` | Advisory migration and preemption plan |
| `/api/scheduler/production-safety` | Rollout gates, readiness, RBAC, and mutation posture |
| `/api/scheduler/operator-status` | Compact blocker and next-action summary |
| `/api/scheduler/vram-calibration` | VRAM model quality and admission readiness |
| `/api/scheduler/evidence-bundle` | Review artifacts and collection commands |
| `/metrics` | Prometheus metrics |

The dashboard explains why work is unplaced instead of returning only a failed
verdict. Readiness errors are classified as API timeout, connectivity, DNS,
TLS, or authorization failures and include a suggested diagnostic command.

### Core configuration

| Variable | Default | Effect |
| --- | --- | --- |
| `KSOLVER_SHADOW_SCHEDULER_NAME` | `ksolver` | Scheduler name watched by shadow mode |
| `KSOLVER_SHADOW_BATCH_SECONDS` | `10` | Delay between solve batches |
| `KSOLVER_SHADOW_SOLVE_SECS` | `10` | CP-SAT time budget |
| `KSOLVER_SHADOW_ADDR` | `127.0.0.1:8090` | Dashboard and API listener |
| `KSOLVER_SHADOW_NAMESPACES` | all | Comma-separated namespace allowlist |
| `KSOLVER_SHADOW_GPU_RESOURCES` | `nvidia.com/gpu` | Exact whole-GPU resource names |
| `KSOLVER_SHADOW_GPU_RESOURCE_PREFIXES` | `nvidia.com/mig-` | GPU-like resource prefixes |
| `KSOLVER_SHADOW_QUOTAS` | none | Namespace GPU caps, such as `team-a=200` |
| `KSOLVER_CANDIDATE_NODE_LIMIT` | `16` | Candidate nodes retained per workload; `0` disables pruning |
| `KSOLVER_ENABLE_NODE_GROUPING` | `false` | Homogeneous-node symmetry reduction |
| `KSOLVER_ENABLE_LEADER_ELECTION` | `false` | Lease-based coordination for replicas |

The Helm chart exposes these under `scheduler.*` and validates rollout modes,
objective profiles, solve windows, and throttles before deployment. See
[`chart/values.yaml`](chart/values.yaml) for the complete configuration
reference.

### Device semantics

- Whole GPUs and advertised MIG profiles use exact Kubernetes extended-resource
  accounting. A request for one MIG profile cannot consume another profile.
- Optional `ksolver.dev/gpu-topology-key` and
  `ksolver.dev/gpu-topology-value` annotations apply a hard node-label filter.
  `ksolver.dev/nvlink-domain` is the shorthand for NVLink-domain matching.
- DRA claims use conservative scalar accounting. Allocated device identities
  are subtracted, but concrete device selection is not claimed.
- Time-sliced GPUs are marked shared and non-isolated. A feasible placement does
  not imply isolated memory or performance.

Unsupported or approximate semantics are carried into each decision as caveats;
they are never silently treated as exact device placement.

### Scheduling and policy

Shadow solves pending work against residual node capacity and preserves
Kubernetes feasibility constraints that it can model. This includes gang
admission, node affinity, topology spread, pod affinity/anti-affinity, quotas,
MIG resources, VRAM fit, and explicit GPU topology labels.

Preferred affinity is a cost-preserving tie-break. Required pod affinity and
anti-affinity are enforced when their selectors and topology domains can be
modeled; unsupported selector behavior is disclosed as a caveat.

Optional policy inputs include:

- priority and business-value weights;
- queue weights and queue age;
- deadlines and predicted runtimes;
- tenant fair-share weights and monthly budgets;
- historical runtime and peak-VRAM predictions.

These inputs are advisory unless their corresponding objective weights or
budget controls are enabled. Traces record the active objective profile,
weights, admission outcome, and reason for every deferral.

### Scale guardrails

Candidate pruning reduces model size, then widens suspicious results
automatically. Set `KSOLVER_CANDIDATE_NODE_LIMIT=0` for a full feasible-set
solve. `KSOLVER_CANDIDATE_WIDEN_MIN_ADMISSION_PERCENT` controls the
low-admission widening trigger.

Node grouping is opt-in. When safe, homogeneous physical nodes are collapsed,
solved as counted groups, expanded back to real nodes, and validated. If
expansion fails, shadow falls back to a physical-node solve.

Every trace reports whether the result was full, grouped, widened, pruned, or a
fallback. Pruned results with unknown regret are not considered safe evidence
for scale or live-binding claims.

### Repair advice

`/api/scheduler/repair-plan` distinguishes resource fragmentation from a
workload that cannot fit any GPU. It proposes migrations before preemptions when
both free equivalent capacity and accounts for:

- PodDisruptionBudget availability;
- `safe-to-evict`, do-not-disrupt, migration, and preemption policies;
- workload priority, business value, deadline urgency, and queue age;
- checkpoint age, progress, running age, and configured disruption cost.

Plans are advisory. They do not evict, migrate, preempt, or bind workloads.

### Admission webhook

`POST /admission/scheduler-name` accepts Kubernetes
`admission.k8s.io/v1` reviews and returns an RFC 6902 patch that assigns
`schedulerName: ksolver` to eligible GPU pods. It never overwrites an existing
scheduler name.

The Helm webhook is disabled by default and requires TLS. Use
`KSOLVER_ADMISSION_OPT_IN_LABEL` (or
`scheduler.admissionOptInLabel`) to require an explicit pod label before
patching. DRA pods require this opt-in because the Pod alone does not identify
the device class.

```bash
helm upgrade --install ksolver ./chart \
  --namespace ksolver --create-namespace \
  --set runtime.mode=shadow \
  --set scheduler.admissionOptInLabel=ksolver.dev/schedule \
  --set admissionWebhook.enabled=true \
  --set admissionWebhook.url=https://ksolver-webhook.example.com/admission/scheduler-name \
  --set admissionWebhook.caBundle=<base64-ca-bundle>
```

### Binding rollout

The binding plan endpoint is always read-only. Each proposed binding includes a
freshness verdict that checks the pod UID, scheduler ownership, pending state,
target node, and latest feasible-node set.

Actual binding is opt-in:

| `KSOLVER_BINDING_ROLLOUT_MODE` | Behavior |
| --- | --- |
| `observe-only` | No mutation-capable client |
| `dry-run` | API validation with `dryRun=All`; nothing persisted |
| `bind-low-risk` | Persist only candidates within the configured GPU threshold |
| `bind-all` | Persist every ready candidate that passes the final live checks |

Additional controls include `KSOLVER_ENABLE_REAL_BINDING`,
`KSOLVER_BINDING_KILL_SWITCH`, `KSOLVER_BINDING_LOW_RISK_MAX_GPUS`, and
`KSOLVER_MAX_BINDS_PER_PASS`. Invalid modes fail closed. Every candidate is
re-read immediately before binding; stale, recreated, terminating, already
bound, or differently owned pods are skipped.

Live binding also requires explicit RBAC for `create` on `pods/binding`.
The Helm chart does not grant it unless
`rbac.allowBindingMutations=true`. Kubernetes Event writes are separately
gated by `KSOLVER_ENABLE_KUBERNETES_EVENTS` and
`rbac.allowEventWrites=true`.

A low-risk rollout therefore looks like:

```bash
helm upgrade --install ksolver ./chart \
  --namespace ksolver --create-namespace \
  --set runtime.mode=shadow \
  --set scheduler.bindingRolloutMode=bind-low-risk \
  --set scheduler.enableRealBinding=true \
  --set rbac.allowBindingMutations=true
```

### Baselines and evidence

Scenario reports compare ksolver with kube-scheduler spread and binpack
policies. Volcano results are included only when a captured gang-aware baseline
is supplied. Every comparison records whether its evidence is live,
cached, or deterministic; missing simulator evidence does not prevent a solve,
but it blocks strong comparative claims.

Useful commands:

```bash
# Diagnose a running shadow service
scripts/shadow-doctor.py \
  --base-url http://127.0.0.1:8090 \
  --require-kss-ready

# Run the end-to-end review gate
scripts/demo-gate.py \
  --base-url http://127.0.0.1:8090 \
  --output-dir /tmp/ksolver-demo-gate \
  --json

# Capture and verify a review packet
scripts/collect-evidence-bundle.py \
  --base-url http://127.0.0.1:8090 \
  --output-dir /tmp/ksolver-evidence
scripts/verify-evidence-bundle.py /tmp/ksolver-evidence
```

For local kube-scheduler-simulator baselines, use `scripts/kss-pool.sh` to
start and inspect a pool, then refresh the scenario cache with
`scripts/kss-cache-grind.sh`. Shadow caches simulator plans by pending-work
signature so dashboard refreshes do not repeatedly reset and import simulator
state.

### Read-only RBAC

A default shadow installation needs only read access:

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

It grants no create, update, patch, delete, eviction, or pod-binding permission.
For deeper implementation and acceptance details, see the
[shadow scheduler design](docs/superpowers/specs/2026-06-30-gpu-scheduler-design.md),
[DRA support design](docs/superpowers/specs/2026-07-01-dra-support-design.md),
[Volcano baseline design](docs/superpowers/specs/2026-07-10-volcano-gang-aware-baseline.md),
and [frontier roadmap](docs/superpowers/plans/2026-07-13-frontier-roadmap.md).

## Feasibility conformance

`ksolver conform` checks that our node-feasibility logic agrees with the real kube-scheduler
Filter phase. For each pending pod and each (non-cordoned) node it gets two verdicts — ours
(`feasible_on_node`) and the scheduler's — and reports every disagreement:

    KSOLVER_SCHEDULER_SIMULATOR_URL=http://localhost:8080 \
      ksolver conform --sample 20 --cluster my-cluster

- The scheduler verdict comes from [kube-scheduler-simulator](https://github.com/kubernetes-sigs/kube-scheduler-simulator): we import a snapshot with exactly **one node** (empty of other pods) plus the pod; the pod binding to that node means Filter passed, unschedulable means it failed. One node isolates Filter from Score.
- Both sides test raw allocatable (empty node), so DaemonSet-reserve/overcommit/headroom — separate ksolver layers, not Filter predicates — don't skew the comparison.
- Pods carrying constructs we intentionally don't model in this Filter harness (required pod affinity/anti-affinity, unsupported `DoNotSchedule` topology spread shapes, and priority / `priorityClassName`) are bucketed as **expected divergence** with per-reason counts. Required node affinity, including OR terms and `metadata.name` `matchFields`, belongs in the strict bucket.
- Only strict-bucket pods must match exactly. `FALSE-POSITIVE` results (we say feasible, the scheduler rejects) are listed first — those are the dangerous ones. The text report includes `strict-gate: pass|fail`; `fail` means at least one strict false-positive.
- `--json` emits a machine-readable report with the same strict/expected-divergence matrices, mismatch lists, expected-divergence reason counts, and `strict_gate_status`.
- `--fail-on-strict-false-positive` exits non-zero after printing the report if the strict gate fails. Expected-divergence mismatches remain advisory and do not trip this CI gate.
- Read-only on the real cluster; only the simulator (a sandbox) is scheduled against. With no simulator URL configured, `conform` prints a skip notice and exits 0. With `--json`, the skip path emits a JSON object with `skipped: true`.
- **Scaling on large fleets.** `conform` is O(pods × nodes) — one simulator reset/import/probe per (pod, node) pair — so a full run on a 100+-node cluster is slow. Two opt-in flags (defaults unchanged) make it tractable:
  - `--max-nodes N` caps the probed candidate set to a sample; the report records `nodes_evaluated` vs `nodes_total` and prints a "node-sampled spot-check … (not full-cluster coverage)" note, so a sampled run is never mistaken for exhaustive coverage.
  - `--dedup-nodes` keeps **exact full coverage** but probes only one representative per feasibility-equivalence class (nodes with identical allocatable + pod-referenced labels + taints yield the same Filter verdict), replicating the verdict to the rest. It falls back to per-node probing for any pod whose verdict could depend on an un-keyed attribute (`matchFields`, PVC volumes), so it can only ever probe *more* than necessary, never return a wrong verdict. The report shows `simulator_probes` vs the pair count.

CI-friendly examples:

```bash
KSOLVER_SCHEDULER_SIMULATOR_URL=http://localhost:8080 \
  ksolver conform --sample 20 --json --fail-on-strict-false-positive

KSOLVER_SCHEDULER_SIMULATOR_URL=http://localhost:8080 \
  ksolver conform --sample 20 --fail-on-strict-false-positive
```

**Live-verified** (2026-07-01) against a self-built arm64 kube-scheduler-simulator (v0.4.0 publishes amd64-only images that crash under emulation on Apple Silicon — build them from source with `docker buildx --platform=linux/arm64`). `conform` ran end-to-end and produced a confusion matrix (agree / false-positive / false-negative) with **zero false-negatives**. Note: the single-node import path can yield spurious false-positives when the imported node isn't marked Ready inside the simulator (its own KWOK re-manages imported node status) — a harness-fidelity caveat, not a Filter-modeling gap.

**Update (2026-07-14):** the local simulator pool (`scripts/kss-pool.sh`) now runs the full import/reset/probe cycle reliably — two KWOK apiserver bugs that previously broke pod import (ServiceAccount admission) and `/reset` drain (etcd-prefix mismatch) are fixed in that script. `conform` runs live end-to-end against a one-command pool, and `--max-nodes`/`--dedup-nodes` (above) make large-fleet runs tractable. Verified on the 113-node `solver-lab` KWOK cluster: a full run and a `--dedup-nodes` run produced **identical verdicts and mismatches**, with the strict gate passing.

## Populating the VRAM history store

The tier-4 VRAM predictor (see the historical predictor above) reads a JSONL store of measured
peak-VRAM observations keyed by workload fingerprint. `ksolver vram-observe` fills that store from a
real GPU-metrics source — a [dcgm-exporter](https://github.com/NVIDIA/dcgm-exporter) scraped by
Prometheus:

    ksolver vram-observe --store /var/lib/ksolver/vram.jsonl \
      --prometheus-url https://prom.example.com \
      [--prometheus-username <u> --prometheus-token <t>] [--window 24h] [--kubeconfig <path>]

It queries each pod's peak framebuffer usage
(`max_over_time(DCGM_FI_DEV_FB_USED{pod!=""}[<window>])`), joins each metric to its pod's spec to
compute the same fingerprint the predictor uses, and appends one row per matched pod. Credentials may
also come from `KSOLVER_PROMETHEUS_{URL,USERNAME,TOKEN}`. It **requires** a live Prometheus and writes
nothing when the exporter reports no matching series — it never fabricates an observation. Run it on a
schedule (e.g. a CronJob) so tier-4 predictions are grounded in real usage.

## License

MIT
