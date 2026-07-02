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

Observes pending pods with `schedulerName: ksolver` that request GPUs, computes
where they *would* be placed, records decision traces, and **binds nothing**:

    KUBECONFIG=~/.kube/config KSOLVER_SHADOW_BATCH_SECONDS=10 \
      cargo run --features rust-cp-sat -- shadow

Environment variables:

- `KSOLVER_SHADOW_SCHEDULER_NAME` (default `ksolver`) — pods whose `spec.schedulerName` matches are in scope.
- `KSOLVER_SHADOW_BATCH_SECONDS` (default `10`) — batch window between solves.
- `KSOLVER_SHADOW_NAMESPACES` — comma-separated namespace allowlist (empty = all).
- `KSOLVER_SHADOW_GPU_RESOURCES` (default `nvidia.com/gpu`) — exact resource names counted as GPUs.
- `KSOLVER_SHADOW_GPU_RESOURCE_PREFIXES` (default `nvidia.com/mig-`) — resource-name prefixes counted as GPUs, so **MIG (mixed strategy)** slices like `nvidia.com/mig-1g.5gb` are observed and placed (via the solver's generic extended-resource path). Slice-profile compatibility is exact: a pod requesting `nvidia.com/mig-3g.20gb` requires residual capacity for that same extended resource, not just any MIG slice or whole-GPU capacity. Whole-GPU (`nvidia.com/gpu`) matching is unchanged; the `single` MIG strategy (slices exposed as `nvidia.com/gpu`) already works via the whole-GPU path. Per-namespace quota counts whole GPUs plus matching slice resources; each requested unit counts as 1 quota unit.
- GPU topology hints — pending pods may require candidate nodes to carry explicit topology labels with `ksolver.dev/gpu-topology-key` + `ksolver.dev/gpu-topology-value`, or the shorthand `ksolver.dev/nvlink-domain=<value>` which requires node label `ksolver.dev/nvlink-domain=<value>`. These are hard feasibility filters for NVLink/NVSwitch/NUMA-island demos and auditability; unmatched pods are dropped with a required-topology reason instead of being treated as generic GPU-capacity work.
- DRA requests are modeled as a shadow-only scalar approximation through synthetic `dra.ksolver/<DeviceClass>` extended resources. Availability subtracts already-allocated device identities from `ResourceClaim.status.allocation` before exposing synthetic capacity. If ksolver cannot materialize a positive synthetic demand for a DRA pod, the pod is dropped with a DRA-specific reason instead of being treated as zero-GPU/free work.
- `KSOLVER_SHADOW_ADDR` (default `127.0.0.1:8090`) — serves a live dashboard at `/` plus `/api/scheduler/traces`, `/api/scheduler/binding-plan`, `/api/scheduler/repair-plan`, `/api/scheduler/decision-events`, `/api/scheduler/repair-events`, `/api/scheduler/binding-events`, `/metrics`, `/healthz`, `/readyz`. Open `http://127.0.0.1:8090/` to watch shadow decisions (placements, gangs, caveats) update live. Unplaced pods report a specific reason — e.g. "no feasible node (insufficient residual capacity or excluded by anti-affinity)", "gang not admitted (insufficient capacity or quota)", or "gang members have heterogeneous requests" — so you can see *why* a pod would not schedule. The repair endpoint renders advisory migration/preemption plans only; it never evicts, migrates, preempts, or binds pods. The event endpoints render `events.k8s.io/v1` payload drafts for placement decisions, repair recommendations, and binding outcomes for auditability only; they do not post Events to the apiserver.
- `KSOLVER_ADMISSION_OPT_IN_LABEL` (default empty) — optional pod label that must be set to `true` before `POST /admission/scheduler-name` returns a schedulerName patch. Empty means every in-scope GPU pod with no existing schedulerName is eligible.
- `KSOLVER_SHADOW_SOLVE_SECS` (default `10`) — CP-SAT solve time budget. Shadow accepts the best incumbent within this budget rather than proving optimality; each trace shows `solve_core_millis` (solver-only time) and `solver_status` (Feasible vs Optimal). Effective cadence is roughly `batch window + snapshot collection + solve`.
- `KSOLVER_CANDIDATE_NODE_LIMIT` (default `16`) — cap on feasible candidate nodes per pending workload before model build. Suspicious pruned results widen automatically: first to double the configured cap, then to the full feasible set if still needed. Set `KSOLVER_CANDIDATE_NODE_LIMIT=0` to disable pruning and solve the full feasible set immediately. Traces include `candidate_node_limit`, `retry_count`, candidate-edge counts, `widening_reason`, and `candidate_quality_metrics` with a conservative regret status such as `full_feasible_set`, `full_retry`, or `pruned_regret_unknown`; if a widened retry has no usable incumbent, ksolver keeps the previous usable placement.
- `KSOLVER_CANDIDATE_WIDEN_MIN_ADMISSION_PERCENT` (default `50`) — when candidate pruning is active, retry with wider candidate sets if the accepted solve admits less than this percentage of observed pending GPU work. Set `0` to disable the low-admission widening trigger while keeping priority, deadline, and no-incumbent widening active.
- `KSOLVER_ENABLE_NODE_GROUPING` (default `false`) — opt into homogeneous-node symmetry reduction for pending-only shadow solves. When enabled and safe, ksolver solves against counted node groups, then expands and validates the result back onto physical nodes before reporting it. If the grouped solve fails or cannot be physically expanded, ksolver falls back to the normal physical-node solve and records the fallback reason in `node_grouping_metrics`.
- `KSOLVER_ENABLE_LEADER_ELECTION` (default `false`) — opt-in flag for Lease-based HA scheduler coordination. When enabled, each shadow replica renews the configured `coordination.k8s.io/Lease`, and solve/bind passes run only on the current holder. Enable the matching Helm RBAC with `rbac.allowLeaderElection=true`.
- `KSOLVER_LEADER_ELECTION_NAMESPACE` (default `POD_NAMESPACE`, then `ksolver`) — namespace for the `coordination.k8s.io/Lease`.
- `KSOLVER_LEADER_ELECTION_LEASE_NAME` (default `ksolver-scheduler`) — Lease name shared by scheduler replicas.
- `KSOLVER_LEADER_ELECTION_IDENTITY` (default `HOSTNAME`, then `ksolver`) — identity each scheduler replica would use in the Lease record.
- `KSOLVER_SHADOW_QUOTAS` (default none) — per-namespace GPU quotas as `ns=cap` pairs, e.g. `KSOLVER_SHADOW_QUOTAS="team-a=200,team-b=300"`. A namespace over its cap gets only as many pending pods admitted as fit under the remaining quota (`cap − GPUs already used by its running pods`, clamped ≥ 0); the rest are reported unplaced with a "capacity or quota" reason. Namespaces without a configured quota are unconstrained. Enforced only in the shadow (partial-admission) path. The quota counts **all** GPU resources — whole `nvidia.com/gpu` plus MIG slices (`nvidia.com/mig-*`) — with each unit counting as 1 toward the cap (a profile-weighted policy is a future refinement).
- `KSOLVER_SHADOW_TENANT_WEIGHTS` (default none) — optional fair-share weights as `tenant=weight` pairs, e.g. `KSOLVER_SHADOW_TENANT_WEIGHTS="research=3,batch=1"`. Tenants absent from the map use weight `1`. By default this is observability-only; when `KSOLVER_OBJECTIVE_PROFILE=gpu-gang-aware` and `KSOLVER_GPU_FAIR_SHARE_WEIGHT>0`, pending work from tenants below their weighted running-GPU share receives an admission boost. Traces include per-tenant configured weight, admitted share, target weighted share, under/over fair-share delta, denied GPU demand, borrowed GPU-milli, and reclaimable borrowed GPU-milli.
- `KSOLVER_SHADOW_TENANT_MONTHLY_BUDGETS` (default none) — optional monthly budget caps as `tenant=amount` pairs, e.g. `KSOLVER_SHADOW_TENANT_MONTHLY_BUDGETS="research=50000,batch=25000"`. Amounts are currency units matching the node pricing catalog and are converted to milli-units. Shadow solves treat each configured tenant budget as a hard admission cap after subtracting already-running GPU placement cost; denied pods report a `budget exhausted` reason and traces still expose per-tenant admitted monthly cost, budget, and budget overage.

Node grouping groundwork is available in the pending-input builder: `analyze_node_grouping` detects homogeneous physical nodes that are safe candidates for future `OptimizationNode.count > 1` symmetry reduction, `group_pending_input_by_node_symmetry` can collapse those nodes into counted groups, and `expand_grouped_solution_to_physical` validates that a grouped result can be mapped back onto real nodes before it is trusted. Live solves still use physical nodes by default unless `KSOLVER_ENABLE_NODE_GROUPING=true`. Grouping is conservatively disabled when co-location, anti-affinity, existing grouped nodes, or same-batch preferred co-placement require physical-node identity. Each shadow trace includes `node_grouping_metrics` so operators can see eligible group count, eligible physical-node count, max group size, grouped node/candidate counts, whether grouping was used, and any fallback reason.

Admission webhook support lives in `scheduler/admission.rs` and the shadow HTTP server exposes `POST /admission/scheduler-name` for Kubernetes `admission.k8s.io/v1` AdmissionReview requests. It returns an RFC 6902 JSONPatch that sets `/spec/schedulerName` to `ksolver` for selected GPU pods, but it does not call the Kubernetes API itself. The policy derives from the shadow scheduler config: scheduler name, namespace allowlist, whole-GPU resource names, and MIG resource prefixes are shared, and `KSOLVER_ADMISSION_OPT_IN_LABEL` can require an explicit pod label with value `true` before patching. DRA `spec.resourceClaims` do not identify the device class on the Pod itself, so DRA pods are patched only when that opt-in label is configured and present. It never overwrites an existing non-empty `schedulerName`; out-of-scope pods are allowed without a patch, so default-scheduler workloads can remain untouched.

The Helm chart can render a disabled-by-default `MutatingWebhookConfiguration` for that endpoint. The endpoint is served only in `runtime.mode=shadow`, and the chart refuses to render the webhook for `serve` mode. Kubernetes calls admission webhooks over HTTPS, so enable it only when `/admission/scheduler-name` is exposed behind valid TLS, either through an external URL or service-level TLS termination:

```bash
helm upgrade --install ksolver ./chart \
  --namespace ksolver --create-namespace \
  --set runtime.mode=shadow \
  --set scheduler.admissionOptInLabel=ksolver.dev/schedule \
  --set admissionWebhook.enabled=true \
  --set admissionWebhook.url=https://ksolver-webhook.example.com/admission/scheduler-name \
  --set admissionWebhook.caBundle=<base64-ca-bundle>
```

With the opt-in label set, only pods labeled `ksolver.dev/schedule=true` can be patched to use `schedulerName: ksolver`.

The Helm chart exposes the same shadow scheduler policy knobs under `scheduler.*`, so production
installs do not need to rely on raw `extraEnv` for core behavior. Important values include:
`scheduler.schedulerName`, `scheduler.namespaces`, `scheduler.gpuResources`,
`scheduler.gpuResourcePrefixes`, `scheduler.quotas`, `scheduler.tenantWeights`,
`scheduler.tenantMonthlyBudgets`, `scheduler.objectiveProfile`, `scheduler.objectiveWeights.*`,
`scheduler.candidateNodeLimit`, `scheduler.candidateWidenMinAdmissionPercent`,
`scheduler.enableNodeGrouping`, `scheduler.repairCandidateLimit`,
and the guarded binding rollout controls (`scheduler.bindingRolloutMode`,
`scheduler.realBindingDryRun`, `scheduler.bindingCanaryMode`,
`scheduler.bindingLowRiskMaxGpus`, `scheduler.maxBindsPerPass`,
`scheduler.bindingReservationTtlSeconds`). The chart validates the enum/range-sensitive values at
render time so invalid rollout modes, objective profiles, zero solve windows, and unsafe throttles
fail before deployment.

Deadline-aware traces use `ksolver.dev/predicted-runtime-seconds` when supplied. Peak VRAM metadata uses `ksolver.dev/predicted-peak-vram-bytes` or `ksolver.dev/predicted-peak-vram-gib` when supplied. If explicit runtime or VRAM hints are absent, ksolver can bootstrap conservative estimates from training hints: `ksolver.dev/model-parameters-billions`, `ksolver.dev/batch-size`, optional `ksolver.dev/sequence-length`, and optional `ksolver.dev/precision` (`bf16`/`fp16`, `fp32`, `fp8`/`int8`). When node GPU memory is known from `nvidia.com/gpu.memory` (MiB, as emitted by NVIDIA labels) or `ksolver.dev/gpu-vram-{bytes,gib}`, ksolver excludes candidate nodes whose per-GPU memory is below the predicted peak, then uses the soft tie-break pass to prefer the smallest adequate known GPU memory among admission/cost-equivalent placements. Nodes without a memory label stay eligible and score neutral. These are deterministic heuristics for early scheduling and demo scenarios, not learned predictors. Prediction data collection has started as a shadow signal: completed GPU pods with `status.startTime`, terminated container finish time, GPU request, command/image fingerprint, peak memory, and any explicit runtime/VRAM predictions are summarized into each trace as `job_observation_metrics`; when predictions were present, those metrics include sample counts, mean absolute percent error in milli-percent, and worst absolute runtime/VRAM error. The scheduler now also has a pure historical predictor core that calibrates runtime/VRAM estimates from exact command-hash observations, falls back to GPU-count-scaled command history, then falls back to lower-confidence job-type/framework history and the pending pod's runtime/VRAM hint fields with sample/confidence metadata. In live shadow solves, historical predictions fill missing pending runtime/VRAM estimates before building the solver input, but explicit pod annotations and training-hint estimates remain authoritative. Those estimates can affect deadline urgency, VRAM feasibility filtering, and VRAM rightsizing tie-breaks. Each live trace includes `prediction_audit_metrics` showing how many pending pods had exact history, scaled history, segment history, hint fallback, or no prediction signal, plus `prediction_audit_details` with per-pod prediction source, confidence, predicted runtime/VRAM point estimates and lower/upper bands, inferred `framework`, inferred `job_type`, and `prediction_key` provenance such as `command_hash:<hash>`, `job_type:kubeflow_pytorchjob`, `framework:jax`, `training_hint`, or `pending_hint`.

Completed GPU job observations also carry inferred `framework` and `job_type` labels for prediction segmentation. ksolver recognizes common signals for Kubeflow `PyTorchJob`/`TFJob`, RayJob, Volcano Job, Argo Workflow, Kubernetes Job, and bare Pod workloads, plus image-name hints for PyTorch, TensorFlow, JAX, and DeepSpeed.

Priority-aware admission is opt-in under `KSOLVER_OBJECTIVE_PROFILE=gpu-gang-aware`. Kubernetes `spec.priority` is normalized into bounded score buckets; `ksolver.dev/priority` overrides it when present. `KSOLVER_GPU_PRIORITY_WEIGHT` rewards that normalized priority in the admission objective. `ksolver.dev/business-value` is a separate non-negative workload hint and `KSOLVER_GPU_BUSINESS_VALUE_WEIGHT` rewards it when operators want business value to break scarce-capacity admission ties. Queue policy is also opt-in: `ksolver.dev/queue` is matched against `KSOLVER_SHADOW_QUEUE_WEIGHTS` entries such as `urgent=100,batch=10`, then `KSOLVER_GPU_QUEUE_WEIGHT` rewards the resulting bounded queue score. Each decision trace carries both the queue name and resolved `queue_score` so operators can audit the configured score used by the solver. `KSOLVER_GPU_QUEUE_WAIT_WEIGHT` rewards queued age in bounded minutes from Kubernetes `metadata.creationTimestamp`, giving operators an opt-in anti-starvation term. `KSOLVER_GPU_FAIR_SHARE_WEIGHT` rewards pending work from tenants below their configured `KSOLVER_SHADOW_TENANT_WEIGHTS` share, using current running GPU allocation as the baseline; it defaults to `0`, so fair-share metrics remain observational unless explicitly enabled. `KSOLVER_GPU_DEADLINE_URGENCY_WEIGHT` rewards explicit-deadline workloads by latest-start urgency (`deadline - predicted runtime`), while `KSOLVER_GPU_DEADLINE_MISS_WEIGHT` penalizes admission score for explicit-deadline workloads whose predicted runtime already exceeds remaining time. These weights default to `0`, so they are observational unless explicitly enabled. Every deadline-bearing decision includes `deadline_slack_seconds`, `predicted_finish_unix_seconds`, and `predicted_deadline_miss`; trace-level `deadline_metrics` counts total predicted misses plus placed versus unplaced predicted misses separately. Unplaced decisions disclose when a pod was deferred below admitted higher-priority, higher-business-value, higher-queue, or more urgent deadline work.

Dry-run repair advice distinguishes fragmentation from GPU-memory incompatibility: if a pod is unplaced because its predicted peak VRAM exceeds every known candidate GPU, ksolver reports that freeing occupied GPU slots will not help instead of suggesting migrations or preemptions. Repair advice is policy-aware: it skips pods marked `cluster-autoscaler.kubernetes.io/safe-to-evict=false`, volume-pinned pods, pods with `ksolver.dev/do-not-disrupt=true`, pods at `ksolver.dev/progress-percent>=95`, pods with `ksolver.dev/migration-allowed=false` and `ksolver.dev/preemption-allowed=false`, pods protected by exhausted or unmodeled PodDisruptionBudgets, running GPU pods whose normalized priority is equal to or higher than the pending positive-priority target, and equal-priority running GPU pods with higher `ksolver.dev/business-value` or a more urgent deadline latest-start. Matching PDB `disruptionsAllowed` budgets are consumed across a proposed repair subset. Candidate disruption cost is the non-negative `ksolver.dev/disruption-cost` plus checkpoint age minutes from `ksolver.dev/checkpoint-age-seconds`, capped running age hours from Kubernetes `status.startTime`, and `ksolver.dev/progress-percent`; preemption adds an additional fixed penalty because it defers or restarts work instead of relocating it. Repair plans therefore prefer lower restart/checkpoint loss, younger running jobs, and migration before preemption when both can free the same GPU capacity. Equal-priority repairable targets are ordered by `target_business_value`, deadline latest-start urgency, then `target_queue_wait_seconds`, so higher-value, deadline-urgent, or longer-waiting blocked gangs surface first. Current placement pressure is exported as `ksolver_shadow_unplaced`, `ksolver_shadow_vram_blocked`, and `ksolver_shadow_high_priority_unplaced`; cumulative trends are exported as `ksolver_shadow_unplaced_total`, `ksolver_shadow_vram_blocked_total`, and `ksolver_shadow_high_priority_unplaced_total`. Priority/deadline observability also includes cumulative `ksolver_shadow_predicted_deadline_misses_total`; latest-solve deadline pressure is exported as `ksolver_shadow_deadline_jobs`, `ksolver_shadow_unplaced_deadline_jobs`, `ksolver_shadow_predicted_deadline_misses`, `ksolver_shadow_placed_predicted_deadline_misses`, `ksolver_shadow_unplaced_predicted_deadline_misses`, and `ksolver_shadow_worst_deadline_slack_seconds`. Queue-wait/starvation observability comes from each pod's Kubernetes `metadata.creationTimestamp`: every decision carries `queue_wait_seconds`, each trace includes `queue_wait_metrics`, and the latest max wait values are exported as `ksolver_shadow_max_queue_wait_seconds` and `ksolver_shadow_high_priority_max_queue_wait_seconds`. Tenant fairness observability is exposed in each trace as `tenant_fairness_metrics`, grouped by pod team annotation when present and namespace otherwise; it reports pending/placed/unplaced pods, requested/admitted/denied GPU demand, admitted monthly cost, budget overage, max queue wait, quota-throttled pods, configured fair-share weight, admitted-share milli, weighted target GPU-milli, under/over fair-share delta, borrowed GPU-milli, and reclaimable borrowed GPU-milli per tenant. Borrow/reclaim fields are audit signals for future enforcement: a tenant is borrowing when admitted demand is above its weighted share, and borrowed capacity is reclaimable only when another tenant has denied demand while below share. Configured tenant budgets are hard shadow-admission caps after subtracting already-running GPU placement cost; the same placement cost model is still exposed for audit as `node monthly price / node GPU capacity * placed pod GPU request` from `OptimizationNode.price`. The latest quota-throttled pressure is exported as `ksolver_shadow_quota_throttled_pods` and `ksolver_shadow_quota_throttled_max_queue_wait_seconds`; latest fairness pressure is exported as `ksolver_shadow_fairness_under_share_tenants`, `ksolver_shadow_fairness_over_share_tenants`, `ksolver_shadow_fairness_borrowed_gpu_milli`, and `ksolver_shadow_fairness_reclaimable_borrowed_gpu_milli`; latest budget pressure is exported as `ksolver_shadow_budget_over_tenants` and `ksolver_shadow_budget_overage_monthly_milli`. Completed-job prediction observation metrics are exported as `ksolver_shadow_job_observation_completed_gpu_pods`, `ksolver_shadow_job_observation_runtime_samples`, `ksolver_shadow_job_observation_failed_gpu_pods`, `ksolver_shadow_job_observation_max_runtime_seconds`, `ksolver_shadow_job_observation_max_peak_memory_bytes`, and `ksolver_shadow_job_observation_unique_command_hashes`. Pending prediction coverage is exported as `ksolver_shadow_prediction_audit_pending_pods`, `ksolver_shadow_prediction_audit_fingerprint_matched_pods`, `ksolver_shadow_prediction_audit_history_exact_pods`, `ksolver_shadow_prediction_audit_history_scaled_pods`, `ksolver_shadow_prediction_audit_history_segment_pods`, `ksolver_shadow_prediction_audit_hint_pods`, `ksolver_shadow_prediction_audit_unknown_pods`, and `ksolver_shadow_prediction_audit_average_confidence`. Candidate-pruning and model-size observability is exposed as `ksolver_shadow_candidate_node_limit`, `ksolver_shadow_candidate_edges_unpruned`, `ksolver_shadow_candidate_edges_initial`, `ksolver_shadow_candidate_edges_final`, `ksolver_shadow_candidate_pruned_workloads`, `ksolver_shadow_candidate_widening_retries`, and `ksolver_shadow_candidate_widening_attempts_total`; candidate-quality gauges add `ksolver_shadow_candidate_pruning_active`, `ksolver_shadow_candidate_widened`, `ksolver_shadow_candidate_edge_reduction_milli`, and one-hot `ksolver_shadow_candidate_regret_status{status=...}` so operators can alert on pruned results with unknown regret. Node-grouping observability is exposed as `ksolver_shadow_node_grouping_enabled`, `ksolver_shadow_node_grouping_used`, `ksolver_shadow_node_grouping_eligible_groups`, `ksolver_shadow_node_grouping_eligible_nodes`, `ksolver_shadow_node_grouping_max_group_size`, `ksolver_shadow_node_grouping_grouped_nodes`, `ksolver_shadow_node_grouping_grouped_candidate_edges`, `ksolver_shadow_node_grouping_used_total`, and `ksolver_shadow_node_grouping_fallback_total`. Each trace includes `repair_metrics` so consumers can distinguish repairable fragmentation from VRAM-incompatible pods, no-total-capacity pods, policy/candidate-budget-blocked repairs, incomplete pending workload model data, and skipped repair candidates by reason bucket. Latest repair pressure is exported as `ksolver_shadow_repair_plans`, `ksolver_shadow_repair_migrations`, `ksolver_shadow_repair_preemptions`, `ksolver_shadow_repair_disruption_cost`, `ksolver_shadow_repair_repairable_targets`, `ksolver_shadow_repair_unrepairable_targets`, `ksolver_shadow_repair_vram_blocked_targets`, `ksolver_shadow_repair_not_enough_total_gpu_targets`, `ksolver_shadow_repair_policy_or_candidate_blocked_targets`, `ksolver_shadow_repair_incomplete_model_targets`, `ksolver_shadow_repair_skipped_candidates`, and `ksolver_shadow_repair_skipped_candidates_by_reason{reason=...}`; cumulative repair trends are exported as `ksolver_shadow_repair_plans_total`, `ksolver_shadow_repair_migrations_total`, `ksolver_shadow_repair_preemptions_total`, and `ksolver_shadow_repair_disruption_cost_total`. Real-binding rollout observability includes `binding_reservation_metrics` and `binding_outcome_metrics` on each trace; `ksolver_shadow_bind_canary_skipped_total` counts candidates skipped by low-risk canary mode separately from other skips, and `ksolver_shadow_bind_skipped_by_reason{reason=...}` exposes latest-pass skip buckets for readiness, identity, scheduler ownership, already-bound pods, DRA safety, throttling, reservation rejection, disabled rollout, binding groups, and unknown reasons. Leader-election observability is exposed as `ksolver_shadow_leader` plus `ksolver_shadow_leader_acquired_total`, `ksolver_shadow_leader_renewed_total`, `ksolver_shadow_leader_wait_total`, `ksolver_shadow_leader_renew_errors_total`, and `ksolver_shadow_leader_skipped_solves_total`. Each trace includes the `objective_profile` and `objective_weights` used for the solve, plus `admission_metrics.{admitted_pods,admitted_gpu_demand}` and `gpu_utilization_metrics.{active_gpu_nodes,stranded_gpu_on_active_nodes}`; the latest admission/utilization values are exported as `ksolver_shadow_admitted_pods`, `ksolver_shadow_admitted_gpu_demand`, `ksolver_shadow_active_gpu_nodes`, and `ksolver_shadow_stranded_gpu_on_active_nodes`.

Each trace also includes `outcome_summary`, a derived operator summary of total/placed/unplaced pods, requested/admitted/unplaced GPU demand, pod and GPU admission percentages, admitted monthly cost, active GPU nodes, stranded GPU on active nodes, total/placed/unplaced predicted deadline misses, and repairable versus unrepairable unplaced targets. The latest outcome summary is exported as `ksolver_shadow_requested_gpu_demand`, `ksolver_shadow_unplaced_gpu_demand`, `ksolver_shadow_pod_admission_percent_milli`, `ksolver_shadow_gpu_admission_percent_milli`, and `ksolver_shadow_admitted_monthly_cost_milli`.

The deterministic `gpu-scenarios --json` report also includes `roi_summary`, aggregating requested GPU demand, useful GPU admission, admission-percent gain, unplaced-pod reduction, active-node reduction, stranded-GPU reduction, synthetic active-node monthly cost, and active-node GPU-utilization deltas across the scenario library. This is backend data for ROI demos; the synthetic scenario costs are stable relative prices for before/after comparisons, not cloud price claims.

Prediction feedback error gauges are also exported as `ksolver_shadow_job_observation_runtime_prediction_samples`, `ksolver_shadow_job_observation_runtime_prediction_mape_milli`, `ksolver_shadow_job_observation_max_runtime_prediction_error_seconds`, `ksolver_shadow_job_observation_vram_prediction_samples`, `ksolver_shadow_job_observation_vram_prediction_mape_milli`, and `ksolver_shadow_job_observation_max_vram_prediction_error_bytes`.

**Preferred (soft) affinity** (`preferredDuringScheduling`) is honored as a cost-tie-break: after the cost-optimal solve, among equally-optimal placements the scheduler prefers higher-scoring nodes. It runs only when the solve proves optimal and **never changes which pods are admitted or the total cost** (a two-phase pass that pins the cost optimum + admitted set, then maximizes soft score). Both dimensions contribute to a single per-node soft score:

- **Preferred node affinity** — a node earns each matching term's `weight`.
- **Preferred pod affinity / anti-affinity** — a node accumulates `+weight` (affinity) or `-weight` (anti-affinity) for **each** running pod that matches the term's label selector (with full namespace scoping) and shares the candidate node's topology domain (`node.labels[topologyKey]`, label-based for every key including `kubernetes.io/hostname`). This mirrors the upstream scheduler's per-pod score accumulation, in **both directions**: *forward* (the pending pod's own preferences vs running pods) and *symmetric* (a running pod's preferred terms steering the pending pod, when the term matches every gang member).
- **Required pod affinity** — shadow placement applies a best-effort hard filter for modelable required `podAffinity` terms when matching already-running peers exist: candidate nodes must share the requested topology domain with a matching peer. Terms with no existing match are not enforced so first-pod/self-affinity bootstrap cases are not over-constrained; decisions still carry a pod-affinity caveat because full kube-scheduler inter-pod affinity semantics are broader.
- **Co-placement of two pending pods** — when two *pending* pods/gangs express mutual (or one-directional) preferred pod **affinity**, the batch solver jointly rewards co-placing them in the same topology domain — going beyond the sequential kube-scheduler, which can only score against already-running pods. The reward is applied only in the cost-preserving second phase, so it never changes admission or cost. Soft anti-affinity between two *pending* pods is out of scope (its hard form is enforced by cross-workload anti-affinity above).

Placements on **time-sliced** GPU nodes (NVIDIA `nvidia.com/gpu.sharing-strategy=time-slicing`, or `nvidia.com/gpu.replicas>1` when no strategy label) are disclosed with a "time-sliced GPU: shared, no isolation" caveat — such GPUs are oversubscribed with no memory/fault isolation, so "fits" ≠ isolated performance. (MPS nodes are not flagged as time-sliced.)
The deterministic `gpu-scenarios --json` report includes a `time_sliced_gpu_scenario` proof that this caveat appears only for shared GPU placements.

Best-effort pod anti-affinity is modeled for `requiredDuringScheduling` terms with full label selectors — `matchLabels` and `matchExpressions` (`In`/`NotIn`/`Exists`/`DoesNotExist`), same namespace — across **any topology key**: `kubernetes.io/hostname` excludes the exact node; `topology.kubernetes.io/zone`, a rack label, etc. exclude the whole topology *domain* that already holds a matching pod (pending-vs-running, both directions). Cross-namespace terms are modeled via both an explicit `namespaces` list (empty ⇒ own namespace) and a `namespaceSelector` (empty `{}` = all namespaces; else namespaces whose labels match). Terms we still can't model (unsupported label-selector operators) are surfaced as a "pod anti-affinity" caveat on the decision.

**Dry-run binding plan.** `GET /api/scheduler/binding-plan` renders, from the latest decision, the exact `Binding` subresource payloads (`apiVersion: v1`, `kind: Binding`, `metadata`, `target: Node`) that a real binder *would* POST for each placed pod — one entry per placement, with `dry_run: true`, `trace_sequence`, and `solve_millis` so staleness is visible. Each entry also carries a **`readiness`** (`ready`, or `stale` with a reason) computed against the *latest* cluster snapshot — the stale/conflict guard a real binder must run before applying: it flags a vanished target node, a pod gone from the snapshot, missing pod identity/UID, a pod recreated under the same name (uid changed), a pod already scheduled, or a target node that is no longer in the pod's latest feasible-node set. (It is a stale/conflict check over ksolver's normalized snapshot, not a fresh kube-scheduler Filter call.) The dashboard shows the plan with a readiness column. This is the actionable, inspectable output of shadow mode and the groundwork for real binding (a separate, authorization-gated phase); it is **rendered, never applied** — shadow still issues only read/watch/list. A dedicated `no-mutation` test guards both the renderer (no API-call/kube-client symbols) and the shadow loop (no `Binding`/create/patch/delete/evict/replace calls).

**Real binding (Phase 3, opt-in — mutates the cluster).** By default ksolver binds nothing. Setting `KSOLVER_ENABLE_REAL_BINDING=true` arms the executor: after each solve it POSTs a `pods/binding` for every decision whose readiness re-check is `ready`, actually scheduling the pod onto the chosen node. Safety controls:

- `KSOLVER_BINDING_ROLLOUT_MODE` (default inferred from the legacy flags below) — one operator-facing rollout switch:
  - `observe-only` / `observe` — read-only shadow mode; no mutation-capable client is created.
  - `dry-run` — creates the binding client but sends server-side `dryRun=All`; validates without persisting.
  - `bind-low-risk` — persists only candidates at or below `KSOLVER_BINDING_LOW_RISK_MAX_GPUS`.
  - `bind-all` / `live` — persists every ready candidate, still subject to uid/scheduler/readiness/kill-switch/throttle checks.
  - Invalid direct-env values fail closed to `observe-only`; the Helm chart fails rendering invalid values before deployment.
- `KSOLVER_ENABLE_REAL_BINDING` (default `false`) — master switch; when unset/false the scheduler is read-only and no mutation-capable client is even created.
- `KSOLVER_BINDING_KILL_SWITCH` (default `false`) — emergency fail-closed override; when true, no mutation-capable binding client is created and `apply_bindings` skips all entries even if `KSOLVER_ENABLE_REAL_BINDING=true`.
- `KSOLVER_ENABLE_KUBERNETES_EVENTS` (default `false`) — when true and the kill switch is off, POST rendered scheduler decision/binding Events to the Kubernetes Events API. The `/api/scheduler/decision-events`, `/api/scheduler/repair-events`, and `/api/scheduler/binding-events` endpoints remain read-only draft renderers regardless of this flag; repair recommendation Events are not posted by this flag.
- `KSOLVER_REAL_BINDING_DRY_RUN` (default `false`) — send bindings with server-side `dryRun=All`: the apiserver validates them but persists nothing. Use this first to confirm the path before going live.
- `KSOLVER_BINDING_CANARY_MODE` (default `all`) — set to `low-risk` to bind only candidates whose GPU request is at or below `KSOLVER_BINDING_LOW_RISK_MAX_GPUS`; larger ready candidates are skipped with an explicit binding outcome reason. Invalid direct-env values fail to `low-risk`.
- `KSOLVER_BINDING_LOW_RISK_MAX_GPUS` (default `1`) — GPU-request threshold for low-risk canary binding.
- `KSOLVER_MAX_BINDS_PER_PASS` (default `10`) — throttle on bindings applied per solve.
- Every candidate gets a **final live re-check immediately before the POST** (`should_apply`): the pod must still exist, match uid, be owned by our scheduler, be un-terminating, unbound, and Pending — otherwise it is skipped. A post-response parse error is reconciled against live state so a bind that actually succeeded is never miscounted as failed. Per-pod errors never abort the pass. Metrics: `ksolver_shadow_bound_total`, `ksolver_shadow_bind_skipped_total`, `ksolver_shadow_bind_canary_skipped_total`, `ksolver_shadow_bind_skipped_by_reason{reason=...}`, and `ksolver_shadow_bind_failed_total`. Binding skip buckets include `canary`, `readiness`, `identity`, `scheduler`, `already_bound`, `dra`, `throttle`, `reservation`, `disabled`, `group`, and `other`.
- All mutation lives in one module (`scheduler/binder.rs`); a `no-mutation` test keeps `shadow.rs` and the plan renderer free of direct mutating calls.

When real binding is enabled the service account also needs `create` on the pod binding subresource:

```yaml
  - apiGroups: [""]
    resources: [pods/binding]
    verbs: [create]
```

The Helm chart keeps that permission disabled by default. To render it intentionally:

```bash
helm upgrade --install ksolver ./chart \
  --namespace ksolver --create-namespace \
  --set runtime.mode=shadow \
  --set scheduler.bindingRolloutMode=bind-low-risk \
  --set scheduler.enableRealBinding=true \
  --set scheduler.bindingKillSwitch=false \
  --set rbac.allowBindingMutations=true
```

To POST Kubernetes Event objects for auditability without binding pods, enable Event emission and
Event RBAC while leaving real binding off:

```bash
helm upgrade --install ksolver ./chart \
  --namespace ksolver --create-namespace \
  --set runtime.mode=shadow \
  --set scheduler.bindingRolloutMode=observe-only \
  --set scheduler.enableRealBinding=false \
  --set scheduler.bindingKillSwitch=false \
  --set scheduler.enableKubernetesEvents=true \
  --set rbac.allowEventWrites=true
```

Event RBAC grants only `create` on `events.k8s.io/events`; ksolver does not patch or update Event
objects. Event writes are best-effort and observable through
`ksolver_shadow_kubernetes_events_total{event_type="decision|binding",outcome="attempted|created|failed"}`.
Decision Events use distinct reasons for quota and budget policy blocks:
`KsolverQuotaThrottled` / `QuotaThrottled` and
`KsolverBudgetThrottled` / `BudgetThrottled`.

Leader election is disabled by default. Enabling it renders env vars plus a namespaced Role/RoleBinding for `coordination.k8s.io/leases`; the scheduler loop then skips snapshot/solve/bind passes on non-leader replicas:

```bash
helm upgrade --install ksolver ./chart \
  --namespace ksolver --create-namespace \
  --set runtime.mode=shadow \
  --set scheduler.enableLeaderElection=true \
  --set scheduler.leaderElectionNamespace=ksolver \
  --set scheduler.leaderElectionLeaseName=ksolver-scheduler \
  --set rbac.allowLeaderElection=true
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
- Pods carrying constructs we intentionally don't model in this Filter harness (required pod affinity/anti-affinity, unsupported `DoNotSchedule` topology spread shapes, and priority / `priorityClassName`) are bucketed as **expected divergence** with per-reason counts. Required node affinity, including OR terms and `metadata.name` `matchFields`, belongs in the strict bucket.
- Only strict-bucket pods must match exactly. `FALSE-POSITIVE` results (we say feasible, the scheduler rejects) are listed first — those are the dangerous ones. The text report includes `strict-gate: pass|fail`; `fail` means at least one strict false-positive.
- `--json` emits a machine-readable report with the same strict/expected-divergence matrices, mismatch lists, expected-divergence reason counts, and `strict_gate_status`.
- `--fail-on-strict-false-positive` exits non-zero after printing the report if the strict gate fails. Expected-divergence mismatches remain advisory and do not trip this CI gate.
- Read-only on the real cluster; only the simulator (a sandbox) is scheduled against. With no simulator URL configured, `conform` prints a skip notice and exits 0. With `--json`, the skip path emits a JSON object with `skipped: true`.

CI-friendly examples:

```bash
KSOLVER_SCHEDULER_SIMULATOR_URL=http://localhost:8080 \
  ksolver conform --sample 20 --json --fail-on-strict-false-positive

KSOLVER_SCHEDULER_SIMULATOR_URL=http://localhost:8080 \
  ksolver conform --sample 20 --fail-on-strict-false-positive
```

**Live-verified** (2026-07-01) against a self-built arm64 kube-scheduler-simulator (v0.4.0 publishes amd64-only images that crash under emulation on Apple Silicon — build them from source with `docker buildx --platform=linux/arm64`). `conform` ran end-to-end and produced a confusion matrix (agree / false-positive / false-negative) with **zero false-negatives**. Note: the single-node import path can yield spurious false-positives when the imported node isn't marked Ready inside the simulator (its own KWOK re-manages imported node status) — a harness-fidelity caveat, not a Filter-modeling gap.

## License

MIT
