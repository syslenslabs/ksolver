# GPU Scheduler Roadmap — Ranked Missing Features

> **For agentic workers:** This is a ranked product/engineering roadmap, not a single implementation ticket. When executing, split each phase into a focused implementation plan with tests, demo criteria, and rollback notes.

**Goal:** Turn the current ksolver GPU shadow scheduler from a placement/binpacking demo into an SRE-ready GPU scheduling product. The current system can observe pending GPU pods, batch-solve placements, compare against kube-scheduler-simulator, model gangs/quotas/affinity caveats, show cost-ish UI metrics, and scale larger synthetic cases with candidate-node pruning. The missing work is mostly around production semantics: priority, preemption, deadlines, runtime prediction, real binding safety, and explanations that prove value to operators.

**Ranking Principle:** The phases are ordered by expected commercial/product impact for SRE and platform teams, with engineering dependencies considered second. The first phases should make the scheduler answer the question: "Why is this better than kube-scheduler for my expensive GPU fleet?"

## Current Baseline

- Shadow scheduler runs pending-only GPU solves against residual cluster capacity.
- Solver supports partial admission, gang-aware objective weights, namespace GPU quota, co-location, anti-affinity handling, preferred-affinity tie-breaks, DRA scalar approximation, and candidate-node pruning.
- Dashboard shows kube-scheduler-simulator placement versus ksolver placement, node fill, unplaced jobs, objective controls, and rough cost comparisons.
- Real binding exists as guarded scaffolding/dry-run flow, but production binding is not the primary product surface yet.
- Known gap: priority/preemption/deadline/runtime prediction are not modeled as first-class scheduling inputs.

## Phase 1 — Priority, Queue Policy, and Admission Semantics

**Rank:** 1  
**Why it matters:** Operators will not trust an optimizer that treats a critical training run and a disposable experiment equally. Priority is the first feature that turns placement from "binpack nicely" into "make the right business decision under scarcity."

**Build:**

- Read Kubernetes `spec.priority` and `spec.priorityClassName` into `PendingGpuPod` and `OptimizationWorkload`.
- Add a normalized priority score; do not use raw Kubernetes priority directly because system classes can dwarf user jobs.
- Support custom annotations:
  - `ksolver.dev/priority`
  - `ksolver.dev/team`
  - `ksolver.dev/business-value`
  - `ksolver.dev/queue`
- Add objective weight for priority admission.
- Add deterministic priority tie-breaks in the decision trace.
- Surface priority in the UI and explain why a lower-priority pod was not admitted.

**Acceptance Criteria:**

- Given scarce capacity, high-priority jobs are admitted ahead of equal-shape low-priority jobs.
- Existing cost/binpack behavior remains unchanged when priority weight is zero.
- UI shows priority, admission reason, and displaced/deferred lower-priority work.

## Phase 2 — Preemption and Migration Planning

**Rank:** 2  
**Why it matters:** The biggest GPU-cluster problem is not only pending placement. It is fragmentation created by already-running jobs. Without preemption/migration, ksolver can only optimize around the existing mess.

**Build:**

- Add a "repair solve" mode that considers selected running GPU pods as movable or evictable candidates.
- Model three states per workload:
  - keep current placement
  - migrate to a new node
  - preempt/defer
- Add costs for migration/preemption:
  - restart penalty
  - checkpoint loss
  - user disruption
  - already-running job age/progress
- Add PDB/owner checks and "do not preempt" annotations.
- Emit a preemption/migration plan separately from binding plan; never mutate by default.

**Acceptance Criteria:**

- Demo shows a 4-GPU gang blocked by fragmentation, then ksolver proposes moving/preempting lower-value 1-GPU jobs to create a 4-GPU island.
- Trace distinguishes "not enough total GPUs" from "enough GPUs, but fragmented unless migration is allowed."
- No real eviction happens unless an explicit future executor is enabled.

## Phase 3 — Deadline-Aware Scheduling

**Rank:** 3  
**Why it matters:** This is the clearest product wedge beyond basic binpacking. If a scientist says "finish by Monday," the scheduler can use fewer GPUs, cheaper GPUs, or lower-priority windows while preserving the user's actual deadline.

**Build:**

- Add annotations:
  - `ksolver.dev/deadline`
  - `ksolver.dev/min-gpus`
  - `ksolver.dev/max-gpus`
  - `ksolver.dev/preferred-gpus`
  - `ksolver.dev/flexible=true`
- Model slack: deadline minus predicted completion time.
- Objective rewards jobs that meet deadlines and penalizes deadline misses.
- Allow flexible jobs to use fewer replicas when deadline slack is large.
- Add UI fields for deadline, predicted finish time, and resource tradeoff.

**Acceptance Criteria:**

- Demo shows a weekend-flexible job using fewer nodes while urgent jobs get dense capacity.
- UI can answer: "Why did this job get 4 GPUs instead of 8?"
- Jobs without deadline annotations preserve current behavior.

## Phase 4 — Runtime and VRAM Prediction

**Rank:** 4  
**Why it matters:** Deadline-aware scheduling and right-sizing are weak without predictions. This becomes the data-platform moat: ksolver learns workload behavior and predicts the cheapest placement that will still finish on time.

**Build:**

- Collect historical job observations:
  - image/command hash
  - framework/job type
  - requested GPUs
  - GPU model
  - peak VRAM
  - runtime
  - exit status
  - dataset/model-size hints when available
- Parse common submission APIs:
  - PyTorchJob
  - TrainingJob/PyTorchJob via Kubeflow
  - RayJob
  - Volcano Job
  - bare Pod/Job
  - Argo Workflows
- Add optional user hints:
  - model parameters
  - batch size
  - sequence length
  - precision
  - checkpoint interval
- Start with calibrated heuristics, then train regressors after enough observations.

**Acceptance Criteria:**

- UI shows predicted runtime and peak VRAM with confidence bands.
- Scheduler can reject obviously impossible VRAM placements before wasting queue time.
- Prediction misses are logged and fed back into future estimates.

## Phase 5 — Node Grouping and Symmetry Reduction

**Rank:** 5  
**Why it matters:** Candidate pruning made the 1000-job/500-node synthetic benchmark solvable, but it can lose global optimality. Homogeneous GPU fleets should be modeled as counted pools where possible, then expanded back to physical nodes.

**Build:**

- Group equivalent nodes by:
  - GPU type/count
  - CPU/memory shape
  - labels relevant to current pods
  - taints/tolerations
  - zone/topology domain
  - current residual GPU profile
- Solve against node groups with `count > 1` when pods do not require individual node identity.
- Expand group assignments back to physical nodes deterministically.
- Combine grouping with candidate pruning only when grouping is not safe.

**Acceptance Criteria:**

- 1000-job/500-node homogeneous fleet solves quickly without arbitrary pruning.
- Grouped solve produces the same admission count/cost as full small-scale solve.
- Trace explains when grouping was used and when individual nodes were required.

## Phase 6 — Adaptive Candidate Widening

**Rank:** 6  
**Why it matters:** Fixed `K` is useful for demos but brittle in production. The scheduler should start small and widen only when the result looks suspicious.

**Build:**

- Start with `K=16`.
- Retry with `K=32`, `K=64`, or full feasible set when:
  - high-priority job is unplaced
  - admission drops below a threshold
  - solver returns no usable incumbent
  - regret estimate is too high
- Preserve previous incumbent if widening times out.
- Add trace fields:
  - candidate_node_limit
  - retry_count
  - final_candidate_edges
  - widening_reason

**Acceptance Criteria:**

- Large benchmark uses small `K` when adequate.
- Scarcity/high-priority demos widen automatically.
- UI shows whether a result is "fast-pruned" or "widened."

## Phase 7 — Fair Sharing and Team Budgets

**Rank:** 7  
**Why it matters:** GPU fleets are political. Platform teams need the scheduler to explain fairness, not just efficiency.

**Build:**

- Extend namespace quota into weighted fair sharing.
- Support borrowing idle quota with reclaim rules.
- Add per-team budget/cost caps.
- Track starvation age and queue wait time.
- Add "why this team was throttled" explanations.

**Acceptance Criteria:**

- One team cannot monopolize the fleet unless explicitly allowed.
- Idle quota can be borrowed without permanently stealing capacity.
- UI shows team share, borrowed GPUs, and denied GPUs.

## Phase 8 — Production Binding and Safety

**Rank:** 8  
**Why it matters:** SREs may first buy this as an explainability/shadow product, but production value eventually requires safe action.

**Build:**

- Add leader election.
- Make binding idempotent and auditable.
- Emit Kubernetes Events for decisions.
- Add RBAC-minimal deployment profile.
- Add admission webhook option to set `schedulerName: ksolver` for selected GPU workloads.
- Add a canary mode:
  - observe only
  - dry-run binding
  - bind only low-risk jobs
  - bind all selected jobs
- Add rollback and kill-switch behavior.

**Acceptance Criteria:**

- Production deployment can run in observe-only mode with no mutation rights.
- Binding executor is safe under restarts and duplicate events.
- Operators can disable all mutation with one config change.

## Phase 9 — True DRA, MIG, and Device Topology

**Rank:** 9  
**Why it matters:** This becomes important for advanced GPU fleets, but the first product wedge can survive with scalar approximation. Eventually, device-level correctness matters.

**Build:**

- Model ResourceClaims and device allocation as assignment variables, not scalar resources.
- Understand MIG profiles and mixed strategy.
- Track allocated device identities.
- Model GPU locality:
  - same host
  - same NUMA domain
  - NVLink/NVSwitch island
  - topology labels
- Account for fractional/time-sliced GPUs where exposed.

**Acceptance Criteria:**

- DRA jobs with device-class constraints are scheduled against real device identities.
- UI can explain "this job needs same NVLink island" versus generic GPU capacity.
- Scalar approximation remains available as fast fallback.

## Phase 10 — Value Explanation and ROI Dashboard

**Rank:** 10  
**Why it matters:** The product has to sell itself. The UI should make the benefit obvious to an SRE in 30 seconds.

**Build:**

- Add headline metrics:
  - additional jobs admitted
  - GPU-hours saved
  - stranded GPU reduction
  - active node reduction
  - cost per hour/day/month
  - deadline misses avoided
- Add scenario library:
  - fragmented gang
  - priority preemption
  - deadline-flexible job
  - team quota contention
  - MIG mismatch
  - node-group scale benchmark
- Add "why better than kube-scheduler" narrative per scenario.
- Add regret display for pruning/grouping approximations.

**Acceptance Criteria:**

- Dashboard can show a deterministic before/after scenario without live GPUs.
- Each recommendation has an explanation grounded in constraints and money.
- A platform engineer can screenshot the page and use it in an internal business case.

## Recommended Execution Order

1. Priority + queue policy.
2. Preemption/migration dry-run planner.
3. Deadline-aware objective.
4. Runtime/VRAM prediction data model.
5. Node grouping for scalable exactness.
6. Adaptive candidate widening.
7. Fair sharing and team budgets.
8. Production binding safety.
9. True DRA/MIG/device topology.
10. ROI dashboard and scenario library.

## Near-Term Demo Target

The next compelling demo should combine the top three phases:

1. A high-priority 4-GPU gang arrives.
2. The fleet has enough total GPUs but no contiguous 4-GPU island.
3. kube-scheduler leaves the gang pending.
4. ksolver proposes a migration/preemption plan for low-priority flexible jobs.
5. ksolver admits the high-priority gang, keeps deadline-flexible jobs on track, and reports the dollar/throughput benefit.

That story is stronger than pure binpacking because it answers the real SRE question: "What should I do right now, and why is it worth the disruption?"

## Prioritization Matrix

| Rank | Phase | Customer Value | Engineering Risk | Demo Value | Dependency Weight | Recommendation |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | Priority / queue policy | Very high | Medium | High | Low | Build immediately |
| 2 | Preemption / migration planning | Very high | High | Very high | Medium | Build after priority exists |
| 3 | Deadline-aware scheduling | Very high | Medium | Very high | Medium | Build in parallel with prediction scaffolding |
| 4 | Runtime / VRAM prediction | High | High | Medium | High | Start data model early, ship heuristics first |
| 5 | Node grouping / symmetry reduction | Medium | Medium | Medium | Low | Build to make scale claims credible |
| 6 | Adaptive candidate widening | Medium | Low | Medium | Low | Build after candidate metrics exist |
| 7 | Fair sharing / budgets | High | Medium | Medium | Medium | Build when teams/tenants are first-class |
| 8 | Production binding safety | High | High | Low | High | Keep gated until shadow value is proven |
| 9 | True DRA / MIG / topology | Medium | Very high | Medium | Medium | Defer unless target users need advanced topology |
| 10 | ROI dashboard / scenario library | Very high | Low | Very high | Low | Build continuously around every phase |

## Dependency Graph

The roadmap is ranked by value, but implementation should respect these dependencies:

```text
Priority
  -> Preemption / migration
  -> Fair sharing

Runtime + VRAM prediction
  -> Deadline-aware scheduling
  -> Right-sizing
  -> ROI estimates

Candidate pruning metrics
  -> Adaptive widening
  -> Regret reporting

Node grouping
  -> Larger-scale exact solves
  -> Better 1000+ node demos

Dry-run binding plan
  -> Production binding
  -> Preemption executor

DRA / MIG topology
  -> Device-level correctness
  -> Advanced placement explanations
```

The practical sequencing is:

1. Ship priority first because it is low-dependency and changes the scheduler from "efficient" to "policy-aware."
2. Build preemption/migration as dry-run only, because it creates the strongest visual demo without immediately requiring destructive cluster actions.
3. Add deadline-aware scheduling with simple user-provided runtime hints before full ML prediction exists.
4. Start collecting historical observations early so prediction improves over time.
5. Improve scale performance with grouping and adaptive widening after the product semantics are clear.

## Milestones

### Milestone A — Policy-Aware Shadow Scheduler

**Target:** The scheduler makes better admission decisions under scarcity.

**Includes:**

- Phase 1 priority/queue policy.
- UI priority display.
- Objective profiles that can choose between cost, throughput, gang completion, and priority.
- Deterministic scenario showing high-priority work admitted ahead of low-priority work.

**Done When:**

- A synthetic scarce-capacity scenario proves priority affects admission.
- Existing no-priority behavior is unchanged by default.
- Trace explains which lower-priority pods were deferred and why.

### Milestone B — Defragmentation Advisor

**Target:** ksolver can explain how to make room for a blocked gang without directly mutating the cluster.

**Includes:**

- Phase 2 dry-run preemption/migration planner.
- Repair solve that includes selected running pods.
- Disruption cost model.
- UI panel showing "move/preempt these jobs to admit this gang."

**Done When:**

- Demo shows total GPU capacity is sufficient but fragmented.
- ksolver produces a valid migration/preemption plan.
- Plan has disruption scores and avoids protected pods.

### Milestone C — Deadline-Aware Right-Sizing

**Target:** Flexible jobs can trade runtime for fewer/cheaper GPUs while urgent jobs get capacity.

**Includes:**

- Phase 3 deadline annotations.
- Early Phase 4 heuristic runtime model.
- Finish-time prediction in trace/UI.
- Deadline miss penalty in objective.

**Done When:**

- A weekend-flexible job is assigned fewer GPUs while still predicted to meet deadline.
- An urgent job receives more GPUs or higher admission priority.
- UI shows predicted finish time and resource tradeoff.

### Milestone D — Scale and Regret Controls

**Target:** The scheduler can solve large fleets quickly and explain approximation risk.

**Includes:**

- Phase 5 node grouping.
- Phase 6 adaptive candidate widening.
- Candidate edge counts and retry reasons in traces.
- Small-scale full-versus-reduced regret benchmarks.

**Done When:**

- 1000 jobs / 500 nodes solves under the target latency without `MODEL_INVALID`.
- For small scenarios, reduced solve regret is measured against full solve.
- UI identifies whether placement came from full, grouped, or pruned solve.

### Milestone E — SRE-Ready Shadow Product

**Target:** A platform team can deploy ksolver safely in observe-only mode and use the dashboard for decisions.

**Includes:**

- Phase 7 fair sharing and budget explanations.
- Phase 8 production safety scaffolding, still default read-only.
- Phase 10 scenario library and ROI dashboard.
- Deployment docs and RBAC-minimal shadow mode.

**Done When:**

- Dashboard can explain priority, deadline, fairness, cost, and fragmentation in deterministic scenarios.
- Observe-only deployment requires no mutation rights.
- All mutation-capable paths are explicitly gated and test-covered.

## First Implementation Slice

The first slice should be small enough to finish quickly but meaningful enough to change the demo.

**Slice:** Priority-aware admission.

**Files likely touched:**

- `ksolver/src/scheduler/pod_filter.rs`
- `ksolver/src/scheduler/pending_input.rs`
- `ksolver/src/model.rs`
- `ksolver/src/cpsat_rust.rs`
- `ksolver/src/scheduler/trace.rs`
- `ksolver/src/scheduler/shadow.rs`
- `ksolver/static/shadow.html`
- deterministic scenario manifests or `gpu_scenarios.rs`

**Implementation Steps:**

1. Add `priority: i64` and `priority_class_name: Option<String>` to `PendingGpuPod`.
2. Extract `spec.priority` and `spec.priorityClassName` in classification.
3. Add normalized priority to `OptimizationWorkload`.
4. Add `priority` to `ObjectiveWeights`.
5. Update `admission_score()` to include normalized priority only under the GPU-aware objective.
6. Add UI and trace fields so priority is visible.
7. Add deterministic scenario:
   - low-priority jobs fill fragmented capacity
   - high-priority gang arrives
   - ksolver admits/defer decisions differ by priority profile
8. Validate default behavior with priority weight zero.

**Tests:**

- `pod_filter` extracts Kubernetes priority.
- `pending_input` propagates priority through gangs.
- `cpsat_rust` admits higher-priority workload under scarce capacity.
- UI script check still passes.

## Second Implementation Slice

**Slice:** Dry-run preemption/migration advisor.

**Implementation Steps:**

1. Add a solver mode that includes selected running GPU workloads as movable candidates.
2. Add per-workload `current_node`, `migration_allowed`, `preemption_allowed`, and disruption cost.
3. Extend objective with migration/preemption penalty.
4. Produce a `repair_plan` separate from binding plan.
5. Render repair plan in UI.
6. Add deterministic fragmented-gang scenario.

**Non-Goals For This Slice:**

- Do not actually evict pods.
- Do not bind migrated pods.
- Do not require checkpoint integration yet.
- Do not solve all running pods; start with bounded candidate selection.

## Metrics To Track

Every phase should add or preserve these metrics where relevant:

- `solve_core_millis`
- candidate edges
- candidate node limit
- admission count
- admitted GPU demand
- unplaced high-priority jobs
- stranded GPUs on active nodes
- active GPU nodes
- estimated hourly cost
- migration/preemption count
- disruption score
- predicted deadline misses
- fairness share by namespace/team
- objective profile and weights

These metrics should appear in traces first, then the dashboard. If a metric is not in the trace, the UI cannot be trusted as an audit surface.

## Validation Strategy

Use three layers of validation:

1. **Unit tests** for extraction, model propagation, objective scoring, and trace fields.
2. **Synthetic scenario tests** for deterministic before/after outcomes.
3. **Simulator comparison** for kube-scheduler baseline placement where applicable.

For each new feature, add at least one scenario that demonstrates value and one scenario that proves the feature is inert when disabled.

## Product Demo Script

The eventual sales/SRE demo should be structured as follows:

1. Start with kube-scheduler placement: jobs are valid but inefficient or fragmented.
2. Show ksolver shadow placement: better admission, lower fragmentation, or lower cost.
3. Change objective/profile controls live.
4. Show a high-priority job arriving.
5. Show ksolver explaining what must move or wait.
6. Show deadline-aware right-sizing for flexible jobs.
7. Show cost and utilization deltas.
8. End with a dry-run action plan, not a black-box recommendation.

The demo should avoid claiming "always optimal." The stronger claim is: "better decisions inside a bounded scheduler latency budget, with explicit constraints and measurable regret."

## Explicit Non-Goals For The Next Two Phases

- No destructive eviction by default.
- No full ML predictor before historical data exists.
- No full kube-scheduler score parity.
- No full DRA device matching before scalar DRA is product-proven.
- No hard dependency on live GPUs for the demo.
- No default cluster-wide scheduler takeover; target selected GPU workloads first.

## Completion Definition For This Roadmap

This roadmap is complete when:

- The ranked phases cover product, solver, UI, safety, and scale gaps.
- Each phase has build items and acceptance criteria.
- The document identifies dependencies and near-term execution order.
- The first two implementation slices are concrete enough for a coding agent to start without asking for product direction.
- The validation and metrics sections define how to prove each phase works.
