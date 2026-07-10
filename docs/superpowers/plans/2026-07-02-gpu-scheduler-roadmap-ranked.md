# GPU Scheduler Roadmap - Ranked by Differentiator Strength

> **For agentic workers:** This is a ranked product/engineering roadmap, not a single implementation ticket. When executing, split each phase into a focused implementation plan with tests, KSS proof criteria, demo criteria, and rollback notes. UI implementation must be delegated to Claude; Codex can do backend, tests, docs, and proof harness work.

## Status update (2026-07-09)

Delivered since the roadmap was written:

- **Safer-than-kube, proven live.** Live kube-scheduler-simulator comparison now detects the
  liabilities kube incurs when it "admits" more — CUDA-OOM risk and split/partial co-located
  gangs — and surfaces them as a passing **"kube safety advantage" proof gate** + a green
  live-trace callout (dashboard). This reframes ksolver's lower raw admission as the *correct*,
  safer call. (Phase 4 evidence; addresses the "gang-only wins are table stakes" gap.)
- **Repair explainability.** Unrepairable targets now name the blocking pod/policy instead of a
  bare "no plan" (Phase 1 hardening).
- **VRAM estimator is real + promoted.** Collected real RTX-4090 samples incl. **real-framework
  (torchvision) probes** (verified_real_framework 0 → 5); refit + promoted the model on 276 rows.
  (Phase 2 data foundation.)
- **The VRAM → DRA wedge (new, cross-cutting Phase 2/5).** Predict peak VRAM via a confidence
  cascade (explicit → historical-fingerprint → config → static-sniff → advisory) and inject it as
  a DRA consumable-capacity claim + node feasibility, wired into ksolver's real mutating webhook
  (fail-open). Full design + how-to: `docs/superpowers/specs/2026-07-08-vram-dra-wedge-design.md`
  and `vram-model-lab/WEDGE.md`. All four cascade tiers have live paths; hardened against
  extrapolation (advisory guard), dual VRAM labels (ksolver GiB / NVIDIA GFD MiB), and
  Rust↔Python fingerprint parity.

**Frontier (infra-gated, not code):** in-cluster `MutatingWebhookConfiguration` + TLS; a
VRAM-metrics exporter (DCGM) to auto-populate the tier-4 store; a GPU DRA driver for the full
allocation loop; cross-SKU + true-OOM data. A gang-aware/Volcano baseline still needed before
external competitive claims.

## Goal

Turn ksolver from a kube-scheduler comparison demo into an SRE-ready GPU scheduling product that can explain and safely act on scarce GPU placement problems.

The differentiator is **not gang scheduling**. Volcano, Kueue, and YuniKorn already cover gang and queue semantics. ksolver must differentiate on:

- dry-run repair and defragmentation plans for already-running GPU workloads;
- VRAM/runtime prediction that prevents impossible or wasteful placements;
- whole-queue objective tradeoffs with visible regret and provenance;
- observe-only evidence bundles that SREs can trust before mutation;
- explicit comparison against kube-scheduler-simulator and, where possible, gang-aware/Volcano-like baselines.

## Proof Standard

Every roadmap phase must ship proof, not only implementation.

- **KSS baseline required:** Any scenario that claims "better than kube" must run through kube-scheduler-simulator for spread and binpack variants. Deterministic fallback is disabled; a missing live/cached KSS baseline must fail closed.
- **Best baseline wins:** Compare ksolver against the strongest kube baseline by useful GPU/cost, not the easiest baseline.
- **Gang-only claims are table stakes:** A win caused only by avoiding partial gangs should be labeled "beats default kube, not proven versus Volcano."
- **Repair proof split:** KSS can prove the baseline scheduler leaves a target pending or fragmented. ksolver must separately prove the dry-run migrate/preempt plan, disruption cost, safety skips, and resulting capacity.
- **VRAM proof split:** KSS can prove scalar kube scheduling ignores predicted VRAM. ksolver must separately prove VRAM feasibility filters, confidence source, and "no repair will help" behavior for too-small devices.
- **Live provenance required:** Reports must expose live/cached/fallback simulator mode, timeout/failure reason, and cache key.

## Current Baseline

- Shadow scheduler observes pending GPU pods and solves against residual capacity.
- It already has partial admission, gang-aware objective weights, namespace quota, fair-share/budget signals, priority/queue/deadline weights, candidate widening, node grouping guardrails, repair advice, prediction audit fields, MIG/profile resource matching, topology label filters, and DRA scalar approximation.
- Dashboard/report data already includes production safety, ROI, repair, prediction, fairness, scale, and device-correctness summaries.
- The strongest current KSS-proven scenario wins are still mostly gang correctness wins. That is not enough against Volcano.
- The next work must prove differentiated value from repair/defragmentation first, then VRAM prediction.

## Phase 1 - Preemption and Migration Planner

**Rank:** 1  
**Differentiator strength:** Very high  
**Why it matters:** Existing schedulers mostly decide whether new pods can be admitted. Platform teams also need to know what to do when enough total GPU exists but the fleet is fragmented by already-running work. A safe dry-run repair plan is more differentiated than gang admission.

**Build:**

- Treat selected running GPU pods as repair candidates, not as ordinary pending work.
- Model candidate actions:
  - keep current placement;
  - migrate to another feasible node;
  - preempt/defer when migration is not safe or insufficient.
- Score disruption using:
  - explicit `ksolver.dev/disruption-cost`;
  - checkpoint age;
  - running age;
  - progress percent;
  - preemption penalty;
  - PDB and policy constraints.
- Preserve safety skips:
  - `safe-to-evict=false`;
  - `ksolver.dev/do-not-disrupt=true`;
  - exhausted/unmodeled PDB;
  - volume-pinned or non-migratable pods;
  - higher/equal priority protected work;
  - higher business value or more urgent deadline work.
- Emit repair plans separately from binding plans. Default remains read-only.

**KSS proof scenarios:**

- `repair-fragmented-4gpu-gang`: KSS spread/binpack leaves a 4-GPU gang pending or partially/uselessly placed because running/pending blockers fragment the only 4-GPU island; ksolver produces a dry-run migrate/preempt plan that frees the island.
- `repair-policy-blocked-no-action`: KSS leaves the target pending; ksolver proves no action is allowed because blockers are protected by PDB/policy/priority.
- `repair-not-enough-total-gpu`: KSS leaves the target pending; ksolver proves repair is impossible because total feasible GPU capacity is insufficient.

**Acceptance criteria:**

- Full KSS refresh for the proof scenarios reports `mode=live` for both spread and binpack, or the result is explicitly downgraded.
- Report shows baseline placement, target blocked state, proposed action rows, freed GPU, disruption cost, skipped candidates, and fail-closed reasons.
- Unit tests prove repair minimizes disruption versus greedy candidate order.
- No real eviction, migration, or binding happens by default.

## Phase 2 - VRAM Prediction and Feasibility

**Rank:** 2  
**Differentiator strength:** Very high  
**Why it matters:** GPU count alone is too crude. A scheduler that admits an 80 GiB workload onto a 24 GiB GPU has not helped. VRAM prediction can prevent impossible placements and avoid useless repair attempts.

**Build:**

- Preserve explicit VRAM annotations as authoritative:
  - `ksolver.dev/predicted-peak-vram-bytes`;
  - `ksolver.dev/predicted-peak-vram-gib`.
- Fill missing predictions from historical observations when confidence is high enough.
- Use training hints only as lower-confidence fallback:
  - model parameters;
  - batch size;
  - sequence length;
  - precision.
- Filter candidate nodes whose known per-GPU memory is below predicted peak VRAM.
- Keep nodes without memory labels eligible but score them as unknown, not proven safe.
- Block repair advice when the target is VRAM-incompatible; freeing slots on too-small GPUs must not produce a fake repair.

**KSS proof scenarios:**

- `vram-fit-mixed-fleet`: KSS may place or consider the job on any GPU-count-compatible node; ksolver restricts known-safe candidates to adequate VRAM nodes.
- `vram-blocked-no-repair`: KSS leaves the target pending or places blockers normally; ksolver proves the target is blocked by device memory, not fragmentation, and emits no disruptive repair plan.
- `vram-unknown-inventory-advisory`: ksolver keeps unknown-memory nodes advisory/neutral and refuses hard placement claims without inventory evidence.

**Acceptance criteria:**

- KSS live baseline proves kube does not use predicted VRAM in the scenario.
- ksolver proof lists adequate nodes, rejected too-small nodes, unknown nodes, prediction source, and confidence.
- Prediction-sensitive claims stay advisory until calibration samples meet the promotion gate.
- Repair metrics count VRAM-blocked targets separately from repairable fragmentation.

## Phase 3 - Whole-Queue Objective Tradeoffs

**Rank:** 3  
**Differentiator strength:** High  
**Why it matters:** Priority, queue wait, deadline, fair-share, business value, and cost are not differentiators individually. The differentiator is selecting the best subset of work under scarcity and explaining the tradeoff.

**Build:**

- Keep priority/queue/deadline/fair-share/business-value weights opt-in.
- Add scenarios that are not pure gang wins:
  - equal gangs with different deadlines;
  - flexible deadline job versus urgent fixed job;
  - under-share tenant versus over-share tenant;
  - high business value versus low business value with same shape.
- Expose which admitted job displaced or deferred which lower-value work.

**KSS proof scenarios:**

- KSS live spread/binpack baseline for every policy scenario.
- A local gang-aware baseline or real Volcano comparison should be added before external claims.

**Acceptance criteria:**

- The win remains after comparing to the best KSS baseline.
- The report classifies each win as `beats-kube-only`, `beats-gang-aware`, or `not-proven`.
- Weight-zero tests prove metadata is inert by default.

## Phase 4 - Shadow Evidence Bundle and Production Safety

**Rank:** 4  
**Differentiator strength:** High  
**Why it matters:** SREs may not replace a scheduler first. They may buy a trusted observe-only decision system that produces evidence, caveats, and safe action plans.

**Build:**

- Keep observe-only as the default.
- Preserve mutation gates, kill switch, leader election, binding reservation ledger, event drafts, and RBAC-minimal deployment.
- Generate repeatable evidence bundles:
  - latest trace;
  - KSS baseline provenance;
  - repair plan;
  - production safety state;
  - pricing basis;
  - prediction calibration;
  - scale/regret guardrails;
  - device inventory proof.

**Acceptance criteria:**

- A non-demo trace bundle can be captured with repeatable commands.
- Customer-facing claims stay blocked until live evidence is present.
- Mutation-capable paths are gated and test-covered.

## Phase 5 - Device Identity, MIG, DRA, and Topology

**Rank:** 5  
**Differentiator strength:** High potential, high risk  
**Why it matters:** Advanced GPU fleets care about MIG profiles, concrete DRA device allocation, NVLink/NVSwitch locality, NUMA islands, and time-sliced versus exclusive devices. This is differentiating only if modeled exactly.

**Build:**

- Keep exact extended-resource semantics for advertised MIG profiles.
- Keep topology label filters as hard feasibility where labels are explicit.
- Upgrade DRA from scalar approximation to concrete device identity assignment before claiming full DRA correctness.
- Model concrete topology graph before claiming NVLink-optimal placement.

**Acceptance criteria:**

- Device correctness summary separates exact, approximate, and unsupported claims.
- DRA scalar approximation never becomes a hard binding claim.
- NVLink/topology claims require concrete inventory proof.

## Phase 6 - Scale Without Suspicious Pruning

**Rank:** 6  
**Differentiator strength:** Medium  
**Why it matters:** A global optimizer must remain credible on large fleets. Speed is not enough; the user must know whether pruning changed the decision.

**Build:**

- Prefer homogeneous node grouping before candidate pruning.
- Expand grouped solutions back to physical nodes and validate.
- Widen candidates automatically when high-value work is unplaced or admission looks suspicious.
- Preserve full-rerun paths for high-risk actions.

**Acceptance criteria:**

- Reports label results as full, grouped exact, widened, measured-pruned, or unknown-regret.
- High-risk live binding/preemption is blocked when regret is unknown.

## Phase 7 - Fairness, Budgets, and Reclaimable Borrowing

**Rank:** 7  
**Differentiator strength:** Medium  
**Why it matters:** Volcano and batch schedulers already have fairness concepts. ksolver differentiates only if it explains tenant ownership, borrowed capacity, reclaimability, and budget-denial evidence.

**Build:**

- Validate tenant ownership sources against namespace/account metadata.
- Show borrowed and reclaimable GPU capacity.
- Explain budget denials and quota throttling.
- Keep hard enforcement gated until ownership data is proven.

**Acceptance criteria:**

- UI/report answers "why was this team denied?" and "who is borrowing capacity?"
- Missing ownership data causes advisory mode, not hard enforcement.

## Phase 8 - ROI and Scenario Library

**Rank:** 8  
**Differentiator strength:** Medium-high product wedge  
**Why it matters:** ROI sells the product, but it must be downstream of correct evidence. Cost claims without pricing provenance are dangerous.

**Build:**

- Keep ROI tiles adjacent to provenance and regret.
- Require pricing catalog, chargeback export, contract rate sheet, or invoice sample before customer dollar claims.
- Classify scenario wins by proof type:
  - live KSS;
  - cached KSS;
  - deterministic fallback;
  - gang-only;
  - repair-backed;
  - VRAM-backed;
  - prediction-sensitive.

**Acceptance criteria:**

- Dashboard can show deterministic demos, but customer claims require live/cached KSS provenance and customer pricing.
- Each scenario has a short "why this beats kube" and "what this does not prove" statement.

## Recommended Execution Order

1. Preemption/migration planner proof scenarios and report evidence.
2. VRAM prediction proof scenarios and repair-blocking evidence.
3. Whole-queue policy tradeoff scenarios with best-KSS comparison.
4. Shadow evidence bundle hardening and production-safety capture.
5. Device identity/topology correctness.
6. Scale/regret controls.
7. Fairness/budget ownership validation.
8. ROI dashboard and scenario-library polish.

## Near-Term Demo Target

The next compelling demo should be:

1. A high-value 4-GPU or 8-GPU training gang is blocked.
2. KSS live spread/binpack baselines show kube cannot admit the useful target under the current placement.
3. ksolver explains whether the blocker is fragmentation, policy, insufficient total GPU, or VRAM.
4. If fragmentation: ksolver proposes concrete migrate/preempt rows with disruption cost and skipped-candidate reasons.
5. If VRAM: ksolver refuses disruptive repair and explains the required GPU memory class.
6. The evidence bundle shows KSS provenance, repair/VRAM proof, safety gates, and ROI caveats.

This is stronger than "we do gang scheduling" because it answers: **what should the SRE do now, why is it safe, and what evidence proves it?**

## Non-Goals For The Next Two Phases

- No destructive eviction by default.
- No live binding requirement.
- No UI work by Codex; invoke Claude for UI changes.
- No claim that gang scheduling alone differentiates ksolver.
- No customer-facing dollar savings without pricing provenance.
- No hard VRAM placement claim without inventory and calibration evidence.
- No Volcano superiority claim until a gang-aware or real Volcano baseline exists.

## Validation Strategy

Use four layers of validation:

1. **Unit tests** for extraction, repair candidate policy, VRAM filtering, objective scoring, and trace fields.
2. **Synthetic proof tests** for deterministic repair and VRAM scenarios.
3. **KSS comparison** for kube spread/binpack baseline placement with live/cached/fallback provenance.
4. **Non-demo trace bundles** for customer-style evidence before external claims.

For every scenario, record:

- KSS mode for spread and binpack;
- best baseline selected and why;
- useful GPU delta;
- full/partial gang counts;
- active-node cost and stranded GPU;
- repair action rows or VRAM rejected-node rows where relevant;
- what the scenario does not prove.
