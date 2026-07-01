# Soft / Preferred Affinity Scoring — Design Spec

**Status:** Draft for review (design pass; no implementation). Advances the deferred "soft/preferred
affinity" roadmap item.
**Author:** autonomous design pass (2026-07-01), pending user review.
**Related:** `docs/superpowers/specs/2026-06-30-gpu-scheduler-design.md` (§13 status), the anti-affinity
phases (5e–5h, 12, matchExpressions), node-affinity fixes.

## Problem

Today the scheduler models only **required** (`requiredDuringSchedulingIgnoredDuringExecution`)
affinity/anti-affinity (hard constraints) and ignores **preferred**
(`preferredDuringSchedulingIgnoredDuringExecution`) terms entirely. Ignoring preferred terms is
*safe* (they are best-effort by definition, so ignoring them never produces an infeasible/wrong
placement) but *lossy*: kube-scheduler's Score phase honors them, so our placements can differ from
the real scheduler's on nodes that are otherwise tied — hurting conformance fidelity and the "account
for everything" story (e.g. a soft spread preference we don't reflect).

## What preferred terms are

Each `preferred…` entry has a `weight` (1–100) and a term (pod affinity/anti-affinity term, or a
node-affinity `preference`). kube-scheduler sums satisfied weights into the node's score (Score phase,
run only over nodes that passed Filter). Higher score wins; Filter feasibility is unaffected.

## The core design question: where does soft-affinity sit in OUR objective?

ksolver's objective is not kube's Score — it is **cost minimization + a dominating admission reward**
(`cpsat_rust::solve`: `minimize( Σ node_price·y − admission_weight·Σ placed + slack/rightsizing )`,
where `admission_weight` is auto-computed to dominate all cost terms). Soft affinity must slot into
this lexicographic-ish stack **without disturbing the two invariants that matter**:

1. **Never change admission.** Soft affinity must never cause a pod to be dropped that would otherwise
   be admitted (or vice versa). ⇒ its total possible magnitude must be **strictly less than the
   admission weight's per-pod increment**.
2. **Never change feasibility.** It only nudges *which* feasible node — never enables/forbids one.

The remaining choice is soft-affinity **vs cost**:
- **Option T1 — tie-breaker below cost (recommended).** Cost-optimal first; among equally-cheap
  placements, prefer higher soft-affinity score. Weight tier: `cost ≫ soft-affinity`. Preserves the
  "$$$ savings" north-star exactly (never spends more money to satisfy a soft preference).
- **Option T2 — above cost.** Would let soft preferences override cost — contradicts the cost-first
  mission and risks recommending pricier fleets to satisfy a soft rule. **Reject** for v1.

So the weight stack (largest→smallest magnitude): **admission ≫ cost ≫ soft-affinity ≫ 0**. Concretely
the soft-affinity term budget must be < the smallest nonzero cost delta, or modeled as a separate
lexicographic pass (see Implementation options).

## Modeling soft terms in CP-SAT

For each pending workload `w` and feasible node `n`, precompute a **soft score** `s(w,n)` = Σ of
preferred-term weights satisfied if a replica of `w` lands on `n`:
- preferred **node affinity**: `+weight` if node `n`'s labels satisfy the preference selector.
- preferred **pod affinity**: `+weight` if a matching pod (running, or — harder — co-placed) is on
  `n` / in `n`'s topology domain. (v1: evaluate against RUNNING pods only, like the required
  anti-affinity best-effort; co-placement interactions deferred.)
- preferred **pod anti-affinity**: `−weight` (penalty) if a matching pod is on `n` / its domain.

Then add `− soft_scale · Σ_{w,n} s(w,n)·x[w,n]` to the objective (reward higher score). `soft_scale`
chosen per the weight stack so total soft magnitude < min cost delta (T1). Everything is linear in the
existing `x` vars ⇒ no new variable classes; just objective terms.

## Implementation options

- **A. Single weighted objective (simplest).** Add the soft term with a small `soft_scale`. Risk:
  choosing `soft_scale` so it never perturbs cost/admission across arbitrary price magnitudes is
  fragile (same class of numeric issue as the admission weight). Needs an overflow-guarded computed
  scale, like `effective_admission_weight`.
- **B. Two-phase lexicographic (robust, recommended).** Phase 1: solve today's objective (max
  admission + min cost). Phase 2: **maximize soft score** subject to preserving Phase 1's result —
  with three correctness rules (codex):
  1. **Do NOT fix the node-open `y` vars.** `y` is precisely what distinguishes equal-cost alternative
     nodes; fixing it would kill the intended tie-break. Only **fix the admitted set** (the `placed`
     bools / per-workload admitted count) so admission is preserved, and leave `x`/`y` free.
  2. **Constrain the FULL Phase-1 objective value, not just `Σcost`.** Phase 1's objective today
     includes node price, active-node weight, CPU/mem/scalar slack, churn, and (if enabled)
     rightsizing terms. Phase 2 must add `phase1_objective_expr ≤ phase1_objective_value*` (the full
     non-soft objective), otherwise maximizing soft could perturb another existing tie-breaker/term.
     Then maximize `Σ s(w,n)·x[w,n]` alone.
  3. **Only apply soft scoring when Phase 1 is proven OPTIMAL.** Shadow uses a bounded solve and may
     accept a merely *feasible* incumbent; two-phase over a non-proven-optimal Phase 1 would pin the
     incumbent's objective value (not the true cost-first optimum), so soft scoring must be **skipped**
     (fall back to the Phase-1 placement) unless `solver_status == Optimal`. Record in the trace that
     soft tie-breaking was skipped due to the time budget.

  Clean separation ⇒ provably never disturbs admission/cost. Costs a second (bounded) solve.

Recommendation: **B (two-phase)**, gated to the shadow path only, applied only on proven-optimal Phase 1.

## Scope / gating

- **Shadow-only.** Gate behind a `ScenarioConfig` flag (e.g. `enable_soft_affinity`), set by shadow,
  never by the offline planner ⇒ the verified planner objective is untouched (zero regression risk).
- **v1 evaluates preferred terms against running pods + node labels** (best-effort, mirrors required
  anti-affinity). Co-placement-dependent preferred pod affinity (two pending pods preferring each
  other) is deferred.
- Reuse the existing selector machinery (`LabelSelectorReq`, `is_gpu_resource`-style matchers,
  topology-domain logic) for term evaluation.

## Risks / open questions

- **Numeric tiering (Option A)** — fragile; mitigated by choosing Option B (lexicographic).
- **Second-solve latency (Option B)** — bounded; measure via the bench harness; acceptable for the
  small pending model. Soft scoring is skipped when Phase 1 isn't proven optimal (codex), so a slow
  Phase 1 never triggers a wasted Phase 2.
- **Phase-2 must preserve the full Phase-1 objective, not just cost, and must not fix `y`** (codex) —
  otherwise it could break equal-cost node tie-breaks or perturb other objective terms.
- **Preferred pod affinity co-placement** — deferred (v1 running-pods-only); document as a caveat when
  a pod has preferred pod-affinity we only partially evaluate.
- **Conformance interaction** — soft affinity affects Score, not Filter; the Phase 2 harness tests
  Filter only, so it is unaffected. A future Score-conformance check is separate.

## Testing strategy
- Unit: `s(w,n)` computation for preferred node affinity / pod (anti-)affinity vs running pods.
- Solver (two-phase): among equal-cost nodes, the higher-soft-score node is chosen; soft affinity
  NEVER changes which pods are admitted nor the full Phase-1 objective value (assert admission + full
  objective invariant across soft-on/soft-off); and soft scoring is skipped (Phase-1 placement kept)
  when Phase 1 status is not Optimal. Shadow-only; binds nothing.

## Out of scope
Changing the offline planner objective; co-placement preferred pod affinity; Score-phase conformance;
weighted multi-objective configurability beyond this single soft tier.
