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
- **B. Two-phase lexicographic (robust, recommended).** Phase 1: solve for max admission + min cost
  (today's objective). Phase 2: fix the admitted set + node-open decisions (or the achieved cost as a
  constraint `Σcost ≤ cost*`) and re-solve maximizing soft score. Clean separation ⇒ provably never
  disturbs admission/cost. Costs a second solve (bounded, cheap for the pending model). This also
  lays groundwork for lexicographic objective handling generally.

Recommendation: **B (two-phase)**, gated to the shadow path only.

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
  small pending model.
- **Preferred pod affinity co-placement** — deferred (v1 running-pods-only); document as a caveat when
  a pod has preferred pod-affinity we only partially evaluate.
- **Conformance interaction** — soft affinity affects Score, not Filter; the Phase 2 harness tests
  Filter only, so it is unaffected. A future Score-conformance check is separate.

## Testing strategy
- Unit: `s(w,n)` computation for preferred node affinity / pod (anti-)affinity vs running pods.
- Solver (two-phase): among equal-cost nodes, the higher-soft-score node is chosen; and soft affinity
  NEVER changes which pods are admitted nor total cost (assert admission + cost invariant across
  soft-on/soft-off). Shadow-only; binds nothing.

## Out of scope
Changing the offline planner objective; co-placement preferred pod affinity; Score-phase conformance;
weighted multi-objective configurability beyond this single soft tier.
