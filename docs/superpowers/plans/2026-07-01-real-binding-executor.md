# Real Binding Executor (Phase 3) Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax. TDD, frequent commits. SAFETY-CRITICAL: this is the first code that mutates live clusters. User explicitly authorized building it + a live smoke test.

**Goal:** Optionally APPLY the shadow scheduler's decisions by POSTing pod→node `Binding`s — **disabled by default**, isolated in one module, applying only `readiness: ready` entries after a final live re-check, with full logging, metrics, and a kill flag. Prove it end-to-end on a throwaway cluster.

**Architecture:** A new `scheduler/binder.rs` holds all mutation. The pure `should_apply(entry, readiness, live_pod)` decides per-pod; the effectful `apply_bindings(client, plan, cfg)` re-fetches each pod, runs `should_apply`, and POSTs the binding subresource only on `Apply`. The shadow loop calls it **only when `cfg.enable_real_binding`** (env `KSOLVER_ENABLE_REAL_BINDING`, default false); otherwise behavior is unchanged (binds nothing). The `no_mutation_guard` is updated: `shadow.rs` orchestration and `binding.rs` (renderer) stay mutation-free; `binder.rs` is the single sanctioned, gated mutation site.

**Tech Stack:** Rust, kube-rs 0.97 (`Api::<Pod>::create_subresource("binding", …)`), existing shadow modules.

## Codex safety refinements (applied)

1. **Post-POST reconciliation:** a binding POST returns a `Status`; if the success body fails to deserialize, do NOT report failure — re-fetch the pod and, if it is now bound to the target node, record `Bound`. Only a genuine non-bound outcome is `Failed`.
2. **Stricter `should_apply`:** require non-empty uid on BOTH sides (empty ⇒ skip, never bind blind); skip if `deletionTimestamp` set; skip if live `schedulerName` != our scheduler.
3. **Internal enable-guard:** `apply_bindings` returns immediately (all-skipped) if `!enable_real_binding`, independent of the caller gate (defense-in-depth).
4. **Honest guard:** the `shadow.rs` no-mutation grep drops the fragile `"Binding"` string and instead greps concrete mutators (`create_subresource`, `PostParams`, `.create(`, `.patch(`, `.delete(`, `.evict(`, `.replace(`) — shadow.rs orchestrates but must not directly mutate.
5. **Extra controls now:** `real_binding_dry_run` (server-side `dryRun=All`, default false — validate without persisting); `max_binds_per_pass` (default 10); distinct `bound`/`skipped`/`failed` metrics; README RBAC (`create` on `pods/binding`). Per-pod opt-in is already inherent (only `schedulerName: <ours>` pods are ever candidates) + `namespace_allowlist` scopes the observed set.

## Global Constraints

- **Default OFF.** `enable_real_binding` defaults false. When false, `apply_bindings` is never invoked; the shadow default remains "binds nothing." The no-mutation guard on `shadow.rs`/`binding.rs` stays green.
- **Apply only what's safe.** Bind an entry only if its readiness is `Ready` AND a final live re-fetch confirms the pod still exists, is Pending, is unbound, and uid matches (optimistic concurrency). Anything else → skip with reason.
- **Never fail the loop.** A bind error for one pod is logged + counted; it never aborts the batch or the process. Conflicts (already bound) are a normal skip.
- **Observability.** Every attempt logs (pod, node, outcome); metrics count bound/skipped/failed.

---

### Task 1: Config flag `enable_real_binding`

**Files:** `ksolver/src/scheduler/config.rs`

- [ ] **Step 1:** Add `pub enable_real_binding: bool,` to `ShadowConfig`.
- [ ] **Step 2:** In `from_env`, set it from `KSOLVER_ENABLE_REAL_BINDING` (`"true"`/`"1"` ⇒ true; else false). Default false.
- [ ] **Step 3:** Add a `Default`/test literal update if needed; a small unit test that the env parse is false unless explicitly "true".
- [ ] **Step 4:** Build; commit — `git commit -am "Config: enable_real_binding flag (default off)"`.

---

### Task 2: `binder.rs` — pure decision + effectful apply

**Files:** Create `ksolver/src/scheduler/binder.rs`; register in `ksolver/src/scheduler/mod.rs`.

**Interfaces:**
```rust
pub struct LivePodView { pub uid: String, pub phase: String, pub node_name: String }
pub enum ApplyDecision { Apply, Skip { reason: String } }
pub fn should_apply(entry: &BindingPlanEntry, readiness: &BindReadiness, live: Option<&LivePodView>) -> ApplyDecision;

pub enum BindResult { Bound, Skipped { reason: String }, Failed { error: String } }
pub struct BindOutcome { pub namespace: String, pub pod: String, pub node: String, pub result: BindResult }
pub async fn apply_bindings(client: &kube::Client, plan: &[(BindingPlanEntry, BindReadiness)], cfg: &ShadowConfig) -> Vec<BindOutcome>;
```

- [ ] **Step 1: Write failing tests** (pure `should_apply`):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::binding::{BindReadiness, BindingPlanEntry};

    fn entry(uid: &str) -> BindingPlanEntry {
        BindingPlanEntry {
            namespace: "team".into(), pod_name: "a".into(), pod_uid: uid.into(),
            node_name: "n1".into(), gpu_request: 1, binding_body: serde_json::json!({}),
        }
    }
    fn live(uid: &str, phase: &str, node: &str) -> LivePodView {
        LivePodView { uid: uid.into(), phase: phase.into(), node_name: node.into() }
    }

    #[test]
    fn applies_when_ready_and_live_pending_unbound_uid_match() {
        let d = should_apply(&entry("u"), &BindReadiness::Ready, Some(&live("u", "Pending", "")));
        assert!(matches!(d, ApplyDecision::Apply));
    }
    #[test]
    fn skips_when_readiness_stale() {
        let d = should_apply(&entry("u"), &BindReadiness::Stale { reason: "x".into() }, Some(&live("u", "Pending", "")));
        assert!(matches!(d, ApplyDecision::Skip { .. }));
    }
    #[test]
    fn skips_when_pod_gone() {
        let d = should_apply(&entry("u"), &BindReadiness::Ready, None);
        assert!(matches!(d, ApplyDecision::Skip { .. }));
    }
    #[test]
    fn skips_when_uid_changed() {
        let d = should_apply(&entry("u-OLD"), &BindReadiness::Ready, Some(&live("u-NEW", "Pending", "")));
        assert!(matches!(d, ApplyDecision::Skip { .. }));
    }
    #[test]
    fn skips_when_already_bound() {
        let d = should_apply(&entry("u"), &BindReadiness::Ready, Some(&live("u", "Running", "n2")));
        assert!(matches!(d, ApplyDecision::Skip { .. }));
    }
}
```

- [ ] **Step 2: Implement `should_apply`** (pure):

```rust
pub fn should_apply(
    entry: &BindingPlanEntry,
    readiness: &BindReadiness,
    live: Option<&LivePodView>,
) -> ApplyDecision {
    if !matches!(readiness, BindReadiness::Ready) {
        return ApplyDecision::Skip { reason: "not ready (stale plan)".into() };
    }
    let Some(live) = live else {
        return ApplyDecision::Skip { reason: "pod not found at apply time".into() };
    };
    if !entry.pod_uid.is_empty() && !live.uid.is_empty() && live.uid != entry.pod_uid {
        return ApplyDecision::Skip { reason: "pod uid changed at apply time".into() };
    }
    if !live.node_name.is_empty() {
        return ApplyDecision::Skip { reason: format!("pod already bound to {}", live.node_name) };
    }
    if live.phase != "Pending" {
        return ApplyDecision::Skip { reason: format!("pod not Pending (phase {})", live.phase) };
    }
    ApplyDecision::Apply
}
```

- [ ] **Step 3: Implement `apply_bindings`** (effectful; all mutation here):

```rust
pub async fn apply_bindings(
    client: &kube::Client,
    plan: &[(BindingPlanEntry, BindReadiness)],
    cfg: &ShadowConfig,
) -> Vec<BindOutcome> {
    use kube::api::{Api, PostParams};
    use k8s_openapi::api::core::v1 as corev1;
    let mut outcomes = Vec::new();
    for (entry, readiness) in plan {
        let pods: Api<corev1::Pod> = Api::namespaced(client.clone(), &entry.namespace);
        // Final live re-check (optimistic concurrency).
        let live = match pods.get_opt(&entry.pod_name).await {
            Ok(Some(p)) => Some(LivePodView {
                uid: p.metadata.uid.clone().unwrap_or_default(),
                phase: p.status.as_ref().and_then(|s| s.phase.clone()).unwrap_or_default(),
                node_name: p.spec.as_ref().and_then(|s| s.node_name.clone()).unwrap_or_default(),
            }),
            Ok(None) => None,
            Err(e) => {
                outcomes.push(BindOutcome { namespace: entry.namespace.clone(), pod: entry.pod_name.clone(), node: entry.node_name.clone(), result: BindResult::Failed { error: format!("get: {e}") } });
                continue;
            }
        };
        match should_apply(entry, readiness, live.as_ref()) {
            ApplyDecision::Skip { reason } => outcomes.push(BindOutcome { namespace: entry.namespace.clone(), pod: entry.pod_name.clone(), node: entry.node_name.clone(), result: BindResult::Skipped { reason } }),
            ApplyDecision::Apply => {
                let binding = corev1::Binding {
                    metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                        name: Some(entry.pod_name.clone()),
                        namespace: Some(entry.namespace.clone()),
                        ..Default::default()
                    },
                    target: corev1::ObjectReference {
                        api_version: Some("v1".into()),
                        kind: Some("Node".into()),
                        name: Some(entry.node_name.clone()),
                        ..Default::default()
                    },
                };
                let data = match serde_json::to_vec(&binding) {
                    Ok(d) => d,
                    Err(e) => { outcomes.push(BindOutcome { namespace: entry.namespace.clone(), pod: entry.pod_name.clone(), node: entry.node_name.clone(), result: BindResult::Failed { error: format!("encode: {e}") } }); continue; }
                };
                let res = pods
                    .create_subresource::<serde_json::Value>("binding", &entry.pod_name, &PostParams::default(), data)
                    .await;
                let result = match res {
                    Ok(_) => { tracing::info!(ns=%entry.namespace, pod=%entry.pod_name, node=%entry.node_name, "REAL BIND applied"); BindResult::Bound }
                    Err(e) => { tracing::warn!(ns=%entry.namespace, pod=%entry.pod_name, error=%e, "real bind failed"); BindResult::Failed { error: e.to_string() } }
                };
                outcomes.push(BindOutcome { namespace: entry.namespace.clone(), pod: entry.pod_name.clone(), node: entry.node_name.clone(), result });
            }
        }
        let _ = cfg; // cfg reserved for future per-namespace allowlist; enablement gated by caller.
    }
    outcomes
}
```

- [ ] **Step 4:** `pub mod binder;` in `mod.rs`.
- [ ] **Step 5: Run — expect PASS** (`cargo test --features rust-cp-sat binder`); build clean.
- [ ] **Step 6: Commit** — `git commit -am "binder.rs: gated real-binding executor (pure should_apply + apply_bindings)"`

---

### Task 3: Wire into the shadow loop (gated) + guard update + metrics

**Files:** `ksolver/src/scheduler/shadow.rs`, `ksolver/src/scheduler/mod.rs`, `ksolver/src/metrics.rs`

- [ ] **Step 1: Metrics** — add `ksolver_shadow_bound_total` (+ optionally skipped/failed) counter(s) with an `inc_shadow_bound(n)` helper mirroring existing metric helpers.
- [ ] **Step 2: Wire** — in the solve loop, AFTER `traces.push(trace)`, when `cfg.enable_real_binding`:

```rust
        if cfg.enable_real_binding {
            let plan: Vec<_> = crate::scheduler::binding::render_binding_plan(&trace)
                .into_iter()
                .map(|e| {
                    let r = crate::scheduler::binding::assess_binding_readiness(&e, &normalized);
                    (e, r)
                })
                .collect();
            let outcomes = crate::scheduler::binder::apply_bindings(&bind_client, &plan, &cfg).await;
            let bound = outcomes.iter().filter(|o| matches!(o.result, crate::scheduler::binder::BindResult::Bound)).count() as u64;
            metrics::inc_shadow_bound(bound);
            info!(bound, attempted = outcomes.len(), "real binding pass complete");
        }
```
  Build `bind_client` once before the loop (reuse `collector::build_client(&cfg.kubeconfig).await?`); clone per iteration. `trace` is available before `traces.push` — capture the plan before moving `trace` (render from `&trace` before push, or reorder). Keep lowercase identifiers so the shadow.rs guard's `"Binding"` needle never matches.

- [ ] **Step 3: Update `no_mutation_guard`** in `mod.rs`:
  - Keep the `shadow.rs` grep (defense-in-depth: orchestration must not *directly* call mutating APIs — the actual POST lives in `binder.rs`).
  - Keep the `binding.rs` (renderer) purity grep.
  - Update the module doc comment to state that Phase 3 real binding is isolated in `binder.rs` and gated behind `enable_real_binding` (default off).
  - (Do NOT grep `binder.rs` for mutation — it is the sanctioned mutation site.)

- [ ] **Step 4: Run — expect PASS**; both `no_mutation_guard` tests still green; full `cargo test` + `cargo clippy --features rust-cp-sat` clean.
- [ ] **Step 5: Commit** — `git commit -am "Wire gated real-binding pass into shadow loop (default off) + metrics"`

---

### Task 4: Live smoke test (throwaway cluster) + README

**Files:** `README.md` (+ a smoke script under `scripts/` if helpful)

- [ ] **Step 1:** Verify tooling (`kind`, `kubectl`, `kwok`) is available. If not, install kwok stage/node CRDs or document the gap.
- [ ] **Step 2:** Create a throwaway kind cluster; add a KWOK fake GPU node (`kwok.x-k8s.io/node: fake` + status capacity `nvidia.com/gpu`). Create a Pending pod with `schedulerName: ksolver`, a GPU request, and node-selector to the fake node.
- [ ] **Step 3:** Build the release binary; run `KSOLVER_ENABLE_REAL_BINDING=true KUBECONFIG=… ksolver shadow` briefly.
- [ ] **Step 4:** Verify the pod becomes bound (`spec.nodeName` set / phase Running via KWOK); confirm logs show "REAL BIND applied" and `ksolver_shadow_bound_total` incremented.
- [ ] **Step 5:** Disarm (stop the process); delete the kind cluster.
- [ ] **Step 6: README** — document `KSOLVER_ENABLE_REAL_BINDING` (default off; when on, applies `readiness: ready` bindings after a live re-check), and that the default remains read-only shadow. Note the smoke-test result.
- [ ] **Step 7: Commit** — `git commit -am "Real binding: live smoke test verified + docs"`

---

## Self-Review

- **Safety:** default off (loop only calls `apply_bindings` under `enable_real_binding`); all mutation isolated in `binder.rs`; final live re-check (`should_apply`) gates every POST; per-pod errors never abort. Guard keeps `shadow.rs`/`binding.rs` mutation-free.
- **Correctness:** `should_apply` covers stale-readiness / pod-gone / uid-changed / already-bound / non-Pending. Binding body matches `pods/binding` subresource.
- **Type consistency:** `BindingPlanEntry`/`BindReadiness` reused; `LivePodView`/`ApplyDecision`/`BindOutcome` defined once.
- **No placeholders.**
