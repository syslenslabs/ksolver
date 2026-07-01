# Dry-Run Binding Plan Renderer Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax. TDD, frequent commits.

**Goal:** Make the shadow scheduler's decisions *actionable-but-inert*: render the exact Kubernetes `Binding` subresource payloads that a real binder WOULD POST for each placed pod, expose them read-only at `/api/scheduler/binding-plan`, and log a per-solve summary — while **binding nothing**. This is the safe groundwork for Phase 3 (real binding), which stays gated on explicit user authorization.

**Architecture:** A new pure module `scheduler/binding.rs` maps a `DecisionTrace` to `Vec<BindingPlanEntry>`; each entry carries the canonical bindings-subresource JSON body (`apiVersion: v1`, `kind: Binding`, `metadata`, `target: Node`). It imports **no** kube client and issues **no** API calls — it only builds data. `shadow.rs` adds a read-only endpoint that renders the plan from the latest stored trace and a summary log line. The existing `no_mutation_guard` is strengthened to also assert `binding.rs` never calls a mutating/kube-client API.

**Tech Stack:** Rust, axum, serde_json, existing shadow scheduler modules.

## Global Constraints

- **Binds nothing.** `binding.rs` renders payloads only; no `.create/.replace/.patch/.delete/.evict`, no kube `Client`/`Api`. Enforced by an extended `no_mutation_guard` test.
- `shadow.rs` must stay clear of the existing guard's forbidden substrings: the capitalized `"Binding"`, `.evict(`, `.create(`, `.replace(`, `.patch(`, `.delete(`. Use lowercase identifiers (`binding_plan_handler`, `render_binding_plan`, route `"/api/scheduler/binding-plan"`) so no capital-B `Binding` appears in shadow.rs.
- Real binding (actually POSTing) is explicitly OUT OF SCOPE and remains gated on user authorization. This renderer changes no cluster state.

---

### Task 1: Pure `binding.rs` renderer + strengthened guard

**Files:**
- Create: `ksolver/src/scheduler/binding.rs`
- Modify: `ksolver/src/scheduler/mod.rs` (`pub mod binding;` + extend `no_mutation_guard`)

**Interfaces:**
- Produces:
  ```rust
  pub struct BindingPlanEntry {
      pub namespace: String,
      pub pod_name: String,
      pub pod_uid: String,
      pub node_name: String,
      pub binding_body: serde_json::Value, // exact bindings-subresource POST body (dry-run)
  }
  pub fn render_binding_plan(trace: &crate::scheduler::trace::DecisionTrace) -> Vec<BindingPlanEntry>;
  ```

- [ ] **Step 1: Write failing tests** (`binding.rs` `#[cfg(test)]`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::trace::{DecisionTrace, PodDecision, PodPlacement};

    fn trace_with(decisions: Vec<PodDecision>) -> DecisionTrace {
        DecisionTrace {
            sequence: 1,
            observed_pods: decisions.len(),
            decisions,
            solver_status: "OPTIMAL".into(),
            solve_millis: 1,
            solve_core_millis: 1,
            snapshot_age_millis: 0,
            note: String::new(),
        }
    }

    fn placed(ns: &str, name: &str, uid: &str, node: &str) -> PodDecision {
        PodDecision {
            uid: uid.into(),
            namespace: ns.into(),
            name: name.into(),
            gpu_request: 1,
            placement: PodPlacement::Placed { node: node.into() },
            caveats: vec![],
        }
    }

    #[test]
    fn renders_binding_for_each_placed_pod_only() {
        let t = trace_with(vec![
            placed("team", "a", "uid-a", "node-1"),
            PodDecision {
                uid: "uid-b".into(),
                namespace: "team".into(),
                name: "b".into(),
                gpu_request: 1,
                placement: PodPlacement::Unplaced { reason: "no feasible node".into() },
                caveats: vec![],
            },
        ]);
        let plan = render_binding_plan(&t);
        assert_eq!(plan.len(), 1, "only placed pods yield bindings");
        let e = &plan[0];
        assert_eq!(e.namespace, "team");
        assert_eq!(e.pod_name, "a");
        assert_eq!(e.node_name, "node-1");
        assert_eq!(e.binding_body["kind"], "Binding");
        assert_eq!(e.binding_body["apiVersion"], "v1");
        assert_eq!(e.binding_body["metadata"]["name"], "a");
        assert_eq!(e.binding_body["metadata"]["namespace"], "team");
        assert_eq!(e.binding_body["target"]["kind"], "Node");
        assert_eq!(e.binding_body["target"]["name"], "node-1");
    }

    #[test]
    fn empty_when_nothing_placed() {
        let t = trace_with(vec![PodDecision {
            uid: "uid-b".into(),
            namespace: "team".into(),
            name: "b".into(),
            gpu_request: 1,
            placement: PodPlacement::Unplaced { reason: "x".into() },
            caveats: vec![],
        }]);
        assert!(render_binding_plan(&t).is_empty());
    }
}
```

- [ ] **Step 2: Run — expect FAIL** (module/function absent)
- [ ] **Step 3: Implement `binding.rs`**

```rust
//! Dry-run binding plan: renders the exact Kubernetes `Binding` subresource payloads that a real
//! binder WOULD POST for each placed pod. This module is PURE — it builds data only and never
//! contacts the API server (enforced by `no_mutation_guard`). Shadow mode stays read-only.

use crate::scheduler::trace::{DecisionTrace, PodPlacement};
use serde::{Deserialize, Serialize};

/// One rendered (but never sent) pod→node binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BindingPlanEntry {
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub node_name: String,
    /// The canonical `pods/binding` subresource POST body a real binder would send (dry-run only).
    pub binding_body: serde_json::Value,
}

/// Render the pod→node bindings implied by a decision trace. Only `Placed` decisions produce an
/// entry; unplaced pods are skipped. No side effects, no API calls.
pub fn render_binding_plan(trace: &DecisionTrace) -> Vec<BindingPlanEntry> {
    trace
        .decisions
        .iter()
        .filter_map(|d| match &d.placement {
            PodPlacement::Placed { node } => Some(BindingPlanEntry {
                namespace: d.namespace.clone(),
                pod_name: d.name.clone(),
                pod_uid: d.uid.clone(),
                node_name: node.clone(),
                binding_body: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Binding",
                    "metadata": { "name": d.name, "namespace": d.namespace },
                    "target": { "apiVersion": "v1", "kind": "Node", "name": node },
                }),
            }),
            PodPlacement::Unplaced { .. } => None,
        })
        .collect()
}
```

- [ ] **Step 4: Register + strengthen guard** in `mod.rs`:
  - add `pub mod binding;`
  - add to `no_mutation_guard`:
```rust
    const BINDING: &str = include_str!("binding.rs");

    #[test]
    fn binding_renderer_never_mutates_or_calls_api() {
        // The renderer builds payloads only; it must never POST them or touch a kube client.
        // Unambiguous API-call / kube-path signatures (avoid prose-collidable needles like
        // "Client"/"Api<"; keep comments free of "kube::").
        for needle in [
            ".evict(", ".create(", ".replace(", ".patch(", ".delete(", ".request(",
            "PostParams", "DeleteParams", "PatchParams", "EvictParams", "kube::",
        ] {
            assert!(
                !BINDING.contains(needle),
                "binding.rs must render only, never call `{needle}`"
            );
        }
    }
```

- [ ] **Step 5: Run — expect PASS** (`cargo test --features rust-cp-sat binding`); build clean.
- [ ] **Step 6: Commit** — `git commit -am "Dry-run binding plan renderer (pure; binds nothing) + strengthened no-mutation guard"`

---

### Task 2: Read-only `/api/scheduler/binding-plan` endpoint + summary log

**Files:**
- Modify: `ksolver/src/scheduler/shadow.rs`

**Interfaces:**
- Consumes `render_binding_plan` + `TraceStore::recent()`.

- [ ] **Step 1: Implement** — add a handler and route (lowercase identifiers only, so the guard's `"Binding"` needle never matches):

```rust
async fn binding_plan_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    // Render the pod→node bindings implied by the latest trace. DRY-RUN: never applied.
    let latest = s.traces.recent().into_iter().next();
    let (seq, solve_millis) = latest
        .as_ref()
        .map(|t| (t.sequence, t.solve_millis))
        .unwrap_or((0, 0));
    let plan = latest
        .map(|t| crate::scheduler::binding::render_binding_plan(&t))
        .unwrap_or_default();
    Json(serde_json::json!({
        "dry_run": true,
        "note": "rendered from the latest shadow trace; never applied — may be stale",
        "trace_sequence": seq,
        "solve_millis": solve_millis,
        "bindings": plan,
    }))
}
```
  Register: `.route("/api/scheduler/binding-plan", get(binding_plan_handler))`.

- [ ] **Step 2: Per-solve summary log** — where each trace is pushed in the solve loop, add:
```rust
        let would_bind = crate::scheduler::binding::render_binding_plan(&trace).len();
        tracing::info!(would_bind, "dry-run: rendered binding plan (nothing applied)");
```
  (Use whatever logging facade the file already uses; if it uses `println!`/`eprintln!`, match that. Keep the string lowercase — no capital `Binding`.)

- [ ] **Step 3: Run — expect PASS**, and CRITICALLY the `no_mutation_guard::shadow_has_no_binding_or_mutation_calls` test must still pass (verify shadow.rs has no capital `Binding` / mutation substrings). Full `cargo test` + `cargo clippy --features rust-cp-sat` clean.
- [ ] **Step 4: Add a shadow test** that the new route returns `dry_run: true` and a bindings array (mirror the existing dashboard/traces test style if present; else assert the handler builds the expected JSON given a seeded trace store).
- [ ] **Step 5: Commit** — `git commit -am "Serve read-only /api/scheduler/binding-plan + dry-run summary log"`

---

### Task 3: Docs

**Files:** `README.md`

- [ ] **Step 1: README** — under the shadow section, document `/api/scheduler/binding-plan`: it renders the exact `Binding` payloads that would be applied, as a dry run; shadow still issues only read/watch/list and binds nothing. Real binding remains a separate, authorization-gated phase.
- [ ] **Step 2: Full verify** — `cargo test --features rust-cp-sat` + `cargo clippy --features rust-cp-sat` clean, including both `no_mutation_guard` tests.
- [ ] **Step 3: Commit** — `git commit -am "Docs: dry-run binding plan endpoint"`

---

## Self-Review

- **Spec coverage:** pure renderer + guard (T1) → read-only endpoint + log (T2) → docs (T3).
- **Safety:** `binding.rs` is pure (guard test forbids mutation/kube-client substrings); `shadow.rs` stays free of the capitalized `Binding` and mutation-call substrings (existing guard). No cluster state changes anywhere. Real binding remains gated.
- **Type consistency:** `BindingPlanEntry`/`render_binding_plan` used identically in binding.rs, shadow.rs, tests.
- **No placeholders.**
