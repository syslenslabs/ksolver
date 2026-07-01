# Pre-Bind Readiness Check Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax. TDD, frequent commits.

**Goal:** Add the safety guard a responsible binder MUST run before ever applying a binding — detect when a rendered dry-run binding has gone *stale* (target node gone, or pod already scheduled elsewhere) — and surface it on the existing `/api/scheduler/binding-plan` endpoint and dashboard. Pure logic; **mutates nothing**. This is required Phase-3 groundwork so that, once real binding is authorized, no stale/conflicting binding is ever POSTed.

**Architecture:** A pure `assess_binding_readiness(entry, &NormalizedCluster) -> BindReadiness` in `binding.rs` checks a rendered binding against the *latest* cluster snapshot. The shadow loop stores its latest `NormalizedCluster` in the HTTP state (an `Arc<Mutex<Option<…>>>`); the binding-plan handler annotates each entry with its readiness. No kube client, no API calls — the `no_mutation_guard` on `binding.rs` and `shadow.rs` stays green.

**Tech Stack:** Rust, axum, existing shadow scheduler modules.

## Global Constraints

- **Binds nothing.** Readiness is a pure comparison against a snapshot; no mutation, no kube client. Existing guards unchanged (binding.rs must still contain none of `.create(`/`.replace(`/`.patch(`/`.delete(`/`.evict(`/`.request(`/`*Params`/`kube::`; shadow.rs none of capital `Binding`/mutation calls).
- Readiness covers the unambiguous stale/conflict conditions: (1) target node absent from the current snapshot; (2) the pod is gone from the snapshot; (3) the pod was recreated (same `namespace/name`, different `uid`); (4) the pod is already bound (to any node). This requires a **UID** on the collected pod/workload (added in Task 0). It is a stale/conflict guard, NOT a full scheduler-predicate revalidation — live GPU-capacity/taint/affinity rechecks are deferred (the solve ensured fit at decision time; races are a binder-side optimistic-retry concern). Do not oversell it as "no stale binding can ever be POSTed."
- Real binding EXECUTION remains out of scope / authorization-gated.

---

### Task 0: Collect pod UID onto `Pod` → `NormalizedWorkload`

**Files:** `ksolver/src/model.rs`, `ksolver/src/collector.rs`, `ksolver/src/normalizer.rs`

- [ ] **Step 1:** Add `#[serde(default)] pub uid: String,` to `Pod` and to `NormalizedWorkload`.
- [ ] **Step 2:** In the collector's `Pod { … }` construction, set `uid: pod.metadata.uid.clone().unwrap_or_default()` (corev1 `ObjectMeta.uid`).
- [ ] **Step 3:** In the normalizer's `NormalizedWorkload { … }`, set `uid: pod.uid.clone()`.
- [ ] **Step 4:** Build clean (`cargo build --features rust-cp-sat`); fix any literal missing the field. Commit — `git commit -am "Collect pod UID onto Pod + NormalizedWorkload (for pre-bind conflict detection)"`.

---

### Task 1: `BindReadiness` + pure `assess_binding_readiness`

**Files:**
- Modify: `ksolver/src/scheduler/binding.rs`

**Interfaces:**
- Produces:
  ```rust
  pub enum BindReadiness { Ready, Stale { reason: String } }
  pub fn assess_binding_readiness(entry: &BindingPlanEntry, cluster: &crate::model::NormalizedCluster) -> BindReadiness;
  ```
- Also add `pub gpu_request: i64` to `BindingPlanEntry` (a binder needs the request; populate from `PodDecision.gpu_request`).

- [ ] **Step 1: Add `gpu_request` to `BindingPlanEntry`** and populate it in `render_binding_plan` (`d.gpu_request`). Update the existing `renders_binding_for_each_placed_pod_only` test to assert `e.gpu_request`.

- [ ] **Step 2: Write failing tests**

```rust
#[test]
fn readiness_ready_when_node_present_and_pod_unbound() {
    use crate::model::{NormalizedCluster, NormalizedNode, NormalizedWorkload};
    let cluster = NormalizedCluster {
        nodes: vec![NormalizedNode { name: "n1".into(), ..Default::default() }],
        workloads: vec![NormalizedWorkload {
            namespace: "team".into(),
            name: "a".into(),
            current_node: String::new(), // still pending
            ..Default::default()
        }],
        ..Default::default()
    };
    let entry = BindingPlanEntry {
        namespace: "team".into(),
        pod_name: "a".into(),
        pod_uid: "u".into(),
        node_name: "n1".into(),
        gpu_request: 1,
        binding_body: serde_json::json!({}),
    };
    assert!(matches!(assess_binding_readiness(&entry, &cluster), BindReadiness::Ready));
}

#[test]
fn readiness_stale_when_target_node_gone() {
    use crate::model::NormalizedCluster;
    let cluster = NormalizedCluster { nodes: vec![], ..Default::default() };
    let entry = BindingPlanEntry {
        namespace: "team".into(), pod_name: "a".into(), pod_uid: "u".into(),
        node_name: "n1".into(), gpu_request: 1, binding_body: serde_json::json!({}),
    };
    match assess_binding_readiness(&entry, &cluster) {
        BindReadiness::Stale { reason } => assert!(reason.contains("node")),
        _ => panic!("expected stale"),
    }
}

#[test]
fn readiness_stale_when_pod_already_scheduled_elsewhere() {
    use crate::model::{NormalizedCluster, NormalizedNode, NormalizedWorkload};
    let cluster = NormalizedCluster {
        nodes: vec![NormalizedNode { name: "n1".into(), ..Default::default() }],
        workloads: vec![NormalizedWorkload {
            namespace: "team".into(),
            name: "a".into(),
            current_node: "n2".into(), // already bound elsewhere
            ..Default::default()
        }],
        ..Default::default()
    };
    let entry = BindingPlanEntry {
        namespace: "team".into(), pod_name: "a".into(), pod_uid: "u".into(),
        node_name: "n1".into(), gpu_request: 1, binding_body: serde_json::json!({}),
    };
    match assess_binding_readiness(&entry, &cluster) {
        BindReadiness::Stale { reason } => assert!(reason.contains("already")),
        _ => panic!("expected stale"),
    }
}
```

- [ ] **Step 3: Implement**

```rust
/// Whether a rendered (dry-run) binding is still safe to apply against the current cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum BindReadiness {
    Ready,
    Stale { reason: String },
}

/// Re-validate a rendered binding against the LATEST cluster snapshot — the staleness guard a real
/// binder must run before POSTing. Pure: reads only, mutates nothing. Covers (1) the target node
/// having vanished and (2) the pod already being bound. (Live GPU-capacity recheck is deferred; the
/// decision solve already ensured fit, and capacity races are a binder-side optimistic-retry concern.)
pub fn assess_binding_readiness(
    entry: &BindingPlanEntry,
    cluster: &crate::model::NormalizedCluster,
) -> BindReadiness {
    if !cluster.nodes.iter().any(|n| n.name == entry.node_name) {
        return BindReadiness::Stale {
            reason: format!("target node {} no longer present", entry.node_name),
        };
    }
    match cluster
        .workloads
        .iter()
        .find(|w| w.namespace == entry.namespace && w.name == entry.pod_name)
    {
        None => BindReadiness::Stale {
            reason: "pod no longer present in latest snapshot".to_string(),
        },
        // Same name, different identity: the pod was deleted and recreated — this plan is for the
        // old pod. Binding by name would target the wrong pod. (Empty entry uid ⇒ skip the check.)
        Some(w) if !entry.pod_uid.is_empty() && !w.uid.is_empty() && w.uid != entry.pod_uid => {
            BindReadiness::Stale {
                reason: "pod recreated (uid changed) since the plan was rendered".to_string(),
            }
        }
        Some(w) if !w.current_node.is_empty() => BindReadiness::Stale {
            reason: format!("pod already scheduled on {}", w.current_node),
        },
        Some(_) => BindReadiness::Ready,
    }
}
```

- [ ] **Step 3b: Add tests** for pod-absent → Stale and uid-mismatch → Stale (populate `uid` on the workload and `pod_uid` on the entry). Keep the already-bound-elsewhere test.
- [ ] **Step 4: Run — expect PASS** (`cargo test --features rust-cp-sat binding`), including the strengthened `no_mutation_guard`.
- [ ] **Step 5: Commit** — `git commit -am "Pre-bind readiness check (pure stale/conflict guard incl. uid) + gpu_request on plan entry"`

---

### Task 2: Store latest snapshot + annotate the binding-plan endpoint

**Files:**
- Modify: `ksolver/src/scheduler/shadow.rs`

- [ ] **Step 1: Extend `ShadowHttpState`** with `latest_cluster: Arc<Mutex<Option<crate::model::NormalizedCluster>>>` (import `crate::model::NormalizedCluster`; `Mutex` already imported). Initialize `Arc::new(Mutex::new(None))` in `run_shadow`; pass a clone into both the HTTP state and the solve loop.
- [ ] **Step 2: Update the snapshot each solve** — in `run_one_solve` (or the loop after `normalize`), store a clone: `*latest_cluster.lock().unwrap() = Some(normalized.clone());`. (Thread the `Arc` in; if `run_one_solve` is a free function, pass the `Arc` as a param or set it in the loop where `normalized` is available.) Simplest: have the loop set it right after the trace is produced, using the returned/again-available snapshot — or move the store into the loop body where `run_one_solve` returns and re-collect isn't needed. Concretely: change `run_one_solve` to also return the `NormalizedCluster` (or accept the `Arc` and store internally). Choose the lower-ripple option: pass `&Arc<Mutex<Option<NormalizedCluster>>>` into `run_one_solve` and store inside it after `normalize`.
- [ ] **Step 3: Annotate the handler** — in `binding_plan_handler`, after rendering `plan`, read the latest cluster and attach readiness per entry:

```rust
    let cluster = s.latest_cluster.lock().expect("cluster lock").clone();
    let entries: Vec<serde_json::Value> = plan
        .into_iter()
        .map(|e| {
            let readiness = cluster
                .as_ref()
                .map(|c| crate::scheduler::binding::assess_binding_readiness(&e, c));
            let mut v = serde_json::to_value(&e).unwrap_or_default();
            if let (Some(obj), Some(r)) = (v.as_object_mut(), readiness) {
                obj.insert("readiness".to_string(), serde_json::to_value(r).unwrap_or_default());
            }
            v
        })
        .collect();
```
  Return `"bindings": entries` (instead of `plan`). Keep `dry_run`, `trace_sequence`, `solve_millis`, `note`.

- [ ] **Step 4: Update the endpoint test** — seed `latest_cluster` with a cluster where the placed pod's node exists and pod is unbound; assert `bindings[0]["readiness"]["state"] == "ready"`.
- [ ] **Step 5: Run — expect PASS**, and CRITICALLY both `no_mutation_guard` tests still pass (shadow.rs gains `NormalizedCluster`/`assess_binding_readiness` — no capital `Binding`, no mutation calls). Full `cargo test` + `cargo clippy --features rust-cp-sat` clean.
- [ ] **Step 6: Commit** — `git commit -am "Annotate dry-run binding plan with pre-bind readiness (live staleness guard)"`

---

### Task 3: Dashboard readiness column + docs

**Files:** `ksolver/static/shadow.html`, `README.md`

- [ ] **Step 1: Dashboard** — add a "Readiness" column to the binding-plan table; render `b.readiness.state` (default "" if absent) as a pill (`ready`/`stale`), with the stale reason as the cell title/text. XSS-safe via `textContent`.
- [ ] **Step 2: README** — extend the binding-plan paragraph: each entry now carries a `readiness` (`ready` / `stale` with reason) computed against the latest snapshot — the staleness guard a real binder runs before applying.
- [ ] **Step 3: Full verify** — `cargo test --features rust-cp-sat` + `cargo clippy --features rust-cp-sat` clean; `dashboard_asset_is_wired` updated if needed.
- [ ] **Step 4: Commit** — `git commit -am "Dashboard + docs: pre-bind readiness on dry-run binding plan"`

---

## Self-Review

- **Safety:** pure readiness fn (no kube client / mutation); guards stay green; nothing is applied. Real binding still gated.
- **Correctness:** readiness covers node-gone + pod-already-bound (unambiguous staleness); capacity recheck explicitly deferred with rationale.
- **Type consistency:** `BindReadiness`/`assess_binding_readiness`/`BindingPlanEntry.gpu_request` used identically across binding.rs, shadow.rs, tests.
- **No placeholders.**
