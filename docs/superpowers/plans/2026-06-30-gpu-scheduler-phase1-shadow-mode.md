# GPU Scheduler — Phase 1: Shadow Mode — Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a shadow-mode GPU scheduler that watches pending `schedulerName: ksolver` GPU pods, computes where they *would* be placed via the existing CP-SAT call path, records explainable decision traces (with reasons), and **binds nothing** — zero production risk.

**Architecture:** A new `scheduler` module in the existing `ksolver` crate. All correctness-bearing logic is in pure, unit-testable functions (pod classification, watch-event reduction, decision-trace construction, config). Thin I/O wrappers (a self-healing kube watch + snapshot collection) reuse the existing `collector`/`normalizer`/`optimizer_input`/`cpsat_rust` pipeline. A `shadow` subcommand runs the loop and serves traces + metrics + health over HTTP for the simulator/UI.

**Tech Stack:** Rust, tokio, kube-rs 0.97 (`kube::runtime::watcher`), k8s-openapi v1_31 (`corev1::Pod`), axum 0.7, prometheus, OR-Tools CP-SAT via the `cp_sat` crate (behind the `rust-cp-sat` feature).

## Global Constraints

- Crate: `ksolver` (single crate; add a `scheduler` module, do not create a new crate).
- Kubernetes client: call the existing `collector::build_client(&kubeconfig)` (it is `pub(crate)`, so the same-crate `scheduler` module may call it). Do NOT add a client accessor or hand-roll a builder.
- k8s API version pin: `k8s-openapi` feature `v1_31`. Use `k8s_openapi::api::core::v1 as corev1`.
- The CP-SAT solver (`cpsat_rust::solve`) requires the `rust-cp-sat` cargo feature and OR-Tools at build time. **Every unit test in this plan must pass WITHOUT that feature** (they test pure functions only). The watch loop calls `cpsat_rust::solve(&input, &scenario)` exactly as `service::Analyzer` does.
- **Shadow mode MUST NOT mutate the cluster.** No Binding API, no Eviction API, no `create`/`replace`/`patch`/`delete`/`evict`. Task 9 adds a source-level guard test enforcing this.
- Verified API facts (do not re-guess these):
  - Pricing: `pricing::load_pricing_catalog(path: &str) -> anyhow::Result<PricingCatalog>`; `PricingCatalog` derives `Default`. Use `load_pricing_catalog("").unwrap_or_default()`.
  - Normalizer: `normalizer::Normalizer::new(pricing, normalizer::Options::default()).normalize(&snapshot)`.
  - Input: `optimizer_input::build_input_strict(&normalized, false)` — **strict (ungrouped)** so workload ids are `"{namespace}/{name}"` and node names are real; `false` = do NOT drop unschedulable workloads (we must still emit a trace decision for them).
  - Solve: `cpsat_rust::solve(&input, &scenario) -> anyhow::Result<(OptimizationSolution, SolverInfo)>`.
  - `OptimizationSolution.assignments: HashMap<String,String>` (workload id → node name). `SolverInfo.status: String` (use directly, do not `{:?}` it).
  - `OptimizationInput.workloads: Vec<OptimizationWorkload>` where `OptimizationWorkload.id: String == "{namespace}/{name}"` in strict mode.
  - Metrics: crate registry is `metrics::REGISTRY` (a `prometheus::Registry`). `register_metrics()` currently uses `.expect(...)` and is **not idempotent** — Task 4 fixes this.
  - Watcher: `kube::runtime::watcher::Event` variants are `Init`, `InitApply(K)`, `InitDone`, `Apply(K)`, `Delete(K)`. Objects seen via `Apply` but absent by `InitDone` must be treated as deleted.
- Run `cargo fmt` before every commit; keep `cargo clippy` clean.
- Scenario solver string: `"cp-sat-rust"`.

---

## File Structure

- Create `ksolver/src/scheduler/mod.rs` — module root + source-boundary guard test.
- Create `ksolver/src/scheduler/config.rs` — `ShadowConfig` + env parsing.
- Create `ksolver/src/scheduler/pod_filter.rs` — pure pod classification + effective GPU request.
- Create `ksolver/src/scheduler/watch_state.rs` — pure watch-event reducer over the observed-pod map.
- Create `ksolver/src/scheduler/trace.rs` — trace types (with reasons) + bounded `TraceStore`.
- Create `ksolver/src/scheduler/decision.rs` — pure `build_decision_trace` mapping solver output → trace.
- Create `ksolver/src/scheduler/shadow.rs` — I/O: self-healing watch, sequential solve loop, HTTP (traces/metrics/health).
- Modify `ksolver/src/lib.rs` — add `pub mod scheduler;`.
- Modify `ksolver/src/metrics.rs` — idempotent registration + shadow metrics.
- Modify `ksolver/src/main.rs` — add the `shadow` subcommand.
- Modify `ksolver/README.md` — document `shadow` + read-only RBAC.

---

## Task 1: Module scaffold + config

**Files:**
- Create: `ksolver/src/scheduler/mod.rs`, `ksolver/src/scheduler/config.rs`
- Modify: `ksolver/src/lib.rs` (add `pub mod scheduler;` after `pub mod pricing;`)
- Test: inline `#[cfg(test)]` in `config.rs`

**Interfaces:**
- Produces:
  - `config::ShadowConfig { scheduler_name: String, batch_window: Duration, namespace_allowlist: Vec<String>, gpu_resource_names: Vec<String>, cluster_name: String, kubeconfig: String, http_addr: String }`
  - `ShadowConfig::from_env() -> ShadowConfig`
  - `ShadowConfig::namespace_in_scope(&self, ns: &str) -> bool`

- [ ] **Step 1: Add module declaration.** In `ksolver/src/lib.rs` add (alphabetical, after `pub mod pricing;`):
```rust
pub mod scheduler;
```

- [ ] **Step 2: Create `ksolver/src/scheduler/mod.rs`** (guard test added in Task 9; declare only files that exist as you go — add each `pub mod` line when its file is created):
```rust
//! Online GPU scheduler components. Phase 1 = shadow mode only:
//! observe and compute placement decisions; never bind pods.

pub mod config;
pub mod decision;
pub mod pod_filter;
pub mod shadow;
pub mod trace;
pub mod watch_state;
```
If implementing strictly in order, temporarily comment out `pub mod` lines whose files don't exist yet and restore each as its task lands.

- [ ] **Step 3: Write the failing config tests.** Create `ksolver/src/scheduler/config.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> ShadowConfig {
        ShadowConfig {
            scheduler_name: "ksolver".to_string(),
            batch_window: std::time::Duration::from_secs(10),
            namespace_allowlist: vec![],
            gpu_resource_names: vec!["nvidia.com/gpu".to_string()],
            cluster_name: "default".to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
        }
    }

    #[test]
    fn empty_allowlist_allows_all() {
        assert!(base().namespace_in_scope("anything"));
    }

    #[test]
    fn allowlist_restricts_when_set() {
        let mut cfg = base();
        cfg.namespace_allowlist = vec!["team-a".to_string()];
        assert!(cfg.namespace_in_scope("team-a"));
        assert!(!cfg.namespace_in_scope("team-z"));
    }
}
```

- [ ] **Step 4: Run to verify failure.** `cargo test -p ksolver scheduler::config` → FAIL (`ShadowConfig` not found).

- [ ] **Step 5: Implement above the test module** in `config.rs`:
```rust
use std::time::Duration;

/// Shadow-mode scheduler configuration, sourced from environment variables.
#[derive(Debug, Clone)]
pub struct ShadowConfig {
    pub scheduler_name: String,
    pub batch_window: Duration,
    pub namespace_allowlist: Vec<String>,
    /// Exact resource names counted as GPUs (e.g. "nvidia.com/gpu").
    pub gpu_resource_names: Vec<String>,
    pub cluster_name: String,
    pub kubeconfig: String,
    pub http_addr: String,
}

fn csv_env(key: &str) -> Vec<String> {
    std::env::var(key)
        .ok()
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

impl ShadowConfig {
    pub fn from_env() -> Self {
        let batch_secs = std::env::var("KSOLVER_SHADOW_BATCH_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|s| *s > 0)
            .unwrap_or(10);
        let mut gpu_resource_names = csv_env("KSOLVER_SHADOW_GPU_RESOURCES");
        if gpu_resource_names.is_empty() {
            gpu_resource_names = vec!["nvidia.com/gpu".to_string()];
        }
        Self {
            scheduler_name: std::env::var("KSOLVER_SHADOW_SCHEDULER_NAME")
                .unwrap_or_else(|_| "ksolver".to_string()),
            batch_window: Duration::from_secs(batch_secs),
            namespace_allowlist: csv_env("KSOLVER_SHADOW_NAMESPACES"),
            gpu_resource_names,
            cluster_name: std::env::var("KSOLVER_CLUSTER_NAME")
                .unwrap_or_else(|_| "default".to_string()),
            kubeconfig: std::env::var("KUBECONFIG").unwrap_or_default(),
            http_addr: std::env::var("KSOLVER_SHADOW_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8090".to_string()),
        }
    }

    pub fn namespace_in_scope(&self, ns: &str) -> bool {
        self.namespace_allowlist.is_empty() || self.namespace_allowlist.iter().any(|n| n == ns)
    }
}
```

- [ ] **Step 6: Run to verify pass.** `cargo test -p ksolver scheduler::config` → PASS (2).

- [ ] **Step 7: Commit.**
```bash
cargo fmt
git add ksolver/src/lib.rs ksolver/src/scheduler/mod.rs ksolver/src/scheduler/config.rs
git commit -m "feat(scheduler): shadow module scaffold and config"
```

---

## Task 2: Pure pod classification + effective GPU request

**Files:** Create `ksolver/src/scheduler/pod_filter.rs`; inline tests.

**Interfaces:**
- Consumes: `config::ShadowConfig`.
- Produces:
  - `pub struct PendingGpuPod { pub uid: String, pub namespace: String, pub name: String, pub gpu_request: i64 }`
  - `pub fn effective_gpu_request(pod: &corev1::Pod, gpu_names: &[String]) -> i64` — Kubernetes effective request: `max(sum over normal containers, max over init containers)`, using `requests` and falling back to `limits` when a container omits requests. Only resource names in `gpu_names` (exact match) count.
  - `pub fn classify(pod: &corev1::Pod, cfg: &ShadowConfig) -> Option<PendingGpuPod>` — `Some` iff in-scope namespace, `spec.schedulerName == cfg.scheduler_name`, unbound (`spec.nodeName` empty), phase Pending or unset, not deleting, and effective GPU ≥ 1.

- [ ] **Step 1: Failing tests.** Create `ksolver/src/scheduler/pod_filter.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::config::ShadowConfig;
    use k8s_openapi::api::core::v1 as corev1;
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn cfg() -> ShadowConfig {
        ShadowConfig {
            scheduler_name: "ksolver".to_string(),
            batch_window: Duration::from_secs(10),
            namespace_allowlist: vec![],
            gpu_resource_names: vec!["nvidia.com/gpu".to_string()],
            cluster_name: "default".to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
        }
    }

    fn q(map: &[(&str, &str)]) -> BTreeMap<String, Quantity> {
        map.iter()
            .map(|(k, v)| (k.to_string(), Quantity(v.to_string())))
            .collect()
    }

    fn container(name: &str, requests: Option<BTreeMap<String, Quantity>>, limits: Option<BTreeMap<String, Quantity>>) -> corev1::Container {
        corev1::Container {
            name: name.to_string(),
            resources: Some(corev1::ResourceRequirements { requests, limits, ..Default::default() }),
            ..Default::default()
        }
    }

    fn pod(scheduler: &str, node: Option<&str>, phase: Option<&str>, containers: Vec<corev1::Container>, init: Vec<corev1::Container>) -> corev1::Pod {
        corev1::Pod {
            metadata: ObjectMeta {
                name: Some("job-0".to_string()),
                namespace: Some("team-a".to_string()),
                uid: Some("uid-123".to_string()),
                ..Default::default()
            },
            spec: Some(corev1::PodSpec {
                scheduler_name: Some(scheduler.to_string()),
                node_name: node.map(|n| n.to_string()),
                containers,
                init_containers: if init.is_empty() { None } else { Some(init) },
                ..Default::default()
            }),
            status: Some(corev1::PodStatus { phase: phase.map(|p| p.to_string()), ..Default::default() }),
        }
    }

    #[test]
    fn classifies_pending_gpu_pod_with_uid() {
        let p = pod("ksolver", None, Some("Pending"), vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)], vec![]);
        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.uid, "uid-123");
        assert_eq!(got.gpu_request, 4);
    }

    #[test]
    fn rejects_other_scheduler() {
        let p = pod("default-scheduler", None, Some("Pending"), vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)], vec![]);
        assert!(classify(&p, &cfg()).is_none());
    }

    #[test]
    fn rejects_bound_pod() {
        let p = pod("ksolver", Some("node-1"), Some("Running"), vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)], vec![]);
        assert!(classify(&p, &cfg()).is_none());
    }

    #[test]
    fn rejects_zero_gpu() {
        let p = pod("ksolver", None, Some("Pending"), vec![container("main", Some(q(&[("cpu", "2")])), None)], vec![]);
        assert!(classify(&p, &cfg()).is_none());
    }

    #[test]
    fn sums_normal_containers() {
        let p = pod("ksolver", None, Some("Pending"),
            vec![container("a", Some(q(&[("nvidia.com/gpu", "1")])), None),
                 container("b", Some(q(&[("nvidia.com/gpu", "2")])), None)], vec![]);
        assert_eq!(effective_gpu_request(&p, &cfg().gpu_resource_names), 3);
    }

    #[test]
    fn init_container_is_max_not_added() {
        // normal sum = 2, init max = 5 -> effective = max(2,5) = 5
        let p = pod("ksolver", None, Some("Pending"),
            vec![container("a", Some(q(&[("nvidia.com/gpu", "1")])), None),
                 container("b", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![container("init", Some(q(&[("nvidia.com/gpu", "5")])), None)]);
        assert_eq!(effective_gpu_request(&p, &cfg().gpu_resource_names), 5);
    }

    #[test]
    fn falls_back_to_limits_when_no_requests() {
        let p = pod("ksolver", None, Some("Pending"),
            vec![container("a", None, Some(q(&[("nvidia.com/gpu", "2")])))], vec![]);
        assert_eq!(effective_gpu_request(&p, &cfg().gpu_resource_names), 2);
    }

    #[test]
    fn exact_name_match_ignores_gpu_memory() {
        let p = pod("ksolver", None, Some("Pending"),
            vec![container("a", Some(q(&[("nvidia.com/gpu-memory", "8")])), None)], vec![]);
        assert_eq!(effective_gpu_request(&p, &cfg().gpu_resource_names), 0);
    }
}
```

- [ ] **Step 2: Run to verify failure.** `cargo test -p ksolver scheduler::pod_filter` → FAIL.

- [ ] **Step 3: Implement.** Prepend to `pod_filter.rs`:
```rust
use crate::scheduler::config::ShadowConfig;
use k8s_openapi::api::core::v1 as corev1;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingGpuPod {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    pub gpu_request: i64,
}

/// GPU counts are whole numbers; anything non-integer floors to 0 (fractional GPUs
/// are a later phase). Suffix forms like "1" only.
fn parse_gpu_quantity(raw: &str) -> i64 {
    raw.trim().parse::<i64>().unwrap_or(0)
}

/// Sum of GPU resources named in `gpu_names` within one container's effective map
/// (requests, falling back to limits when requests is absent).
fn container_gpu(container: &corev1::Container, gpu_names: &[String]) -> i64 {
    let Some(res) = container.resources.as_ref() else { return 0 };
    let map = res.requests.as_ref().or(res.limits.as_ref());
    let Some(map) = map else { return 0 };
    let mut total = 0i64;
    for (name, qty) in map {
        if gpu_names.iter().any(|g| g == name) {
            total += parse_gpu_quantity(&qty.0);
        }
    }
    total
}

/// Kubernetes effective resource request: max(sum of normal containers,
/// max over init containers).
pub fn effective_gpu_request(pod: &corev1::Pod, gpu_names: &[String]) -> i64 {
    let Some(spec) = pod.spec.as_ref() else { return 0 };
    let normal_sum: i64 = spec.containers.iter().map(|c| container_gpu(c, gpu_names)).sum();
    let init_max: i64 = spec
        .init_containers
        .as_ref()
        .map(|inits| inits.iter().map(|c| container_gpu(c, gpu_names)).max().unwrap_or(0))
        .unwrap_or(0);
    normal_sum.max(init_max)
}

pub fn classify(pod: &corev1::Pod, cfg: &ShadowConfig) -> Option<PendingGpuPod> {
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    let name = pod.metadata.name.clone().unwrap_or_default();
    let uid = pod.metadata.uid.clone().unwrap_or_default();
    if !cfg.namespace_in_scope(&namespace) {
        return None;
    }
    if pod.metadata.deletion_timestamp.is_some() {
        return None;
    }
    let spec = pod.spec.as_ref()?;
    if spec.scheduler_name.as_deref() != Some(cfg.scheduler_name.as_str()) {
        return None;
    }
    if spec.node_name.as_deref().map(|n| !n.is_empty()).unwrap_or(false) {
        return None;
    }
    if let Some(phase) = pod.status.as_ref().and_then(|s| s.phase.as_deref()) {
        if phase != "Pending" {
            return None;
        }
    }
    let gpu = effective_gpu_request(pod, &cfg.gpu_resource_names);
    if gpu < 1 {
        return None;
    }
    Some(PendingGpuPod { uid, namespace, name, gpu_request: gpu })
}

// silence unused import when tests aren't compiled
#[allow(unused_imports)]
use Quantity as _KeepQuantityInScope;
```
(If clippy flags the `_KeepQuantityInScope` shim as unnecessary, delete it and the `Quantity` import — it's only there in case a future edit needs the type. Remove if unused.)

- [ ] **Step 4: Run to verify pass.** `cargo test -p ksolver scheduler::pod_filter` → PASS (8).

- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pod_filter.rs ksolver/src/scheduler/mod.rs
git commit -m "feat(scheduler): pure pod classification with k8s-accurate GPU request"
```

---

## Task 3: Watch-event reducer (pure)

Correctly maintains the observed-pod map across the watcher's Init/InitApply/InitDone/Apply/Delete lifecycle, including relist deletion semantics. Pure and fully unit-tested — no Kubernetes needed.

**Files:** Create `ksolver/src/scheduler/watch_state.rs`; inline tests.

**Interfaces:**
- Consumes: `config::ShadowConfig`, `pod_filter::{classify, PendingGpuPod}`, `k8s_openapi::api::core::v1::Pod`, `kube::runtime::watcher::Event`.
- Produces:
  - `pub struct WatchState { observed: BTreeMap<String, PendingGpuPod>, init_buffer: Option<BTreeMap<String, PendingGpuPod>> }`
  - `WatchState::new()`, `WatchState::apply(&mut self, event: &Event<corev1::Pod>, cfg: &ShadowConfig)`, `WatchState::snapshot(&self) -> Vec<PendingGpuPod>` (values, sorted by uid for determinism), `WatchState::len(&self) -> usize`.
  - Key is the pod uid (fallback `namespace/name` if uid empty).

- [ ] **Step 1: Failing tests.** Create `ksolver/src/scheduler/watch_state.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::config::ShadowConfig;
    use k8s_openapi::api::core::v1 as corev1;
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::runtime::watcher::Event;
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn cfg() -> ShadowConfig {
        ShadowConfig {
            scheduler_name: "ksolver".to_string(),
            batch_window: Duration::from_secs(10),
            namespace_allowlist: vec![],
            gpu_resource_names: vec!["nvidia.com/gpu".to_string()],
            cluster_name: "default".to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
        }
    }

    fn gpu_pod(uid: &str, name: &str) -> corev1::Pod {
        let mut req = BTreeMap::new();
        req.insert("nvidia.com/gpu".to_string(), Quantity("1".to_string()));
        corev1::Pod {
            metadata: ObjectMeta { name: Some(name.to_string()), namespace: Some("team-a".to_string()), uid: Some(uid.to_string()), ..Default::default() },
            spec: Some(corev1::PodSpec {
                scheduler_name: Some("ksolver".to_string()),
                containers: vec![corev1::Container { name: "m".to_string(), resources: Some(corev1::ResourceRequirements { requests: Some(req), ..Default::default() }), ..Default::default() }],
                ..Default::default()
            }),
            status: Some(corev1::PodStatus { phase: Some("Pending".to_string()), ..Default::default() }),
        }
    }

    #[test]
    fn apply_adds_matching_pod() {
        let mut s = WatchState::new();
        s.apply(&Event::Apply(gpu_pod("u1", "p1")), &cfg());
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn delete_removes_pod() {
        let mut s = WatchState::new();
        s.apply(&Event::Apply(gpu_pod("u1", "p1")), &cfg());
        s.apply(&Event::Delete(gpu_pod("u1", "p1")), &cfg());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn relist_drops_pods_absent_after_initdone() {
        let mut s = WatchState::new();
        s.apply(&Event::Apply(gpu_pod("u1", "p1")), &cfg());
        s.apply(&Event::Apply(gpu_pod("u2", "p2")), &cfg());
        // Relist only reports u2 -> u1 must be dropped on InitDone.
        s.apply(&Event::Init, &cfg());
        s.apply(&Event::InitApply(gpu_pod("u2", "p2")), &cfg());
        s.apply(&Event::InitDone, &cfg());
        let snap = s.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].uid, "u2");
    }
}
```

- [ ] **Step 2: Run to verify failure.** `cargo test -p ksolver scheduler::watch_state` → FAIL.

- [ ] **Step 3: Implement.** Prepend to `watch_state.rs`:
```rust
use crate::scheduler::config::ShadowConfig;
use crate::scheduler::pod_filter::{classify, PendingGpuPod};
use k8s_openapi::api::core::v1 as corev1;
use kube::runtime::watcher::Event;
use std::collections::BTreeMap;

fn key_of(pod: &corev1::Pod) -> String {
    match pod.metadata.uid.as_deref() {
        Some(u) if !u.is_empty() => u.to_string(),
        _ => format!(
            "{}/{}",
            pod.metadata.namespace.clone().unwrap_or_default(),
            pod.metadata.name.clone().unwrap_or_default()
        ),
    }
}

/// Maintains the set of in-scope pending GPU pods across the watcher lifecycle.
pub struct WatchState {
    observed: BTreeMap<String, PendingGpuPod>,
    init_buffer: Option<BTreeMap<String, PendingGpuPod>>,
}

impl WatchState {
    pub fn new() -> Self {
        Self { observed: BTreeMap::new(), init_buffer: None }
    }

    pub fn apply(&mut self, event: &Event<corev1::Pod>, cfg: &ShadowConfig) {
        match event {
            Event::Init => {
                self.init_buffer = Some(BTreeMap::new());
            }
            Event::InitApply(pod) => {
                if let Some(p) = classify(pod, cfg) {
                    let buf = self.init_buffer.get_or_insert_with(BTreeMap::new);
                    buf.insert(key_of(pod), p);
                }
            }
            Event::InitDone => {
                if let Some(buf) = self.init_buffer.take() {
                    self.observed = buf;
                }
            }
            Event::Apply(pod) => {
                let key = key_of(pod);
                match classify(pod, cfg) {
                    Some(p) => {
                        self.observed.insert(key, p);
                    }
                    None => {
                        self.observed.remove(&key);
                    }
                }
            }
            Event::Delete(pod) => {
                self.observed.remove(&key_of(pod));
            }
        }
    }

    pub fn snapshot(&self) -> Vec<PendingGpuPod> {
        self.observed.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.observed.len()
    }

    pub fn is_empty(&self) -> bool {
        self.observed.is_empty()
    }
}

impl Default for WatchState {
    fn default() -> Self {
        Self::new()
    }
}
```

- [ ] **Step 4: Run to verify pass.** `cargo test -p ksolver scheduler::watch_state` → PASS (3).

- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/watch_state.rs ksolver/src/scheduler/mod.rs
git commit -m "feat(scheduler): pure watch-event reducer with relist semantics"
```

---

## Task 4: Idempotent metrics registration + shadow metrics

**Files:** Modify `ksolver/src/metrics.rs`; inline test.

**Interfaces:**
- Produces: `inc_shadow_pod_observations(n: u64)`, `set_shadow_pending(n: i64)`, `inc_shadow_solves()`, `inc_shadow_solve_errors()`, `observe_shadow_solve_seconds(secs: f64)`, `inc_shadow_unplaced(n: u64)`.
- Fixes: `register_metrics()` becomes idempotent (safe to call multiple times).

- [ ] **Step 1: Read the current metric/register pattern.** `sed -n '1,72p' ksolver/src/metrics.rs`. Note it uses `REGISTRY: Registry`, `IntCounterVec`/`HistogramVec`, and `register_metrics()` with `.expect(...)`.

- [ ] **Step 2: Failing test.** Add to `ksolver/src/metrics.rs`:
```rust
#[cfg(test)]
mod shadow_metric_tests {
    use super::*;

    #[test]
    fn register_is_idempotent_and_shadow_metrics_render() {
        register_metrics();
        register_metrics(); // must not panic
        inc_shadow_pod_observations(3);
        set_shadow_pending(2);
        inc_shadow_solves();
        inc_shadow_solve_errors();
        observe_shadow_solve_seconds(0.05);
        inc_shadow_unplaced(1);
        let out = render_metrics();
        assert!(out.contains("ksolver_shadow_pod_observations_total"));
        assert!(out.contains("ksolver_shadow_pending_pods"));
        assert!(out.contains("ksolver_shadow_solves_total"));
        assert!(out.contains("ksolver_shadow_solve_errors_total"));
        assert!(out.contains("ksolver_shadow_solve_seconds"));
        assert!(out.contains("ksolver_shadow_unplaced_total"));
    }
}
```

- [ ] **Step 3: Run to verify failure.** `cargo test -p ksolver shadow_metric_tests` → FAIL (panics on double register / missing fns).

- [ ] **Step 4: Make registration idempotent.** In `register_metrics()`, replace each `.expect("solver metric can be registered")` with a helper that ignores `AlreadyReg`. Add near the top of `metrics.rs`:
```rust
fn register_ignoring_dup(c: Box<dyn prometheus::core::Collector>) {
    match REGISTRY.register(c) {
        Ok(()) => {}
        Err(prometheus::Error::AlreadyReg) => {}
        Err(e) => panic!("failed to register metric: {e}"),
    }
}
```
Rewrite the three existing registrations in `register_metrics()` to use it, e.g.:
```rust
register_ignoring_dup(Box::new(SOLVE_DURATION_SECONDS.clone()));
register_ignoring_dup(Box::new(SOLVES_TOTAL.clone()));
register_ignoring_dup(Box::new(SOLVES_IN_FLIGHT.clone()));
```

- [ ] **Step 5: Add shadow statics + accessors + registration.** In the `lazy_static! { ... }` block add:
```rust
    pub static ref SHADOW_POD_OBSERVATIONS: prometheus::IntCounter =
        prometheus::IntCounter::new("ksolver_shadow_pod_observations_total",
            "Pending GPU pod observations across shadow solve windows (not unique pods)").unwrap();
    pub static ref SHADOW_PENDING: prometheus::IntGauge =
        prometheus::IntGauge::new("ksolver_shadow_pending_pods",
            "Current count of in-scope pending GPU pods observed").unwrap();
    pub static ref SHADOW_SOLVES: prometheus::IntCounter =
        prometheus::IntCounter::new("ksolver_shadow_solves_total", "Shadow solves started").unwrap();
    pub static ref SHADOW_SOLVE_ERRORS: prometheus::IntCounter =
        prometheus::IntCounter::new("ksolver_shadow_solve_errors_total", "Shadow solves that errored").unwrap();
    pub static ref SHADOW_SOLVE_SECONDS: prometheus::Histogram =
        prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
            "ksolver_shadow_solve_seconds", "Shadow solve wall-clock seconds")).unwrap();
    pub static ref SHADOW_UNPLACED: prometheus::IntCounter =
        prometheus::IntCounter::new("ksolver_shadow_unplaced_total", "Pending GPU pods with no placement in a solve").unwrap();
```
In `register_metrics()` also register these six via `register_ignoring_dup(Box::new(<STATIC>.clone()))`. Then add the accessors at module scope:
```rust
pub fn inc_shadow_pod_observations(n: u64) { SHADOW_POD_OBSERVATIONS.inc_by(n); }
pub fn set_shadow_pending(n: i64) { SHADOW_PENDING.set(n); }
pub fn inc_shadow_solves() { SHADOW_SOLVES.inc(); }
pub fn inc_shadow_solve_errors() { SHADOW_SOLVE_ERRORS.inc(); }
pub fn observe_shadow_solve_seconds(secs: f64) { SHADOW_SOLVE_SECONDS.observe(secs); }
pub fn inc_shadow_unplaced(n: u64) { SHADOW_UNPLACED.inc_by(n); }
```
Ensure `IntCounter`, `IntGauge`, `Histogram`, `HistogramOpts` are imported (add to the existing `use prometheus::{...}` line if missing).

- [ ] **Step 6: Run to verify pass.** `cargo test -p ksolver shadow_metric_tests` → PASS. Also run `cargo test -p ksolver` to confirm existing metric tests still pass with idempotent registration.

- [ ] **Step 7: Commit.**
```bash
cargo fmt
git add ksolver/src/metrics.rs
git commit -m "feat(scheduler): idempotent metric registration and shadow metrics"
```

---

## Task 5: Trace types with reasons + bounded store

**Files:** Create `ksolver/src/scheduler/trace.rs`; inline tests.

**Interfaces:**
- Produces:
  - `pub enum PodPlacement { Placed { node: String }, Unplaced { reason: String } }` (serde tag = "kind", lowercase).
  - `pub struct PodDecision { pub uid: String, pub namespace: String, pub name: String, pub gpu_request: i64, pub placement: PodPlacement }`
  - `pub struct DecisionTrace { pub sequence: u64, pub observed_pods: usize, pub decisions: Vec<PodDecision>, pub solver_status: String, pub solve_millis: u64, pub snapshot_age_millis: u64, pub note: String }`
  - `pub struct TraceStore` with `new(capacity)`, `push`, `recent() -> Vec<DecisionTrace>` (newest first), `next_sequence() -> u64`.

- [ ] **Step 1: Failing tests.** Create `ksolver/src/scheduler/trace.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn trace(seq: u64) -> DecisionTrace {
        DecisionTrace {
            sequence: seq, observed_pods: 1,
            decisions: vec![PodDecision {
                uid: "u1".into(), namespace: "team-a".into(), name: "job-0".into(), gpu_request: 4,
                placement: PodPlacement::Placed { node: "node-1".into() },
            }],
            solver_status: "OPTIMAL".into(), solve_millis: 12, snapshot_age_millis: 3, note: String::new(),
        }
    }

    #[test]
    fn recent_is_newest_first() {
        let s = TraceStore::new(8);
        s.push(trace(1)); s.push(trace(2));
        let r = s.recent();
        assert_eq!(r[0].sequence, 2);
        assert_eq!(r[1].sequence, 1);
    }

    #[test]
    fn evicts_oldest_beyond_capacity() {
        let s = TraceStore::new(2);
        s.push(trace(1)); s.push(trace(2)); s.push(trace(3));
        let r = s.recent();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].sequence, 3);
    }

    #[test]
    fn sequence_is_monotonic() {
        let s = TraceStore::new(4);
        assert_eq!(s.next_sequence(), 1);
        assert_eq!(s.next_sequence(), 2);
    }
}
```

- [ ] **Step 2: Run to verify failure.** `cargo test -p ksolver scheduler::trace` → FAIL.

- [ ] **Step 3: Implement.** Prepend to `trace.rs`:
```rust
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum PodPlacement {
    Placed { node: String },
    Unplaced { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PodDecision {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    pub gpu_request: i64,
    pub placement: PodPlacement,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecisionTrace {
    pub sequence: u64,
    pub observed_pods: usize,
    pub decisions: Vec<PodDecision>,
    pub solver_status: String,
    pub solve_millis: u64,
    pub snapshot_age_millis: u64,
    pub note: String,
}

pub struct TraceStore {
    capacity: usize,
    inner: Mutex<VecDeque<DecisionTrace>>,
    seq: AtomicU64,
}

impl TraceStore {
    pub fn new(capacity: usize) -> Self {
        Self { capacity: capacity.max(1), inner: Mutex::new(VecDeque::new()), seq: AtomicU64::new(0) }
    }
    pub fn next_sequence(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }
    pub fn push(&self, trace: DecisionTrace) {
        let mut g = self.inner.lock().expect("trace store poisoned");
        if g.len() == self.capacity {
            g.pop_front();
        }
        g.push_back(trace);
    }
    pub fn recent(&self) -> Vec<DecisionTrace> {
        let g = self.inner.lock().expect("trace store poisoned");
        g.iter().rev().cloned().collect()
    }
}
```

- [ ] **Step 4: Run to verify pass.** `cargo test -p ksolver scheduler::trace` → PASS (3).

- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/trace.rs ksolver/src/scheduler/mod.rs
git commit -m "feat(scheduler): decision trace types with reasons and bounded store"
```

---

## Task 6: Pure decision-trace builder (maps solver output → trace with reasons)

The builder derives each pod's workload id the same way `build_input_strict` does (`"{namespace}/{name}"`), looks it up in the actual `OptimizationInput.workloads` to distinguish "not submitted to solver" from "submitted but unplaced", then reads `OptimizationSolution.assignments`.

**Files:** Create `ksolver/src/scheduler/decision.rs`; inline tests.

**Interfaces:**
- Consumes: `pod_filter::PendingGpuPod`, `trace::{DecisionTrace, PodDecision, PodPlacement}`, `crate::model::{OptimizationInput, OptimizationSolution}`.
- Produces:
  - `pub fn build_decision_trace(sequence: u64, pending: &[PendingGpuPod], input: &OptimizationInput, solution: &OptimizationSolution, solver_status: &str, solve_millis: u64, snapshot_age_millis: u64) -> DecisionTrace`

- [ ] **Step 1: Failing tests.** Create `ksolver/src/scheduler/decision.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{OptimizationInput, OptimizationSolution, OptimizationWorkload};
    use crate::scheduler::pod_filter::PendingGpuPod;
    use crate::scheduler::trace::PodPlacement;
    use std::collections::HashMap;

    fn pod(ns: &str, name: &str) -> PendingGpuPod {
        PendingGpuPod { uid: format!("uid-{name}"), namespace: ns.into(), name: name.into(), gpu_request: 1 }
    }

    fn workload(ns: &str, name: &str) -> OptimizationWorkload {
        OptimizationWorkload { id: format!("{ns}/{name}"), namespace: ns.into(), name: name.into(), ..Default::default() }
    }

    #[test]
    fn placed_unplaced_and_not_submitted() {
        let pending = vec![pod("team-a", "placed"), pod("team-a", "unplaced"), pod("team-a", "ghost")];
        // Solver saw "placed" and "unplaced"; not "ghost".
        let input = OptimizationInput { workloads: vec![workload("team-a", "placed"), workload("team-a", "unplaced")], ..Default::default() };
        let mut assignments = HashMap::new();
        assignments.insert("team-a/placed".to_string(), "node-1".to_string());
        let solution = OptimizationSolution { assignments, ..Default::default() };

        let t = build_decision_trace(5, &pending, &input, &solution, "OPTIMAL", 20, 4);
        assert_eq!(t.sequence, 5);
        assert_eq!(t.observed_pods, 3);
        assert_eq!(t.decisions[0].placement, PodPlacement::Placed { node: "node-1".into() });
        match &t.decisions[1].placement { PodPlacement::Unplaced { reason } => assert!(reason.contains("no feasible")), _ => panic!("want unplaced") }
        match &t.decisions[2].placement { PodPlacement::Unplaced { reason } => assert!(reason.contains("not submitted")), _ => panic!("want unplaced") }
    }
}
```

- [ ] **Step 2: Run to verify failure.** `cargo test -p ksolver scheduler::decision` → FAIL.

- [ ] **Step 3: Implement.** Prepend to `decision.rs`:
```rust
use crate::model::{OptimizationInput, OptimizationSolution};
use crate::scheduler::pod_filter::PendingGpuPod;
use crate::scheduler::trace::{DecisionTrace, PodDecision, PodPlacement};
use std::collections::HashSet;

/// The strict-mode workload id for a pod ("{namespace}/{name}").
fn workload_id(p: &PendingGpuPod) -> String {
    format!("{}/{}", p.namespace, p.name)
}

pub fn build_decision_trace(
    sequence: u64,
    pending: &[PendingGpuPod],
    input: &OptimizationInput,
    solution: &OptimizationSolution,
    solver_status: &str,
    solve_millis: u64,
    snapshot_age_millis: u64,
) -> DecisionTrace {
    let submitted: HashSet<&str> = input.workloads.iter().map(|w| w.id.as_str()).collect();
    let mut decisions = Vec::with_capacity(pending.len());
    for p in pending {
        let id = workload_id(p);
        let placement = if !submitted.contains(id.as_str()) {
            PodPlacement::Unplaced {
                reason: "not submitted to solver (filtered as unschedulable during input build)".to_string(),
            }
        } else {
            match solution.assignments.get(&id) {
                Some(node) if !node.is_empty() => PodPlacement::Placed { node: node.clone() },
                _ => PodPlacement::Unplaced { reason: "no feasible placement found".to_string() },
            }
        };
        decisions.push(PodDecision {
            uid: p.uid.clone(),
            namespace: p.namespace.clone(),
            name: p.name.clone(),
            gpu_request: p.gpu_request,
            placement,
        });
    }
    DecisionTrace {
        sequence,
        observed_pods: pending.len(),
        decisions,
        solver_status: solver_status.to_string(),
        solve_millis,
        snapshot_age_millis,
        note: String::new(),
    }
}
```

- [ ] **Step 4: Run to verify pass.** `cargo test -p ksolver scheduler::decision` → PASS.

- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/decision.rs ksolver/src/scheduler/mod.rs
git commit -m "feat(scheduler): pure decision-trace builder with placement reasons"
```

---

## Task 7: Shadow I/O — self-healing watch, sequential solve loop, HTTP

Wires pure pieces to kube + the existing solve pipeline. No new unit tests (all logic here is already tested; this is I/O). Verified by feature build + clippy + Task 10 manual run.

**Files:** Create `ksolver/src/scheduler/shadow.rs`.

**Interfaces:**
- Consumes: everything above + `collector::{build_client, KubeCollector}`, `normalizer`, `optimizer_input::build_input_strict`, `cpsat_rust`, `crate::model::ScenarioConfig`, `pricing::load_pricing_catalog`.
- Produces: `pub async fn run_shadow(cfg: ShadowConfig) -> anyhow::Result<()>`.

- [ ] **Step 1: Confirm analyzer call shapes.** `sed -n '300,345p' ksolver/src/service.rs` and `sed -n '360,445p' ksolver/src/service.rs`. Mirror `Normalizer::new(...).normalize(&snapshot)`, `build_input_*`, and `cpsat_rust::solve` calls exactly.

- [ ] **Step 2: Implement `shadow.rs`.**
```rust
use crate::model::ScenarioConfig;
use crate::scheduler::config::ShadowConfig;
use crate::scheduler::decision::build_decision_trace;
use crate::scheduler::trace::{DecisionTrace, PodPlacement, TraceStore};
use crate::scheduler::watch_state::WatchState;
use crate::{collector, cpsat_rust, metrics, normalizer, optimizer_input, pricing};
use anyhow::Result;
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use futures_util::StreamExt;
use k8s_openapi::api::core::v1 as corev1;
use kube::runtime::watcher;
use kube::Api;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::{error, info, warn};

#[derive(Clone)]
struct ShadowHttpState {
    traces: Arc<TraceStore>,
    watch_healthy: Arc<AtomicBool>,
}

async fn traces_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "traces": s.traces.recent() }))
}

async fn metrics_handler() -> (axum::http::StatusCode, [(&'static str, &'static str); 1], String) {
    (axum::http::StatusCode::OK,
     [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
     metrics::render_metrics())
}

async fn healthz() -> &'static str { "ok" }

async fn readyz(State(s): State<ShadowHttpState>) -> (axum::http::StatusCode, &'static str) {
    if s.watch_healthy.load(Ordering::SeqCst) {
        (axum::http::StatusCode::OK, "ready")
    } else {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "watch not healthy")
    }
}

/// Shadow-mode scheduler: observe pending GPU pods, periodically solve, record
/// decision traces, serve them. NEVER binds or mutates cluster state.
pub async fn run_shadow(cfg: ShadowConfig) -> Result<()> {
    metrics::register_metrics();
    let traces = Arc::new(TraceStore::new(64));
    let observed: Arc<Mutex<WatchState>> = Arc::new(Mutex::new(WatchState::new()));
    let watch_healthy = Arc::new(AtomicBool::new(false));

    // HTTP server (traces / metrics / health).
    let http_state = ShadowHttpState { traces: traces.clone(), watch_healthy: watch_healthy.clone() };
    let app = Router::new()
        .route("/api/scheduler/traces", get(traces_handler))
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(http_state);
    let http_addr = cfg.http_addr.clone();
    tokio::spawn(async move {
        match tokio::net::TcpListener::bind(&http_addr).await {
            Ok(l) => {
                info!(addr = %http_addr, "shadow HTTP server listening");
                if let Err(e) = axum::serve(l, app).await { error!(error = %e, "shadow HTTP failed"); }
            }
            Err(e) => error!(error = %e, addr = %http_addr, "failed to bind shadow HTTP addr"),
        }
    });

    // Self-healing watch task: recreate the watcher if the stream ends.
    let client = collector::build_client(&cfg.kubeconfig).await?;
    let pods_api: Api<corev1::Pod> = Api::all(client);
    let watch_cfg = cfg.clone();
    let watch_observed = observed.clone();
    let watch_flag = watch_healthy.clone();
    tokio::spawn(async move {
        loop {
            watch_flag.store(false, Ordering::SeqCst);
            let mut stream = watcher(pods_api.clone(), watcher::Config::default()).boxed();
            info!("pod watch (re)started");
            while let Some(event) = stream.next().await {
                match event {
                    Ok(ev) => {
                        if matches!(ev, watcher::Event::InitDone) {
                            watch_flag.store(true, Ordering::SeqCst);
                        }
                        let mut st = watch_observed.lock().expect("watch state poisoned");
                        st.apply(&ev, &watch_cfg);
                        metrics::set_shadow_pending(st.len() as i64);
                    }
                    Err(e) => warn!(error = %e, "watch error; will resync"),
                }
            }
            watch_flag.store(false, Ordering::SeqCst);
            warn!("watch stream ended; restarting after backoff");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    });

    // Sequential solve loop: sleep AFTER each solve so a slow solve never overlaps itself.
    loop {
        tokio::time::sleep(cfg.batch_window).await;
        let pending = { observed.lock().expect("watch state poisoned").snapshot() };
        if pending.is_empty() {
            continue;
        }
        metrics::inc_shadow_pod_observations(pending.len() as u64);
        let seq = traces.next_sequence();
        match run_one_solve(&cfg, seq, &pending).await {
            Ok(trace) => {
                let unplaced = trace.decisions.iter()
                    .filter(|d| matches!(d.placement, PodPlacement::Unplaced { .. })).count() as u64;
                metrics::inc_shadow_unplaced(unplaced);
                info!(sequence = trace.sequence, observed = trace.observed_pods, unplaced,
                    status = %trace.solver_status, solve_millis = trace.solve_millis,
                    "shadow decision recorded (bound nothing)");
                traces.push(trace);
            }
            Err(e) => { metrics::inc_shadow_solve_errors(); error!(error = %e, "shadow solve failed"); }
        }
    }
}

async fn run_one_solve(
    cfg: &ShadowConfig,
    sequence: u64,
    pending: &[crate::scheduler::pod_filter::PendingGpuPod],
) -> Result<DecisionTrace> {
    metrics::inc_shadow_solves();
    let started = Instant::now();

    // 1. Snapshot the cluster (read-only) via the existing collector.
    let coll = collector::KubeCollector::new(cfg.cluster_name.clone(), cfg.kubeconfig.clone()).await?;
    let snapshot = coll.collect().await?;
    let snapshot_age_millis = started.elapsed().as_millis() as u64;

    // 2. Normalize + build strict (ungrouped) input + solve, mirroring service::Analyzer.
    let pricing_catalog = pricing::load_pricing_catalog("").unwrap_or_default();
    let normalized = normalizer::Normalizer::new(pricing_catalog, normalizer::Options::default())
        .normalize(&snapshot);
    // strict + keep-unschedulable(false=do not drop) so pending pods still appear as workloads.
    let input = optimizer_input::build_input_strict(&normalized, false);

    let scenario = ScenarioConfig { solver: "cp-sat-rust".to_string(), ..Default::default() };
    let (solution, status) = match cpsat_rust::solve(&input, &scenario) {
        Ok((sol, info)) => (sol, info.status),
        Err(e) => {
            warn!(error = %e, "solver error; recording infeasible");
            (Default::default(), "error".to_string())
        }
    };

    let solve_millis = started.elapsed().as_millis() as u64;
    metrics::observe_shadow_solve_seconds(started.elapsed().as_secs_f64());

    Ok(build_decision_trace(sequence, pending, &input, &solution, &status, solve_millis, snapshot_age_millis))
}
```

- [ ] **Step 3: Feature build.** `cargo build -p ksolver --features rust-cp-sat`. Fix any signature mismatches against the real analyzer API the compiler points to (do NOT touch the already-tested pure modules).

- [ ] **Step 4: Full unit tests (no feature).** `cargo test -p ksolver` → all prior tests green.

- [ ] **Step 5: Clippy.** `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → clean.

- [ ] **Step 6: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/shadow.rs
git commit -m "feat(scheduler): self-healing watch, sequential solve loop, HTTP endpoints"
```

---

## Task 8: `shadow` subcommand + docs + read-only RBAC

**Files:** Modify `ksolver/src/main.rs`, `ksolver/README.md`.

- [ ] **Step 1: Add subcommand.** In `ksolver/src/main.rs`, add before the `_ =>` arm:
```rust
        Some("shadow") => {
            metrics::register_metrics();
            let cfg = ksolver::scheduler::config::ShadowConfig::from_env();
            info!(scheduler_name = %cfg.scheduler_name, batch_seconds = cfg.batch_window.as_secs(),
                  http_addr = %cfg.http_addr, namespaces = ?cfg.namespace_allowlist,
                  "starting shadow-mode GPU scheduler (binds nothing)");
            ksolver::scheduler::shadow::run_shadow(cfg).await?;
        }
```
Extend the usage string to include `  ksolver shadow`.

- [ ] **Step 2: Build.** `cargo build -p ksolver --features rust-cp-sat` → compiles.

- [ ] **Step 3: README.** Add to `ksolver/README.md`:
````markdown
## Shadow-mode GPU scheduler

Observes pending pods with `schedulerName: ksolver` that request GPUs, computes
where they *would* be placed, records decision traces, and **binds nothing**:

    KUBECONFIG=~/.kube/config KSOLVER_SHADOW_BATCH_SECONDS=10 \
      cargo run --features rust-cp-sat -- shadow

Env vars: `KSOLVER_SHADOW_SCHEDULER_NAME` (default `ksolver`),
`KSOLVER_SHADOW_BATCH_SECONDS` (default `10`), `KSOLVER_SHADOW_NAMESPACES`
(comma-separated allowlist; empty = all), `KSOLVER_SHADOW_GPU_RESOURCES`
(default `nvidia.com/gpu`), `KSOLVER_SHADOW_ADDR` (default `127.0.0.1:8090`;
serves `/api/scheduler/traces`, `/metrics`, `/healthz`, `/readyz`).

Shadow mode issues only read/watch/list. Minimal RBAC (read-only):

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

It grants NO `create`/`update`/`patch`/`delete` and no `pods/binding` — shadow
mode cannot mutate the cluster even if a bug tried to.
````

- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/main.rs ksolver/README.md
git commit -m "feat(scheduler): shadow subcommand, docs, and read-only RBAC"
```

---

## Task 9: Source-level no-mutation guard test

A cheap compile-time-ish guard proving shadow code never references cluster-mutating APIs. Uses `include_str!` on the scheduler sources.

**Files:** Modify `ksolver/src/scheduler/mod.rs` (append a test module).

- [ ] **Step 1: Failing test.** Append to `ksolver/src/scheduler/mod.rs`:
```rust
#[cfg(test)]
mod no_mutation_guard {
    // These sources must never call cluster-mutating APIs in Phase 1.
    const SHADOW: &str = include_str!("shadow.rs");

    #[test]
    fn shadow_has_no_binding_or_mutation_calls() {
        for needle in ["Binding", ".evict(", ".create(", ".replace(", ".patch(", ".delete("] {
            assert!(!SHADOW.contains(needle), "shadow.rs must not contain `{needle}` in Phase 1");
        }
    }
}
```

- [ ] **Step 2: Run.** `cargo test -p ksolver no_mutation_guard` → PASS (the Task 7 code uses none of these; if it fails, a mutation call slipped in — remove it).

- [ ] **Step 3: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/mod.rs
git commit -m "test(scheduler): guard that shadow mode issues no cluster mutations"
```

---

## Task 10: Manual verification against a cluster

- [ ] **Step 1: Gate.** `cargo test -p ksolver && cargo clippy -p ksolver --features rust-cp-sat --all-targets` → all green.

- [ ] **Step 2: Run shadow mode** (kind/minikube or dev cluster):
```bash
KUBECONFIG=$HOME/.kube/config KSOLVER_SHADOW_BATCH_SECONDS=5 \
  cargo run --features rust-cp-sat -- shadow
```
Expect: `starting shadow-mode GPU scheduler (binds nothing)`, `shadow HTTP server listening`, `pod watch (re)started`.

- [ ] **Step 3: Create a pending GPU pod** with `spec.schedulerName: ksolver` and `nvidia.com/gpu: "1"` (on a cluster without GPUs it stays Pending — exactly what shadow observes). Within a window: `shadow decision recorded (bound nothing)`.

- [ ] **Step 4: Trace served.** `curl -s localhost:8090/api/scheduler/traces | jq '.traces[0]'` → a `DecisionTrace` with `observed_pods >= 1` and a `decisions[]` entry with `placement.kind` `placed` or `unplaced` (with a `reason`).

- [ ] **Step 5: Nothing bound (core safety).** `kubectl get pod <pod> -o jsonpath='{.spec.nodeName}'` → empty.

- [ ] **Step 6: Delete/relist.** `kubectl delete pod <pod>`; within a window+watch cycle the pod disappears from `/api/scheduler/traces` decisions and `ksolver_shadow_pending_pods` drops.

- [ ] **Step 7: Metrics.** `curl -s localhost:8090/metrics | grep ksolver_shadow_` → all six shadow metrics present.

- [ ] **Step 8: Readiness reflects watch health.** `curl -s -o /dev/null -w '%{http_code}' localhost:8090/readyz` → `200` once the initial relist completes.

---

## Self-Review Notes (coverage vs spec Phase 1 + review fixes)

- Spec §9 phase 1 (translate state, compute, bind nothing, emit traces) → Tasks 2/3 (observe), 6/7 (translate+solve+map), 5/7 (traces), 9/10 (bind-nothing proof). ✅
- Review fix: watcher Init/InitApply/InitDone relist semantics → Task 3 reducer, tested. ✅
- Review fix: idempotent `register_metrics` + crate `REGISTRY` → Task 4. ✅
- Review fix: strict ungrouped input + real workload-id mapping + `ignore_unschedulable=false` + explicit unplaced/not-submitted reasons → Task 6. ✅
- Review fix: sequential loop (no interval bursting / overlap) → Task 7 (`sleep` after solve). ✅
- Review fix: honest metric names (observations vs unique pending gauge), solve-error counter → Task 4/7. ✅
- Review fix: k8s-accurate GPU request (init max, limits fallback, exact name) → Task 2. ✅
- Review fix: pod UID identity, snapshot-age in trace → Tasks 2/5/6. ✅
- Review fix: self-healing watch + readiness → Task 7; RBAC read-only + no-mutation guard → Tasks 8/9. ✅
- Correct scaffolding caveat (still recorded): Phase 1 solves the whole cluster via the existing pipeline and reads back the pending pods' assignments; the dedicated "place these pending pods against fixed running context" L1 formulation (spec §4) replaces `run_one_solve`'s solve step in **Phase 4**. Traces label pods honestly (placed / no-feasible / not-submitted) so this scaffolding is not misrepresented.
- Deferred to later phases (NOT here): reservation ledger, feasibility conformance suite, gang atomicity, preemption, topology, fractional, fair-share.
```
