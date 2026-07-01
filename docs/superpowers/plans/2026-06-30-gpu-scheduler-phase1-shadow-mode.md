# GPU Scheduler — Phase 1: Shadow Mode — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a shadow-mode GPU scheduler that watches pending `schedulerName: ksolver` GPU pods, computes where they *would* be placed via the existing CP-SAT call path, records explainable decision traces, and **binds nothing** — zero production risk.

**Architecture:** A new `scheduler` module in the existing `ksolver` crate. Logic is split into pure, unit-testable units (pod classification, decision-trace construction, config) and thin I/O wrappers (a kube watch loop + snapshot collection that reuse the existing `collector`/`normalizer`/`optimizer_input`/`cpsat_rust` pipeline). A new `shadow` subcommand runs the loop and serves traces + metrics over HTTP so the existing simulator/UI can display them.

**Tech Stack:** Rust, tokio, kube-rs 0.97 (`kube::runtime::watcher`), k8s-openapi v1_31 (`corev1::Pod`), axum 0.7, prometheus, OR-Tools CP-SAT via the `cp_sat` crate (behind the `rust-cp-sat` feature).

## Global Constraints

- Crate: `ksolver` (single crate; add a `scheduler` module, do not create a new crate).
- Kubernetes client: reuse `kube::Client` construction — mirror `collector::build_client` / `KubeCollector::new(cluster_name, kubeconfig)`; do not hand-roll a new client builder.
- k8s API version pin: `k8s-openapi` feature `v1_31`. Use `k8s_openapi::api::core::v1 as corev1`.
- The CP-SAT solver (`cpsat_rust::solve`) requires the `rust-cp-sat` cargo feature (`--features rust-cp-sat`) and OR-Tools at build time. **All unit tests in this plan must NOT depend on that feature** — they test pure functions only. The watch loop calls `cpsat_rust::solve` exactly as `service::Analyzer` does today.
- Shadow mode MUST NOT call the Binding API or Eviction API. No mutation of cluster state whatsoever.
- Existing style: `tracing` for logs (`info!`/`warn!`/`debug!`), `anyhow::Result` for fallible I/O, `serde` derive on data types. Match surrounding code.
- Solver name string used by the existing pipeline: `"cp-sat-rust"` (see `main.rs` / `ScenarioConfig.solver`).
- Run `cargo fmt` before every commit; run `cargo clippy --all-targets` and keep it clean.

---

## File Structure

- Create `ksolver/src/scheduler/mod.rs` — module root; re-exports `config`, `pod_filter`, `trace`, `shadow`.
- Create `ksolver/src/scheduler/config.rs` — `ShadowConfig` + env parsing.
- Create `ksolver/src/scheduler/pod_filter.rs` — pure pod classification + GPU request extraction.
- Create `ksolver/src/scheduler/trace.rs` — `DecisionTrace`/`PodDecision` types + in-memory `TraceStore`.
- Create `ksolver/src/scheduler/shadow.rs` — watch loop, batch window, snapshot→solve call path, pure `build_decision_trace`, metrics, HTTP router.
- Modify `ksolver/src/lib.rs` — add `pub mod scheduler;`.
- Modify `ksolver/src/metrics.rs` — register shadow metrics.
- Modify `ksolver/src/main.rs` — add the `shadow` subcommand.
- Modify `ksolver/README.md` — document the `shadow` subcommand.

---

## Task 1: Scheduler module scaffold + config

**Files:**
- Create: `ksolver/src/scheduler/mod.rs`
- Create: `ksolver/src/scheduler/config.rs`
- Modify: `ksolver/src/lib.rs` (add `pub mod scheduler;` after `pub mod pricing;`)
- Test: inline `#[cfg(test)]` module in `config.rs`

**Interfaces:**
- Produces:
  - `scheduler::config::ShadowConfig { scheduler_name: String, batch_window: std::time::Duration, namespace_allowlist: Vec<String>, gpu_resource_prefixes: Vec<String>, cluster_name: String, kubeconfig: String, http_addr: String }`
  - `ShadowConfig::from_env() -> ShadowConfig`
  - `ShadowConfig::namespace_in_scope(&self, ns: &str) -> bool` (empty allowlist ⇒ all namespaces in scope)

- [ ] **Step 1: Add the module declaration to lib.rs**

Modify `ksolver/src/lib.rs`, adding this line in alphabetical position (after `pub mod pricing;`):

```rust
pub mod scheduler;
```

- [ ] **Step 2: Create the module root**

Create `ksolver/src/scheduler/mod.rs`:

```rust
//! Online GPU scheduler components. Phase 1 provides shadow mode only:
//! it observes and computes placement decisions but never binds pods.

pub mod config;
pub mod pod_filter;
pub mod shadow;
pub mod trace;
```

Note: `pod_filter`, `shadow`, and `trace` files are created in later tasks. If you are implementing tasks strictly in order, temporarily comment out the three not-yet-created lines and restore them as each file lands. (They are listed here so the module layout is unambiguous.)

- [ ] **Step 3: Write the failing test for config parsing**

Create `ksolver/src/scheduler/config.rs` with only the test first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_allowlist_empty_allows_all() {
        let cfg = ShadowConfig {
            scheduler_name: "ksolver".to_string(),
            batch_window: std::time::Duration::from_secs(10),
            namespace_allowlist: vec![],
            gpu_resource_prefixes: vec!["nvidia.com/gpu".to_string()],
            cluster_name: "default".to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
        };
        assert!(cfg.namespace_in_scope("anything"));
    }

    #[test]
    fn namespace_allowlist_restricts_when_set() {
        let cfg = ShadowConfig {
            scheduler_name: "ksolver".to_string(),
            batch_window: std::time::Duration::from_secs(10),
            namespace_allowlist: vec!["team-a".to_string(), "team-b".to_string()],
            gpu_resource_prefixes: vec!["nvidia.com/gpu".to_string()],
            cluster_name: "default".to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
        };
        assert!(cfg.namespace_in_scope("team-a"));
        assert!(!cfg.namespace_in_scope("team-z"));
    }
}
```

- [ ] **Step 4: Run the test to verify it fails to compile**

Run: `cargo test -p ksolver scheduler::config -- --nocapture`
Expected: FAIL — `cannot find type ShadowConfig`.

- [ ] **Step 5: Implement the config type above the test module**

Prepend to `ksolver/src/scheduler/config.rs` (before the `#[cfg(test)]` block):

```rust
use std::time::Duration;

/// Configuration for the shadow-mode scheduler, sourced from environment variables.
#[derive(Debug, Clone)]
pub struct ShadowConfig {
    /// Only pods with `spec.schedulerName` equal to this are considered in scope.
    pub scheduler_name: String,
    /// How long to accumulate observed pending pods before running a solve.
    pub batch_window: Duration,
    /// If non-empty, only these namespaces are in scope.
    pub namespace_allowlist: Vec<String>,
    /// Resource-name prefixes that mark a container as GPU-consuming.
    pub gpu_resource_prefixes: Vec<String>,
    /// Cluster name passed through to the collector.
    pub cluster_name: String,
    /// Path to kubeconfig; empty means in-cluster / default.
    pub kubeconfig: String,
    /// Address the shadow HTTP server (traces + metrics) binds to.
    pub http_addr: String,
}

impl ShadowConfig {
    pub fn from_env() -> Self {
        let batch_secs = std::env::var("KSOLVER_SHADOW_BATCH_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10);
        let namespace_allowlist = std::env::var("KSOLVER_SHADOW_NAMESPACES")
            .ok()
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let gpu_resource_prefixes = std::env::var("KSOLVER_SHADOW_GPU_PREFIXES")
            .ok()
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_else(|| vec!["nvidia.com/gpu".to_string()]);
        Self {
            scheduler_name: std::env::var("KSOLVER_SHADOW_SCHEDULER_NAME")
                .unwrap_or_else(|_| "ksolver".to_string()),
            batch_window: Duration::from_secs(batch_secs),
            namespace_allowlist,
            gpu_resource_prefixes,
            cluster_name: std::env::var("KSOLVER_CLUSTER_NAME")
                .unwrap_or_else(|_| "default".to_string()),
            kubeconfig: std::env::var("KUBECONFIG").unwrap_or_default(),
            http_addr: std::env::var("KSOLVER_SHADOW_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8090".to_string()),
        }
    }

    pub fn namespace_in_scope(&self, ns: &str) -> bool {
        self.namespace_allowlist.is_empty()
            || self.namespace_allowlist.iter().any(|n| n == ns)
    }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cargo test -p ksolver scheduler::config`
Expected: PASS (2 tests).

- [ ] **Step 7: Commit**

```bash
cargo fmt
git add ksolver/src/lib.rs ksolver/src/scheduler/mod.rs ksolver/src/scheduler/config.rs
git commit -m "feat(scheduler): add shadow module scaffold and config"
```

---

## Task 2: Pure pod classification + GPU request extraction

**Files:**
- Create: `ksolver/src/scheduler/pod_filter.rs`
- Test: inline `#[cfg(test)]` module in `pod_filter.rs`

**Interfaces:**
- Consumes: `scheduler::config::ShadowConfig`.
- Produces:
  - `pub struct PendingGpuPod { pub namespace: String, pub name: String, pub gpu_request: i64 }`
  - `pub fn gpu_request(pod: &corev1::Pod, gpu_prefixes: &[String]) -> i64`
  - `pub fn classify(pod: &corev1::Pod, cfg: &ShadowConfig) -> Option<PendingGpuPod>` — returns `Some` iff the pod is: in an in-scope namespace, has `spec.schedulerName == cfg.scheduler_name`, is unbound (`spec.nodeName` empty/none), is Pending (or has no phase yet), not being deleted, and requests ≥1 GPU.

- [ ] **Step 1: Write the failing tests**

Create `ksolver/src/scheduler/pod_filter.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::config::ShadowConfig;
    use k8s_openapi::api::core::v1 as corev1;
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn cfg() -> ShadowConfig {
        ShadowConfig {
            scheduler_name: "ksolver".to_string(),
            batch_window: Duration::from_secs(10),
            namespace_allowlist: vec![],
            gpu_resource_prefixes: vec!["nvidia.com/gpu".to_string()],
            cluster_name: "default".to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
        }
    }

    fn gpu_pod(scheduler: &str, node: Option<&str>, phase: &str, gpus: &str) -> corev1::Pod {
        let mut requests = BTreeMap::new();
        requests.insert("nvidia.com/gpu".to_string(), Quantity(gpus.to_string()));
        corev1::Pod {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                name: Some("job-0".to_string()),
                namespace: Some("team-a".to_string()),
                ..Default::default()
            },
            spec: Some(corev1::PodSpec {
                scheduler_name: Some(scheduler.to_string()),
                node_name: node.map(|n| n.to_string()),
                containers: vec![corev1::Container {
                    name: "main".to_string(),
                    resources: Some(corev1::ResourceRequirements {
                        requests: Some(requests),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(corev1::PodStatus {
                phase: Some(phase.to_string()),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn classifies_pending_ksolver_gpu_pod() {
        let pod = gpu_pod("ksolver", None, "Pending", "4");
        let got = classify(&pod, &cfg()).expect("should classify");
        assert_eq!(got.namespace, "team-a");
        assert_eq!(got.name, "job-0");
        assert_eq!(got.gpu_request, 4);
    }

    #[test]
    fn rejects_other_scheduler() {
        let pod = gpu_pod("default-scheduler", None, "Pending", "4");
        assert!(classify(&pod, &cfg()).is_none());
    }

    #[test]
    fn rejects_already_bound_pod() {
        let pod = gpu_pod("ksolver", Some("node-1"), "Running", "4");
        assert!(classify(&pod, &cfg()).is_none());
    }

    #[test]
    fn rejects_pod_without_gpu() {
        let pod = gpu_pod("ksolver", None, "Pending", "0");
        assert!(classify(&pod, &cfg()).is_none());
    }

    #[test]
    fn sums_gpu_across_containers_and_prefixes() {
        let mut pod = gpu_pod("ksolver", None, "Pending", "1");
        let mut req2 = std::collections::BTreeMap::new();
        req2.insert("nvidia.com/gpu".to_string(), Quantity("2".to_string()));
        if let Some(spec) = pod.spec.as_mut() {
            spec.containers.push(corev1::Container {
                name: "sidecar".to_string(),
                resources: Some(corev1::ResourceRequirements {
                    requests: Some(req2),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        assert_eq!(gpu_request(&pod, &cfg().gpu_resource_prefixes), 3);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p ksolver scheduler::pod_filter`
Expected: FAIL — `cannot find function classify` / `PendingGpuPod`.

- [ ] **Step 3: Implement the classifier**

Prepend to `ksolver/src/scheduler/pod_filter.rs`:

```rust
use crate::scheduler::config::ShadowConfig;
use k8s_openapi::api::core::v1 as corev1;

/// A pending pod that requests GPUs and is in scope for the shadow scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingGpuPod {
    pub namespace: String,
    pub name: String,
    pub gpu_request: i64,
}

/// Parse a Kubernetes quantity string as an integer GPU count.
/// GPU counts are always whole numbers; fractional/suffixed values floor to 0 here
/// (fractional GPUs are a later phase and out of scope for shadow mode).
fn parse_gpu_quantity(raw: &str) -> i64 {
    raw.trim().parse::<i64>().unwrap_or(0)
}

/// Total GPU count requested across all containers, for any resource whose name
/// starts with one of the configured prefixes (e.g. "nvidia.com/gpu").
pub fn gpu_request(pod: &corev1::Pod, gpu_prefixes: &[String]) -> i64 {
    let Some(spec) = pod.spec.as_ref() else {
        return 0;
    };
    let mut total = 0i64;
    for container in &spec.containers {
        let Some(resources) = container.resources.as_ref() else {
            continue;
        };
        if let Some(requests) = resources.requests.as_ref() {
            for (name, qty) in requests {
                if gpu_prefixes.iter().any(|p| name.starts_with(p)) {
                    total += parse_gpu_quantity(&qty.0);
                }
            }
        }
    }
    total
}

/// Return `Some(PendingGpuPod)` iff the pod is an in-scope, pending, GPU-requesting
/// pod owned by our scheduler. Returns `None` otherwise.
pub fn classify(pod: &corev1::Pod, cfg: &ShadowConfig) -> Option<PendingGpuPod> {
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    let name = pod.metadata.name.clone().unwrap_or_default();
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
    // Unbound: no node assigned yet.
    if spec.node_name.as_deref().map(|n| !n.is_empty()).unwrap_or(false) {
        return None;
    }
    // Pending or not-yet-phased.
    if let Some(status) = pod.status.as_ref() {
        if let Some(phase) = status.phase.as_deref() {
            if phase != "Pending" {
                return None;
            }
        }
    }
    let gpu = gpu_request(pod, &cfg.gpu_resource_prefixes);
    if gpu < 1 {
        return None;
    }
    Some(PendingGpuPod {
        namespace,
        name,
        gpu_request: gpu,
    })
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p ksolver scheduler::pod_filter`
Expected: PASS (5 tests).

- [ ] **Step 5: Restore the `pod_filter` line in `mod.rs` if you commented it out in Task 1, then commit**

```bash
cargo fmt
git add ksolver/src/scheduler/pod_filter.rs ksolver/src/scheduler/mod.rs
git commit -m "feat(scheduler): pure pod classification and GPU request extraction"
```

---

## Task 3: Decision trace types + in-memory trace store

**Files:**
- Create: `ksolver/src/scheduler/trace.rs`
- Test: inline `#[cfg(test)]` module in `trace.rs`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces:
  - `pub enum PodPlacement { Placed { node: String }, Unplaced }` (serde-tagged)
  - `pub struct PodDecision { pub namespace: String, pub name: String, pub gpu_request: i64, pub placement: PodPlacement }`
  - `pub struct DecisionTrace { pub sequence: u64, pub observed_pods: usize, pub decisions: Vec<PodDecision>, pub solver_status: String, pub solve_millis: u64, pub note: String }`
  - `pub struct TraceStore` with `new(capacity: usize)`, `push(&self, trace: DecisionTrace)`, `recent(&self) -> Vec<DecisionTrace>`, `next_sequence(&self) -> u64`.

- [ ] **Step 1: Write the failing tests**

Create `ksolver/src/scheduler/trace.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn trace(seq: u64) -> DecisionTrace {
        DecisionTrace {
            sequence: seq,
            observed_pods: 1,
            decisions: vec![PodDecision {
                namespace: "team-a".to_string(),
                name: "job-0".to_string(),
                gpu_request: 4,
                placement: PodPlacement::Placed {
                    node: "node-1".to_string(),
                },
            }],
            solver_status: "optimal".to_string(),
            solve_millis: 12,
            note: String::new(),
        }
    }

    #[test]
    fn store_returns_recent_newest_first() {
        let store = TraceStore::new(8);
        store.push(trace(1));
        store.push(trace(2));
        let recent = store.recent();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].sequence, 2);
        assert_eq!(recent[1].sequence, 1);
    }

    #[test]
    fn store_evicts_oldest_beyond_capacity() {
        let store = TraceStore::new(2);
        store.push(trace(1));
        store.push(trace(2));
        store.push(trace(3));
        let recent = store.recent();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].sequence, 3);
        assert_eq!(recent[1].sequence, 2);
    }

    #[test]
    fn next_sequence_is_monotonic() {
        let store = TraceStore::new(4);
        assert_eq!(store.next_sequence(), 1);
        assert_eq!(store.next_sequence(), 2);
        assert_eq!(store.next_sequence(), 3);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p ksolver scheduler::trace`
Expected: FAIL — `cannot find type TraceStore`.

- [ ] **Step 3: Implement the trace types and store**

Prepend to `ksolver/src/scheduler/trace.rs`:

```rust
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Where the solver proposed a pod would go (shadow mode — never actually bound).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum PodPlacement {
    Placed { node: String },
    Unplaced,
}

/// The shadow decision for a single pending GPU pod.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PodDecision {
    pub namespace: String,
    pub name: String,
    pub gpu_request: i64,
    pub placement: PodPlacement,
}

/// A single shadow-mode solve result, recorded for observation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecisionTrace {
    pub sequence: u64,
    pub observed_pods: usize,
    pub decisions: Vec<PodDecision>,
    pub solver_status: String,
    pub solve_millis: u64,
    pub note: String,
}

/// Bounded, thread-safe ring buffer of recent decision traces.
pub struct TraceStore {
    capacity: usize,
    inner: Mutex<VecDeque<DecisionTrace>>,
    seq: AtomicU64,
}

impl TraceStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
            seq: AtomicU64::new(0),
        }
    }

    /// Return the next monotonic sequence number (starts at 1).
    pub fn next_sequence(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn push(&self, trace: DecisionTrace) {
        let mut guard = self.inner.lock().expect("trace store mutex poisoned");
        if guard.len() == self.capacity {
            guard.pop_front();
        }
        guard.push_back(trace);
    }

    /// Recent traces, newest first.
    pub fn recent(&self) -> Vec<DecisionTrace> {
        let guard = self.inner.lock().expect("trace store mutex poisoned");
        guard.iter().rev().cloned().collect()
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p ksolver scheduler::trace`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add ksolver/src/scheduler/trace.rs ksolver/src/scheduler/mod.rs
git commit -m "feat(scheduler): decision trace types and bounded trace store"
```

---

## Task 4: Shadow metrics registration

**Files:**
- Modify: `ksolver/src/metrics.rs`
- Test: inline `#[cfg(test)]` in `metrics.rs` (or extend existing tests if present)

**Interfaces:**
- Produces (public functions on the `metrics` module):
  - `pub fn inc_shadow_pods_observed(n: u64)`
  - `pub fn inc_shadow_solves()`
  - `pub fn observe_shadow_solve_seconds(secs: f64)`
  - `pub fn inc_shadow_unplaced(n: u64)`
- These MUST be registered by the existing `register_metrics()` path so `render_metrics()` includes them.

- [ ] **Step 1: Inspect the existing metrics pattern**

Run: `sed -n '1,60p' ksolver/src/metrics.rs`
Expected: shows how counters/histograms are declared (likely `lazy_static!` + `prometheus`), and the `register_metrics()` / `render_metrics()` functions. Follow this exact pattern in the next step.

- [ ] **Step 2: Write the failing test**

Add to the `#[cfg(test)]` module in `ksolver/src/metrics.rs` (create the module if none exists):

```rust
#[cfg(test)]
mod shadow_metric_tests {
    use super::*;

    #[test]
    fn shadow_metrics_render() {
        register_metrics();
        inc_shadow_pods_observed(3);
        inc_shadow_solves();
        observe_shadow_solve_seconds(0.05);
        inc_shadow_unplaced(1);
        let out = render_metrics();
        assert!(out.contains("ksolver_shadow_pods_observed_total"));
        assert!(out.contains("ksolver_shadow_solves_total"));
        assert!(out.contains("ksolver_shadow_solve_seconds"));
        assert!(out.contains("ksolver_shadow_unplaced_total"));
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p ksolver shadow_metric_tests`
Expected: FAIL — `cannot find function inc_shadow_pods_observed`.

- [ ] **Step 4: Implement the metrics following the existing pattern**

Add these declarations alongside the existing metric statics in `ksolver/src/metrics.rs` (adapt the macro syntax to match what Step 1 revealed — this uses the common `lazy_static!` + `prometheus` shape):

```rust
lazy_static::lazy_static! {
    static ref SHADOW_PODS_OBSERVED: prometheus::IntCounter = prometheus::IntCounter::new(
        "ksolver_shadow_pods_observed_total",
        "Total pending GPU pods observed by the shadow scheduler"
    ).expect("create ksolver_shadow_pods_observed_total");

    static ref SHADOW_SOLVES: prometheus::IntCounter = prometheus::IntCounter::new(
        "ksolver_shadow_solves_total",
        "Total shadow-mode solves executed"
    ).expect("create ksolver_shadow_solves_total");

    static ref SHADOW_SOLVE_SECONDS: prometheus::Histogram = prometheus::Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "ksolver_shadow_solve_seconds",
            "Shadow-mode solve wall-clock duration in seconds"
        )
    ).expect("create ksolver_shadow_solve_seconds");

    static ref SHADOW_UNPLACED: prometheus::IntCounter = prometheus::IntCounter::new(
        "ksolver_shadow_unplaced_total",
        "Total pending GPU pods the shadow solver could not place"
    ).expect("create ksolver_shadow_unplaced_total");
}

pub fn inc_shadow_pods_observed(n: u64) {
    SHADOW_PODS_OBSERVED.inc_by(n);
}

pub fn inc_shadow_solves() {
    SHADOW_SOLVES.inc();
}

pub fn observe_shadow_solve_seconds(secs: f64) {
    SHADOW_SOLVE_SECONDS.observe(secs);
}

pub fn inc_shadow_unplaced(n: u64) {
    SHADOW_UNPLACED.inc_by(n);
}
```

In `register_metrics()`, register each new metric with the default registry, matching how existing metrics are registered (use `prometheus::register(Box::new(SHADOW_PODS_OBSERVED.clone()))` or the crate's default-registry helper, mirroring the existing code — and ignore an `AlreadyReg` error the same way existing code does, since `register_metrics()` may be called more than once, e.g. by tests).

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p ksolver shadow_metric_tests`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add ksolver/src/metrics.rs
git commit -m "feat(scheduler): register shadow-mode prometheus metrics"
```

---

## Task 5: Pure decision-trace builder from a solver solution

This is the pure core of the shadow loop: given the observed pending GPU pods and the existing solver's `OptimizationSolution`/`OptimizationInput`, produce a `DecisionTrace`. Kept pure so it is unit-tested without kube or the solver feature.

**Files:**
- Create: `ksolver/src/scheduler/shadow.rs` (builder + tests only in this task; the loop lands in Task 6)
- Test: inline `#[cfg(test)]` in `shadow.rs`

**Interfaces:**
- Consumes: `scheduler::pod_filter::PendingGpuPod`, `scheduler::trace::{DecisionTrace, PodDecision, PodPlacement}`, and from `crate::model`: `OptimizationSolution` (has `assignments: HashMap<String, String>` mapping workload id → node name, and `assignment_counts: HashMap<String, HashMap<String,i32>>` per the planner's usage).
- Produces:
  - `pub fn build_decision_trace(sequence: u64, pending: &[PendingGpuPod], workload_id_for: &dyn Fn(&PendingGpuPod) -> String, solution: &OptimizationSolution, solver_status: &str, solve_millis: u64) -> DecisionTrace`

**Note on workload id mapping:** the existing pipeline groups pods into workloads keyed by an id (namespace/owner). Shadow mode maps each observed pod to a workload id via the injected `workload_id_for` closure (the loop in Task 6 supplies the real mapping using `namespace/name` as a fallback key). The builder itself is agnostic to how the id is derived, which keeps it testable.

- [ ] **Step 1: Confirm the `OptimizationSolution` shape**

Run: `sed -n '/pub struct OptimizationSolution/,/^}/p' ksolver/src/model.rs`
Expected: shows fields including `assignments: HashMap<String, String>` and `assignment_counts`. Use `assignments` (workload id → node) for the builder. If the field names differ, adjust the code below to match exactly.

- [ ] **Step 2: Write the failing tests**

Create `ksolver/src/scheduler/shadow.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::OptimizationSolution;
    use crate::scheduler::pod_filter::PendingGpuPod;
    use crate::scheduler::trace::PodPlacement;
    use std::collections::HashMap;

    fn pod(ns: &str, name: &str, gpu: i64) -> PendingGpuPod {
        PendingGpuPod {
            namespace: ns.to_string(),
            name: name.to_string(),
            gpu_request: gpu,
        }
    }

    #[test]
    fn maps_placed_and_unplaced_pods() {
        let pending = vec![pod("team-a", "job-0", 4), pod("team-a", "job-1", 8)];
        let mut assignments = HashMap::new();
        assignments.insert("team-a/job-0".to_string(), "node-1".to_string());
        let solution = OptimizationSolution {
            assignments,
            ..Default::default()
        };
        let id_for = |p: &PendingGpuPod| format!("{}/{}", p.namespace, p.name);
        let trace = build_decision_trace(7, &pending, &id_for, &solution, "optimal", 15);

        assert_eq!(trace.sequence, 7);
        assert_eq!(trace.observed_pods, 2);
        assert_eq!(trace.solver_status, "optimal");
        assert_eq!(trace.solve_millis, 15);
        assert_eq!(trace.decisions.len(), 2);
        assert_eq!(
            trace.decisions[0].placement,
            PodPlacement::Placed { node: "node-1".to_string() }
        );
        assert_eq!(trace.decisions[1].placement, PodPlacement::Unplaced);
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p ksolver scheduler::shadow`
Expected: FAIL — `cannot find function build_decision_trace`.

- [ ] **Step 4: Implement the builder**

Prepend to `ksolver/src/scheduler/shadow.rs`:

```rust
use crate::model::OptimizationSolution;
use crate::scheduler::pod_filter::PendingGpuPod;
use crate::scheduler::trace::{DecisionTrace, PodDecision, PodPlacement};

/// Build a shadow decision trace from the existing solver's solution.
/// `workload_id_for` maps each observed pod to the workload id the solver used,
/// so we can look up its assigned node in `solution.assignments`.
pub fn build_decision_trace(
    sequence: u64,
    pending: &[PendingGpuPod],
    workload_id_for: &dyn Fn(&PendingGpuPod) -> String,
    solution: &OptimizationSolution,
    solver_status: &str,
    solve_millis: u64,
) -> DecisionTrace {
    let mut decisions = Vec::with_capacity(pending.len());
    for p in pending {
        let id = workload_id_for(p);
        let placement = match solution.assignments.get(&id) {
            Some(node) if !node.is_empty() => PodPlacement::Placed { node: node.clone() },
            _ => PodPlacement::Unplaced,
        };
        decisions.push(PodDecision {
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
        note: String::new(),
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p ksolver scheduler::shadow`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add ksolver/src/scheduler/shadow.rs
git commit -m "feat(scheduler): pure shadow decision-trace builder"
```

---

## Task 6: Shadow watch loop + HTTP server (I/O wiring)

Wires the pure pieces to kube-rs and the existing solve pipeline. This task has no new unit tests (it is thin I/O over already-tested units); it is verified by compilation, clippy, and the manual run in Task 8.

**Files:**
- Modify: `ksolver/src/scheduler/shadow.rs` (append the loop + server; keep the tested builder untouched)

**Interfaces:**
- Consumes: `ShadowConfig`, `pod_filter::classify`, `build_decision_trace`, `TraceStore`, `metrics::*`, `collector::KubeCollector`, `normalizer::Normalizer`, `optimizer_input::build_input`, `cpsat_rust`, and `crate::model::ScenarioConfig`.
- Produces:
  - `pub async fn run_shadow(cfg: ShadowConfig) -> anyhow::Result<()>`

- [ ] **Step 1: Confirm the collect→normalize→build_input→solve call shape**

Run: `sed -n '279,340p' ksolver/src/service.rs` and `sed -n '360,460p' ksolver/src/service.rs`
Expected: shows exact constructor/method calls for `Normalizer::new(pricing_catalog, options).normalize(&snapshot)`, `build_input(&normalized, ignore_unschedulable)`, and `cpsat_rust::solve(&input, &scenario)`. Mirror these calls exactly in Step 3, including how `pricing_catalog` and `NormalizerOptions` are obtained (use defaults where the analyzer uses defaults). If pricing is required, load with the same helper the analyzer uses (`crate::pricing`), passing an empty pricing file to get defaults.

- [ ] **Step 2: Append the watch loop and HTTP server to `shadow.rs`**

Append to `ksolver/src/scheduler/shadow.rs` (after the builder, before the `#[cfg(test)]` module):

```rust
use crate::model::ScenarioConfig;
use crate::scheduler::config::ShadowConfig;
use crate::scheduler::pod_filter::classify;
use crate::scheduler::trace::TraceStore;
use crate::{cpsat_rust, metrics};
use anyhow::Result;
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use futures_util::StreamExt;
use k8s_openapi::api::core::v1 as corev1;
use kube::runtime::watcher;
use kube::{Api, Client};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{error, info, warn};

#[derive(Clone)]
struct ShadowState {
    traces: Arc<TraceStore>,
}

async fn traces_handler(State(state): State<ShadowState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "traces": state.traces.recent() }))
}

async fn metrics_handler() -> (axum::http::StatusCode, [(&'static str, &'static str); 1], String) {
    (
        axum::http::StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        metrics::render_metrics(),
    )
}

/// Run the shadow-mode scheduler: observe pending GPU pods, periodically solve,
/// record decision traces, and serve them over HTTP. NEVER binds pods.
pub async fn run_shadow(cfg: ShadowConfig) -> Result<()> {
    metrics::register_metrics();
    let traces = Arc::new(TraceStore::new(64));

    // Shared, observed set of pending GPU pods keyed by "namespace/name".
    let observed: Arc<Mutex<BTreeMap<String, crate::scheduler::pod_filter::PendingGpuPod>>> =
        Arc::new(Mutex::new(BTreeMap::new()));

    // HTTP server for traces + metrics (for the simulator/UI).
    let http_state = ShadowState { traces: traces.clone() };
    let app = Router::new()
        .route("/api/scheduler/traces", get(traces_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(http_state);
    let http_addr = cfg.http_addr.clone();
    tokio::spawn(async move {
        match tokio::net::TcpListener::bind(&http_addr).await {
            Ok(listener) => {
                info!(addr = %http_addr, "shadow scheduler HTTP server listening");
                if let Err(err) = axum::serve(listener, app).await {
                    error!(error = %err, "shadow HTTP server failed");
                }
            }
            Err(err) => error!(error = %err, addr = %http_addr, "failed to bind shadow HTTP addr"),
        }
    });

    // Watch pending pods and maintain the observed set.
    let client = build_shadow_client(&cfg).await?;
    let pods_api: Api<corev1::Pod> = Api::all(client);
    let watch_cfg = cfg.clone();
    let watch_observed = observed.clone();
    tokio::spawn(async move {
        let wc = watcher::Config::default();
        let mut stream = watcher(pods_api, wc).boxed();
        while let Some(event) = stream.next().await {
            match event {
                Ok(watcher::Event::Apply(pod)) | Ok(watcher::Event::InitApply(pod)) => {
                    let key = format!(
                        "{}/{}",
                        pod.metadata.namespace.clone().unwrap_or_default(),
                        pod.metadata.name.clone().unwrap_or_default()
                    );
                    match classify(&pod, &watch_cfg) {
                        Some(p) => {
                            watch_observed.lock().await.insert(key, p);
                        }
                        None => {
                            watch_observed.lock().await.remove(&key);
                        }
                    }
                }
                Ok(watcher::Event::Delete(pod)) => {
                    let key = format!(
                        "{}/{}",
                        pod.metadata.namespace.clone().unwrap_or_default(),
                        pod.metadata.name.clone().unwrap_or_default()
                    );
                    watch_observed.lock().await.remove(&key);
                }
                Ok(_) => {}
                Err(err) => warn!(error = %err, "pod watch error; continuing"),
            }
        }
        warn!("pod watch stream ended");
    });

    // Batch window: periodically snapshot, solve, and record a trace.
    let mut ticker = interval(cfg.batch_window);
    loop {
        ticker.tick().await;
        let pending: Vec<_> = observed.lock().await.values().cloned().collect();
        if pending.is_empty() {
            continue;
        }
        metrics::inc_shadow_pods_observed(pending.len() as u64);
        let seq = traces.next_sequence();
        match run_one_solve(&cfg, seq, &pending).await {
            Ok(trace) => {
                let unplaced = trace
                    .decisions
                    .iter()
                    .filter(|d| matches!(d.placement, crate::scheduler::trace::PodPlacement::Unplaced))
                    .count() as u64;
                metrics::inc_shadow_unplaced(unplaced);
                info!(
                    sequence = trace.sequence,
                    observed = trace.observed_pods,
                    unplaced,
                    status = %trace.solver_status,
                    solve_millis = trace.solve_millis,
                    "shadow decision recorded (bound nothing)"
                );
                traces.push(trace);
            }
            Err(err) => error!(error = %err, "shadow solve failed"),
        }
    }
}

async fn build_shadow_client(cfg: &ShadowConfig) -> Result<Client> {
    // Reuse the collector's client so kubeconfig/in-cluster handling is identical.
    let collector =
        crate::collector::KubeCollector::new(cfg.cluster_name.clone(), cfg.kubeconfig.clone())
            .await?;
    Ok(collector.client())
}

async fn run_one_solve(
    cfg: &ShadowConfig,
    sequence: u64,
    pending: &[crate::scheduler::pod_filter::PendingGpuPod],
) -> Result<crate::scheduler::trace::DecisionTrace> {
    metrics::inc_shadow_solves();
    let started = Instant::now();

    // 1. Snapshot the cluster via the existing collector.
    let collector =
        crate::collector::KubeCollector::new(cfg.cluster_name.clone(), cfg.kubeconfig.clone())
            .await?;
    let snapshot = collector.collect().await?;

    // 2. Normalize + build input + solve, mirroring service::Analyzer.
    //    Use default pricing and default normalizer options (see Step 1 findings).
    let pricing_catalog = crate::pricing::load_catalog("").unwrap_or_default();
    let normalizer_options = crate::normalizer::Options::default();
    let normalized = crate::normalizer::Normalizer::new(pricing_catalog, normalizer_options)
        .normalize(&snapshot);
    let input = crate::optimizer_input::build_input(&normalized, true);

    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        ignore_unschedulable_workloads: true,
        ..Default::default()
    };

    let (solution, status) = match cpsat_rust::solve(&input, &scenario) {
        Ok((sol, info)) => (sol, format!("{:?}", info.status)),
        Err(err) => {
            warn!(error = %err, "solver returned error; recording as infeasible");
            (Default::default(), "error".to_string())
        }
    };

    let solve_millis = started.elapsed().as_millis() as u64;
    metrics::observe_shadow_solve_seconds(started.elapsed().as_secs_f64());

    let id_for = |p: &crate::scheduler::pod_filter::PendingGpuPod| {
        format!("{}/{}", p.namespace, p.name)
    };
    Ok(build_decision_trace(
        sequence,
        pending,
        &id_for,
        &solution,
        &status,
        solve_millis,
    ))
}
```

Notes for the implementer:
- `KubeCollector` currently holds a private `client` field. Add a public accessor to `collector.rs`: `pub fn client(&self) -> Client { self.client.clone() }`. This is a one-line addition; include it in this task's commit.
- `SolverInfo.status` may not implement `Debug`/exist under that name — check `sed -n '/pub struct SolverInfo/,/^}/p' ksolver/src/model.rs` and format whatever status/objective field exists (fall back to `"solved"` if there is no status field). Keep the string human-readable.
- `crate::pricing::load_catalog` / `crate::normalizer::Options` names must match the real API surfaced in Step 1 of this task and Task 5 Step 1; adjust the calls to the actual signatures (the analyzer is the source of truth).

- [ ] **Step 2b: Add the client accessor to the collector**

Modify `ksolver/src/collector.rs` — inside `impl KubeCollector`, add:

```rust
    /// Clone of the underlying Kubernetes client (used by the shadow scheduler).
    pub fn client(&self) -> Client {
        self.client.clone()
    }
```

- [ ] **Step 3: Build with the solver feature to typecheck the full path**

Run: `cargo build -p ksolver --features rust-cp-sat`
Expected: compiles. Fix any signature mismatches against the real analyzer API (the compiler errors will point to the exact call to correct). Do not change the tested builder or pure units.

- [ ] **Step 4: Run all existing unit tests (no feature) to confirm nothing broke**

Run: `cargo test -p ksolver`
Expected: PASS (all prior tasks' tests still green).

- [ ] **Step 5: Clippy**

Run: `cargo clippy -p ksolver --features rust-cp-sat --all-targets`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add ksolver/src/scheduler/shadow.rs ksolver/src/collector.rs
git commit -m "feat(scheduler): shadow watch loop, solve call path, and HTTP traces"
```

---

## Task 7: `shadow` subcommand + docs

**Files:**
- Modify: `ksolver/src/main.rs` (add a `shadow` arm)
- Modify: `ksolver/README.md`

**Interfaces:**
- Consumes: `scheduler::config::ShadowConfig::from_env`, `scheduler::shadow::run_shadow`.

- [ ] **Step 1: Add the subcommand**

In `ksolver/src/main.rs`, add a new match arm before the `_ =>` fallback:

```rust
        Some("shadow") => {
            metrics::register_metrics();
            let cfg = ksolver::scheduler::config::ShadowConfig::from_env();
            info!(
                scheduler_name = %cfg.scheduler_name,
                batch_seconds = cfg.batch_window.as_secs(),
                http_addr = %cfg.http_addr,
                namespaces = ?cfg.namespace_allowlist,
                "starting shadow-mode GPU scheduler (binds nothing)"
            );
            ksolver::scheduler::shadow::run_shadow(cfg).await?;
        }
```

Also extend the usage string in the `_ =>` arm to include:

```
  syslens-solver shadow
```

- [ ] **Step 2: Build to confirm wiring**

Run: `cargo build -p ksolver --features rust-cp-sat`
Expected: compiles.

- [ ] **Step 3: Document in README**

Add a section to `ksolver/README.md`:

```markdown
## Shadow-mode GPU scheduler

Run the scheduler in shadow mode — it observes pending pods with
`schedulerName: ksolver` that request GPUs, computes where they *would* be
placed, records decision traces, and **binds nothing**:

    KUBECONFIG=~/.kube/config \
    KSOLVER_SHADOW_SCHEDULER_NAME=ksolver \
    KSOLVER_SHADOW_BATCH_SECONDS=10 \
    cargo run --features rust-cp-sat -- shadow

Environment variables:

- `KSOLVER_SHADOW_SCHEDULER_NAME` (default `ksolver`) — pods whose `spec.schedulerName` matches are in scope.
- `KSOLVER_SHADOW_BATCH_SECONDS` (default `10`) — batch window between solves.
- `KSOLVER_SHADOW_NAMESPACES` — comma-separated namespace allowlist (empty = all).
- `KSOLVER_SHADOW_GPU_PREFIXES` (default `nvidia.com/gpu`) — resource-name prefixes counted as GPUs.
- `KSOLVER_SHADOW_ADDR` (default `127.0.0.1:8090`) — serves `GET /api/scheduler/traces` and `/metrics`.

Shadow mode never calls the Binding or Eviction APIs.
```

- [ ] **Step 4: Commit**

```bash
cargo fmt
git add ksolver/src/main.rs ksolver/README.md
git commit -m "feat(scheduler): add shadow subcommand and docs"
```

---

## Task 8: Manual verification against a cluster

No code — a verification checklist proving shadow mode works end to end and binds nothing.

- [ ] **Step 1: Full test + lint gate**

Run: `cargo test -p ksolver && cargo clippy -p ksolver --features rust-cp-sat --all-targets`
Expected: all tests pass; clippy clean.

- [ ] **Step 2: Start shadow mode against a test cluster (kind/minikube with a fake GPU resource, or a real dev cluster)**

Run:
```bash
KUBECONFIG=$HOME/.kube/config KSOLVER_SHADOW_BATCH_SECONDS=5 \
  cargo run --features rust-cp-sat -- shadow
```
Expected logs: `starting shadow-mode GPU scheduler (binds nothing)` and `shadow scheduler HTTP server listening`.

- [ ] **Step 3: Create a pending GPU pod that opts into ksolver**

Apply a pod with `spec.schedulerName: ksolver` and a `nvidia.com/gpu` request (on kind without GPUs it will stay Pending, which is exactly what shadow mode observes). Expected: within one batch window, a log line `shadow decision recorded (bound nothing)`.

- [ ] **Step 4: Confirm a trace is served**

Run: `curl -s localhost:8090/api/scheduler/traces | jq '.traces[0]'`
Expected: a JSON `DecisionTrace` with `observed_pods >= 1` and a `decisions` array containing the pod with a `placement` of `placed` or `unplaced`.

- [ ] **Step 5: Confirm NOTHING was bound**

Run: `kubectl get pod <pod> -o jsonpath='{.spec.nodeName}'`
Expected: empty (pod still unbound). This is the core safety guarantee of shadow mode.

- [ ] **Step 6: Confirm metrics**

Run: `curl -s localhost:8090/metrics | grep ksolver_shadow_`
Expected: `ksolver_shadow_pods_observed_total`, `ksolver_shadow_solves_total`, `ksolver_shadow_solve_seconds`, `ksolver_shadow_unplaced_total` present with non-zero values.

---

## Self-Review Notes (coverage vs Phase 1 of the spec)

- Spec §9 phase 1 "translate live state, compute decisions, bind nothing, emit traces" → Tasks 2 (classify), 5/6 (translate+solve), 3/6 (traces), 8 (bind-nothing proof). ✅
- Spec §3 "simulator displays what the scheduler is doing" → Task 6 `/api/scheduler/traces` endpoint (basic traces; rich replay deferred per spec §11). ✅
- Spec §10 safety "dry-run / never mutate" → shadow mode is dry-run by construction; verified in Task 8 Step 5. ✅
- Spec §2 "NVIDIA-first GPU detection" → Task 2 `gpu_request` prefix match, configurable. ✅
- Deferred correctly (NOT in this plan): reservation ledger, feasibility conformance suite, real L1 formulation, gang atomicity, preemption, topology, fractional, fair-share — these are Phases 2–10 and get their own plans.
- Out-of-scope note surfaced: this phase reuses the existing whole-cluster cost solve as the "call path" scaffolding; the dedicated place-these-pending-pods L1 formulation (spec §4) lands in Phase 4 and will replace `run_one_solve`'s solve step.
```
