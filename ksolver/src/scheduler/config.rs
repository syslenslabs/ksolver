use std::collections::BTreeMap;
use std::time::Duration;

use crate::model::{ObjectiveProfile, ObjectiveWeights};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingCanaryMode {
    /// Apply every ready binding candidate, subject to the usual safety checks.
    All,
    /// Apply only low-risk candidates, currently bounded by GPU request size.
    LowRisk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingRolloutMode {
    /// Observe and render recommendations only; no mutation-capable binding client is created.
    ObserveOnly,
    /// POST bindings with server-side dryRun=All; validates without persisting.
    DryRun,
    /// Persist only low-risk binding candidates, currently bounded by GPU request size.
    BindLowRisk,
    /// Persist every ready binding candidate, subject to all other safety gates.
    BindAll,
}

/// Shadow-mode scheduler configuration, sourced from environment variables.
#[derive(Debug, Clone)]
pub struct ShadowConfig {
    pub scheduler_name: String,
    pub batch_window: Duration,
    pub namespace_allowlist: Vec<String>,
    /// Exact resource names counted as GPUs (e.g. "nvidia.com/gpu").
    pub gpu_resource_names: Vec<String>,
    /// Resource-name prefixes counted as GPUs — for MIG (mixed strategy) slices like
    /// `nvidia.com/mig-1g.5gb`. Default `["nvidia.com/mig-"]`.
    pub gpu_resource_prefixes: Vec<String>,
    pub cluster_name: String,
    pub kubeconfig: String,
    pub http_addr: String,
    /// Optional pod label whose value must be "true" before the future admission webhook patches
    /// schedulerName. Empty means every in-scope GPU pod can be patched.
    pub admission_opt_in_label: String,
    /// Pod label whose value groups pods into a gang (all-or-nothing). Empty disables grouping.
    pub gang_label_key: String,
    /// Pod label whose value "true" marks a gang as requiring single-node co-location.
    pub gang_colocate_label: String,
    /// CP-SAT solve time limit (seconds) for shadow solves — accept the best incumbent
    /// within this budget rather than proving optimality. Default 10.
    pub solve_time_limit_secs: i64,
    /// Per-namespace GPU quotas (namespace -> max GPUs). Namespaces absent from the map
    /// are unconstrained. Enforced as a hard cap on admitted pods in that namespace.
    pub namespace_gpu_quotas: BTreeMap<String, i64>,
    /// Optional tenant fair-share weights (tenant -> positive weight). Tenants absent from
    /// the map use weight 1 in trace metrics; this is observability-only, not enforcement.
    pub tenant_share_weights: BTreeMap<String, i64>,
    /// Optional tenant monthly budget caps in milli-currency units (tenant -> monthly budget * 1000).
    /// Shadow solves enforce these as hard pending-admission caps after subtracting already-running
    /// GPU placement cost.
    pub tenant_monthly_budgets_milli: BTreeMap<String, i64>,
    /// Optional queue policy scores (`ksolver.dev/queue` value -> non-negative score). Scores are
    /// inert unless the GPU-aware objective queue weight is also positive.
    pub queue_weights: BTreeMap<String, i64>,
    /// PHASE 3 (real binding): when true, the scheduler ACTUALLY POSTs pod→node bindings for
    /// `readiness: ready` decisions (mutates the cluster). Default FALSE — shadow stays read-only.
    pub enable_real_binding: bool,
    /// Coarse production rollout stage. `KSOLVER_BINDING_ROLLOUT_MODE`, when set, derives the
    /// legacy binding flags from one operator-facing mode.
    pub binding_rollout_mode: BindingRolloutMode,
    /// Emergency fail-closed guard: when true, no real binding client is created and the binder
    /// skips all mutation even if `enable_real_binding` is also true. Default FALSE.
    pub binding_kill_switch: bool,
    /// When true, ksolver may POST Kubernetes Event objects for auditability. Default FALSE.
    /// The binding kill switch disables this mutation path too.
    pub enable_kubernetes_events: bool,
    /// When real binding is enabled, send the binding with server-side `dryRun=All` (the apiserver
    /// validates but does NOT persist). A safe intermediate before live binding. Default false.
    pub real_binding_dry_run: bool,
    /// Canary policy for real binding. Default `All` preserves the explicit opt-in behavior.
    pub binding_canary_mode: BindingCanaryMode,
    /// Max GPU request considered low-risk when `binding_canary_mode == LowRisk`.
    pub binding_low_risk_max_gpus: i64,
    /// Upper bound on bindings applied per solve pass (throttle). Default 10.
    pub max_binds_per_pass: usize,
    /// TTL for binding reservations held after live binds until informer state catches up.
    /// Default 60 seconds.
    pub binding_reservation_ttl: Duration,
    /// Solver objective profile used by shadow GPU scheduling.
    pub objective_profile: ObjectiveProfile,
    /// Solver objective weights used by objective profiles that need extra policy weights.
    pub objective_weights: ObjectiveWeights,
    /// Optional cap on feasible candidate nodes per pending workload/gang before CP-SAT model build.
    /// 0 disables pruning and preserves the full feasible set.
    pub candidate_node_limit: usize,
    /// Admission ratio below which a pruned solve is considered suspicious and retried with wider
    /// candidates. Stored as milli-percent; 50_000 means 50.000%. 0 disables this widening trigger.
    pub candidate_widen_min_admission_percent_milli: i64,
    /// Opt-in symmetry reduction for homogeneous pending-input nodes. Disabled by default until
    /// more production traces prove grouped expansion safety across real fleet shapes.
    pub enable_node_grouping: bool,
    /// Upper bound on running GPU pods considered per node when rendering dry-run repair plans.
    pub repair_candidate_limit: usize,
    /// Opt-in HA coordination switch. When enabled, shadow solve/bind passes require this replica
    /// to hold the configured coordination.k8s.io Lease.
    pub enable_leader_election: bool,
    /// Namespace that will hold the coordination.k8s.io Lease.
    pub leader_election_namespace: String,
    /// Lease name used by ksolver scheduler replicas.
    pub leader_election_lease_name: String,
    /// Stable identity for this scheduler replica when participating in leader election.
    pub leader_election_identity: String,
}

/// Parse `KSOLVER_SHADOW_QUOTAS="ns=cap,ns2=cap2"` (pure/testable). Entries with a
/// non-negative integer cap and non-empty namespace are kept; malformed parts skipped.
fn parse_quotas(v: Option<String>) -> BTreeMap<String, i64> {
    let mut m = BTreeMap::new();
    if let Some(s) = v {
        for part in s.split(',') {
            let p = part.trim();
            if let Some((k, val)) = p.split_once('=') {
                if let Ok(n) = val.trim().parse::<i64>() {
                    if n >= 0 && !k.trim().is_empty() {
                        m.insert(k.trim().to_string(), n);
                    }
                }
            }
        }
    }
    m
}

/// Parse `KSOLVER_SHADOW_TENANT_WEIGHTS="tenant=weight,tenant2=weight2"`.
/// Positive integer weights are kept; malformed, zero, and negative entries are skipped.
fn parse_tenant_share_weights(v: Option<String>) -> BTreeMap<String, i64> {
    let mut m = BTreeMap::new();
    if let Some(s) = v {
        for part in s.split(',') {
            let p = part.trim();
            if let Some((k, val)) = p.split_once('=') {
                if let Ok(n) = val.trim().parse::<i64>() {
                    if n > 0 && !k.trim().is_empty() {
                        m.insert(k.trim().to_string(), n);
                    }
                }
            }
        }
    }
    m
}

/// Parse `KSOLVER_SHADOW_TENANT_MONTHLY_BUDGETS="tenant=1234.56,tenant2=5000"`.
/// Values are monthly currency units and are stored as milli-units for trace stability.
fn parse_tenant_monthly_budgets_milli(v: Option<String>) -> BTreeMap<String, i64> {
    let mut m = BTreeMap::new();
    if let Some(s) = v {
        for part in s.split(',') {
            let p = part.trim();
            if let Some((k, val)) = p.split_once('=') {
                if let Ok(n) = val.trim().parse::<f64>() {
                    if n >= 0.0 && n.is_finite() && !k.trim().is_empty() {
                        m.insert(k.trim().to_string(), (n * 1000.0).round() as i64);
                    }
                }
            }
        }
    }
    m
}

/// Parse `KSOLVER_SHADOW_QUEUE_WEIGHTS="urgent=100,batch=10"`.
/// Non-negative integer scores are kept; malformed and negative entries are skipped.
fn parse_queue_weights(v: Option<String>) -> BTreeMap<String, i64> {
    let mut m = BTreeMap::new();
    if let Some(s) = v {
        for part in s.split(',') {
            let p = part.trim();
            if let Some((k, val)) = p.split_once('=') {
                if let Ok(n) = val.trim().parse::<i64>() {
                    if n >= 0 && !k.trim().is_empty() {
                        m.insert(k.trim().to_string(), n);
                    }
                }
            }
        }
    }
    m
}

/// Parse `KSOLVER_SHADOW_SOLVE_SECS` (pure/testable): positive int or default 10.
fn parse_solve_secs(v: Option<String>) -> i64 {
    v.and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(10)
}

/// Parse a boolean env value (pure/testable): only "true"/"1" (case-insensitive) ⇒ true.
fn parse_bool(v: Option<String>) -> bool {
    matches!(
        v.map(|s| s.trim().to_ascii_lowercase()).as_deref(),
        Some("true") | Some("1")
    )
}

/// Parse `max_binds_per_pass` (pure/testable): positive int or default 10.
fn parse_max_binds(v: Option<String>) -> usize {
    v.and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(10)
}

fn parse_i64_env(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(default)
}

fn parse_i64_env_with_fallback(primary: &str, fallback: &str, default: i64) -> i64 {
    std::env::var(primary)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or_else(|| parse_i64_env(fallback, default))
}

fn parse_usize_env(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(default)
}

fn parse_candidate_node_limit(v: Option<String>) -> usize {
    v.and_then(|s| s.trim().parse::<usize>().ok()).unwrap_or(16)
}

fn parse_candidate_widen_min_admission_percent_milli(v: Option<String>) -> i64 {
    v.and_then(|s| s.trim().parse::<i64>().ok())
        .filter(|n| (0..=100).contains(n))
        .unwrap_or(50)
        .saturating_mul(1000)
}

fn env_or_fallback(primary: Option<String>, fallback: Option<String>, default: &str) -> String {
    primary
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            fallback
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| default.to_string())
}

fn resolve_leader_election_namespace(
    configured: Option<String>,
    pod_namespace: Option<String>,
) -> String {
    env_or_fallback(configured, pod_namespace, "ksolver")
}

fn resolve_leader_election_identity(
    configured: Option<String>,
    hostname: Option<String>,
) -> String {
    env_or_fallback(configured, hostname, "ksolver")
}

fn parse_objective_profile(v: Option<String>) -> ObjectiveProfile {
    match v
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("gpu-gang-aware") | Some("gpu") | Some("gpu_throughput") | Some("gpu-throughput") => {
            ObjectiveProfile::GpuGangAware
        }
        _ => ObjectiveProfile::CostBinpack,
    }
}

fn parse_binding_canary_mode(v: Option<String>) -> Option<BindingCanaryMode> {
    match v
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("all") => Some(BindingCanaryMode::All),
        Some("low-risk") | Some("low_risk") | Some("canary") => Some(BindingCanaryMode::LowRisk),
        _ => None,
    }
}

fn parse_binding_rollout_mode(v: Option<String>) -> Option<BindingRolloutMode> {
    match v
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("observe") | Some("observe-only") | Some("observe_only") | Some("shadow") => {
            Some(BindingRolloutMode::ObserveOnly)
        }
        Some("dry-run") | Some("dry_run") | Some("dryrun") | Some("validate") => {
            Some(BindingRolloutMode::DryRun)
        }
        Some("bind-low-risk") | Some("bind_low_risk") | Some("low-risk") | Some("canary") => {
            Some(BindingRolloutMode::BindLowRisk)
        }
        Some("bind-all") | Some("bind_all") | Some("live") | Some("all") => {
            Some(BindingRolloutMode::BindAll)
        }
        _ => None,
    }
}

fn infer_binding_rollout_mode(
    enable_real_binding: bool,
    real_binding_dry_run: bool,
    binding_canary_mode: BindingCanaryMode,
) -> BindingRolloutMode {
    if !enable_real_binding {
        BindingRolloutMode::ObserveOnly
    } else if real_binding_dry_run {
        BindingRolloutMode::DryRun
    } else if binding_canary_mode == BindingCanaryMode::LowRisk {
        BindingRolloutMode::BindLowRisk
    } else {
        BindingRolloutMode::BindAll
    }
}

fn resolve_legacy_binding_canary_mode(v: Option<String>) -> BindingCanaryMode {
    match v {
        None => BindingCanaryMode::All,
        Some(raw) => parse_binding_canary_mode(Some(raw)).unwrap_or(BindingCanaryMode::LowRisk),
    }
}

fn resolve_binding_rollout_mode(
    rollout_mode_env: Option<String>,
    enable_real_binding: bool,
    real_binding_dry_run: bool,
    binding_canary_mode: BindingCanaryMode,
) -> BindingRolloutMode {
    match rollout_mode_env {
        Some(raw) => {
            parse_binding_rollout_mode(Some(raw)).unwrap_or(BindingRolloutMode::ObserveOnly)
        }
        None => infer_binding_rollout_mode(
            enable_real_binding,
            real_binding_dry_run,
            binding_canary_mode,
        ),
    }
}

fn rollout_mode_flags(mode: BindingRolloutMode) -> (bool, bool, BindingCanaryMode) {
    match mode {
        BindingRolloutMode::ObserveOnly => (false, false, BindingCanaryMode::All),
        BindingRolloutMode::DryRun => (true, true, BindingCanaryMode::All),
        BindingRolloutMode::BindLowRisk => (true, false, BindingCanaryMode::LowRisk),
        BindingRolloutMode::BindAll => (true, false, BindingCanaryMode::All),
    }
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
        let mut gpu_resource_prefixes = csv_env("KSOLVER_SHADOW_GPU_RESOURCE_PREFIXES");
        if gpu_resource_prefixes.is_empty() {
            gpu_resource_prefixes = vec!["nvidia.com/mig-".to_string()];
        }
        let objective_profile =
            parse_objective_profile(std::env::var("KSOLVER_OBJECTIVE_PROFILE").ok());
        let default_weights = ObjectiveWeights::default();
        let objective_weights = ObjectiveWeights {
            admission: parse_i64_env("KSOLVER_GPU_ADMISSION_WEIGHT", default_weights.admission),
            gpu_demand: parse_i64_env("KSOLVER_GPU_DEMAND_WEIGHT", default_weights.gpu_demand),
            gang_complete: parse_i64_env(
                "KSOLVER_GPU_GANG_COMPLETE_WEIGHT",
                default_weights.gang_complete,
            ),
            priority: parse_i64_env("KSOLVER_GPU_PRIORITY_WEIGHT", default_weights.priority),
            business_value: parse_i64_env(
                "KSOLVER_GPU_BUSINESS_VALUE_WEIGHT",
                default_weights.business_value,
            ),
            queue: parse_i64_env("KSOLVER_GPU_QUEUE_WEIGHT", default_weights.queue),
            queue_wait: parse_i64_env("KSOLVER_GPU_QUEUE_WAIT_WEIGHT", default_weights.queue_wait),
            fair_share: parse_i64_env("KSOLVER_GPU_FAIR_SHARE_WEIGHT", default_weights.fair_share),
            deadline_urgency: parse_i64_env_with_fallback(
                "KSOLVER_GPU_DEADLINE_URGENCY_WEIGHT",
                "KSOLVER_GPU_DEADLINE_WEIGHT",
                default_weights.deadline_urgency,
            ),
            deadline_miss: parse_i64_env(
                "KSOLVER_GPU_DEADLINE_MISS_WEIGHT",
                default_weights.deadline_miss,
            ),
            gpu_fragmentation: parse_i64_env(
                "KSOLVER_GPU_FRAGMENTATION_WEIGHT",
                default_weights.gpu_fragmentation,
            ),
        };
        let legacy_enable_real_binding =
            parse_bool(std::env::var("KSOLVER_ENABLE_REAL_BINDING").ok());
        let legacy_real_binding_dry_run =
            parse_bool(std::env::var("KSOLVER_REAL_BINDING_DRY_RUN").ok());
        let legacy_canary_mode =
            resolve_legacy_binding_canary_mode(std::env::var("KSOLVER_BINDING_CANARY_MODE").ok());
        let rollout_mode_env = std::env::var("KSOLVER_BINDING_ROLLOUT_MODE").ok();
        let rollout_mode_override = rollout_mode_env.is_some();
        let rollout_mode = resolve_binding_rollout_mode(
            rollout_mode_env,
            legacy_enable_real_binding,
            legacy_real_binding_dry_run,
            legacy_canary_mode,
        );
        let (enable_real_binding, real_binding_dry_run, binding_canary_mode) =
            if rollout_mode_override {
                rollout_mode_flags(rollout_mode)
            } else {
                (
                    legacy_enable_real_binding,
                    legacy_real_binding_dry_run,
                    legacy_canary_mode,
                )
            };

        Self {
            scheduler_name: std::env::var("KSOLVER_SHADOW_SCHEDULER_NAME")
                .unwrap_or_else(|_| "ksolver".to_string()),
            batch_window: Duration::from_secs(batch_secs),
            namespace_allowlist: csv_env("KSOLVER_SHADOW_NAMESPACES"),
            gpu_resource_names,
            gpu_resource_prefixes,
            cluster_name: std::env::var("KSOLVER_CLUSTER_NAME")
                .unwrap_or_else(|_| "default".to_string()),
            kubeconfig: std::env::var("KUBECONFIG").unwrap_or_default(),
            http_addr: std::env::var("KSOLVER_SHADOW_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8090".to_string()),
            admission_opt_in_label: std::env::var("KSOLVER_ADMISSION_OPT_IN_LABEL")
                .unwrap_or_default(),
            gang_label_key: std::env::var("KSOLVER_SHADOW_GANG_LABEL")
                .unwrap_or_else(|_| "scheduling.x-k8s.io/pod-group".to_string()),
            gang_colocate_label: std::env::var("KSOLVER_SHADOW_COLOCATE_LABEL")
                .unwrap_or_else(|_| "scheduling.x-k8s.io/gang-colocate".to_string()),
            solve_time_limit_secs: parse_solve_secs(
                std::env::var("KSOLVER_SHADOW_SOLVE_SECS").ok(),
            ),
            namespace_gpu_quotas: parse_quotas(std::env::var("KSOLVER_SHADOW_QUOTAS").ok()),
            tenant_share_weights: parse_tenant_share_weights(
                std::env::var("KSOLVER_SHADOW_TENANT_WEIGHTS").ok(),
            ),
            tenant_monthly_budgets_milli: parse_tenant_monthly_budgets_milli(
                std::env::var("KSOLVER_SHADOW_TENANT_MONTHLY_BUDGETS").ok(),
            ),
            queue_weights: parse_queue_weights(std::env::var("KSOLVER_SHADOW_QUEUE_WEIGHTS").ok()),
            enable_real_binding,
            binding_rollout_mode: rollout_mode,
            binding_kill_switch: parse_bool(std::env::var("KSOLVER_BINDING_KILL_SWITCH").ok()),
            enable_kubernetes_events: parse_bool(
                std::env::var("KSOLVER_ENABLE_KUBERNETES_EVENTS").ok(),
            ),
            real_binding_dry_run,
            binding_canary_mode,
            binding_low_risk_max_gpus: parse_i64_env("KSOLVER_BINDING_LOW_RISK_MAX_GPUS", 1).max(0),
            max_binds_per_pass: parse_max_binds(std::env::var("KSOLVER_MAX_BINDS_PER_PASS").ok()),
            binding_reservation_ttl: Duration::from_secs(
                std::env::var("KSOLVER_BINDING_RESERVATION_TTL_SECONDS")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .filter(|s| *s > 0)
                    .unwrap_or(60),
            ),
            objective_profile,
            objective_weights,
            candidate_node_limit: parse_candidate_node_limit(
                std::env::var("KSOLVER_CANDIDATE_NODE_LIMIT").ok(),
            ),
            candidate_widen_min_admission_percent_milli:
                parse_candidate_widen_min_admission_percent_milli(
                    std::env::var("KSOLVER_CANDIDATE_WIDEN_MIN_ADMISSION_PERCENT").ok(),
                ),
            enable_node_grouping: parse_bool(std::env::var("KSOLVER_ENABLE_NODE_GROUPING").ok()),
            repair_candidate_limit: parse_usize_env("KSOLVER_REPAIR_CANDIDATE_LIMIT", 8).max(1),
            enable_leader_election: parse_bool(
                std::env::var("KSOLVER_ENABLE_LEADER_ELECTION").ok(),
            ),
            leader_election_namespace: resolve_leader_election_namespace(
                std::env::var("KSOLVER_LEADER_ELECTION_NAMESPACE").ok(),
                std::env::var("POD_NAMESPACE").ok(),
            ),
            leader_election_lease_name: std::env::var("KSOLVER_LEADER_ELECTION_LEASE_NAME")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "ksolver-scheduler".to_string()),
            leader_election_identity: resolve_leader_election_identity(
                std::env::var("KSOLVER_LEADER_ELECTION_IDENTITY").ok(),
                std::env::var("HOSTNAME").ok(),
            ),
        }
    }

    pub fn namespace_in_scope(&self, ns: &str) -> bool {
        self.namespace_allowlist.is_empty() || self.namespace_allowlist.iter().any(|n| n == ns)
    }

    /// Whether ksolver is allowed to create a mutation-capable binding client and POST bindings.
    pub fn real_binding_mutations_enabled(&self) -> bool {
        self.enable_real_binding && !self.binding_kill_switch
    }

    /// Whether ksolver is allowed to create Kubernetes Event objects.
    pub fn kubernetes_event_writes_enabled(&self) -> bool {
        self.enable_kubernetes_events && !self.binding_kill_switch
    }

    /// Whether this process should participate in Lease-based leader election.
    pub fn leader_election_configured(&self) -> bool {
        self.enable_leader_election
    }

    /// Whether an extended-resource name counts as a GPU: an exact `gpu_resource_names` match
    /// or a `gpu_resource_prefixes` prefix (MIG mixed-strategy slices, e.g. nvidia.com/mig-*).
    pub fn is_gpu_resource(&self, name: &str) -> bool {
        self.gpu_resource_names.iter().any(|n| n == name)
            || self
                .gpu_resource_prefixes
                .iter()
                .any(|p| name.starts_with(p))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> ShadowConfig {
        ShadowConfig {
            scheduler_name: "ksolver".to_string(),
            batch_window: std::time::Duration::from_secs(10),
            namespace_allowlist: vec![],
            gpu_resource_names: vec!["nvidia.com/gpu".to_string()],
            gpu_resource_prefixes: vec!["nvidia.com/mig-".to_string()],
            cluster_name: "default".to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
            admission_opt_in_label: String::new(),
            gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),
            gang_colocate_label: "scheduling.x-k8s.io/gang-colocate".to_string(),
            solve_time_limit_secs: 10,
            namespace_gpu_quotas: BTreeMap::new(),
            tenant_share_weights: BTreeMap::new(),
            tenant_monthly_budgets_milli: BTreeMap::new(),
            queue_weights: BTreeMap::new(),
            enable_real_binding: false,
            binding_rollout_mode: BindingRolloutMode::ObserveOnly,
            binding_kill_switch: false,
            enable_kubernetes_events: false,
            real_binding_dry_run: false,
            binding_canary_mode: BindingCanaryMode::All,
            binding_low_risk_max_gpus: 1,
            max_binds_per_pass: 10,
            binding_reservation_ttl: Duration::from_secs(60),
            objective_profile: ObjectiveProfile::CostBinpack,
            objective_weights: ObjectiveWeights::default(),
            candidate_node_limit: 0,
            candidate_widen_min_admission_percent_milli: 50_000,
            enable_node_grouping: false,
            repair_candidate_limit: 8,
            enable_leader_election: false,
            leader_election_namespace: "ksolver".to_string(),
            leader_election_lease_name: "ksolver-scheduler".to_string(),
            leader_election_identity: "ksolver".to_string(),
        }
    }

    #[test]
    fn parse_bool_only_true_or_one() {
        assert!(parse_bool(Some("true".to_string())));
        assert!(parse_bool(Some("TRUE".to_string())));
        assert!(parse_bool(Some("1".to_string())));
        assert!(!parse_bool(Some("false".to_string())));
        assert!(!parse_bool(Some("yes".to_string())));
        assert!(!parse_bool(None));
    }

    #[test]
    fn objective_weights_default_deadline_miss_is_inert() {
        assert_eq!(ObjectiveWeights::default().deadline_miss, 0);
    }

    #[test]
    fn real_binding_mutations_require_enable_and_no_kill_switch() {
        let mut cfg = base();
        assert!(!cfg.real_binding_mutations_enabled());

        cfg.enable_real_binding = true;
        assert!(cfg.real_binding_mutations_enabled());

        cfg.binding_kill_switch = true;
        assert!(!cfg.real_binding_mutations_enabled());
    }

    #[test]
    fn kubernetes_event_writes_require_enable_and_no_kill_switch() {
        let mut cfg = base();
        assert!(!cfg.kubernetes_event_writes_enabled());

        cfg.enable_kubernetes_events = true;
        assert!(cfg.kubernetes_event_writes_enabled());

        cfg.binding_kill_switch = true;
        assert!(!cfg.kubernetes_event_writes_enabled());
    }

    #[test]
    fn leader_election_is_explicitly_configured() {
        let mut cfg = base();
        assert!(!cfg.leader_election_configured());

        cfg.enable_leader_election = true;
        assert!(cfg.leader_election_configured());
    }

    #[test]
    fn leader_election_namespace_uses_pod_namespace_fallback() {
        assert_eq!(
            resolve_leader_election_namespace(
                Some("custom".to_string()),
                Some("pod-ns".to_string())
            ),
            "custom"
        );
        assert_eq!(
            resolve_leader_election_namespace(Some(" ".to_string()), Some("pod-ns".to_string())),
            "pod-ns"
        );
        assert_eq!(
            resolve_leader_election_namespace(None, Some(" ".to_string())),
            "ksolver"
        );
    }

    #[test]
    fn leader_election_identity_uses_hostname_fallback() {
        assert_eq!(
            resolve_leader_election_identity(
                Some("replica-a".to_string()),
                Some("pod-a".to_string())
            ),
            "replica-a"
        );
        assert_eq!(
            resolve_leader_election_identity(Some(" ".to_string()), Some("pod-a".to_string())),
            "pod-a"
        );
        assert_eq!(
            resolve_leader_election_identity(None, Some(" ".to_string())),
            "ksolver"
        );
    }

    #[test]
    fn parse_binding_canary_mode_accepts_known_values() {
        assert_eq!(parse_binding_canary_mode(None), None);
        assert_eq!(
            parse_binding_canary_mode(Some("low-risk".to_string())),
            Some(BindingCanaryMode::LowRisk)
        );
        assert_eq!(
            parse_binding_canary_mode(Some("canary".to_string())),
            Some(BindingCanaryMode::LowRisk)
        );
        assert_eq!(
            parse_binding_canary_mode(Some("all".to_string())),
            Some(BindingCanaryMode::All)
        );
        assert_eq!(parse_binding_canary_mode(Some("nope".to_string())), None);
    }

    #[test]
    fn legacy_canary_mode_defaults_all_but_invalid_value_fails_to_low_risk() {
        assert_eq!(
            resolve_legacy_binding_canary_mode(None),
            BindingCanaryMode::All
        );
        assert_eq!(
            resolve_legacy_binding_canary_mode(Some("all".to_string())),
            BindingCanaryMode::All
        );
        assert_eq!(
            resolve_legacy_binding_canary_mode(Some("low-risk".to_string())),
            BindingCanaryMode::LowRisk
        );
        assert_eq!(
            resolve_legacy_binding_canary_mode(Some("typo".to_string())),
            BindingCanaryMode::LowRisk
        );
    }

    #[test]
    fn parse_binding_rollout_mode_accepts_operator_modes() {
        assert_eq!(parse_binding_rollout_mode(None), None);
        assert_eq!(
            parse_binding_rollout_mode(Some("observe-only".to_string())),
            Some(BindingRolloutMode::ObserveOnly)
        );
        assert_eq!(
            parse_binding_rollout_mode(Some("dry-run".to_string())),
            Some(BindingRolloutMode::DryRun)
        );
        assert_eq!(
            parse_binding_rollout_mode(Some("bind-low-risk".to_string())),
            Some(BindingRolloutMode::BindLowRisk)
        );
        assert_eq!(
            parse_binding_rollout_mode(Some("live".to_string())),
            Some(BindingRolloutMode::BindAll)
        );
        assert_eq!(parse_binding_rollout_mode(Some("nope".to_string())), None);
    }

    #[test]
    fn rollout_mode_flags_map_to_existing_binding_gates() {
        assert_eq!(
            rollout_mode_flags(BindingRolloutMode::ObserveOnly),
            (false, false, BindingCanaryMode::All)
        );
        assert_eq!(
            rollout_mode_flags(BindingRolloutMode::DryRun),
            (true, true, BindingCanaryMode::All)
        );
        assert_eq!(
            rollout_mode_flags(BindingRolloutMode::BindLowRisk),
            (true, false, BindingCanaryMode::LowRisk)
        );
        assert_eq!(
            rollout_mode_flags(BindingRolloutMode::BindAll),
            (true, false, BindingCanaryMode::All)
        );
    }

    #[test]
    fn legacy_flags_infer_rollout_mode_when_no_mode_is_set() {
        assert_eq!(
            infer_binding_rollout_mode(false, false, BindingCanaryMode::All),
            BindingRolloutMode::ObserveOnly
        );
        assert_eq!(
            infer_binding_rollout_mode(true, true, BindingCanaryMode::All),
            BindingRolloutMode::DryRun
        );
        assert_eq!(
            infer_binding_rollout_mode(true, false, BindingCanaryMode::LowRisk),
            BindingRolloutMode::BindLowRisk
        );
        assert_eq!(
            infer_binding_rollout_mode(true, false, BindingCanaryMode::All),
            BindingRolloutMode::BindAll
        );
    }

    #[test]
    fn rollout_mode_resolver_fails_closed_when_operator_mode_is_invalid() {
        assert_eq!(
            resolve_binding_rollout_mode(
                Some("typo".to_string()),
                true,
                false,
                BindingCanaryMode::All
            ),
            BindingRolloutMode::ObserveOnly
        );
        assert_eq!(
            resolve_binding_rollout_mode(
                Some("bind-low-risk".to_string()),
                false,
                false,
                BindingCanaryMode::All
            ),
            BindingRolloutMode::BindLowRisk
        );
        assert_eq!(
            resolve_binding_rollout_mode(None, true, true, BindingCanaryMode::All),
            BindingRolloutMode::DryRun
        );
    }

    #[test]
    fn parse_max_binds_defaults_and_overrides() {
        assert_eq!(parse_max_binds(None), 10);
        assert_eq!(parse_max_binds(Some("0".to_string())), 10);
        assert_eq!(parse_max_binds(Some("3".to_string())), 3);
        assert_eq!(parse_max_binds(Some("nope".to_string())), 10);
    }

    #[test]
    fn parse_quotas_parses_and_skips_malformed() {
        assert!(parse_quotas(None).is_empty());
        assert!(parse_quotas(Some(String::new())).is_empty());
        let m = parse_quotas(Some("team-a=200, team-b=300".to_string()));
        assert_eq!(m.get("team-a"), Some(&200));
        assert_eq!(m.get("team-b"), Some(&300));
        // malformed / negative / empty-key entries are skipped; a valid zero is kept.
        let m2 = parse_quotas(Some("x,=5,ns=,bad=-1,ok=0,q=7".to_string()));
        assert_eq!(m2.get("ok"), Some(&0));
        assert_eq!(m2.get("q"), Some(&7));
        assert!(!m2.contains_key("bad"));
        assert!(!m2.contains_key("ns"));
        assert_eq!(m2.len(), 2);
    }

    #[test]
    fn parse_tenant_share_weights_keeps_positive_integer_weights() {
        assert!(parse_tenant_share_weights(None).is_empty());
        assert!(parse_tenant_share_weights(Some(String::new())).is_empty());
        let m = parse_tenant_share_weights(Some(
            "research=3, batch=1, bad=-1, zero=0, nope=x, =5".to_string(),
        ));
        assert_eq!(m.get("research"), Some(&3));
        assert_eq!(m.get("batch"), Some(&1));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn parse_tenant_monthly_budgets_milli_accepts_decimal_caps() {
        assert!(parse_tenant_monthly_budgets_milli(None).is_empty());
        assert!(parse_tenant_monthly_budgets_milli(Some(String::new())).is_empty());
        let m = parse_tenant_monthly_budgets_milli(Some(
            "research=1234.56,batch=0,bad=-1,nope=x,=5".to_string(),
        ));
        assert_eq!(m.get("research"), Some(&1_234_560));
        assert_eq!(m.get("batch"), Some(&0));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn parse_queue_weights_keeps_nonnegative_integer_scores() {
        assert!(parse_queue_weights(None).is_empty());
        assert!(parse_queue_weights(Some(String::new())).is_empty());
        let m = parse_queue_weights(Some(
            "urgent=100,batch=10,best-effort=0,bad=-1,nope=x,=5".to_string(),
        ));
        assert_eq!(m.get("urgent"), Some(&100));
        assert_eq!(m.get("batch"), Some(&10));
        assert_eq!(m.get("best-effort"), Some(&0));
        assert_eq!(m.len(), 3);
    }

    #[test]
    fn parse_solve_secs_defaults_and_overrides() {
        assert_eq!(parse_solve_secs(None), 10);
        assert_eq!(parse_solve_secs(Some("5".to_string())), 5);
        assert_eq!(parse_solve_secs(Some("0".to_string())), 10);
        assert_eq!(parse_solve_secs(Some("x".to_string())), 10);
        assert_eq!(parse_solve_secs(Some("-3".to_string())), 10);
    }

    #[test]
    fn parse_candidate_node_limit_defaults_to_adaptive_k_and_allows_full_escape() {
        assert_eq!(parse_candidate_node_limit(None), 16);
        assert_eq!(parse_candidate_node_limit(Some("".to_string())), 16);
        assert_eq!(parse_candidate_node_limit(Some("x".to_string())), 16);
        assert_eq!(parse_candidate_node_limit(Some("0".to_string())), 0);
        assert_eq!(parse_candidate_node_limit(Some("64".to_string())), 64);
    }

    #[test]
    fn parse_candidate_widen_min_admission_percent_defaults_clamps_and_allows_disable() {
        assert_eq!(
            parse_candidate_widen_min_admission_percent_milli(None),
            50_000
        );
        assert_eq!(
            parse_candidate_widen_min_admission_percent_milli(Some("75".to_string())),
            75_000
        );
        assert_eq!(
            parse_candidate_widen_min_admission_percent_milli(Some("0".to_string())),
            0
        );
        assert_eq!(
            parse_candidate_widen_min_admission_percent_milli(Some("100".to_string())),
            100_000
        );
        assert_eq!(
            parse_candidate_widen_min_admission_percent_milli(Some("-1".to_string())),
            50_000
        );
        assert_eq!(
            parse_candidate_widen_min_admission_percent_milli(Some("101".to_string())),
            50_000
        );
        assert_eq!(
            parse_candidate_widen_min_admission_percent_milli(Some("x".to_string())),
            50_000
        );
    }

    #[test]
    fn empty_allowlist_allows_all() {
        assert!(base().namespace_in_scope("anything"));
    }

    #[test]
    fn is_gpu_resource_matches_exact_and_mig_prefix() {
        let cfg = base();
        assert!(cfg.is_gpu_resource("nvidia.com/gpu"));
        assert!(cfg.is_gpu_resource("nvidia.com/mig-1g.5gb"));
        assert!(cfg.is_gpu_resource("nvidia.com/mig-3g.20gb"));
        assert!(!cfg.is_gpu_resource("cpu"));
        assert!(!cfg.is_gpu_resource("example.com/fpga"));
    }

    #[test]
    fn allowlist_restricts_when_set() {
        let mut cfg = base();
        cfg.namespace_allowlist = vec!["team-a".to_string()];
        assert!(cfg.namespace_in_scope("team-a"));
        assert!(!cfg.namespace_in_scope("team-z"));
    }
}
