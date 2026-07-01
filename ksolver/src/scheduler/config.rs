use std::collections::BTreeMap;
use std::time::Duration;

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
    /// PHASE 3 (real binding): when true, the scheduler ACTUALLY POSTs pod→node bindings for
    /// `readiness: ready` decisions (mutates the cluster). Default FALSE — shadow stays read-only.
    pub enable_real_binding: bool,
    /// When real binding is enabled, send the binding with server-side `dryRun=All` (the apiserver
    /// validates but does NOT persist). A safe intermediate before live binding. Default false.
    pub real_binding_dry_run: bool,
    /// Upper bound on bindings applied per solve pass (throttle). Default 10.
    pub max_binds_per_pass: usize,
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
            gang_label_key: std::env::var("KSOLVER_SHADOW_GANG_LABEL")
                .unwrap_or_else(|_| "scheduling.x-k8s.io/pod-group".to_string()),
            gang_colocate_label: std::env::var("KSOLVER_SHADOW_COLOCATE_LABEL")
                .unwrap_or_else(|_| "scheduling.x-k8s.io/gang-colocate".to_string()),
            solve_time_limit_secs: parse_solve_secs(
                std::env::var("KSOLVER_SHADOW_SOLVE_SECS").ok(),
            ),
            namespace_gpu_quotas: parse_quotas(std::env::var("KSOLVER_SHADOW_QUOTAS").ok()),
            enable_real_binding: parse_bool(std::env::var("KSOLVER_ENABLE_REAL_BINDING").ok()),
            real_binding_dry_run: parse_bool(std::env::var("KSOLVER_REAL_BINDING_DRY_RUN").ok()),
            max_binds_per_pass: parse_max_binds(std::env::var("KSOLVER_MAX_BINDS_PER_PASS").ok()),
        }
    }

    pub fn namespace_in_scope(&self, ns: &str) -> bool {
        self.namespace_allowlist.is_empty() || self.namespace_allowlist.iter().any(|n| n == ns)
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
            gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),
            gang_colocate_label: "scheduling.x-k8s.io/gang-colocate".to_string(),
            solve_time_limit_secs: 10,
            namespace_gpu_quotas: BTreeMap::new(),
            enable_real_binding: false,
            real_binding_dry_run: false,
            max_binds_per_pass: 10,
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
    fn parse_solve_secs_defaults_and_overrides() {
        assert_eq!(parse_solve_secs(None), 10);
        assert_eq!(parse_solve_secs(Some("5".to_string())), 5);
        assert_eq!(parse_solve_secs(Some("0".to_string())), 10);
        assert_eq!(parse_solve_secs(Some("x".to_string())), 10);
        assert_eq!(parse_solve_secs(Some("-3".to_string())), 10);
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
