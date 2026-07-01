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
    /// Pod label whose value groups pods into a gang (all-or-nothing). Empty disables grouping.
    pub gang_label_key: String,
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
            gang_label_key: std::env::var("KSOLVER_SHADOW_GANG_LABEL")
                .unwrap_or_else(|_| "scheduling.x-k8s.io/pod-group".to_string()),
        }
    }

    pub fn namespace_in_scope(&self, ns: &str) -> bool {
        self.namespace_allowlist.is_empty() || self.namespace_allowlist.iter().any(|n| n == ns)
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
            cluster_name: "default".to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
            gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),
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
