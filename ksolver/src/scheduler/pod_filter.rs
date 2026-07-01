use crate::scheduler::config::ShadowConfig;
use k8s_openapi::api::core::v1 as corev1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingGpuPod {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    pub gpu_request: i64,
}

/// GPU counts are whole numbers; anything non-integer floors to 0 (fractional GPUs
/// are a later phase).
fn parse_gpu_quantity(raw: &str) -> i64 {
    raw.trim().parse::<i64>().unwrap_or(0)
}

/// Sum of GPU resources named in `gpu_names` within one container's effective map
/// (requests, falling back to limits when requests is absent).
fn container_gpu(container: &corev1::Container, gpu_names: &[String]) -> i64 {
    let Some(res) = container.resources.as_ref() else {
        return 0;
    };
    let map = res.requests.as_ref().or(res.limits.as_ref());
    let Some(map) = map else {
        return 0;
    };
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
    let Some(spec) = pod.spec.as_ref() else {
        return 0;
    };
    let normal_sum: i64 = spec
        .containers
        .iter()
        .map(|c| container_gpu(c, gpu_names))
        .sum();
    let init_max: i64 = spec
        .init_containers
        .as_ref()
        .map(|inits| {
            inits
                .iter()
                .map(|c| container_gpu(c, gpu_names))
                .max()
                .unwrap_or(0)
        })
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
    if spec
        .node_name
        .as_deref()
        .map(|n| !n.is_empty())
        .unwrap_or(false)
    {
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
    Some(PendingGpuPod {
        uid,
        namespace,
        name,
        gpu_request: gpu,
    })
}

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

    fn container(
        name: &str,
        requests: Option<BTreeMap<String, Quantity>>,
        limits: Option<BTreeMap<String, Quantity>>,
    ) -> corev1::Container {
        corev1::Container {
            name: name.to_string(),
            resources: Some(corev1::ResourceRequirements {
                requests,
                limits,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn pod(
        scheduler: &str,
        node: Option<&str>,
        phase: Option<&str>,
        containers: Vec<corev1::Container>,
        init: Vec<corev1::Container>,
    ) -> corev1::Pod {
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
            status: Some(corev1::PodStatus {
                phase: phase.map(|p| p.to_string()),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn classifies_pending_gpu_pod_with_uid() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)],
            vec![],
        );
        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.uid, "uid-123");
        assert_eq!(got.gpu_request, 4);
    }

    #[test]
    fn rejects_other_scheduler() {
        let p = pod(
            "default-scheduler",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)],
            vec![],
        );
        assert!(classify(&p, &cfg()).is_none());
    }

    #[test]
    fn rejects_bound_pod() {
        let p = pod(
            "ksolver",
            Some("node-1"),
            Some("Running"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)],
            vec![],
        );
        assert!(classify(&p, &cfg()).is_none());
    }

    #[test]
    fn rejects_zero_gpu() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("cpu", "2")])), None)],
            vec![],
        );
        assert!(classify(&p, &cfg()).is_none());
    }

    #[test]
    fn sums_normal_containers() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![
                container("a", Some(q(&[("nvidia.com/gpu", "1")])), None),
                container("b", Some(q(&[("nvidia.com/gpu", "2")])), None),
            ],
            vec![],
        );
        assert_eq!(effective_gpu_request(&p, &cfg().gpu_resource_names), 3);
    }

    #[test]
    fn init_container_is_max_not_added() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![
                container("a", Some(q(&[("nvidia.com/gpu", "1")])), None),
                container("b", Some(q(&[("nvidia.com/gpu", "1")])), None),
            ],
            vec![container("init", Some(q(&[("nvidia.com/gpu", "5")])), None)],
        );
        assert_eq!(effective_gpu_request(&p, &cfg().gpu_resource_names), 5);
    }

    #[test]
    fn falls_back_to_limits_when_no_requests() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("a", None, Some(q(&[("nvidia.com/gpu", "2")])))],
            vec![],
        );
        assert_eq!(effective_gpu_request(&p, &cfg().gpu_resource_names), 2);
    }

    #[test]
    fn exact_name_match_ignores_gpu_memory() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container(
                "a",
                Some(q(&[("nvidia.com/gpu-memory", "8")])),
                None,
            )],
            vec![],
        );
        assert_eq!(effective_gpu_request(&p, &cfg().gpu_resource_names), 0);
    }
}
