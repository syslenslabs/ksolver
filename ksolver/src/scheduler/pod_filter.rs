use crate::scheduler::config::ShadowConfig;
use k8s_openapi::api::core::v1 as corev1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingGpuPod {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    pub gpu_request: i64,
    /// `Some("{namespace}/{label-value}")` when the configured gang label is present.
    pub gang_key: Option<String>,
    /// True when the configured co-location label is set to "true" (gang wants one node).
    pub colocate: bool,
    /// Scheduling constraints present on the pod that shadow mode does NOT model
    /// (e.g. "pod affinity", "pod anti-affinity", "topology spread"); surfaced as
    /// per-decision caveats so recommendations are not silently misleading.
    pub unmodeled_constraints: Vec<String>,
    /// matchLabels selectors of *modeled* hostname pod-anti-affinity terms used for
    /// best-effort node exclusion against running pods. Empty when none/unmodeled.
    /// (The "pod anti-affinity" caveat is still raised — enforcement is partial.)
    pub anti_affinity_host_selectors: Vec<std::collections::BTreeMap<String, String>>,
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
    let gang_key = if cfg.gang_label_key.is_empty() {
        None
    } else {
        pod.metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(&cfg.gang_label_key))
            .filter(|v| !v.is_empty())
            .map(|v| format!("{namespace}/{v}"))
    };
    let colocate = !cfg.gang_colocate_label.is_empty()
        && pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(&cfg.gang_colocate_label))
            .map(|v| v == "true")
            .unwrap_or(false);
    let unmodeled_constraints = unmodeled_constraints(spec);
    let anti_affinity_host_selectors = modeled_anti_affinity_host_selectors(spec);
    Some(PendingGpuPod {
        uid,
        namespace,
        name,
        gpu_request: gpu,
        gang_key,
        colocate,
        unmodeled_constraints,
        anti_affinity_host_selectors,
    })
}

/// matchLabels of required pod-anti-affinity terms we can fully model for best-effort
/// node exclusion: topologyKey == hostname, non-empty matchLabels, NO matchExpressions,
/// and no cross-namespace scoping (namespaces / namespaceSelector). Anything else is
/// left unmodeled (and still caveated).
fn modeled_anti_affinity_host_selectors(
    spec: &corev1::PodSpec,
) -> Vec<std::collections::BTreeMap<String, String>> {
    let mut out = Vec::new();
    let Some(terms) = spec
        .affinity
        .as_ref()
        .and_then(|a| a.pod_anti_affinity.as_ref())
        .and_then(|a| {
            a.required_during_scheduling_ignored_during_execution
                .as_ref()
        })
    else {
        return out;
    };
    for term in terms {
        if term.topology_key != "kubernetes.io/hostname" {
            continue;
        }
        // Reject cross-namespace scoping (we only model same-namespace).
        if term
            .namespaces
            .as_ref()
            .map(|n| !n.is_empty())
            .unwrap_or(false)
            || term.namespace_selector.is_some()
        {
            continue;
        }
        let Some(ls) = term.label_selector.as_ref() else {
            continue;
        };
        // Reject matchExpressions (the collector drops them; we cannot model them).
        if ls
            .match_expressions
            .as_ref()
            .map(|e| !e.is_empty())
            .unwrap_or(false)
        {
            continue;
        }
        match ls.match_labels.as_ref() {
            Some(ml) if !ml.is_empty() => out.push(ml.clone()),
            _ => continue,
        }
    }
    out
}

/// Names of hard scheduling constraints present on the pod that shadow does not model:
/// required pod affinity, required pod anti-affinity, and DoNotSchedule topology spread.
/// (Node affinity IS enforced by feasibility, so it is not listed.)
fn unmodeled_constraints(spec: &corev1::PodSpec) -> Vec<String> {
    let mut out = Vec::new();
    let has_terms = |t: &Option<Vec<corev1::PodAffinityTerm>>| {
        t.as_ref().map(|v| !v.is_empty()).unwrap_or(false)
    };
    if let Some(aff) = spec.affinity.as_ref() {
        if aff
            .pod_affinity
            .as_ref()
            .map(|a| has_terms(&a.required_during_scheduling_ignored_during_execution))
            .unwrap_or(false)
        {
            out.push("pod affinity".to_string());
        }
        if aff
            .pod_anti_affinity
            .as_ref()
            .map(|a| has_terms(&a.required_during_scheduling_ignored_during_execution))
            .unwrap_or(false)
        {
            out.push("pod anti-affinity".to_string());
        }
    }
    // Only DoNotSchedule spread is a hard feasibility constraint; ScheduleAnyway is soft.
    let hard_spread = spec
        .topology_spread_constraints
        .as_ref()
        .map(|v| v.iter().any(|c| c.when_unsatisfiable == "DoNotSchedule"))
        .unwrap_or(false);
    if hard_spread {
        out.push("topology spread".to_string());
    }
    out
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
            gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),
            gang_colocate_label: "scheduling.x-k8s.io/gang-colocate".to_string(),
            solve_time_limit_secs: 10,
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

    #[test]
    fn extracts_gang_key_from_label() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        p.metadata.labels = Some(BTreeMap::from([(
            "scheduling.x-k8s.io/pod-group".to_string(),
            "job-7".to_string(),
        )]));
        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.gang_key.as_deref(), Some("team-a/job-7"));
    }

    #[test]
    fn no_gang_key_when_label_absent() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        let got = classify(&p, &cfg()).unwrap();
        assert_eq!(got.gang_key, None);
        assert!(!got.colocate);
    }

    #[test]
    fn detects_pod_anti_affinity_and_spread() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        if let Some(spec) = p.spec.as_mut() {
            spec.affinity = Some(corev1::Affinity {
                pod_anti_affinity: Some(corev1::PodAntiAffinity {
                    required_during_scheduling_ignored_during_execution: Some(vec![
                        corev1::PodAffinityTerm {
                            topology_key: "kubernetes.io/hostname".to_string(),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            });
            spec.topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
                max_skew: 1,
                topology_key: "zone".to_string(),
                when_unsatisfiable: "DoNotSchedule".to_string(),
                ..Default::default()
            }]);
        }
        let got = classify(&p, &cfg()).expect("classify");
        assert!(got
            .unmodeled_constraints
            .contains(&"pod anti-affinity".to_string()));
        assert!(got
            .unmodeled_constraints
            .contains(&"topology spread".to_string()));
    }

    #[test]
    fn no_caveats_for_plain_pod() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        assert!(classify(&p, &cfg())
            .unwrap()
            .unmodeled_constraints
            .is_empty());
    }

    #[test]
    fn schedule_anyway_spread_is_not_a_caveat() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        if let Some(spec) = p.spec.as_mut() {
            spec.topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
                max_skew: 1,
                topology_key: "zone".to_string(),
                when_unsatisfiable: "ScheduleAnyway".to_string(),
                ..Default::default()
            }]);
        }
        assert!(classify(&p, &cfg())
            .unwrap()
            .unmodeled_constraints
            .is_empty());
    }

    fn set_anti_affinity(pod: &mut corev1::Pod, terms: Vec<corev1::PodAffinityTerm>) {
        if let Some(spec) = pod.spec.as_mut() {
            spec.affinity = Some(corev1::Affinity {
                pod_anti_affinity: Some(corev1::PodAntiAffinity {
                    required_during_scheduling_ignored_during_execution: Some(terms),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }

    fn aa_term(
        topology_key: &str,
        match_labels: &[(&str, &str)],
        with_expr: bool,
        namespaces: Option<Vec<String>>,
    ) -> corev1::PodAffinityTerm {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
            LabelSelector, LabelSelectorRequirement,
        };
        let ml: BTreeMap<String, String> = match_labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        corev1::PodAffinityTerm {
            topology_key: topology_key.to_string(),
            namespaces,
            label_selector: Some(LabelSelector {
                match_labels: if ml.is_empty() { None } else { Some(ml) },
                match_expressions: if with_expr {
                    Some(vec![LabelSelectorRequirement {
                        key: "team".into(),
                        operator: "Exists".into(),
                        values: None,
                    }])
                } else {
                    None
                },
            }),
            ..Default::default()
        }
    }

    fn gpu_pending() -> corev1::Pod {
        pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("m", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        )
    }

    #[test]
    fn hostname_matchlabels_extracted_but_still_caveated() {
        let mut p = gpu_pending();
        set_anti_affinity(
            &mut p,
            vec![aa_term(
                "kubernetes.io/hostname",
                &[("app", "trainer")],
                false,
                None,
            )],
        );
        let got = classify(&p, &cfg()).unwrap();
        assert_eq!(got.anti_affinity_host_selectors.len(), 1);
        assert_eq!(
            got.anti_affinity_host_selectors[0].get("app").unwrap(),
            "trainer"
        );
        assert!(got
            .unmodeled_constraints
            .contains(&"pod anti-affinity".to_string()));
    }

    #[test]
    fn zone_topology_is_not_modeled() {
        let mut p = gpu_pending();
        set_anti_affinity(
            &mut p,
            vec![aa_term(
                "topology.kubernetes.io/zone",
                &[("app", "trainer")],
                false,
                None,
            )],
        );
        let got = classify(&p, &cfg()).unwrap();
        assert!(got.anti_affinity_host_selectors.is_empty());
        assert!(got
            .unmodeled_constraints
            .contains(&"pod anti-affinity".to_string()));
    }

    #[test]
    fn matchexpressions_term_is_not_modeled() {
        let mut p = gpu_pending();
        set_anti_affinity(
            &mut p,
            vec![aa_term(
                "kubernetes.io/hostname",
                &[("app", "trainer")],
                true,
                None,
            )],
        );
        assert!(classify(&p, &cfg())
            .unwrap()
            .anti_affinity_host_selectors
            .is_empty());
    }

    #[test]
    fn cross_namespace_term_is_not_modeled() {
        let mut p = gpu_pending();
        set_anti_affinity(
            &mut p,
            vec![aa_term(
                "kubernetes.io/hostname",
                &[("app", "trainer")],
                false,
                Some(vec!["other".into()]),
            )],
        );
        assert!(classify(&p, &cfg())
            .unwrap()
            .anti_affinity_host_selectors
            .is_empty());
    }

    #[test]
    fn colocate_label_true_sets_colocate() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        p.metadata.labels = Some(BTreeMap::from([
            (
                "scheduling.x-k8s.io/pod-group".to_string(),
                "job".to_string(),
            ),
            (
                "scheduling.x-k8s.io/gang-colocate".to_string(),
                "true".to_string(),
            ),
        ]));
        assert!(classify(&p, &cfg()).unwrap().colocate);
    }
}
