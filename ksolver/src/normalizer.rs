use crate::model::*;
use crate::pricing::PricingCatalog;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct Options {
    pub cpu_headroom_percent: f64,
    pub memory_headroom_percent: f64,
    pub storage_headroom_percent: f64,
    pub pods_headroom: i64,
    pub disallowed_pools: Vec<String>,
    pub node_pool_label_keys: Vec<String>,
    pub ignore_taints: bool,
    pub relax_preferred_affinity: bool,
    pub relax_required_anti_affinity: bool,
    pub cpu_overcommit_ratio: f64,
    pub memory_overcommit_ratio: f64,
    pub use_usage_adjusted_requests: bool,
    pub usage_request_floor_ratio: f64,
    pub cpu_usage_safety_factor: f64,
    pub memory_usage_safety_factor: f64,
    pub max_memory_overflow_probability_percent: f64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cpu_headroom_percent: 0.0,
            memory_headroom_percent: 0.0,
            storage_headroom_percent: 0.0,
            pods_headroom: 0,
            disallowed_pools: Vec::new(),
            node_pool_label_keys: default_node_pool_label_keys(),
            ignore_taints: false,
            relax_preferred_affinity: false,
            relax_required_anti_affinity: false,
            cpu_overcommit_ratio: 1.0,
            memory_overcommit_ratio: 1.0,
            use_usage_adjusted_requests: false,
            usage_request_floor_ratio: 0.5,
            cpu_usage_safety_factor: 1.5,
            memory_usage_safety_factor: 2.0,
            max_memory_overflow_probability_percent: 1.0,
        }
    }
}

#[cfg(test)]
mod regression_tests {
    use super::{Normalizer, Options};
    use crate::model::{ClusterMetadata, ClusterSnapshot, DaemonSet, Node, Pod, ResourceList};
    use crate::pricing::PricingCatalog;

    #[test]
    fn current_pod_occupancy_does_not_make_node_infeasible() {
        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "test".to_string(),
                collected: None,
                ..Default::default()
            },
            nodes: vec![Node {
                name: "node-a".to_string(),
                allocatable: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 4096,
                    ephemeral_storage: 0,
                    pods: 32,
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let normalized =
            Normalizer::new(PricingCatalog::default(), Options::default()).normalize(&snapshot);

        assert_eq!(normalized.nodes[0].effective_capacity.milli_cpu, 1000);
        assert_eq!(normalized.nodes[0].effective_capacity.memory_bytes, 4096);
    }

    #[test]
    fn applies_cpu_and_memory_overcommit_to_effective_capacity() {
        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "test".to_string(),
                collected: None,
                ..Default::default()
            },
            nodes: vec![Node {
                name: "node-a".to_string(),
                allocatable: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 4096,
                    ephemeral_storage: 0,
                    pods: 32,
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let normalized = Normalizer::new(
            PricingCatalog::default(),
            Options {
                cpu_overcommit_ratio: 1.5,
                memory_overcommit_ratio: 1.25,
                ..Default::default()
            },
        )
        .normalize(&snapshot);

        assert_eq!(normalized.nodes[0].effective_capacity.milli_cpu, 1500);
        assert_eq!(normalized.nodes[0].effective_capacity.memory_bytes, 5120);
    }

    #[test]
    fn excludes_terminal_pods_from_modeled_workloads_and_current_pod_counts() {
        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "test".to_string(),
                collected: None,
                ..Default::default()
            },
            nodes: vec![Node {
                name: "node-a".to_string(),
                allocatable: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 4096,
                    ephemeral_storage: 0,
                    pods: 32,
                },
                ..Default::default()
            }],
            pods: vec![
                Pod {
                    namespace: "ns".to_string(),
                    name: "running".to_string(),
                    node_name: "node-a".to_string(),
                    phase: "Running".to_string(),
                    requests: ResourceList {
                        milli_cpu: 100,
                        memory_bytes: 128,
                        ephemeral_storage: 0,
                        pods: 1,
                    },
                    ..Default::default()
                },
                Pod {
                    namespace: "ns".to_string(),
                    name: "failed".to_string(),
                    node_name: "node-a".to_string(),
                    phase: "Failed".to_string(),
                    requests: ResourceList {
                        milli_cpu: 100,
                        memory_bytes: 128,
                        ephemeral_storage: 0,
                        pods: 1,
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let normalized =
            Normalizer::new(PricingCatalog::default(), Options::default()).normalize(&snapshot);

        assert_eq!(normalized.workloads.len(), 1);
        assert_eq!(normalized.workloads[0].name, "running");
        assert_eq!(normalized.nodes[0].current_pods, 1);
    }

    #[test]
    fn treats_daemonset_usage_as_node_overhead_not_workload() {
        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "test".to_string(),
                collected: None,
                ..Default::default()
            },
            nodes: vec![Node {
                name: "node-a".to_string(),
                allocatable: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 4096,
                    ephemeral_storage: 0,
                    pods: 32,
                },
                ..Default::default()
            }],
            daemon_sets: vec![DaemonSet {
                namespace: "kube-system".to_string(),
                name: "agent".to_string(),
                requests: ResourceList {
                    milli_cpu: 200,
                    memory_bytes: 512,
                    ephemeral_storage: 0,
                    pods: 1,
                },
                ..Default::default()
            }],
            pods: vec![Pod {
                namespace: "kube-system".to_string(),
                name: "agent-node-a".to_string(),
                node_name: "node-a".to_string(),
                phase: "Running".to_string(),
                owner_kind: "DaemonSet".to_string(),
                owner_name: "agent".to_string(),
                requests: ResourceList {
                    milli_cpu: 200,
                    memory_bytes: 512,
                    ephemeral_storage: 0,
                    pods: 1,
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let normalized =
            Normalizer::new(PricingCatalog::default(), Options::default()).normalize(&snapshot);

        assert!(normalized.workloads.is_empty());
        assert_eq!(normalized.nodes[0].current_pods, 0);
        assert_eq!(normalized.nodes[0].reserved.milli_cpu, 200);
        assert_eq!(normalized.nodes[0].reserved.memory_bytes, 512);
        assert_eq!(normalized.nodes[0].reserved.pods, 1);
        assert_eq!(normalized.nodes[0].effective_capacity.milli_cpu, 800);
        assert_eq!(normalized.nodes[0].effective_capacity.memory_bytes, 3584);
        assert_eq!(normalized.nodes[0].effective_capacity.pods, 31);
    }

    #[test]
    fn exists_toleration_with_empty_key_matches_any_taint_key() {
        use super::tolerated;
        use crate::model::{Taint, Toleration};

        let taint = Taint {
            key: "persistent-node".to_string(),
            value: "yes".to_string(),
            effect: "NoSchedule".to_string(),
        };
        let tolerations = vec![Toleration {
            key: String::new(),
            operator: "Exists".to_string(),
            value: String::new(),
            effect: String::new(),
        }];

        assert!(tolerated(&taint, &tolerations));
    }

    #[test]
    fn reports_safe_to_evict_false_as_constraint_impact() {
        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "test".to_string(),
                collected: None,
                ..Default::default()
            },
            nodes: vec![Node {
                name: "node-a".to_string(),
                allocatable: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 4096,
                    ephemeral_storage: 0,
                    pods: 32,
                },
                price: crate::model::Money {
                    currency: "USD".to_string(),
                    monthly: 100.0,
                },
                ..Default::default()
            }],
            pods: vec![Pod {
                namespace: "ns".to_string(),
                name: "pod-a".to_string(),
                node_name: "node-a".to_string(),
                phase: "Running".to_string(),
                autoscaler_not_safe_to_evict: true,
                requests: ResourceList {
                    milli_cpu: 100,
                    memory_bytes: 128,
                    ephemeral_storage: 0,
                    pods: 1,
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let normalized =
            Normalizer::new(PricingCatalog::default(), Options::default()).normalize(&snapshot);

        let impact = normalized
            .constraint_impacts
            .iter()
            .find(|impact| impact.name == "cluster autoscaler safe-to-evict=false")
            .expect("expected autoscaler impact");
        assert_eq!(impact.workloads_affected, 1);
        assert_eq!(impact.reason_count, 1);
        assert!(impact.estimated_monthly >= 0.0);
    }

    #[test]
    fn usage_adjusted_requests_respect_usage_bound_and_floor() {
        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "test".to_string(),
                collected: None,
                ..Default::default()
            },
            nodes: vec![Node {
                name: "node-a".to_string(),
                allocatable: ResourceList {
                    milli_cpu: 4000,
                    memory_bytes: 16_384,
                    ephemeral_storage: 0,
                    pods: 32,
                },
                ..Default::default()
            }],
            pods: vec![Pod {
                namespace: "ns".to_string(),
                name: "pod-a".to_string(),
                node_name: "node-a".to_string(),
                phase: "Running".to_string(),
                requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 8192,
                    ephemeral_storage: 0,
                    pods: 1,
                },
                usage: crate::model::ResourceUsage {
                    cpu_usage_milli: 200,
                    memory_bytes: 1024,
                    ephemeral_bytes: 0,
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let normalized = Normalizer::new(
            PricingCatalog::default(),
            Options {
                use_usage_adjusted_requests: true,
                usage_request_floor_ratio: 0.5,
                cpu_usage_safety_factor: 1.5,
                memory_usage_safety_factor: 2.0,
                max_memory_overflow_probability_percent: 1.0,
                ..Default::default()
            },
        )
        .normalize(&snapshot);

        assert_eq!(normalized.workloads[0].requests.milli_cpu, 500);
        assert_eq!(normalized.workloads[0].requests.memory_bytes, 4096);
    }

    #[test]
    fn usage_adjusted_requests_do_not_raise_underrequested_workloads() {
        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "test".to_string(),
                collected: None,
                ..Default::default()
            },
            nodes: vec![Node {
                name: "node-a".to_string(),
                allocatable: ResourceList {
                    milli_cpu: 4000,
                    memory_bytes: 16_384,
                    ephemeral_storage: 0,
                    pods: 32,
                },
                ..Default::default()
            }],
            pods: vec![Pod {
                namespace: "ns".to_string(),
                name: "pod-a".to_string(),
                node_name: "node-a".to_string(),
                phase: "Running".to_string(),
                requests: ResourceList {
                    milli_cpu: 100,
                    memory_bytes: 1024,
                    ephemeral_storage: 0,
                    pods: 1,
                },
                usage: crate::model::ResourceUsage {
                    cpu_usage_milli: 200,
                    memory_bytes: 4096,
                    ephemeral_bytes: 0,
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let normalized = Normalizer::new(
            PricingCatalog::default(),
            Options {
                use_usage_adjusted_requests: true,
                usage_request_floor_ratio: 0.5,
                cpu_usage_safety_factor: 1.5,
                memory_usage_safety_factor: 2.0,
                max_memory_overflow_probability_percent: 1.0,
                ..Default::default()
            },
        )
        .normalize(&snapshot);

        assert_eq!(normalized.workloads[0].requests.milli_cpu, 100);
        assert_eq!(normalized.workloads[0].requests.memory_bytes, 1024);
    }

    #[test]
    fn usage_adjusted_memory_requests_use_historical_quantile_bound() {
        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "test".to_string(),
                collected: None,
                ..Default::default()
            },
            nodes: vec![Node {
                name: "node-a".to_string(),
                allocatable: ResourceList {
                    milli_cpu: 4000,
                    memory_bytes: 16_384,
                    ephemeral_storage: 0,
                    pods: 32,
                },
                ..Default::default()
            }],
            pods: vec![Pod {
                namespace: "ns".to_string(),
                name: "pod-a".to_string(),
                node_name: "node-a".to_string(),
                phase: "Running".to_string(),
                requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 7000,
                    ephemeral_storage: 0,
                    pods: 1,
                },
                usage: crate::model::ResourceUsage {
                    cpu_usage_milli: 200,
                    memory_bytes: 1024,
                    ephemeral_bytes: 0,
                },
                memory_history: crate::model::MemoryHistory {
                    sample_count: 5,
                    mean_bytes: 0,
                    max_bytes: 6000,
                    stddev_bytes: 0,
                    samples: vec![
                        crate::model::TimeSeriesPoint {
                            timestamp_unix: 1,
                            value: 1000,
                        },
                        crate::model::TimeSeriesPoint {
                            timestamp_unix: 2,
                            value: 2000,
                        },
                        crate::model::TimeSeriesPoint {
                            timestamp_unix: 3,
                            value: 3000,
                        },
                        crate::model::TimeSeriesPoint {
                            timestamp_unix: 4,
                            value: 4000,
                        },
                        crate::model::TimeSeriesPoint {
                            timestamp_unix: 5,
                            value: 6000,
                        },
                    ],
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let safe = Normalizer::new(
            PricingCatalog::default(),
            Options {
                use_usage_adjusted_requests: true,
                usage_request_floor_ratio: 0.5,
                cpu_usage_safety_factor: 1.5,
                memory_usage_safety_factor: 2.0,
                max_memory_overflow_probability_percent: 1.0,
                ..Default::default()
            },
        )
        .normalize(&snapshot);

        let aggressive = Normalizer::new(
            PricingCatalog::default(),
            Options {
                use_usage_adjusted_requests: true,
                usage_request_floor_ratio: 0.5,
                cpu_usage_safety_factor: 1.5,
                memory_usage_safety_factor: 2.0,
                max_memory_overflow_probability_percent: 40.0,
                ..Default::default()
            },
        )
        .normalize(&snapshot);

        assert_eq!(safe.workloads[0].requests.memory_bytes, 6000);
        assert_eq!(aggressive.workloads[0].requests.memory_bytes, 4000);
        assert!(
            safe.workloads[0].requests.memory_bytes > aggressive.workloads[0].requests.memory_bytes
        );
    }
}

pub struct Normalizer {
    pricing: PricingCatalog,
    options: Options,
}

impl Normalizer {
    // @lineage
    // reads: snapshot.nodes, snapshot.pods, snapshot.volumes, snapshot.daemon_sets, snapshot.pdbs, pricing.*
    pub fn new(pricing: PricingCatalog, options: Options) -> Self {
        Self { pricing, options }
    }

    // @lineage
    // reads: snapshot.*
    // writes: normalized.nodes, normalized.workloads, normalized.blockers, normalized.warnings
    pub fn normalize(&self, snapshot: &ClusterSnapshot) -> NormalizedCluster {
        info!(
            cluster = %snapshot.metadata.name,
            nodes = snapshot.nodes.len(),
            pods = snapshot.pods.len(),
            volumes = snapshot.volumes.len(),
            daemonsets = snapshot.daemon_sets.len(),
            pdbs = snapshot.pdbs.len(),
            "normalization starting"
        );
        let volumes_by_claim: BTreeMap<String, VolumeAttachment> = snapshot
            .volumes
            .iter()
            .cloned()
            .map(|v| (namespaced_name(&v.namespace, &v.claim_name), v))
            .collect();
        let modeled_pods: Vec<&Pod> = snapshot
            .pods
            .iter()
            .filter(|pod| should_model_pod(pod))
            .collect();
        let skipped_terminal_pods = snapshot
            .pods
            .iter()
            .filter(|pod| is_terminal_or_deleting_pod(pod))
            .count();
        let skipped_daemonset_pods = snapshot
            .pods
            .iter()
            .filter(|pod| pod.owner_kind == "DaemonSet" && !is_terminal_or_deleting_pod(pod))
            .count();
        let usage_observed_pods = modeled_pods
            .iter()
            .filter(|pod| pod.usage.cpu_usage_milli > 0 || pod.usage.memory_bytes > 0)
            .count();

        let mut pods_by_node: BTreeMap<String, i32> = BTreeMap::new();
        for pod in &modeled_pods {
            if !pod.node_name.is_empty() {
                *pods_by_node.entry(pod.node_name.clone()).or_default() += 1;
            }
        }

        let mut normalized = NormalizedCluster {
            cluster_name: snapshot.metadata.name.clone(),
            collected: snapshot.metadata.collected,
            warnings: snapshot.warnings.clone(),
            ..Default::default()
        };
        normalized_warning(
            &mut normalized.warnings,
            skipped_terminal_pods,
            skipped_daemonset_pods,
        );
        if self.options.use_usage_adjusted_requests && usage_observed_pods == 0 {
            normalized.warnings.push(
                "usage-adjusted requests were enabled but no pod usage data was present; modeled requests remain unchanged"
                    .to_string(),
            );
        }
        let disallowed_pools = self
            .options
            .disallowed_pools
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();

        for node in &snapshot.nodes {
            let derived_pool = pool_name_for_node(node, &self.options.node_pool_label_keys);
            if !disallowed_pools.is_empty() && disallowed_pools.contains(&derived_pool) {
                continue;
            }
            let price = self.pricing.price_for_node(node);
            let reserved = observed_daemonset_reserve(node, &snapshot.pods);
            let headroom = self.headroom_reserve(&node.allocatable);
            let total_reserved = add_resource_list(&reserved, &headroom);
            let effective =
                self.apply_overcommit(&subtract_resource_list(&node.allocatable, &total_reserved));
            normalized.current_monthly_cost.monthly += price.monthly;
            normalized.current_monthly_cost.currency = price.currency.clone();
            normalized.nodes.push(NormalizedNode {
                name: node.name.clone(),
                pool: derived_pool,
                instance_type: first_non_empty(&[
                    node.labels.get("node.kubernetes.io/instance-type").cloned(),
                    node.labels.get("beta.kubernetes.io/instance-type").cloned(),
                ]),
                labels: node.labels.clone(),
                taints: node.taints.clone(),
                allocatable: node.allocatable.clone(),
                extended_resources: node.extended_resources.clone(),
                reserved: total_reserved,
                effective_capacity: effective,
                usage: node.usage.clone(),
                current_pods: *pods_by_node.get(&node.name).unwrap_or(&0),
                price,
            });
        }

        let vpa_map: BTreeMap<(String, String), &crate::model::VerticalPodAutoscaler> = snapshot
            .vpas
            .iter()
            .map(|vpa| ((vpa.namespace.clone(), vpa.target_ref_name.clone()), vpa))
            .collect();

        for pod in modeled_pods {
            let recommended = always_compute_recommended_requests(pod, &self.options);
            let effective_requests = if self.options.use_usage_adjusted_requests {
                recommended.clone()
            } else {
                pod.requests.clone()
            };
            let vpa = vpa_map.get(&(pod.namespace.clone(), pod.owner_name.clone()));
            let candidates = build_candidate_levels(pod, &recommended, vpa.copied());
            let mut workload = NormalizedWorkload {
                namespace: pod.namespace.clone(),
                name: pod.name.clone(),
                owner_kind: pod.owner_kind.clone(),
                owner_name: pod.owner_name.clone(),
                current_node: pod.node_name.clone(),
                current_requests: pod.requests.clone(),
                recommended_requests: recommended,
                requests: effective_requests,
                candidate_levels: candidates,
                extended_resource_requests: pod.extended_resource_requests.clone(),
                usage: pod.usage.clone(),
                pinned_by_volume: has_volume_pins(pod, &volumes_by_claim),
                has_required_affinity: !pod.required_affinity.is_empty(),
                has_required_anti_affinity: !pod.required_anti.is_empty(),
                has_required_node_affinity: !pod.required_node_affinity.is_empty(),
                topology_spread_constraints: pod.topology_spread_constraints,
                qos_class: pod.qos_class.clone(),
                autoscaler_not_safe_to_evict: pod.autoscaler_not_safe_to_evict,
                ..Default::default()
            };

            let mut feasible = 0;
            for node in &normalized.nodes {
                let reasons = feasible_on_node(pod, node, &volumes_by_claim, &self.options);
                if reasons.is_empty() {
                    feasible += 1;
                    workload.feasible_node_names.push(node.name.clone());
                } else {
                    workload.reasons.extend(reasons);
                }
            }
            workload.feasible_nodes = feasible;
            workload.feasible_node_names.sort();
            workload.reasons = unique_strings(workload.reasons);

            if workload.topology_spread_constraints > 0 {
                normalized.warnings.push(format!(
                    "workload {}/{} uses topology spread constraints; solver does not model them exactly yet",
                    workload.namespace, workload.name
                ));
            }
            if workload.has_required_affinity {
                normalized.warnings.push(format!(
                    "workload {}/{} uses required pod affinity; solver does not model it exactly yet",
                    workload.namespace, workload.name
                ));
            }
            if workload.has_required_anti_affinity {
                normalized.warnings.push(format!(
                    "workload {}/{} uses required pod anti-affinity; solver does not model it exactly yet",
                    workload.namespace, workload.name
                ));
            }

            if feasible == 0 {
                let capacity_summary = summarize_capacity_candidates(
                    pod,
                    &normalized.nodes,
                    &volumes_by_claim,
                    &self.options,
                );
                let reason = workload
                    .reasons
                    .first()
                    .cloned()
                    .unwrap_or_else(|| {
                        "no feasible nodes after requests, taints, selectors, volume locality, or pod limits"
                            .to_string()
                    });
                warn!(
                    workload = %namespaced_name(&pod.namespace, &pod.name),
                    owner_kind = %pod.owner_kind,
                    owner_name = %pod.owner_name,
                    current_node = if pod.node_name.is_empty() {
                        "<unassigned>"
                    } else {
                        pod.node_name.as_str()
                    },
                    requests_milli_cpu = pod.requests.milli_cpu,
                    requests_memory_bytes = pod.requests.memory_bytes,
                    requests_ephemeral_storage = pod.requests.ephemeral_storage,
                    pvc_count = pod.pvcs.len(),
                    node_selector = if pod.node_selector.is_empty() {
                        "<none>".to_string()
                    } else {
                        format_label_map(&pod.node_selector)
                    },
                    tolerations = if pod.tolerations.is_empty() {
                        "<none>".to_string()
                    } else {
                        format_tolerations(&pod.tolerations)
                    },
                    required_affinity_terms = pod.required_affinity.len(),
                    required_anti_affinity_terms = pod.required_anti.len(),
                    non_capacity_candidate_nodes = capacity_summary.non_capacity_candidates,
                    max_candidate_milli_cpu = capacity_summary.max_candidate.milli_cpu,
                    max_candidate_memory_bytes = capacity_summary.max_candidate.memory_bytes,
                    max_candidate_ephemeral_storage = capacity_summary.max_candidate.ephemeral_storage,
                    max_candidate_pods = capacity_summary.max_candidate.pods,
                    best_candidate_node = if capacity_summary.best_candidate_node.is_empty() {
                        "<none>"
                    } else {
                        capacity_summary.best_candidate_node.as_str()
                    },
                    best_candidate_allocatable_milli_cpu = capacity_summary.best_candidate_allocatable.milli_cpu,
                    best_candidate_reserved_milli_cpu = capacity_summary.best_candidate_reserved.milli_cpu,
                    best_candidate_effective_milli_cpu = capacity_summary.best_candidate_effective.milli_cpu,
                    best_candidate_allocatable_memory_bytes = capacity_summary.best_candidate_allocatable.memory_bytes,
                    best_candidate_reserved_memory_bytes = capacity_summary.best_candidate_reserved.memory_bytes,
                    best_candidate_effective_memory_bytes = capacity_summary.best_candidate_effective.memory_bytes,
                    max_cluster_milli_cpu = capacity_summary.max_cluster.milli_cpu,
                    max_cluster_memory_bytes = capacity_summary.max_cluster.memory_bytes,
                    max_cluster_ephemeral_storage = capacity_summary.max_cluster.ephemeral_storage,
                    max_cluster_pods = capacity_summary.max_cluster.pods,
                    reasons = %workload.reasons.join(" | "),
                    "workload has no feasible nodes"
                );
                normalized.blockers.push(Blocker {
                    scope: namespaced_name(&pod.namespace, &pod.name),
                    message: reason,
                });
            }
            if feasible == 1 {
                workload.reasons.push("single feasible node".to_string());
                workload.reasons = unique_strings(workload.reasons);
            }
            normalized.workloads.push(workload);
        }

        if normalized.current_monthly_cost.currency.is_empty() {
            normalized.current_monthly_cost.currency = "USD".to_string();
        }
        normalized.constraint_impacts = estimate_constraint_impacts(&normalized);
        normalized.pool_summaries = summarize_pools(&normalized);
        info!(
            cluster = %normalized.cluster_name,
            normalized_nodes = normalized.nodes.len(),
            normalized_workloads = normalized.workloads.len(),
            blockers = normalized.blockers.len(),
            current_monthly = normalized.current_monthly_cost.monthly,
            "normalization complete"
        );
        normalized
    }

    fn headroom_reserve(&self, alloc: &ResourceList) -> ResourceList {
        ResourceList {
            milli_cpu: percent_reserve(alloc.milli_cpu, self.options.cpu_headroom_percent),
            memory_bytes: percent_reserve(alloc.memory_bytes, self.options.memory_headroom_percent),
            ephemeral_storage: percent_reserve(
                alloc.ephemeral_storage,
                self.options.storage_headroom_percent,
            ),
            pods: self.options.pods_headroom,
        }
    }

    fn apply_overcommit(&self, capacity: &ResourceList) -> ResourceList {
        ResourceList {
            milli_cpu: ((capacity.milli_cpu as f64) * self.options.cpu_overcommit_ratio).round()
                as i64,
            memory_bytes: ((capacity.memory_bytes as f64) * self.options.memory_overcommit_ratio)
                .round() as i64,
            ephemeral_storage: capacity.ephemeral_storage,
            pods: capacity.pods,
        }
    }
}

// @lineage
// reads: pod.requests, pod.usage, pod.memory_history.samples, options.usage_request_floor_ratio, options.cpu_usage_safety_factor, options.memory_usage_safety_factor, options.max_memory_overflow_probability_percent
fn always_compute_recommended_requests(pod: &Pod, options: &Options) -> ResourceList {
    let mut adjusted = pod.requests.clone();
    adjusted.milli_cpu = adjusted_resource_value(
        pod.requests.milli_cpu,
        pod.usage.cpu_usage_milli,
        options.usage_request_floor_ratio,
        options.cpu_usage_safety_factor,
    );
    adjusted.memory_bytes = adjusted_memory_request_value(pod, options);
    adjusted
}

// @lineage
// reads: pod.requests, pod.usage, pod.memory_history.samples, vpa.container_recommendations
fn build_candidate_levels(
    pod: &Pod,
    recommended: &ResourceList,
    vpa: Option<&crate::model::VerticalPodAutoscaler>,
) -> Vec<crate::model::CandidateLevel> {
    use crate::model::CandidateLevel;

    let current = &pod.requests;
    let has_delta = current.milli_cpu > recommended.milli_cpu
        || current.memory_bytes > recommended.memory_bytes;

    let vpa_target = vpa.and_then(aggregate_vpa_target);
    let vpa_lower = vpa.and_then(aggregate_vpa_lower_bound);
    let has_vpa = vpa_target.is_some();

    if !has_delta && !has_vpa {
        return Vec::new();
    }

    let mut levels = Vec::new();

    levels.push(CandidateLevel {
        key: "current".to_string(),
        label: "Current requests".to_string(),
        requests: current.clone(),
        risk_score: 0,
    });

    if has_delta {
        levels.push(CandidateLevel {
            key: "recommended".to_string(),
            label: "Usage-adjusted".to_string(),
            requests: recommended.clone(),
            risk_score: 50,
        });

        let mid_cpu = (current.milli_cpu + recommended.milli_cpu) / 2;
        let mid_mem = (current.memory_bytes + recommended.memory_bytes) / 2;
        if mid_cpu != current.milli_cpu || mid_mem != current.memory_bytes {
            levels.push(CandidateLevel {
                key: "moderate".to_string(),
                label: "Moderate reduction".to_string(),
                requests: ResourceList {
                    milli_cpu: mid_cpu,
                    memory_bytes: mid_mem,
                    ephemeral_storage: current.ephemeral_storage,
                    pods: current.pods,
                },
                risk_score: 25,
            });
        }
    }

    if let Some(target) = vpa_target {
        if target.milli_cpu < current.milli_cpu || target.memory_bytes < current.memory_bytes {
            let already_present = levels.iter().any(|l| {
                l.requests.milli_cpu == target.milli_cpu
                    && l.requests.memory_bytes == target.memory_bytes
            });
            if !already_present {
                levels.push(CandidateLevel {
                    key: "vpa-target".to_string(),
                    label: "VPA target".to_string(),
                    requests: target,
                    risk_score: 30,
                });
            }
        }
    }

    if let Some(lower) = vpa_lower {
        if lower.milli_cpu < current.milli_cpu || lower.memory_bytes < current.memory_bytes {
            let already_present = levels.iter().any(|l| {
                l.requests.milli_cpu == lower.milli_cpu
                    && l.requests.memory_bytes == lower.memory_bytes
            });
            if !already_present {
                levels.push(CandidateLevel {
                    key: "vpa-lower".to_string(),
                    label: "VPA lower bound".to_string(),
                    requests: lower,
                    risk_score: 15,
                });
            }
        }
    }

    levels.sort_by(|a, b| b.requests.memory_bytes.cmp(&a.requests.memory_bytes));
    levels
}

fn aggregate_vpa_target(vpa: &crate::model::VerticalPodAutoscaler) -> Option<ResourceList> {
    if vpa.container_recommendations.is_empty() {
        return None;
    }
    let mut total = ResourceList::default();
    for rec in &vpa.container_recommendations {
        total.milli_cpu += rec.target.milli_cpu;
        total.memory_bytes += rec.target.memory_bytes;
    }
    if total.milli_cpu == 0 && total.memory_bytes == 0 {
        return None;
    }
    total.pods = 1;
    Some(total)
}

fn aggregate_vpa_lower_bound(vpa: &crate::model::VerticalPodAutoscaler) -> Option<ResourceList> {
    if vpa.container_recommendations.is_empty() {
        return None;
    }
    let mut total = ResourceList::default();
    for rec in &vpa.container_recommendations {
        total.milli_cpu += rec.lower_bound.milli_cpu;
        total.memory_bytes += rec.lower_bound.memory_bytes;
    }
    if total.milli_cpu == 0 && total.memory_bytes == 0 {
        return None;
    }
    total.pods = 1;
    Some(total)
}

// @lineage
// reads: pod.requests.memory_bytes, pod.usage.memory_bytes, pod.memory_history.samples, options.usage_request_floor_ratio, options.memory_usage_safety_factor, options.max_memory_overflow_probability_percent
fn adjusted_memory_request_value(pod: &Pod, options: &Options) -> i64 {
    let floor =
        ((pod.requests.memory_bytes as f64) * options.usage_request_floor_ratio).round() as i64;
    let historical_bound = historical_memory_quantile_bound(
        &pod.memory_history.samples,
        options.max_memory_overflow_probability_percent,
    );
    let observed_bound = historical_bound.unwrap_or_else(|| {
        ((pod.usage.memory_bytes as f64) * options.memory_usage_safety_factor).round() as i64
    });
    pod.requests
        .memory_bytes
        .min(floor.max(observed_bound).max(1))
}

// @lineage
// reads: samples[].value, overflow_probability_percent
fn historical_memory_quantile_bound(
    samples: &[TimeSeriesPoint],
    overflow_probability_percent: f64,
) -> Option<i64> {
    if samples.is_empty() {
        return None;
    }

    let mut values = samples
        .iter()
        .map(|sample| sample.value)
        .collect::<Vec<_>>();
    values.sort_unstable();

    let allowed_overflow = (overflow_probability_percent.clamp(0.0, 100.0)) / 100.0;
    let target_quantile = (1.0 - allowed_overflow).clamp(0.0, 1.0);
    let index = ((values.len().saturating_sub(1)) as f64 * target_quantile).ceil() as usize;
    values
        .get(index.min(values.len().saturating_sub(1)))
        .copied()
}

// @lineage
// reads: current_request, observed_usage
fn adjusted_resource_value(
    current_request: i64,
    observed_usage: i64,
    floor_ratio: f64,
    safety_factor: f64,
) -> i64 {
    if observed_usage <= 0 {
        return current_request;
    }

    let floor = ((current_request as f64) * floor_ratio).round() as i64;
    let usage_bound = ((observed_usage as f64) * safety_factor).round() as i64;
    current_request.min(floor.max(usage_bound).max(1))
}

// @lineage
// reads: pod.phase, pod.deleting
fn is_terminal_or_deleting_pod(pod: &Pod) -> bool {
    pod.deleting || matches!(pod.phase.as_str(), "Succeeded" | "Failed")
}

// @lineage
// reads: pod.phase, pod.deleting, pod.owner_kind
fn should_model_pod(pod: &Pod) -> bool {
    !is_terminal_or_deleting_pod(pod) && pod.owner_kind != "DaemonSet"
}

// @lineage
// writes: warnings[]
fn normalized_warning(
    warnings: &mut Vec<String>,
    skipped_terminal_pods: usize,
    skipped_daemonset_pods: usize,
) {
    if skipped_terminal_pods > 0 {
        warnings.push(format!(
            "excluded {skipped_terminal_pods} terminal or deleting pods from optimization modeling"
        ));
    }
    if skipped_daemonset_pods > 0 {
        warnings.push(format!(
            "excluded {skipped_daemonset_pods} DaemonSet pods from optimization variables; accounted for them as node overhead instead"
        ));
    }
}

// @lineage
// reads: node.name, pods[].owner_kind, pods[].node_name, pods[].requests
fn observed_daemonset_reserve(node: &Node, pods: &[Pod]) -> ResourceList {
    pods.iter().fold(ResourceList::default(), |acc, pod| {
        if pod.owner_kind == "DaemonSet"
            && pod.node_name == node.name
            && !is_terminal_or_deleting_pod(pod)
        {
            add_resource_list(&acc, &pod.requests)
        } else {
            acc
        }
    })
}

// @lineage
// reads: snapshot.pods.*, normalized.nodes.*, snapshot.volumes.*, normalizer.options.*
fn format_label_map(labels: &BTreeMap<String, String>) -> String {
    labels
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_tolerations(tolerations: &[Toleration]) -> String {
    tolerations
        .iter()
        .map(|t| format!("{}:{}:{}:{}", t.key, t.operator, t.value, t.effect))
        .collect::<Vec<_>>()
        .join(",")
}
fn feasible_on_node(
    pod: &Pod,
    node: &NormalizedNode,
    volumes_by_claim: &BTreeMap<String, VolumeAttachment>,
    options: &Options,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if !matches_node_selector(&node.labels, &pod.node_selector) {
        reasons.push("node selector mismatch".to_string());
    }
    if !options.ignore_taints && !tolerates_all(&node.taints, &pod.tolerations) {
        reasons.push("taint not tolerated".to_string());
    }
    if pod.requests.milli_cpu > node.effective_capacity.milli_cpu
        || pod.requests.memory_bytes > node.effective_capacity.memory_bytes
        || (pod.requests.ephemeral_storage > 0
            && pod.requests.ephemeral_storage > node.effective_capacity.ephemeral_storage)
    {
        reasons.push("insufficient effective capacity".to_string());
    }
    if !extended_resources_compatible(pod, node) {
        reasons.push("extended resource capacity or device mismatch".to_string());
    }
    if !volume_compatible(pod, node, volumes_by_claim) {
        reasons.push("volume topology constraint".to_string());
    }
    if !matches_required_node_affinity(&node.labels, &pod.required_node_affinity) {
        reasons.push("required node affinity mismatch".to_string());
    }
    reasons
}

fn matches_required_node_affinity(
    node_labels: &BTreeMap<String, String>,
    terms: &[crate::model::NodeAffinityTerm],
) -> bool {
    for term in terms {
        let node_value = node_labels.get(&term.key);
        let matched = match term.operator.as_str() {
            "In" => node_value.map(|v| term.values.contains(v)).unwrap_or(false),
            "NotIn" => node_value.map(|v| !term.values.contains(v)).unwrap_or(true),
            "Exists" => node_value.is_some(),
            "DoesNotExist" => node_value.is_none(),
            "Gt" => node_value
                .and_then(|v| v.parse::<i64>().ok())
                .zip(term.values.first().and_then(|v| v.parse::<i64>().ok()))
                .map(|(nv, tv)| nv > tv)
                .unwrap_or(false),
            "Lt" => node_value
                .and_then(|v| v.parse::<i64>().ok())
                .zip(term.values.first().and_then(|v| v.parse::<i64>().ok()))
                .map(|(nv, tv)| nv < tv)
                .unwrap_or(false),
            _ => true,
        };
        if !matched {
            return false;
        }
    }
    true
}

#[derive(Default)]
struct CapacitySummary {
    non_capacity_candidates: usize,
    max_candidate: ResourceList,
    max_cluster: ResourceList,
    best_candidate_node: String,
    best_candidate_allocatable: ResourceList,
    best_candidate_reserved: ResourceList,
    best_candidate_effective: ResourceList,
}

fn summarize_capacity_candidates(
    pod: &Pod,
    nodes: &[NormalizedNode],
    volumes_by_claim: &BTreeMap<String, VolumeAttachment>,
    options: &Options,
) -> CapacitySummary {
    let mut summary = CapacitySummary::default();

    for node in nodes {
        summary.max_cluster = max_resource_list(&summary.max_cluster, &node.effective_capacity);

        let mut blocked = false;
        if !matches_node_selector(&node.labels, &pod.node_selector) {
            blocked = true;
        }
        if !options.ignore_taints && !tolerates_all(&node.taints, &pod.tolerations) {
            blocked = true;
        }
        if !volume_compatible(pod, node, volumes_by_claim) {
            blocked = true;
        }

        if !blocked {
            summary.non_capacity_candidates += 1;
            if node.effective_capacity.memory_bytes > summary.max_candidate.memory_bytes {
                summary.best_candidate_node = node.name.clone();
                summary.best_candidate_allocatable = node.allocatable.clone();
                summary.best_candidate_reserved = node.reserved.clone();
                summary.best_candidate_effective = node.effective_capacity.clone();
            }
            summary.max_candidate =
                max_resource_list(&summary.max_candidate, &node.effective_capacity);
        }
    }

    summary
}

fn max_resource_list(current: &ResourceList, next: &ResourceList) -> ResourceList {
    ResourceList {
        milli_cpu: current.milli_cpu.max(next.milli_cpu),
        memory_bytes: current.memory_bytes.max(next.memory_bytes),
        ephemeral_storage: current.ephemeral_storage.max(next.ephemeral_storage),
        pods: current.pods.max(next.pods),
    }
}

fn extended_resources_compatible(pod: &Pod, node: &NormalizedNode) -> bool {
    pod.extended_resource_requests
        .iter()
        .all(|(name, requested)| *requested <= *node.extended_resources.get(name).unwrap_or(&0))
}

fn volume_compatible(
    pod: &Pod,
    node: &NormalizedNode,
    volumes_by_claim: &BTreeMap<String, VolumeAttachment>,
) -> bool {
    if pod.pvcs.is_empty() {
        return true;
    }
    let node_zone = first_non_empty(&[
        node.labels.get("topology.kubernetes.io/zone").cloned(),
        node.labels
            .get("failure-domain.beta.kubernetes.io/zone")
            .cloned(),
    ]);
    for claim in &pod.pvcs {
        if let Some(volume) = volumes_by_claim.get(&namespaced_name(&pod.namespace, claim)) {
            if volume.bound_node_zones.is_empty() {
                continue;
            }
            if node_zone.is_empty() || !volume.bound_node_zones.iter().any(|z| z == &node_zone) {
                return false;
            }
        }
    }
    true
}

fn matches_node_selector(
    labels: &BTreeMap<String, String>,
    selector: &BTreeMap<String, String>,
) -> bool {
    selector.iter().all(|(k, v)| labels.get(k) == Some(v))
}

fn tolerates_all(taints: &[Taint], tolerations: &[Toleration]) -> bool {
    taints
        .iter()
        .filter(|t| t.effect == "NoSchedule" || t.effect == "NoExecute")
        .all(|taint| tolerated(taint, tolerations))
}

fn tolerated(taint: &Taint, tolerations: &[Toleration]) -> bool {
    tolerations.iter().any(|tol| {
        (tol.effect.is_empty() || tol.effect == taint.effect)
            && match tol.operator.as_str() {
                "" | "Equal" => tol.key == taint.key && tol.value == taint.value,
                "Exists" => tol.key.is_empty() || tol.key == taint.key,
                _ => false,
            }
    })
}

fn has_volume_pins(pod: &Pod, volumes_by_claim: &BTreeMap<String, VolumeAttachment>) -> bool {
    pod.pvcs.iter().any(|claim| {
        volumes_by_claim
            .get(&namespaced_name(&pod.namespace, claim))
            .map(|v| !v.bound_node_zones.is_empty())
            .unwrap_or(false)
    })
}

fn add_resource_list(left: &ResourceList, right: &ResourceList) -> ResourceList {
    ResourceList {
        milli_cpu: left.milli_cpu + right.milli_cpu,
        memory_bytes: left.memory_bytes + right.memory_bytes,
        ephemeral_storage: left.ephemeral_storage + right.ephemeral_storage,
        pods: left.pods + right.pods,
    }
}

fn subtract_resource_list(left: &ResourceList, right: &ResourceList) -> ResourceList {
    ResourceList {
        milli_cpu: (left.milli_cpu - right.milli_cpu).max(0),
        memory_bytes: (left.memory_bytes - right.memory_bytes).max(0),
        ephemeral_storage: (left.ephemeral_storage - right.ephemeral_storage).max(0),
        pods: (left.pods - right.pods).max(0),
    }
}

fn unique_strings(values: Vec<String>) -> Vec<String> {
    let mut set = BTreeSet::new();
    for value in values.into_iter().filter(|v| !v.is_empty()) {
        set.insert(value);
    }
    set.into_iter().collect()
}

fn namespaced_name(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

fn first_non_empty(values: &[Option<String>]) -> String {
    values
        .iter()
        .flatten()
        .find(|v| !v.is_empty())
        .cloned()
        .unwrap_or_default()
}

fn percent_reserve(value: i64, pct: f64) -> i64 {
    if value <= 0 || pct <= 0.0 {
        0
    } else {
        ((value as f64) * pct).ceil() as i64
    }
}

fn estimate_constraint_impacts(cluster: &NormalizedCluster) -> Vec<ConstraintImpact> {
    if cluster.workloads.is_empty() || cluster.nodes.is_empty() {
        return Vec::new();
    }
    let avg_cost =
        cluster.nodes.iter().map(|n| n.price.monthly).sum::<f64>() / cluster.nodes.len() as f64;
    let mut acc: BTreeMap<&'static str, (BTreeSet<String>, i32)> = [
        "taints/tolerations",
        "node selectors",
        "volume locality",
        "resource capacity",
        "required node affinity",
        "required affinity",
        "required anti-affinity",
        "topology spread constraints",
        "cluster autoscaler safe-to-evict=false",
    ]
    .into_iter()
    .map(|k| (k, (BTreeSet::new(), 0)))
    .collect();

    for workload in &cluster.workloads {
        let key = namespaced_name(&workload.namespace, &workload.name);
        if workload.has_required_node_affinity {
            let entry = acc.get_mut("required node affinity").unwrap();
            entry.0.insert(key.clone());
            entry.1 += 1;
        }
        if workload.has_required_affinity {
            let entry = acc.get_mut("required affinity").unwrap();
            entry.0.insert(key.clone());
            entry.1 += 1;
        }
        if workload.has_required_anti_affinity {
            let entry = acc.get_mut("required anti-affinity").unwrap();
            entry.0.insert(key.clone());
            entry.1 += 1;
        }
        if workload.topology_spread_constraints > 0 {
            let entry = acc.get_mut("topology spread constraints").unwrap();
            entry.0.insert(key.clone());
            entry.1 += workload.topology_spread_constraints;
        }
        if workload.autoscaler_not_safe_to_evict {
            let entry = acc
                .get_mut("cluster autoscaler safe-to-evict=false")
                .unwrap();
            entry.0.insert(key.clone());
            entry.1 += 1;
        }
        if workload.pinned_by_volume {
            let entry = acc.get_mut("volume locality").unwrap();
            entry.0.insert(key.clone());
            entry.1 += 1;
        }
        for reason in &workload.reasons {
            let bucket = match reason.as_str() {
                "taint not tolerated" => Some("taints/tolerations"),
                "node selector mismatch" => Some("node selectors"),
                "volume topology constraint" => Some("volume locality"),
                "insufficient effective capacity" => Some("resource capacity"),
                "required node affinity mismatch" => Some("required node affinity"),
                _ => None,
            };
            if let Some(bucket) = bucket {
                let entry = acc.get_mut(bucket).unwrap();
                entry.0.insert(key.clone());
                entry.1 += 1;
            }
        }
    }

    let mut impacts: Vec<_> = acc
        .into_iter()
        .filter_map(|(name, (workloads, reasons))| {
            if workloads.is_empty() && reasons == 0 {
                return None;
            }
            Some(ConstraintImpact {
                name: name.to_string(),
                workloads_affected: workloads.len() as i32,
                reason_count: reasons,
                estimated_monthly: (reasons as f64 / cluster.workloads.len().max(1) as f64)
                    * avg_cost,
            })
        })
        .collect();

    impacts.sort_by(|a, b| {
        b.estimated_monthly
            .partial_cmp(&a.estimated_monthly)
            .unwrap_or(Ordering::Equal)
    });
    impacts
}

fn summarize_pools(cluster: &NormalizedCluster) -> Vec<PoolSummary> {
    let node_by_name = cluster
        .nodes
        .iter()
        .map(|node| (node.name.clone(), node))
        .collect::<BTreeMap<_, _>>();
    let mut allocated_by_pool: BTreeMap<String, ResourceList> = BTreeMap::new();
    for workload in &cluster.workloads {
        if let Some(node) = node_by_name.get(&workload.current_node) {
            let entry = allocated_by_pool.entry(node.pool.clone()).or_default();
            *entry = add_resource_list(entry, &workload.requests);
        }
    }

    let mut nodes_by_pool: BTreeMap<String, Vec<&NormalizedNode>> = BTreeMap::new();
    for node in &cluster.nodes {
        nodes_by_pool
            .entry(node.pool.clone())
            .or_default()
            .push(node);
    }

    let mut pools = nodes_by_pool
        .into_iter()
        .map(|(pool, nodes)| {
            let mut current_monthly_cost = Money::default();
            let mut total_cpu_capacity_milli = 0_i64;
            let mut total_memory_capacity_bytes = 0_i64;
            for node in &nodes {
                current_monthly_cost.monthly += node.price.monthly;
                if current_monthly_cost.currency.is_empty() {
                    current_monthly_cost.currency = node.price.currency.clone();
                }
                total_cpu_capacity_milli += node.effective_capacity.milli_cpu;
                total_memory_capacity_bytes += node.effective_capacity.memory_bytes;
            }
            let allocated = allocated_by_pool.get(&pool).cloned().unwrap_or_default();
            PoolSummary {
                pool,
                node_count: nodes.len() as i32,
                current_monthly_cost,
                allocated_cpu_request_milli: allocated.milli_cpu,
                allocated_memory_request_bytes: allocated.memory_bytes,
                total_cpu_capacity_milli,
                total_memory_capacity_bytes,
                fragmentation_percent: fragmentation_for_pool(
                    total_cpu_capacity_milli,
                    total_memory_capacity_bytes,
                    allocated.milli_cpu,
                    allocated.memory_bytes,
                ),
                emptiable: None,
                emptiability_blockers: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    pools.sort_by(|a, b| {
        b.current_monthly_cost
            .monthly
            .partial_cmp(&a.current_monthly_cost.monthly)
            .unwrap_or(Ordering::Equal)
    });
    pools
}

fn pool_name_for_node(node: &Node, label_keys: &[String]) -> String {
    for key in label_keys {
        if let Some(value) = node.labels.get(key) {
            if !value.trim().is_empty() {
                return value.clone();
            }
        }
    }
    first_non_empty(&[
        Some(node.pool.clone()),
        node.labels.get("node.kubernetes.io/instance-type").cloned(),
        node.labels.get("beta.kubernetes.io/instance-type").cloned(),
    ])
}

fn default_node_pool_label_keys() -> Vec<String> {
    vec![
        "eks.amazonaws.com/nodegroup".to_string(),
        "cloud.google.com/gke-nodepool".to_string(),
        "agentpool".to_string(),
    ]
}

fn fragmentation_for_pool(
    total_cpu_capacity_milli: i64,
    total_memory_capacity_bytes: i64,
    allocated_cpu_request_milli: i64,
    allocated_memory_request_bytes: i64,
) -> f64 {
    let cpu_unused = residual_fraction(total_cpu_capacity_milli, allocated_cpu_request_milli);
    let mem_unused = residual_fraction(total_memory_capacity_bytes, allocated_memory_request_bytes);
    (cpu_unused - mem_unused).abs() * 100.0
}

fn residual_fraction(capacity: i64, used: i64) -> f64 {
    if capacity <= 0 {
        0.0
    } else {
        ((capacity - used).max(0) as f64) / capacity as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_keeps_currently_full_nodes_feasible_for_repacking() {
        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "test".to_string(),
                collected: None,
                ..Default::default()
            },
            nodes: vec![
                Node {
                    name: "node-a".to_string(),
                    pool: "pool-a".to_string(),
                    allocatable: ResourceList {
                        pods: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                Node {
                    name: "node-b".to_string(),
                    pool: "pool-a".to_string(),
                    allocatable: ResourceList {
                        pods: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            pods: vec![
                Pod {
                    namespace: "default".to_string(),
                    name: "pod-a".to_string(),
                    node_name: "node-a".to_string(),
                    owner_kind: "Deployment".to_string(),
                    owner_name: "app-a".to_string(),
                    ..Default::default()
                },
                Pod {
                    namespace: "default".to_string(),
                    name: "pod-b".to_string(),
                    node_name: "node-b".to_string(),
                    owner_kind: "Deployment".to_string(),
                    owner_name: "app-b".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let normalized =
            Normalizer::new(PricingCatalog::default(), Options::default()).normalize(&snapshot);

        for workload in &normalized.workloads {
            assert_eq!(workload.feasible_nodes, 2);
            assert_eq!(
                workload.feasible_node_names,
                vec!["node-a".to_string(), "node-b".to_string()]
            );
        }
        assert!(normalized.blockers.is_empty());
    }

    #[test]
    fn constraint_impacts_do_not_report_dead_max_pods_bucket() {
        let cluster = NormalizedCluster {
            workloads: vec![NormalizedWorkload {
                namespace: "default".to_string(),
                name: "pod-a".to_string(),
                reasons: vec!["insufficient effective capacity".to_string()],
                ..Default::default()
            }],
            nodes: vec![NormalizedNode {
                price: Money {
                    currency: "USD".to_string(),
                    monthly: 100.0,
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let impacts = estimate_constraint_impacts(&cluster);
        assert!(!impacts.iter().any(|impact| impact.name == "max pods"));
        assert!(impacts
            .iter()
            .any(|impact| impact.name == "resource capacity"));
    }

    #[test]
    fn required_node_affinity_filters_infeasible_nodes() {
        use crate::model::NodeAffinityTerm;
        use std::collections::BTreeMap;

        let labels_match: BTreeMap<String, String> =
            [("zone".to_string(), "us-east-1a".to_string())].into();
        let labels_no_match: BTreeMap<String, String> =
            [("zone".to_string(), "eu-west-1a".to_string())].into();
        let terms = vec![NodeAffinityTerm {
            key: "zone".to_string(),
            operator: "In".to_string(),
            values: vec!["us-east-1a".to_string(), "us-east-1b".to_string()],
        }];

        assert!(super::matches_required_node_affinity(&labels_match, &terms));
        assert!(!super::matches_required_node_affinity(
            &labels_no_match,
            &terms
        ));
        assert!(!super::matches_required_node_affinity(
            &BTreeMap::new(),
            &terms
        ));
    }

    #[test]
    fn node_affinity_exists_operator() {
        use crate::model::NodeAffinityTerm;
        use std::collections::BTreeMap;

        let labels: BTreeMap<String, String> = [("gpu".to_string(), "true".to_string())].into();
        let terms = vec![NodeAffinityTerm {
            key: "gpu".to_string(),
            operator: "Exists".to_string(),
            values: Vec::new(),
        }];
        assert!(super::matches_required_node_affinity(&labels, &terms));
        assert!(!super::matches_required_node_affinity(
            &BTreeMap::new(),
            &terms
        ));
    }
}
