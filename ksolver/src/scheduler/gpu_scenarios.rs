use crate::model::{
    OptimizationInput, OptimizationNode, OptimizationWorkload, OptimizationWorkloadMember,
    ResourceList, ScenarioConfig,
};
use anyhow::Context;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

const GPU_RESOURCE: &str = "nvidia.com/gpu";

#[derive(Debug, Clone)]
struct GpuNodeSpec {
    name: String,
    gpus: i64,
}

#[derive(Debug, Clone)]
struct JobSpec {
    name: String,
    gpus_per_pod: i64,
    pods: usize,
    colocate: bool,
}

impl JobSpec {
    fn singleton(name: &str, gpus: i64) -> Self {
        Self {
            name: name.to_string(),
            gpus_per_pod: gpus,
            pods: 1,
            colocate: false,
        }
    }

    fn colocated_gang(name: &str, pods: usize, gpus_per_pod: i64) -> Self {
        Self {
            name: name.to_string(),
            gpus_per_pod,
            pods,
            colocate: true,
        }
    }

    fn total_gpus(&self) -> i64 {
        self.gpus_per_pod * self.pods as i64
    }

    fn pod_names(&self) -> Vec<String> {
        if self.pods == 1 {
            vec![self.name.clone()]
        } else {
            (0..self.pods)
                .map(|i| format!("{}-{i}", self.name))
                .collect()
        }
    }
}

#[derive(Debug, Clone)]
struct ScenarioSpec {
    name: String,
    description: String,
    nodes: Vec<GpuNodeSpec>,
    jobs: Vec<JobSpec>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Placement {
    pub pod: String,
    pub node: Option<String>,
    pub gpus: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PlacementMetrics {
    pub useful_gpu: i64,
    pub placed_pods: usize,
    pub unplaced_pods: usize,
    pub large_jobs_admitted: usize,
    pub full_gangs: usize,
    pub partial_or_invalid_gangs: usize,
    pub active_nodes: usize,
    pub stranded_gpu_on_active_nodes: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EngineResult {
    pub engine: String,
    pub source: String,
    pub solve_millis: u64,
    pub metrics: PlacementMetrics,
    pub placements: Vec<Placement>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioResult {
    pub name: String,
    pub description: String,
    pub benefit_score: i64,
    pub headline: String,
    pub kube: EngineResult,
    pub ksolver: EngineResult,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkReport {
    pub simulator_url: Option<String>,
    pub sorted_by: String,
    pub scenarios: Vec<ScenarioResult>,
}

pub async fn run_benchmark(simulator_url: Option<&str>) -> anyhow::Result<BenchmarkReport> {
    let mut results = Vec::new();
    for scenario in deterministic_scenarios() {
        let kube = if let Some(url) = simulator_url.filter(|u| !u.trim().is_empty()) {
            run_kube_simulator(&scenario, url)
                .await
                .unwrap_or_else(|err| {
                    let mut r = run_greedy_spread(&scenario);
                    r.source = format!("greedy-spread fallback (simulator failed: {err})");
                    r
                })
        } else {
            run_greedy_spread(&scenario)
        };
        let ksolver = run_ksolver(&scenario)?;
        let benefit_score = benefit_score(&kube.metrics, &ksolver.metrics);
        let headline = headline(&kube.metrics, &ksolver.metrics);
        results.push(ScenarioResult {
            name: scenario.name,
            description: scenario.description,
            benefit_score,
            headline,
            kube,
            ksolver,
        });
    }
    results.sort_by(|a, b| {
        b.benefit_score
            .cmp(&a.benefit_score)
            .then_with(|| a.name.cmp(&b.name))
    });
    Ok(BenchmarkReport {
        simulator_url: simulator_url.map(|s| s.to_string()),
        sorted_by: "benefit_score = useful GPU + large-job + full-gang wins, minus invalid gang and fragmentation losses".to_string(),
        scenarios: results,
    })
}

pub fn print_table(report: &BenchmarkReport) {
    println!(
        "{:<3} {:<28} {:>7} {:>13} {:>13} {:>11} {:>11}  headline",
        "#", "scenario", "score", "kube useful", "ksolver useful", "kube bad", "ks bad"
    );
    for (i, r) in report.scenarios.iter().enumerate() {
        println!(
            "{:<3} {:<28} {:>7} {:>13} {:>13} {:>11} {:>11}  {}",
            i + 1,
            r.name,
            r.benefit_score,
            r.kube.metrics.useful_gpu,
            r.ksolver.metrics.useful_gpu,
            r.kube.metrics.partial_or_invalid_gangs,
            r.ksolver.metrics.partial_or_invalid_gangs,
            r.headline
        );
    }
}

fn deterministic_scenarios() -> Vec<ScenarioSpec> {
    vec![
        scenario(
            "fragment-4gpu-node",
            "Small jobs arrive before one 4-GPU job; sequential scheduling can strand fragments.",
            &[1, 1, 4, 4],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("small-c", 1),
                JobSpec::singleton("small-d", 1),
                JobSpec::singleton("large-4g", 4),
                JobSpec::singleton("medium-2g", 2),
            ],
        ),
        scenario(
            "colocated-gang-vs-large",
            "A 4-worker colocated gang competes with useful 4/2/1-GPU work.",
            &[1, 1, 1, 1, 4, 4, 4],
            vec![
                JobSpec::colocated_gang("gang-a", 4, 1),
                JobSpec::singleton("large-4g", 4),
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
            ],
        ),
        scenario(
            "two-large-after-smalls",
            "Two 4-GPU jobs arrive after many 1-GPU jobs.",
            &[1, 1, 1, 4, 4, 4],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("small-c", 1),
                JobSpec::singleton("small-d", 1),
                JobSpec::singleton("small-e", 1),
                JobSpec::singleton("large-a", 4),
                JobSpec::singleton("large-b", 4),
            ],
        ),
        scenario(
            "scarce-big-node",
            "Only one 8-GPU node exists; low-width work can consume it before an 8-GPU job.",
            &[2, 2, 4, 8],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
                JobSpec::singleton("huge-8g", 8),
            ],
        ),
        scenario(
            "gang-or-throughput",
            "A colocated gang is all-or-nothing; admitting partial gang pods should not count as useful work.",
            &[1, 1, 4, 4],
            vec![
                JobSpec::colocated_gang("gang-b", 4, 1),
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
            ],
        ),
        scenario(
            "balanced-mixed-fleet",
            "A mixed fleet where both engines should be close; useful as a sanity check.",
            &[1, 1, 2, 2, 4, 4],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
                JobSpec::singleton("large-a", 4),
            ],
        ),
        scenario(
            "many-mediums-one-large",
            "2-GPU jobs can fill 4-GPU nodes in ways that block a late 4-GPU job.",
            &[2, 2, 4, 4],
            vec![
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
                JobSpec::singleton("medium-c", 2),
                JobSpec::singleton("large-a", 4),
                JobSpec::singleton("small-a", 1),
            ],
        ),
        scenario(
            "three-gangs-one-fleet",
            "Multiple colocated gangs compete; partial gang admission is especially misleading.",
            &[1, 1, 4, 4, 4],
            vec![
                JobSpec::colocated_gang("gang-a", 4, 1),
                JobSpec::colocated_gang("gang-b", 4, 1),
                JobSpec::colocated_gang("gang-c", 4, 1),
                JobSpec::singleton("large-a", 4),
            ],
        ),
        scenario(
            "packing-preserves-2gpu",
            "1-GPU jobs can strand 2-GPU capacity; solver should prefer tighter packing.",
            &[1, 1, 2, 2, 2],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("small-c", 1),
                JobSpec::singleton("small-d", 1),
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
            ],
        ),
        scenario(
            "oversubscribed-training-day",
            "Oversubscribed queue with small, medium, large, and colocated work.",
            &[1, 1, 1, 4, 4, 8],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("medium-a", 2),
                JobSpec::colocated_gang("gang-a", 4, 1),
                JobSpec::singleton("large-a", 4),
                JobSpec::singleton("huge-a", 8),
            ],
        ),
    ]
}

fn scenario(name: &str, description: &str, gpu_nodes: &[i64], jobs: Vec<JobSpec>) -> ScenarioSpec {
    ScenarioSpec {
        name: name.to_string(),
        description: description.to_string(),
        nodes: gpu_nodes
            .iter()
            .enumerate()
            .map(|(i, gpus)| GpuNodeSpec {
                name: format!("gpu-{}g-{}", gpus, i),
                gpus: *gpus,
            })
            .collect(),
        jobs,
    }
}

fn run_greedy_spread(s: &ScenarioSpec) -> EngineResult {
    let started = Instant::now();
    let mut used: BTreeMap<String, i64> = s.nodes.iter().map(|n| (n.name.clone(), 0)).collect();
    let mut placements = Vec::new();
    for job in &s.jobs {
        for pod in job.pod_names() {
            let best = s
                .nodes
                .iter()
                .filter(|n| n.gpus - used.get(&n.name).copied().unwrap_or(0) >= job.gpus_per_pod)
                .max_by_key(|n| (n.gpus - used.get(&n.name).copied().unwrap_or(0), &n.name));
            if let Some(node) = best {
                *used.entry(node.name.clone()).or_default() += job.gpus_per_pod;
                placements.push(Placement {
                    pod,
                    node: Some(node.name.clone()),
                    gpus: job.gpus_per_pod,
                });
            } else {
                placements.push(Placement {
                    pod,
                    node: None,
                    gpus: job.gpus_per_pod,
                });
            }
        }
    }
    EngineResult {
        engine: "kube".to_string(),
        source: "deterministic greedy-spread baseline (no simulator URL)".to_string(),
        solve_millis: started.elapsed().as_millis() as u64,
        metrics: metrics(s, &placements),
        placements,
    }
}

fn run_ksolver(s: &ScenarioSpec) -> anyhow::Result<EngineResult> {
    let started = Instant::now();
    let input = OptimizationInput {
        nodes: s
            .nodes
            .iter()
            .map(|n| {
                let mut ext = BTreeMap::new();
                ext.insert(GPU_RESOURCE.to_string(), n.gpus);
                OptimizationNode {
                    name: n.name.clone(),
                    count: 1,
                    effective_capacity: ResourceList {
                        milli_cpu: 64000,
                        memory_bytes: 512 << 30,
                        pods: 128,
                        ..Default::default()
                    },
                    extended_resources: ext,
                    ..Default::default()
                }
            })
            .collect(),
        workloads: s
            .jobs
            .iter()
            .map(|j| {
                let mut ext = BTreeMap::new();
                ext.insert(GPU_RESOURCE.to_string(), j.total_gpus());
                OptimizationWorkload {
                    id: j.name.clone(),
                    namespace: "bench".to_string(),
                    name: j.name.clone(),
                    group_size: j.pods as i32,
                    members: j
                        .pod_names()
                        .into_iter()
                        .map(|name| OptimizationWorkloadMember {
                            namespace: "bench".to_string(),
                            name,
                            current_node: String::new(),
                        })
                        .collect(),
                    requests: ResourceList {
                        milli_cpu: 1000 * j.pods as i64,
                        memory_bytes: (2 << 30) * j.pods as i64,
                        pods: j.pods as i64,
                        ..Default::default()
                    },
                    extended_resource_requests: ext,
                    feasible_nodes: s.nodes.iter().map(|n| n.name.clone()).collect(),
                    colocate: j.colocate,
                    ..Default::default()
                }
            })
            .collect(),
        ..Default::default()
    };
    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        partial_admission: true,
        solve_time_limit_secs: 5,
        ..Default::default()
    };
    let (solution, _) = crate::cpsat_rust::solve(&input, &scenario)
        .with_context(|| format!("ksolver solve failed for {}", s.name))?;
    let mut placements = Vec::new();
    for job in &s.jobs {
        let counts = solution.assignment_counts.get(&job.name);
        if let Some(counts) = counts {
            let mut nodes = Vec::new();
            for (node, count) in counts {
                for _ in 0..*count {
                    nodes.push(node.clone());
                }
            }
            nodes.sort();
            for (pod, node) in job.pod_names().into_iter().zip(nodes.into_iter()) {
                placements.push(Placement {
                    pod,
                    node: Some(node),
                    gpus: job.gpus_per_pod,
                });
            }
            if counts.values().map(|v| *v as usize).sum::<usize>() < job.pods {
                let placed = counts.values().map(|v| *v as usize).sum::<usize>();
                for pod in job.pod_names().into_iter().skip(placed) {
                    placements.push(Placement {
                        pod,
                        node: None,
                        gpus: job.gpus_per_pod,
                    });
                }
            }
        } else {
            for pod in job.pod_names() {
                placements.push(Placement {
                    pod,
                    node: None,
                    gpus: job.gpus_per_pod,
                });
            }
        }
    }
    Ok(EngineResult {
        engine: "ksolver".to_string(),
        source: "local CP-SAT batch optimizer".to_string(),
        solve_millis: started.elapsed().as_millis() as u64,
        metrics: metrics(s, &placements),
        placements,
    })
}

async fn run_kube_simulator(s: &ScenarioSpec, simulator_url: &str) -> anyhow::Result<EngineResult> {
    use crate::verifier::{
        pod_assigned_node, schedule_snapshot, SimulatorImportPayload, SimulatorResources,
    };
    let started = Instant::now();
    let nodes = s.nodes.iter().map(k8s_node).collect::<Vec<_>>();
    let namespace = k8s_openapi::api::core::v1::Namespace {
        metadata: kube::api::ObjectMeta {
            name: Some("bench".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut bound_pods = Vec::new();
    let mut placements = Vec::new();
    let raw = SimulatorResources {
        nodes: nodes.clone(),
        namespaces: vec![namespace.clone()],
        ..Default::default()
    };

    for job in &s.jobs {
        for pod_name in job.pod_names() {
            let pod = k8s_pod(&pod_name, job);
            let scope = format!("bench/{pod_name}");
            let mut pods = bound_pods.clone();
            pods.push(pod.clone());
            let payload = SimulatorImportPayload {
                pods,
                nodes: nodes.clone(),
                namespaces: vec![namespace.clone()],
                pvs: raw.pvs.clone(),
                pvcs: raw.pvcs.clone(),
                storage_classes: raw.storage_classes.clone(),
                priority_classes: raw.priority_classes.clone(),
                scheduler_config: crate::verifier::default_scheduler_config(),
            };
            let export = schedule_snapshot(simulator_url, &payload, &scope).await?;
            let scheduled = export
                .pods
                .iter()
                .find(|p| crate::verifier::pod_scope(p) == scope)
                .and_then(pod_assigned_node);
            if let Some(node) = scheduled.clone() {
                let mut bound = pod;
                if let Some(spec) = bound.spec.as_mut() {
                    spec.node_name = Some(node.clone());
                }
                bound_pods.push(bound);
            }
            placements.push(Placement {
                pod: pod_name,
                node: scheduled,
                gpus: job.gpus_per_pod,
            });
        }
    }
    Ok(EngineResult {
        engine: "kube".to_string(),
        source: format!(
            "kube-scheduler-simulator at {}",
            simulator_url.trim_end_matches('/')
        ),
        solve_millis: started.elapsed().as_millis() as u64,
        metrics: metrics(s, &placements),
        placements,
    })
}

fn k8s_node(n: &GpuNodeSpec) -> k8s_openapi::api::core::v1::Node {
    use k8s_openapi::api::core::v1::{Node, NodeCondition, NodeStatus};
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    let mut capacity = BTreeMap::new();
    capacity.insert("cpu".to_string(), Quantity("64".to_string()));
    capacity.insert("memory".to_string(), Quantity("512Gi".to_string()));
    capacity.insert("pods".to_string(), Quantity("128".to_string()));
    capacity.insert(GPU_RESOURCE.to_string(), Quantity(n.gpus.to_string()));
    Node {
        metadata: kube::api::ObjectMeta {
            name: Some(n.name.clone()),
            labels: Some(BTreeMap::from([
                ("kubernetes.io/hostname".to_string(), n.name.clone()),
                ("bench.ksolver.dev/gpu-node".to_string(), "true".to_string()),
            ])),
            ..Default::default()
        },
        status: Some(NodeStatus {
            allocatable: Some(capacity.clone()),
            capacity: Some(capacity),
            conditions: Some(vec![NodeCondition {
                type_: "Ready".to_string(),
                status: "True".to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn k8s_pod(name: &str, job: &JobSpec) -> k8s_openapi::api::core::v1::Pod {
    use k8s_openapi::api::core::v1::{Container, Pod, PodSpec, ResourceRequirements};
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    let mut req = BTreeMap::new();
    req.insert("cpu".to_string(), Quantity("1".to_string()));
    req.insert("memory".to_string(), Quantity("2Gi".to_string()));
    req.insert(
        GPU_RESOURCE.to_string(),
        Quantity(job.gpus_per_pod.to_string()),
    );
    Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("bench".to_string()),
            labels: Some(BTreeMap::from([(
                "bench.ksolver.dev/job".to_string(),
                job.name.clone(),
            )])),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "main".to_string(),
                image: Some("registry.k8s.io/pause:3.9".to_string()),
                resources: Some(ResourceRequirements {
                    requests: Some(req.clone()),
                    limits: Some(req),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn metrics(s: &ScenarioSpec, placements: &[Placement]) -> PlacementMetrics {
    let by_pod: HashMap<&str, &Placement> =
        placements.iter().map(|p| (p.pod.as_str(), p)).collect();
    let mut useful_gpu = 0;
    let mut large_jobs_admitted = 0;
    let mut full_gangs = 0;
    let mut partial_or_invalid_gangs = 0;
    for job in &s.jobs {
        let pod_names = job.pod_names();
        let placed: Vec<&Placement> = pod_names
            .iter()
            .filter_map(|p| by_pod.get(p.as_str()).copied())
            .filter(|p| p.node.is_some())
            .collect();
        let full = placed.len() == job.pods;
        let colocated = if job.colocate {
            placed
                .iter()
                .filter_map(|p| p.node.as_ref())
                .collect::<BTreeSet<_>>()
                .len()
                == 1
        } else {
            true
        };
        if full && colocated {
            useful_gpu += job.total_gpus();
            if job.total_gpus() >= 4 {
                large_jobs_admitted += 1;
            }
            if job.pods > 1 {
                full_gangs += 1;
            }
        } else if job.pods > 1 && !placed.is_empty() {
            partial_or_invalid_gangs += 1;
        }
    }

    let mut used_by_node: BTreeMap<String, i64> = BTreeMap::new();
    for p in placements {
        if let Some(node) = &p.node {
            *used_by_node.entry(node.clone()).or_default() += p.gpus;
        }
    }
    let active_nodes = used_by_node.len();
    let capacity_by_node: BTreeMap<_, _> = s.nodes.iter().map(|n| (&n.name, n.gpus)).collect();
    let stranded_gpu_on_active_nodes = used_by_node
        .iter()
        .map(|(node, used)| {
            capacity_by_node
                .get(node)
                .copied()
                .unwrap_or(0)
                .saturating_sub(*used)
        })
        .sum();

    PlacementMetrics {
        useful_gpu,
        placed_pods: placements.iter().filter(|p| p.node.is_some()).count(),
        unplaced_pods: placements.iter().filter(|p| p.node.is_none()).count(),
        large_jobs_admitted,
        full_gangs,
        partial_or_invalid_gangs,
        active_nodes,
        stranded_gpu_on_active_nodes,
    }
}

fn benefit_score(kube: &PlacementMetrics, ksolver: &PlacementMetrics) -> i64 {
    (ksolver.useful_gpu - kube.useful_gpu) * 100
        + (ksolver.large_jobs_admitted as i64 - kube.large_jobs_admitted as i64) * 40
        + (ksolver.full_gangs as i64 - kube.full_gangs as i64) * 35
        + (kube.partial_or_invalid_gangs as i64 - ksolver.partial_or_invalid_gangs as i64) * 50
        + (kube.stranded_gpu_on_active_nodes - ksolver.stranded_gpu_on_active_nodes) * 5
        + (kube.active_nodes as i64 - ksolver.active_nodes as i64) * 5
}

fn headline(kube: &PlacementMetrics, ksolver: &PlacementMetrics) -> String {
    let useful_delta = ksolver.useful_gpu - kube.useful_gpu;
    let invalid_delta =
        kube.partial_or_invalid_gangs as i64 - ksolver.partial_or_invalid_gangs as i64;
    let large_delta = ksolver.large_jobs_admitted as i64 - kube.large_jobs_admitted as i64;
    format!(
        "{:+} useful GPUs, {:+} large jobs, {:+} invalid gangs avoided",
        useful_delta, large_delta, invalid_delta
    )
}
