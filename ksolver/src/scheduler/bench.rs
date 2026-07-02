//! Deterministic synthetic scale-benchmark harness for the shadow scheduler's
//! solver core (`build_pending_input` + `cpsat_rust::solve`). No RNG — reproducible.

use crate::cpsat_rust;
use crate::model::{
    NormalizedCluster, NormalizedNode, NormalizedWorkload, ResourceList, ScenarioConfig,
};
use crate::scheduler::pending_input::{
    build_pending_input, build_pending_input_with_candidate_limit,
};
use crate::scheduler::pod_filter::PendingGpuPod;
use std::collections::BTreeMap;
use std::time::Instant;

const GIB: i64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AntiAffinity {
    None,
    /// Each gang gets a unique label/selector -> self-spread only (no cross graph).
    SelfSpread,
    /// Every pod shares one label/selector -> all-pairs cross anti-affinity (STRESS).
    GlobalStress,
}

pub struct BenchScenario {
    pub name: &'static str,
    pub nodes: usize,
    pub gpus_per_node: i64,
    pub jobs: usize,
    pub gang_size: usize,
    pub colocate: bool,
    pub anti: AntiAffinity,
    /// GPUs pre-consumed per node by running pods (residual pressure).
    pub running_fill: i64,
    /// Inclusive expected-admitted band for validation.
    pub expect_admitted: (usize, usize),
    /// Optional cap on feasible candidate nodes per workload/gang before solving. 0 = full set.
    pub candidate_node_limit: usize,
}

fn rl(cpu: i64, mem: i64) -> ResourceList {
    ResourceList {
        milli_cpu: cpu,
        memory_bytes: mem,
        ephemeral_storage: 0,
        pods: 0,
    }
}

fn gpu_map(n: i64) -> BTreeMap<String, i64> {
    let mut m = BTreeMap::new();
    m.insert("nvidia.com/gpu".to_string(), n);
    m
}

/// Build a synthetic normalized cluster and the pending pods for a scenario.
pub fn generate(s: &BenchScenario) -> (NormalizedCluster, Vec<PendingGpuPod>) {
    let node_names: Vec<String> = (0..s.nodes).map(|k| format!("n{k}")).collect();

    let mut cluster = NormalizedCluster {
        cluster_name: "bench".to_string(),
        ..Default::default()
    };

    for name in &node_names {
        cluster.nodes.push(NormalizedNode {
            name: name.clone(),
            effective_capacity: ResourceList {
                milli_cpu: 64_000,
                memory_bytes: 256 * GIB,
                ephemeral_storage: 0,
                pods: 110,
            },
            extended_resources: gpu_map(s.gpus_per_node),
            ..Default::default()
        });
    }

    // Running pods for residual pressure (consume `running_fill` GPUs per node).
    if s.running_fill > 0 {
        for name in &node_names {
            for j in 0..s.running_fill {
                cluster.workloads.push(NormalizedWorkload {
                    namespace: "run".to_string(),
                    name: format!("r-{name}-{j}"),
                    labels: BTreeMap::from([("app".to_string(), "running".to_string())]),
                    current_node: name.clone(),
                    requests: rl(1000, 4 * GIB),
                    extended_resource_requests: gpu_map(1),
                    feasible_node_names: vec![name.clone()],
                    ..Default::default()
                });
            }
        }
    }

    // Pending workloads.
    let mut pending = Vec::new();
    for i in 0..s.jobs {
        let members = s.gang_size.max(1);
        let gang_key = if members > 1 {
            Some(format!("bench/g{i}"))
        } else {
            None
        };
        let mut labels = BTreeMap::from([("app".to_string(), "trainer".to_string())]);
        if s.anti == AntiAffinity::SelfSpread {
            labels.insert("job".to_string(), format!("job{i}"));
        }
        // matchLabels-equivalent anti-affinity selector: key In [value], own-namespace scope.
        let in_req = |k: &str, v: String| {
            vec![crate::model::AntiAffinitySelector {
                reqs: vec![crate::model::LabelSelectorReq {
                    key: k.to_string(),
                    operator: "In".to_string(),
                    values: vec![v],
                }],
                namespaces: Vec::new(),
                namespace_selector: None,
            }]
        };
        let selectors: Vec<crate::model::AntiAffinitySelector> = match s.anti {
            AntiAffinity::None => Vec::new(),
            AntiAffinity::SelfSpread => in_req("job", format!("job{i}")),
            AntiAffinity::GlobalStress => in_req("app", "trainer".to_string()),
        };
        for j in 0..members {
            let name = if members > 1 {
                format!("g{i}-m{j}")
            } else {
                format!("g{i}")
            };
            cluster.workloads.push(NormalizedWorkload {
                namespace: "bench".to_string(),
                name: name.clone(),
                labels: labels.clone(),
                current_node: String::new(),
                requests: rl(1000, 4 * GIB),
                extended_resource_requests: gpu_map(1),
                feasible_node_names: node_names.clone(),
                ..Default::default()
            });
            pending.push(PendingGpuPod {
                uid: format!("uid-{name}"),
                namespace: "bench".to_string(),
                name,
                gpu_request: 1,
                priority: 0,
                priority_class_name: None,
                team: None,
                queue: None,
                business_value: 0,
                queue_wait_seconds: 0,
                deadline_unix_seconds: 0,
                min_gpus: 0,
                max_gpus: 0,
                preferred_gpus: 0,
                flexible: false,
                predicted_runtime_seconds: 0,
                predicted_peak_vram_bytes: 0,
                required_gpu_topology: vec![],
                gang_key: gang_key.clone(),
                colocate: s.colocate,
                unmodeled_constraints: Vec::new(),
                anti_affinity_host_selectors: selectors.clone(),
                affinity_topology_selectors: vec![],
                anti_affinity_topology_selectors: vec![],
                preferred_node_affinity: vec![],
                preferred_pod_affinity: vec![],
            });
        }
    }

    (cluster, pending)
}

pub struct BenchResult {
    pub name: &'static str,
    pub nodes: usize,
    pub pending_pods: usize,
    pub workloads: usize,
    pub anti_pairs: usize,
    pub workers: i32,
    pub build_ms: u128,
    pub solve_ms: u128,
    pub status: String,
    pub admitted: usize,
    pub valid: bool,
    pub in_band: bool,
}

/// Generate, then time build_pending_input and cpsat_rust::solve (solver-core only).
pub fn run_scenario(s: &BenchScenario) -> BenchResult {
    let (cluster, pending) = generate(s);

    let t0 = Instant::now();
    let input = if s.candidate_node_limit > 0 {
        build_pending_input_with_candidate_limit(
            &cluster,
            &pending,
            &BTreeMap::new(),
            s.candidate_node_limit,
        )
    } else {
        build_pending_input(&cluster, &pending, &BTreeMap::new())
    };
    let build_ms = t0.elapsed().as_millis();

    let workers = cpsat_rust::recommended_worker_count(&input);
    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        partial_admission: true,
        solve_time_limit_secs: bench_solve_cap_secs(),
        ..Default::default()
    };

    let t1 = Instant::now();
    let (solution, status) = match cpsat_rust::solve(&input, &scenario) {
        Ok((sol, info)) => (sol, first_line(&info.status)),
        Err(e) => (Default::default(), format!("error: {e}")),
    };
    let solve_ms = t1.elapsed().as_millis();

    let admitted = solution
        .assignment_counts
        .values()
        .filter(|c| c.values().any(|v| *v > 0))
        .count();
    let valid = !input.workloads.is_empty();
    let in_band = admitted >= s.expect_admitted.0 && admitted <= s.expect_admitted.1;

    BenchResult {
        name: s.name,
        nodes: s.nodes,
        pending_pods: pending.len(),
        workloads: input.workloads.len(),
        anti_pairs: input.anti_affinity_pairs.len(),
        workers,
        build_ms,
        solve_ms,
        status,
        admitted,
        valid,
        in_band,
    }
}

fn first_line(s: &str) -> String {
    s.split(';').next().unwrap_or(s).trim().to_string()
}

pub fn default_matrix() -> Vec<BenchScenario> {
    let g = 8;
    vec![
        sc(
            "baseline-50j-100n",
            100,
            g,
            50,
            1,
            false,
            AntiAffinity::None,
            0,
            (50, 50),
        ),
        sc(
            "baseline-500j-100n",
            100,
            g,
            500,
            1,
            false,
            AntiAffinity::None,
            0,
            (500, 500),
        ),
        sc(
            "scarce-900j-100n",
            100,
            g,
            900,
            1,
            false,
            AntiAffinity::None,
            0,
            (800, 800),
        ),
        sc(
            "fragmented-500j-100n",
            100,
            g,
            500,
            1,
            false,
            AntiAffinity::None,
            6,
            (200, 200),
        ),
        sc(
            "gang8-spread-125j-100n",
            100,
            g,
            125,
            8,
            false,
            AntiAffinity::None,
            0,
            (100, 100),
        ),
        sc(
            "gang8-colocated-125j-100n",
            100,
            g,
            125,
            8,
            true,
            AntiAffinity::None,
            0,
            (100, 100),
        ),
        sc(
            "selfspread-gang8-125j-100n",
            100,
            g,
            125,
            8,
            false,
            AntiAffinity::SelfSpread,
            0,
            (100, 100),
        ),
        sc(
            "global-aa-stress-200j-100n",
            100,
            g,
            200,
            1,
            false,
            AntiAffinity::GlobalStress,
            0,
            (100, 100),
        ),
        sc(
            "global-aa-stress-500j-100n",
            100,
            g,
            500,
            1,
            false,
            AntiAffinity::GlobalStress,
            0,
            (100, 100),
        ),
    ]
}

#[allow(clippy::too_many_arguments)]
fn sc(
    name: &'static str,
    nodes: usize,
    gpus_per_node: i64,
    jobs: usize,
    gang_size: usize,
    colocate: bool,
    anti: AntiAffinity,
    running_fill: i64,
    expect_admitted: (usize, usize),
) -> BenchScenario {
    BenchScenario {
        name,
        nodes,
        gpus_per_node,
        jobs,
        gang_size,
        colocate,
        anti,
        running_fill,
        expect_admitted,
        candidate_node_limit: 0,
    }
}

pub fn run_matrix(scenarios: &[BenchScenario]) -> Vec<BenchResult> {
    scenarios.iter().map(run_scenario).collect()
}

fn bench_solve_cap_secs() -> i64 {
    std::env::var("KSOLVER_BENCH_SOLVE_SECS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(60)
}

pub fn custom_scenario(
    name: &'static str,
    nodes: usize,
    jobs: usize,
    candidate_node_limit: usize,
) -> BenchScenario {
    let expected = jobs.min(nodes.saturating_mul(8));
    let mut scenario = sc(
        name,
        nodes,
        8,
        jobs,
        1,
        false,
        AntiAffinity::None,
        0,
        (expected, expected),
    );
    scenario.candidate_node_limit = candidate_node_limit;
    scenario
}

pub fn print_table(results: &[BenchResult]) {
    println!(
        "solver-core benchmark (build + solve only; solve capped at {}s; not full collect/normalize)",
        bench_solve_cap_secs()
    );
    println!(
        "{:<30} {:>5} {:>6} {:>6} {:>9} {:>4} {:>9} {:>9} {:>10} {:>5} {:>4}",
        "scenario",
        "nodes",
        "pods",
        "wkls",
        "anti_prs",
        "wrk",
        "build_ms",
        "solve_ms",
        "status",
        "adm",
        "band"
    );
    for r in results {
        println!(
            "{:<30} {:>5} {:>6} {:>6} {:>9} {:>4} {:>9} {:>9} {:>10} {:>5} {:>4}",
            r.name,
            r.nodes,
            r.pending_pods,
            r.workloads,
            r.anti_pairs,
            r.workers,
            r.build_ms,
            r.solve_ms,
            &r.status,
            r.admitted,
            if !r.valid {
                "INVALID"
            } else if r.in_band {
                "ok"
            } else {
                "OOB"
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_matching_singletons() {
        let s = sc("t", 4, 8, 6, 1, false, AntiAffinity::None, 0, (6, 6));
        let (cluster, pending) = generate(&s);
        assert_eq!(pending.len(), 6);
        // 6 pending workloads present in the cluster with feasible sets.
        let pend_wls = cluster
            .workloads
            .iter()
            .filter(|w| w.current_node.is_empty())
            .count();
        assert_eq!(pend_wls, 6);
        // every pending (unbound) workload has a non-empty feasible set.
        assert!(cluster
            .workloads
            .iter()
            .filter(|w| w.current_node.is_empty())
            .all(|w| !w.feasible_node_names.is_empty()));
    }

    #[test]
    fn generates_gangs_with_shared_key() {
        let s = sc("t", 4, 8, 2, 3, false, AntiAffinity::None, 0, (2, 2));
        let (_c, pending) = generate(&s);
        assert_eq!(pending.len(), 6);
        let keys: std::collections::HashSet<_> =
            pending.iter().filter_map(|p| p.gang_key.clone()).collect();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn selfspread_sets_unique_labels() {
        let s = sc("t", 4, 8, 2, 2, false, AntiAffinity::SelfSpread, 0, (2, 2));
        let (_c, pending) = generate(&s);
        // each gang member carries a job=job{i} selector
        assert!(pending.iter().all(|p| p
            .anti_affinity_host_selectors
            .iter()
            .any(|sel| sel.reqs.iter().any(|r| r.key == "job"))));
    }

    #[test]
    fn inputs_survive_builder() {
        // gang8-spread style: 3 gangs of 8 on 100 nodes -> 3 workloads reach the solver.
        let s = sc("t", 100, 8, 3, 8, false, AntiAffinity::None, 0, (3, 3));
        let (cluster, pending) = generate(&s);
        let input = build_pending_input(&cluster, &pending, &BTreeMap::new());
        assert_eq!(input.workloads.len(), 3);
        assert!(input.workloads.iter().all(|w| w.group_size == 8));
    }
}
