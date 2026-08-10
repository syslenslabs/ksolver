use anyhow::{bail, Context, Result};
use ksolver::model::{
    Money, ObjectiveProfile, OptimizationInput, OptimizationNode, OptimizationWorkload,
    OptimizationWorkloadMember, ResourceList, ScenarioConfig,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;

const VRAM: &str = "ksolver.dev/vram-mib";
const COMPUTE: &str = "ksolver.dev/snapshot-compute-percent";
const SLOTS: &str = "ksolver.dev/shared-workload-slots";

#[derive(Debug, Deserialize)]
struct Snapshot {
    gpus: Vec<SnapshotGpu>,
}

#[derive(Debug, Deserialize)]
struct SnapshotGpu {
    node: String,
    uuid: String,
    total_mib: i64,
    used_mib: i64,
    gpu_util_pct: i64,
    process_count: usize,
}

#[derive(Debug, Serialize)]
struct Policy {
    vram_reserve_mib: i64,
    vram_headroom_percent: i64,
    compute_floor_percent: i64,
    compute_cap_percent: i64,
    max_workloads_per_gpu: i64,
}

#[derive(Debug, Serialize)]
struct WorkloadResult {
    id: String,
    source_gpu: String,
    destination_gpu: String,
    observed_vram_mib: i64,
    reserved_vram_mib: i64,
    observed_gpu_util_percent: i64,
    reserved_compute_percent: i64,
    exclusive: bool,
}

#[derive(Debug, Serialize)]
struct Report {
    source: String,
    policy: Policy,
    scanned_gpus: usize,
    active_gpu_workloads: usize,
    unknown_or_idle_gpus: usize,
    exclusive_workloads: usize,
    baseline_active_gpus: usize,
    packed_active_gpus: usize,
    recoverable_h100_equivalents: i64,
    placements: Vec<WorkloadResult>,
    note: &'static str,
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|index| args.get(index + 1).cloned())
}

fn parse_i64(args: &[String], name: &str, default: i64) -> Result<i64> {
    flag(args, name)
        .map(|value| value.parse().with_context(|| format!("invalid {name}")))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(input) = flag(&args, "--input") else {
        bail!("usage: snapshot_pack --input <snapshot-manifest.json> [policy flags]");
    };
    let policy = Policy {
        vram_reserve_mib: parse_i64(&args, "--vram-reserve-mib", 10_240)?,
        vram_headroom_percent: parse_i64(&args, "--vram-headroom-percent", 25)?,
        compute_floor_percent: parse_i64(&args, "--compute-floor-percent", 15)?,
        compute_cap_percent: parse_i64(&args, "--compute-cap-percent", 70)?,
        max_workloads_per_gpu: parse_i64(&args, "--max-workloads-per-gpu", 2)?,
    };
    if policy.vram_reserve_mib < 0
        || policy.vram_headroom_percent < 0
        || policy.compute_floor_percent < 0
        || policy.compute_cap_percent <= 0
        || policy.max_workloads_per_gpu <= 0
    {
        bail!("all policy values must be non-negative and capacity values positive");
    }

    let input_path = PathBuf::from(input);
    let snapshot: Snapshot = serde_json::from_slice(
        &std::fs::read(&input_path).with_context(|| format!("read {}", input_path.display()))?,
    )
    .with_context(|| format!("parse {}", input_path.display()))?;

    let mut nodes = Vec::new();
    let mut workloads = Vec::new();
    let mut source = HashMap::new();
    let mut unknown_or_idle_gpus = 0_usize;
    for gpu in &snapshot.gpus {
        if gpu.uuid.is_empty() || gpu.total_mib <= policy.vram_reserve_mib {
            continue;
        }
        let usable_vram = gpu.total_mib - policy.vram_reserve_mib;
        let node_id = format!("{}:{}", gpu.node, gpu.uuid);
        nodes.push(OptimizationNode {
            name: node_id.clone(),
            count: 1,
            effective_capacity: ResourceList {
                pods: policy.max_workloads_per_gpu,
                ..Default::default()
            },
            extended_resources: BTreeMap::from([
                (VRAM.to_string(), usable_vram),
                (COMPUTE.to_string(), policy.compute_cap_percent),
                (SLOTS.to_string(), policy.max_workloads_per_gpu),
            ]),
            price: Money {
                monthly: 1.0,
                ..Default::default()
            },
            ..Default::default()
        });
        if gpu.process_count == 0 {
            unknown_or_idle_gpus += 1;
            continue;
        }

        let observed_vram = gpu.used_mib.max(0);
        let observed_compute = gpu.gpu_util_pct.clamp(0, 100);
        let mut reserved_vram = (observed_vram * (100 + policy.vram_headroom_percent) + 99) / 100;
        let mut reserved_compute = observed_compute.max(policy.compute_floor_percent);
        let exclusive =
            reserved_vram > usable_vram || reserved_compute > policy.compute_cap_percent;
        if exclusive {
            reserved_vram = usable_vram;
            reserved_compute = policy.compute_cap_percent;
        }
        let workload_id = format!("workload:{}", gpu.uuid);
        source.insert(
            workload_id.clone(),
            (
                gpu.uuid.clone(),
                observed_vram,
                reserved_vram,
                observed_compute,
                reserved_compute,
                exclusive,
            ),
        );
        workloads.push(OptimizationWorkload {
            id: workload_id.clone(),
            namespace: "snapshot-import".to_string(),
            name: workload_id.clone(),
            group_size: 1,
            members: vec![OptimizationWorkloadMember {
                namespace: "snapshot-import".to_string(),
                name: workload_id,
                current_node: node_id.clone(),
            }],
            current_node: node_id.clone(),
            current_counts: HashMap::from([(node_id.clone(), 1)]),
            extended_resource_requests: BTreeMap::from([
                (VRAM.to_string(), reserved_vram),
                (COMPUTE.to_string(), reserved_compute),
                (SLOTS.to_string(), 1),
            ]),
            feasible_nodes: nodes.iter().map(|node| node.name.clone()).collect(),
            ..Default::default()
        });
    }

    // `feasible_nodes` must include every physical GPU, including nodes observed after a workload.
    let all_nodes: Vec<String> = nodes.iter().map(|node| node.name.clone()).collect();
    for workload in &mut workloads {
        workload.feasible_nodes = all_nodes.clone();
    }
    let solver_input = OptimizationInput {
        nodes,
        workloads: workloads.clone(),
        ..Default::default()
    };
    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        cost_weight: 1,
        active_node_weight: 1,
        memory_slack_weight: 0,
        cpu_slack_weight: 0,
        // This is an explicit counterfactual re-pack, not an online placement decision.
        churn_weight: 0,
        solve_time_limit_secs: 30,
        objective_profile: ObjectiveProfile::CostBinpack,
        ..Default::default()
    };
    let (solution, _) = ksolver::cpsat_rust::solve(&solver_input, &scenario)?;
    let active: BTreeSet<String> = solution
        .assignments
        .values()
        .filter(|node| !node.is_empty())
        .cloned()
        .collect();
    let mut placements = Vec::new();
    for workload in &workloads {
        let Some(destination) = solution.assignments.get(&workload.id) else {
            bail!("solver returned no placement for {}", workload.id);
        };
        let (
            source_gpu,
            observed_vram,
            reserved_vram,
            observed_compute,
            reserved_compute,
            exclusive,
        ) = source.remove(&workload.id).expect("source record exists");
        placements.push(WorkloadResult {
            id: workload.id.clone(),
            source_gpu,
            destination_gpu: destination.clone(),
            observed_vram_mib: observed_vram,
            reserved_vram_mib: reserved_vram,
            observed_gpu_util_percent: observed_compute,
            reserved_compute_percent: reserved_compute,
            exclusive,
        });
    }
    placements.sort_by(|a, b| a.id.cmp(&b.id));
    let baseline = workloads.len();
    let report = Report {
        source: input_path.display().to_string(),
        policy,
        scanned_gpus: snapshot.gpus.len(),
        active_gpu_workloads: baseline,
        unknown_or_idle_gpus,
        exclusive_workloads: placements.iter().filter(|placement| placement.exclusive).count(),
        baseline_active_gpus: baseline,
        packed_active_gpus: active.len(),
        recoverable_h100_equivalents: baseline as i64 - active.len() as i64,
        placements,
        note: "Snapshot-only advisory. GPU utilization is not historical SM demand; this does not prove whole-run safety or authorize migration/overcommit.",
    };
    serde_json::to_writer_pretty(std::io::stdout(), &report)?;
    println!();
    Ok(())
}
