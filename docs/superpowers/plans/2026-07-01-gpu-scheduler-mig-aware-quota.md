# GPU Scheduler — MIG-Aware Per-Namespace Quota — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the per-namespace GPU quota count **all** GPU resources (whole `nvidia.com/gpu` + MIG slices `nvidia.com/mig-*`), so MIG-heavy namespaces no longer evade their quota.

**Why:** Phase 11 quota sums only `nvidia.com/gpu`; Phase F1 made MIG-sliced pods schedulable but they request `nvidia.com/mig-*`, so they are invisible to the quota — a namespace could exceed its GPU budget entirely via slices. Units policy (documented default): **each GPU-resource unit counts as 1** toward the cap (a MIG slice = 1 unit, like a whole GPU). This is conservative for a budget cap (never under-counts) and simple; a profile-weighted/fractional policy is a future refinement.

**Architecture:** Generalize the quota from a single resource to a **set** of GPU resource names. `QuotaGroup.resource: String` → `resources: Vec<String>`; the solver's quota coefficient becomes `Σ_{r∈resources} total_r(w)` per workload. The builder (a) sums running usage per namespace over ALL GPU resources (not just `nvidia.com/gpu`), and (b) emits each quota group's `resources` = the set of GPU resource names observed. To know which resources are GPUs, thread the GPU-resource matcher (`ShadowConfig::is_gpu_resource`) into `build_pending_input_diagnosed`. No node/feasibility change.

**Tech Stack:** Rust; `model.rs` (QuotaGroup), `cpsat_rust.rs` (quota constraint + tests), `scheduler/pending_input.rs` (builder + threading), `scheduler/shadow.rs` (pass matcher).

## Global Constraints

- **Units policy:** each GPU-resource unit = 1 toward the namespace cap (documented; conservative). Not profile-weighted (future).
- **No feasibility/placement change:** only quota counting changes. Node capacity / residual / anti-affinity untouched.
- **Backward compatible:** with only `nvidia.com/gpu` present, behavior is identical to Phase 11 (the set is `{nvidia.com/gpu}`).
- **Zero-ripple for non-quota callers:** the 2-arg test wrapper `build_pending_input` stays; thread the matcher only through `build_pending_input_diagnosed`. Bench/tests that call it pass a default matcher (whole GPU only) or the real one.
- `cargo fmt` + clean clippy; update quota solver tests to `resources`; add a MIG-quota builder test. Binds nothing.

## Tasks

### Task 1: QuotaGroup carries a resource set
- [ ] `model.rs`: change `pub resource: String` → `pub resources: Vec<String>` on `QuotaGroup` (serde default). Build; the compiler flags the solver + builder + test literals.

### Task 2: Solver sums over the resource set
- [ ] `cpsat_rust.rs` quota block: coefficient per workload `w` = `group.resources.iter().map(|r| w.extended_resource_requests.get(r).copied().unwrap_or(0)).sum()`; guard `group.limit >= 0` and non-empty `resources`. Update the 3 quota tests to build `resources: vec!["nvidia.com/gpu".into()]` (identical behavior). Add a test: a workload with `nvidia.com/mig-1g.5gb: 1` under a group `resources:["nvidia.com/mig-1g.5gb"]`, limit 0 ⇒ not admitted (proves MIG counts). Run → commit.

### Task 3: Builder — matcher threading + MIG-aware sums
- [ ] `build_pending_input_diagnosed` gains a parameter: `is_gpu_resource: &dyn Fn(&str) -> bool` (or a small `GpuResources { names, prefixes }` value). The 2-arg test wrapper and callers that don't care pass a closure matching only `nvidia.com/gpu`.
- [ ] Running-usage pass: `running_gpu_by_ns` sums, per running pod, `Σ_{res: is_gpu_resource(res)} qty` (not just `nvidia.com/gpu`).
- [ ] Collect `gpu_resource_set: BTreeSet<String>` = every resource name across nodes+workloads for which `is_gpu_resource(name)` holds. Emit each `QuotaGroup.resources` = that set (sorted vec). Quota `remaining = (cap - running_gpu_by_ns[ns]).max(0)` unchanged in spirit (now MIG-inclusive).
- [ ] Update all `build_pending_input_diagnosed` call sites (shadow passes `|n| cfg.is_gpu_resource(n)`; bench/tests pass a whole-GPU closure). Test: a namespace with a running MIG-slice pod + a pending MIG-slice pod, quota=1 ⇒ the quota group's `resources` includes the mig name and `remaining` reflects the running slice. Run → commit.

### Task 4: Wire shadow + docs
- [ ] `shadow::run_one_solve`: pass `&|n| cfg.is_gpu_resource(n)` to the diagnosed builder.
- [ ] Full `cargo test --features rust-cp-sat` + clippy. README: quota now counts whole GPUs + MIG slices (each unit = 1; profile-weighted quota is a future refinement). Update memory.

## Self-Review Notes
- Quota now MIG-inclusive; whole-GPU-only clusters unchanged (set = {nvidia.com/gpu}).
- Units policy documented (per-allocation, conservative); weighted policy deferred.
- Feasibility/placement untouched; only quota coefficient + running sum change.
- 2-arg wrapper keeps non-quota callers/tests zero-ripple.
