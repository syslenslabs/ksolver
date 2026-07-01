# GPU Scheduler — Phase F1: MIG (mixed strategy) Support — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the shadow scheduler observe and place MIG-sliced GPU pods (NVIDIA `mixed` strategy, resources like `nvidia.com/mig-1g.5gb`), which today are invisible because `effective_gpu_request` only matches exact configured names.

**Why:** MIG is the most common real fractional-GPU deployment. Per `docs/superpowers/specs/2026-07-01-fractional-gpu-design.md` (F1), the solver ALREADY handles MIG slices as generic integer extended resources (placement, residual capacity, gang scaling all flow through `extended_resource_requests`/`node.extended_resources`). The only gap is *scope detection*: a pending MIG pod has `effective_gpu_request == 0` under exact-name matching, so `classify` drops it and shadow never considers it. Fixing the GPU-resource matcher to recognize `nvidia.com/mig-*` closes the gap.

**Architecture:** Introduce a GPU-resource matcher: a resource name is a GPU iff it exactly equals a configured `gpu_resource_names` entry OR matches a configured prefix glob (default set includes `nvidia.com/mig-`). `container_gpu`/`effective_gpu_request` sum ALL matching resources (whole GPUs + MIG slices). Once a MIG pod is in-scope, the existing pipeline (`build_pending_input` uses the full `extended_resource_requests` map; solver constrains each resource generically; residual uses `node.extended_resources`) places it with zero further change. Quota stays keyed on whole `nvidia.com/gpu` for now (per-resource MIG quota is a documented follow-up — the spec's open question). Decision-trace `gpu_request` becomes the summed matching-slice count.

**Tech Stack:** Rust; `scheduler/config.rs`, `scheduler/pod_filter.rs`. Solver/builder unchanged.

## Global Constraints

- **Scope: MIG `mixed` strategy** (distinct `nvidia.com/mig-*` resources). `single` strategy (MIG exposed as `nvidia.com/gpu`) already works via the whole-GPU path — no change, profile invisible.
- **No solver/builder change:** MIG placement uses the existing generic extended-resource machinery. This plan only changes scope detection + request summing + display.
- **Quota unchanged (documented):** namespace GPU quota still counts `nvidia.com/gpu`; MIG-aware quota is a follow-up (spec open question — per-resource caps).
- **No regression:** exact-name matching (`nvidia.com/gpu`) behaves identically; a pod with only non-GPU extended resources stays out of scope.
- `cargo fmt` + clean clippy; unit tests; binds nothing.

## File Structure

- `ksolver/src/scheduler/config.rs` — add `gpu_resource_prefixes: Vec<String>` (default `["nvidia.com/mig-"]`), parsed from env `KSOLVER_SHADOW_GPU_RESOURCE_PREFIXES` (CSV); a matcher method `is_gpu_resource(&self, name) -> bool`.
- `ksolver/src/scheduler/pod_filter.rs` — `container_gpu`/`effective_gpu_request` take the matcher (config) instead of a plain `&[String]` exact list; sum matching resources.

## Tasks

### Task 1: Config matcher
- [ ] Add `pub gpu_resource_prefixes: Vec<String>` to `ShadowConfig`; default `vec!["nvidia.com/mig-".to_string()]`; `from_env` reads CSV `KSOLVER_SHADOW_GPU_RESOURCE_PREFIXES` (empty ⇒ default). Add:
```rust
pub fn is_gpu_resource(&self, name: &str) -> bool {
    self.gpu_resource_names.iter().any(|n| n == name)
        || self.gpu_resource_prefixes.iter().any(|p| name.starts_with(p))
}
```
- [ ] Update all `ShadowConfig { .. }` literals (config/pod_filter/watch_state tests) with `gpu_resource_prefixes: ...`. Tests: `is_gpu_resource("nvidia.com/gpu")` true; `is_gpu_resource("nvidia.com/mig-1g.5gb")` true; `is_gpu_resource("cpu")`/`"example.com/fpga"` false; empty-env ⇒ default prefix present. Run → commit.

### Task 2: Request summing over the matcher
- [ ] Change `container_gpu(container, gpu_names: &[String])` → `container_gpu(container, cfg: &ShadowConfig)` (or pass a `&dyn Fn(&str)->bool`), summing `qty` for every resource where `cfg.is_gpu_resource(name)`. Likewise `effective_gpu_request(pod, cfg)`. Update `classify` call site (`effective_gpu_request(pod, cfg)`).
- [ ] `parse_gpu_quantity` unchanged (integer; MIG slice counts are integers).
- [ ] Tests: a pod requesting `nvidia.com/mig-1g.5gb: 2` ⇒ `effective_gpu_request == 2`; a pod with `nvidia.com/gpu: 1` + `nvidia.com/mig-1g.5gb: 1` ⇒ 2; a pod with only `cpu`/`memory` ⇒ 0 (out of scope). Update the public `effective_gpu_request` signature's other callers if any (grep). Run → commit.

### Task 3: Verify end-to-end + docs
- [ ] Add a `pod_filter`/`pending_input` test (or reuse): a MIG node (`extended_resources: {"nvidia.com/mig-1g.5gb": 7}`) + a classified MIG pod ⇒ `build_pending_input` emits the workload and it's feasible on the node (proves the generic path places MIG once in-scope). [Use a synthetic `PendingGpuPod` with `gpu_request` set + a `NormalizedWorkload` carrying `extended_resource_requests {"nvidia.com/mig-1g.5gb":1}` and the node capacity.]
- [ ] Full `cargo test --features rust-cp-sat` + clippy. README: document `KSOLVER_SHADOW_GPU_RESOURCE_PREFIXES` and that MIG (mixed) pods are now observed/placed; quota still counts whole `nvidia.com/gpu` (MIG-aware quota is a follow-up). Update memory. Optional cluster smoke: a fake node with `nvidia.com/mig-1g.5gb` capacity + a pending MIG ksolver pod ⇒ placed in trace; binds nothing.

## Self-Review Notes
- Solver/builder untouched — MIG rides the existing generic extended-resource path; only scope detection + summing + display change.
- `mixed` strategy scoped; `single` unaffected (documented).
- Exact-name behavior unchanged ⇒ no regression to whole-GPU pods.
- Quota deliberately whole-GPU for F1 (documented follow-up).
