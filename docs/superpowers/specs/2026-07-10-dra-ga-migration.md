# DRA GA migration spec (resource.k8s.io/v1)

**Status:** proposed — needs an explicit go-ahead (breaking core-dependency upgrade).
**Why:** the VRAM→DRA wedge emits `resource.k8s.io/v1` claims (GA in k8s 1.34), but ksolver's own
DRA demand modeling (`ksolver/src/dra.rs`, `collector.rs`) reads `resource.k8s.io/v1alpha3`, because
`k8s-openapi` is pinned to `v1_32`. On a cluster serving only GA DRA, ksolver's demand modeling
silently no-ops (it fails safe — warns and skips — so no over-admit, but it's incoherent long-term).

## Verified version matrix (2026-07-10, via `cargo add --dry-run`)

| Need | Version |
|------|---------|
| DRA `resource.k8s.io/v1` types (feature `v1_34`) | `k8s-openapi` **0.26 or 0.27** (0.24 → max `v1_32`; 0.25 → max `v1_33` = DRA v1beta only) |
| `kube-rs` paired with k8s-openapi 0.26/0.27 | **0.101+** (currently `kube 0.98`; resolve exact pin at migration time) |

So this is a **coupled major dependency upgrade** (`k8s-openapi` 0.24→0.26/0.27 **and** `kube-rs`
0.98→0.101+), not a Cargo feature flip. `kube` is used across ~13 files; its 0.9x→0.10x bumps carry
breaking API changes (Api/watcher/discovery signatures have churned).

## Scope of work

1. **Cargo.toml**: bump `k8s-openapi` to 0.26/0.27 feature `v1_34`; bump `kube` to the paired version.
2. **kube-rs breakage sweep** (~13 files using `kube::`): fix Api/Client/watcher/discovery API changes.
   Compile-driven; expect changes in the collector, shadow loop, binder, leader election, admission.
3. **Port `ksolver/src/dra.rs`** from `k8s_openapi::api::resource::v1alpha3` to `::v1`:
   - `DeviceRequest` flat fields (`device_class_name`, `count`, `allocation_mode`, `selectors`) move
     under `exactly` (a `DeviceSubRequest`) or become `first_available` (a list of alternatives).
   - `demand_from_device_claim` must read `req.exactly` (and optionally sum `first_available` worst
     case) instead of the flat fields. Keep the existing caveat discipline (unevaluable → not
     counted + disclosure; `All`/unknown allocationMode → caveat).
   - `compute_availability` / ResourceSlice + DeviceClass selectors: re-check field renames.
4. **collector.rs**: the DRA augmentation lists `resource.k8s.io/v1alpha3`; switch to `v1` and update
   the "DRA API not available" warning string.
5. **Tests**: `dra.rs` unit tests use `v1alpha3` fixtures — rebuild against `v1` shapes. Keep the
   cross-checks (count respected, allocationMode caveated, unevaluable not counted).

## Risks / decisions

- **Minimum k8s version**: targeting `v1_34` means ksolver's typed client assumes k8s 1.34 API
  shapes. Confirm that's acceptable for the deployment targets (older clusters still work over the
  wire for core types, but DRA modeling requires GA DRA present).
- Do it on a branch/worktree; it's compile-driven and may take a few iterations to converge.
- Alternative (smaller): target `v1beta2` (k8s-openapi 0.25, feature `v1_33`) instead of GA `v1` —
  less version jump, but then the wedge's emitted `v1` claims and the modeled `v1beta2` still differ
  by one version. Only worth it if the deployment target is 1.33.

## Acceptance

- `cargo build` + `cargo test -p ksolver` (and `--features rust-cp-sat`) green on the new deps.
- `dra.rs` models `resource.k8s.io/v1` claims/slices/classes with the same safety caveats.
- `examples/dra-bundle.yaml` (already `resource.k8s.io/v1`) still validates via server dry-run.
