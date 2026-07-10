# DRA GA migration spec (resource.k8s.io/v1)

**Status:** EXECUTED + verified on an unmerged branch (2026-07-10) — awaiting a merge decision.
Branch `worktree-agent-aac1cc842eaddd3a5` (based on the `scheduler` tip), 3 commits (`a08570a` deps,
`089a8d4` kube 4.0 API, `91698e9` chrono→jiff). Independently verified: `cargo test -p ksolver
--lib` = 525 passed; `--features rust-cp-sat` = 574 passed; clippy clean; DRA safety caveats intact.
Not run: `examples/dra-bundle.yaml` server dry-run (needs a live cluster). Merging is gated on the
maintainer because it resets the dependency baseline / minimum Kubernetes version. The rest of this
spec is the executed plan of record.
**Why:** the VRAM→DRA wedge emits `resource.k8s.io/v1` claims (GA in k8s 1.34), but ksolver's own
DRA demand modeling (`ksolver/src/dra.rs`, `collector.rs`) reads `resource.k8s.io/v1alpha3`, because
`k8s-openapi` is pinned to `v1_32`. On a cluster serving only GA DRA, ksolver's demand modeling
silently no-ops (it fails safe — warns and skips — so no over-admit, but it's incoherent long-term).

## Verified version matrix + empirical blast radius (2026-07-10)

Measured by actually bumping the deps and building (then reverting — main is untouched):

| Need | Version |
|------|---------|
| DRA `resource.k8s.io/v1` types | k8s-openapi feature `v1_34` |
| k8s-openapi carrying `v1_34` **that kube 4.0 pulls** | **0.28** (0.26/0.27 also expose `v1_34`, but `kube 4.0` depends on 0.28, and only one k8s-openapi may carry the feature — so pin 0.28) |
| kube-rs | **4.0** — kube went major since 0.98; latest is 4.0.0 (a multi-major jump, NOT 0.10x as first guessed) |

Toolchain is fine (rustc 1.91; kube 4.0 + k8s-openapi 0.28 resolve and their own code compiles).

**Empirical breakage: 51 compile errors in our code**, across:

| File | errors | nature |
|------|--------|--------|
| `scheduler/gpu_scenarios.rs` | 20 | kube/k8s-openapi type + time changes |
| `dra.rs` | 13 | the v1alpha3 → v1 API shape port |
| `collector.rs` | 11 | **chrono → jiff** time handling + kube Api changes |
| `scheduler/leader.rs` | 3 | chrono → jiff (lease renew time math) |
| `scheduler/binder.rs` | 2 | `create_subresource` now takes 2 generics (`::<serde_json::Value, T>`) |
| `scheduler/pod_filter.rs` | 1 | type change |

**Biggest surprise:** k8s-openapi 0.28 switched its time types from `chrono` to **`jiff::Timestamp`**
(`Time`/`MicroTime` now wrap `jiff`). Our code interops k8s times with `chrono` (`.timestamp()`,
`signed_duration_since`) — those break and need jiff↔chrono conversion (or moving our time handling
to jiff). This is the meatiest sub-task, beyond the DRA port itself.

So: a coupled major upgrade (`kube` 0.98→4.0, `k8s-openapi` 0.24→0.28), ~51 mechanical-to-moderate
fixes across 6 files, dominated by the DRA v1 port + a chrono→jiff time migration. Bounded and
doable (roughly a day), but a real breaking-dependency decision — hence the go-ahead gate.

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
