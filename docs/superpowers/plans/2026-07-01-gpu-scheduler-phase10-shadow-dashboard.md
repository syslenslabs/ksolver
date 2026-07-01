# GPU Scheduler — Phase 10: Shadow Dashboard — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the shadow scheduler's decisions *visible*. Today all the logic (gangs, co-location, anti-affinity, caveats, placements) is only observable via raw JSON at `/api/scheduler/traces`. Serve a small self-contained HTML dashboard at `/` (from `run_shadow`) that polls the traces API and renders the latest decision trace live — summary + per-pod decision table + recent-sequence history. Aligns with the original "see what's happening" goal.

**Why:** The scheduler now makes rich decisions; without a view they're invisible. A dashboard turns the shadow scheduler into something an operator can watch. Deferred in Phase 1; the traces API (Phase 1) + fields (Phases 5–9: caveats, solve_core_millis, solver_status, per-member placement) now make it worthwhile.

**Architecture:** A dependency-free static HTML+JS page (`ksolver/static/shadow.html`) embedded via `include_str!` and served at `GET /` by the shadow HTTP router. Vanilla JS polls `GET /api/scheduler/traces` every few seconds and renders. **All dynamic text is inserted via `textContent`/DOM APIs (never `innerHTML` with data)** to avoid XSS from pod/namespace/node names or caveat strings. No build step, no external assets/CDN.

**Tech Stack:** Rust (axum route + `include_str!`); vanilla HTML/CSS/JS (no deps).

## Global Constraints

- Path convention: `server.rs` uses `include_str!("../static/index.html")`; `shadow.rs` is in `src/scheduler/`, so `include_str!("../../static/shadow.html")`.
- Trace JSON shape (from `DecisionTrace`/`PodDecision`, serde): `{ traces: [ { sequence, observed_pods, decisions: [ { uid, namespace, name, gpu_request, placement: { kind: "placed"|"unplaced", node?, reason? }, caveats: [..] } ], solver_status, solve_millis, solve_core_millis, snapshot_age_millis, note } ] }`. `recent()` returns newest-first.
- **XSS safety:** never interpolate trace strings into `innerHTML`. Build rows with `document.createElement` + `textContent`. This is a hard requirement (pod names/labels/caveats are user-controlled).
- No external network (no CDN/fonts/frameworks) — self-contained so it works in an air-gapped cluster. Inline CSS/JS.
- Read-only view; polling only; never POSTs. Shadow still binds nothing.
- The route is added to the existing shadow `Router` (which already has `/api/scheduler/traces`, `/metrics`, `/healthz`, `/readyz`).
- `cargo fmt` + clean clippy (Rust side). HTML/JS is not linted by clippy.

## File Structure

- Create `ksolver/static/shadow.html` — the dashboard (self-contained).
- Modify `ksolver/src/scheduler/shadow.rs` — `const SHADOW_HTML` + `GET /` route + handler.
- Modify `ksolver/src/scheduler/mod.rs` (test) or shadow.rs — a test asserting the embedded asset contains key markers.
- Modify `README.md` — mention the dashboard at the shadow addr.

## Tasks

### Task 1: The dashboard page
- [ ] **Step 1:** Create `ksolver/static/shadow.html`: a title, an auto-refresh (JS `setInterval`, ~3s), a summary line (sequence, observed_pods, solver_status first token, solve_core_millis, snapshot_age_millis), and a decisions table with columns: Namespace, Name, GPUs, Placement (placed=green / unplaced=amber), Node, Caveats. Include a "last updated" timestamp and a small recent-sequences strip (e.g. last 10: seq → placed/unplaced counts). All data via `textContent`. Fetch `/api/scheduler/traces`; handle empty (`no traces yet`) and fetch errors gracefully (show a banner, keep polling). Poll interval configurable via a `?refresh=SECONDS` query param (default 3), clamped.
- [ ] **Step 2:** Keep it small and readable; inline `<style>` and `<script>`. No external URLs.

### Task 2: Serve it
- [ ] **Step 1:** In `shadow.rs`, add `const SHADOW_HTML: &str = include_str!("../../static/shadow.html");` and a handler `async fn dashboard() -> axum::response::Html<&'static str> { axum::response::Html(SHADOW_HTML) }`; add `.route("/", get(dashboard))` to the shadow `Router`.
- [ ] **Step 2: Test.** Add a unit test (no feature needed) asserting `SHADOW_HTML` contains `"/api/scheduler/traces"` and an element id the JS relies on (e.g. `id="decisions"`), so the asset stays wired. (Place in a `#[cfg(test)] mod` in shadow.rs; it doesn't need the solver feature — but note shadow.rs currently compiles only... it's always compiled. The const + test compile without the feature.)
- [ ] **Step 3: Build + tests + clippy.** `cargo build -p ksolver`; `cargo test -p ksolver`; `cargo clippy -p ksolver --all-targets` → green.
- [ ] **Step 4: README.** Note "open `http://<KSOLVER_SHADOW_ADDR>/` for the live shadow dashboard."
- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/static/shadow.html ksolver/src/scheduler/shadow.rs README.md
git commit -m "feat(scheduler): live shadow-decisions dashboard served at /"
```

### Task 3: Verify (cluster)
- [ ] **Step 1:** On `kind-solver-lab`, add a GPU node + a few pending ksolver GPU pods (incl. a gang and an anti-affine one so caveats show). Run `shadow`; `curl -s localhost:8090/ | head` shows the HTML; load it in a browser (or `curl` + check it references the traces API and renders table structure). Confirm decisions appear and update; nothing bound.
- [ ] **Step 2:** Confirm a pod name containing HTML metacharacters (e.g. `weird<b>name`) is NOT interpreted (rendered as text) — XSS guard. (Kubernetes pod names can't contain `<`, but labels/caveats are safer to treat as untrusted; verify escaping via a crafted trace or code review of `textContent` usage.)

## Self-Review Notes
- Self-contained (no CDN/deps); works air-gapped.
- XSS-safe: all trace data rendered via `textContent`/DOM, never `innerHTML`.
- Read-only polling; shadow still binds nothing; guard test unaffected (dashboard is a GET of a static asset).
- Reuses the existing traces API + fields from Phases 1/5–9 (caveats, solve_core_millis, solver_status).
- Rust side is tiny (const + route + asset test); HTML is the bulk, kept small and inline.
