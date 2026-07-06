# Global GPU Scheduling as Constrained Optimization

### A CP-SAT formulation for admission, packing, and gang placement on GPU fleets

*Technical whitepaper — ksolver GPU scheduler*
*Audience: operations-research / optimization reviewers*

> **Format note.** This document is Markdown with LaTeX math; it converts to PDF via
> `pandoc --pdf-engine=tectonic -H docs/whitepaper-preamble.tex`. Math and tables render on GitHub;
> the TikZ figure in §0 renders only in the PDF build (its content is duplicated as a table just
> below it, which renders everywhere). A standalone `.tex` can be produced on request.

---

## 0. Preliminaries — Kubernetes objects, in OR terms

This paper is written for an operations-research reader who may not use Kubernetes day to day.
Kubernetes is the cluster's **workload-placement layer**; the objects this model reasons about nest as
**Cluster $\supset$ Node $\supset$ Pod $\supset$ Container**. Figure 1 shows that nesting and maps each
object to the notation of §2; the table below states the same mapping in text.

```{=latex}
\begin{center}
\begin{tikzpicture}[
  font=\small,
  cluster/.style={draw=black!55, thick, rounded corners=6pt},
  nodebox/.style={draw=blue!55!black, thick, rounded corners=4pt, fill=blue!4},
  podbox/.style={draw=green!45!black, thick, rounded corners=3pt, fill=green!8},
  ctr/.style={draw=orange!65!black, rounded corners=2pt, fill=orange!20,
              minimum width=1.6cm, minimum height=0.72cm, align=center, font=\footnotesize},
  gpu/.style={draw=black!45, minimum size=0.52cm, inner sep=0pt, font=\scriptsize, fill=gray!12},
  gpuon/.style={gpu, fill=green!40, draw=green!45!black},
  ann/.style={font=\footnotesize\itshape, text=black!65, align=left},
]

% Cluster
\node[cluster, minimum width=11.9cm, minimum height=4.85cm, anchor=north west] (cl) at (0,0) {};
\node[anchor=north west, font=\bfseries\footnotesize, text=black!60]
  at (cl.north west) [shift={(6pt,-5pt)}] {Cluster — we model the candidate GPU-node set $N$};

% Node
\node[nodebox, minimum width=11.0cm, minimum height=3.5cm, anchor=north west] (nd)
  at ($(cl.north west)+(0.45,-0.66)$) {};
\node[anchor=north west, font=\bfseries] at (nd.north west) [shift={(7pt,-6pt)}]
  {Node $n$ — one physical machine \; \small(a bin / knapsack)};
\node[anchor=north west, ann, text=blue!40!black] at (nd.north west) [shift={(7pt,-26pt)}]
  {8\,GPU $\cdot$ 64\,vCPU $\cdot$ 512\,GiB allocatable\; $\to$\; residual capacity $c_{n,r}$, cost $\pi_n$};

% GPU row; shade the 2 the pod occupies
\foreach \i in {1,...,8}{
  \node[gpu, anchor=north west] (g\i) at ($(nd.north west)+(0.6+0.64*\i-0.64, -1.28)$) {\i};
}
\node[gpuon, anchor=north west] at (g3.north west) {3};
\node[gpuon, anchor=north west] at (g4.north west) {4};

% Pod
\node[podbox, minimum width=4.15cm, minimum height=1.4cm, anchor=north west] (pod)
  at ($(nd.north west)+(0.6,-2.0)$) {};
\node[anchor=north west, font=\bfseries\footnotesize, text=green!30!black]
  at (pod.north west) [shift={(5pt,-4pt)}] {Pod — a replica of $w$};
\node[ctr, anchor=north west] (c1) at ($(pod.north west)+(0.22,-0.5)$) {container};
\node[ctr, anchor=north west] (c2) at ($(c1.north east)+(0.16,0)$) {container};

% Pod -> its 2 GPUs
\draw[-{Stealth}, green!45!black, thick, dashed]
  (pod.north) .. controls +(0,0.55) and +(0,-0.65) .. ($(g3.south east)!0.5!(g4.south west)$);
\node[anchor=west, ann, text=green!35!black] at (pod.east) [shift={(12pt,0)}]
  {demand $\rho_{w,r} =$ 2 GPU\\ (whole, exclusive)};

% Gang panel
\node[anchor=north west, font=\bfseries] (gh) at (0,-5.5)
  {Workload $w$ — one pod, or a \textbf{gang} of $g_w$ identical pods};
\foreach \i in {1,...,4}{
  \node[podbox, minimum width=1.55cm, minimum height=0.74cm, anchor=north west] (p\i)
    at ($(gh.south west)+(0.15+1.8*\i-1.8, -0.28)$) {pod};
}
\draw[decorate, decoration={brace, amplitude=6pt, mirror}]
  (p1.south west) -- (p4.south east)
  node[midway, below=7pt, ann] (brlbl) {\textbf{all-or-nothing} admission};
\node[anchor=north, ann] at (brlbl.south) [yshift=-1pt]
  {either all $g_w$ placed ($p_w{=}1$) or none:\quad $\displaystyle\sum_{n} x_{w,n} = g_w\, p_w$};

\end{tikzpicture}
\end{center}
```

**Figure 1 — Kubernetes objects and their role in the optimization model.** A *node* is a physical
machine and acts as a capacity-constrained bin; a *pod* is the atomic unit the scheduler places (one
replica of a workload $w$) and occupies **whole** GPUs; *containers* are the processes inside a pod.
A *workload* $w$ is a single pod or a **gang** of $g_w$ identical pods that is admitted all-or-nothing.

| Kubernetes object | What it is (plain) | Role in this model | Symbol / section |
|---|---|---|---|
| **Cluster** | the whole fleet of machines | the **candidate GPU-node subset** we model (not necessarily every node) | $N$ (§2.1) |
| **Node** | one physical machine (e.g. an 8-GPU server) | a capacity-constrained **bin / knapsack** with an activation cost | $n\in N$; **residual** capacity $c_{n,r}$; cost $\pi_n$ (§2, §4.1) |
| **Pod** | smallest deployable unit; runs on **exactly one** node | the **item to place** — one replica of a workload; consumes whole GPUs | replica of $w$; placement $x_{w,n}$; demand $\rho_{w,r}$ (§3, §4.1) |
| **Container** | a process (with its GPU/CPU/RAM) running inside a pod | app-container requests sum (Kubernetes also takes init-container maxes + pod overhead); the effective pod request folds into $\rho_{w,r}$ | folded into $\rho_{w,r}$ |
| **Workload / gang** | one pod, or a set of $g_w$ identical pods that must run together | the **atomic admission** unit | $w\in W$; gang size $g_w$; admission $p_w$ (§2, §4.2) |
| **Request** | resources a pod reserves (a *contract*, not a measurement) | the packing coefficient | $\rho_{w,r}$; **CPU/mem only** may become $\hat\rho_{w,r}$ (§9.1); GPU stays integer |

> **Two facts to carry into §4.** (i) A pod runs on **exactly one** node — placement is an assignment,
> not a divisible flow to be split across machines. (ii) Whole GPUs are **integer and exclusive**: a
> pod requesting 2 GPUs occupies 2 entire devices. Sharing changes only the *units* exposed to the
> scheduler — MIG advertises smaller integer slices, time-slicing inflates the integer count, DRA
> exposes per-class device units (§8) — it is never fractional. This is why node capacity (§4.1) is an
> **integer bin-packing** constraint, not a continuous one — the source of the combinatorial hardness
> in §10.

---

## 1. Problem statement

The default Kubernetes scheduler is a **greedy, one-pod-at-a-time, score-per-node** heuristic. For
general-purpose services this is the right default: it is fast, online, and locally reasonable. On
**GPU fleets it does not globally optimize the objective we care about** (admitted GPU workloads —
by count or value — and fleet cost), for three structural reasons:

1. **Myopia (no lookahead).** Pods are placed sequentially; each decision is final. The out-of-the-box
   `NodeResourcesFit` scoring is `LeastAllocated` (spread) — configurable, but this is what most
   clusters run — which *fragments* GPU nodes: a stream of 1-GPU jobs is smeared across many 8-GPU
   nodes, stranding partial capacity so that a later 4- or 8-GPU job cannot be placed even though the
   aggregate free GPU count is sufficient. (A `MostAllocated` bin-packing profile mitigates this but
   is still greedy and gang-unaware; we compare against **both** configs in §11.)
2. **No atomic gangs.** Distributed-training jobs need *all* `k` workers scheduled together
   (co-located or spread by policy) or *none* — a half-placed gang holds GPUs hostage while making no
   progress. The default scheduler has no all-or-nothing admission primitive; partial placement is a
   silent waste sink.
3. **No global objective.** There is no notion of "minimize the number of powered-on GPU nodes"
   or "maximize admitted high-value GPU-hours subject to quota and topology." Consolidation for
   scale-down, tenant fairness, and cost are emergent at best.

GPUs are the expensive, shape-sensitive, easily-fragmented resource in the cluster, so these
inefficiencies translate directly into stranded capacity and dollars. The thesis of this work is
that **GPU placement should be solved, not greedily scored** — as a batch, over the whole pending
queue and the whole fleet, with an explicit objective and hard constraints. We formulate it as a
mixed-integer linear program and solve it with Google OR-Tools **CP-SAT**.

*Prior art.* Batch and gang scheduling on Kubernetes are not new — Volcano, Kueue, and YuniKorn add
gang/queue semantics, and cluster-autoscaler bin-packs for scale-down. Our claims are therefore
scoped to the **default kube-scheduler's one-pod scoring loop** (the baseline in most production GPU
clusters), and the contribution is the *explicit global optimization model* (objective + constraints
below) and its GPU-specific extensions, evaluated head-to-head against the real kube-scheduler (§11).

The system (**ksolver**) runs in **shadow mode** by default: it observes pending GPU pods, computes
a placement (optimal when proven, otherwise the best feasible incumbent within a time budget — §10),
and records a decision trace, **binding nothing** unless real binding is explicitly enabled. This
lets it be evaluated against the production scheduler without risk.

### 1.1 Model class and scope

This is a **mixed-integer linear model** (0-1 and bounded-integer variables, linear constraints and
objective) solved with OR-Tools **CP-SAT** — chosen over a branch-and-bound MILP mainly because the
reified indicator constraints (co-location, presence, co-placement) and the large integer objective
weights are handled well by CP-SAT's SAT-based search and integer propagation, and because we want an
**anytime** solver that returns a good feasible incumbent under a latency budget. Structurally it is a
**restricted generalized-assignment / multiple-knapsack / bin-packing model with admission
variables** — hence NP-hard (§10). It is not a nonlinear or general (finite-domain) CP model.

**Optimization universe (what is fixed vs. decided).** Running pods are *fixed context*: their
allocation is folded into the **residual** capacities $c_{n,r}$ (allocatable minus running requests,
DaemonSet reserve, and headroom). The decision problem is therefore: *given the fixed running
allocation, choose admission and placement of the **pending** workloads $W$ over residual capacity.*
Partially-scheduled gang members are not represented — a gang is either wholly pending or its placed
members are fixed context.

---

## 2. Notation

### 2.1 Sets and indices

| Symbol | Meaning |
|---|---|
| $N$ | set of candidate nodes $n$ (physical GPU nodes; in shadow mode each has multiplicity 1) |
| $W$ | set of pending *workloads* $w$ — a workload is a single pod or a gang of $g_w$ identical pods grouped by a gang label |
| $R$ | set of schedulable resource types $r$: $\{\text{cpu}, \text{mem}, \text{ephemeral}, \text{pods}\}\cup R^{\text{gpu}}$ |
| $R^{\text{gpu}}$ | GPU-class extended resources: whole GPU `nvidia.com/gpu`, MIG slices `nvidia.com/mig-*`, and synthetic DRA classes `dra.ksolver/<class>` |
| $F_w \subseteq N$ | **feasible node set** of workload $w$ after the pre-solve projection (§6) |
| $K$ | topology keys (e.g. `kubernetes.io/hostname`, `topology.kubernetes.io/zone`, rack) |
| $D_k$ | the partition of nodes into topology domains for key $k\in K$ (hostname domains are singletons) |
| $A^{=}\subseteq W\times K$ | self-spread rules $(w,k)$: $\le 1$ replica of $w$ per domain in $D_k$ |
| $A^{\ne}\subseteq W\times W\times K$ | cross-workload anti-affinity rules $(a,b,k)$ (after §6 projection) |
| $P\subseteq W\times W\times K$ | soft co-placement rules $(a,b,k)$: $a,b$ prefer to share a domain in $D_k$ |
| $Q$ | quota groups $q$, each a set $W_q\subseteq W$ with per-resource caps $\ell_{q,r}$ |

### 2.2 Parameters

| Symbol | Meaning |
|---|---|
| $g_w \in \mathbb{Z}_{>0}$ | gang size of $w$ (1 for singletons) |
| $\rho_{w,r} \in \mathbb{Z}_{\ge 0}$ | **per-replica** request of resource $r$ by workload $w$ |
| $c_{n,r} \in \mathbb{Z}_{\ge 0}$ | **residual** capacity of resource $r$ on node $n$ (allocatable minus already-running pods, minus DaemonSet reserve/headroom) |
| $\pi_n \in \mathbb{Z}_{\ge 0}$ | monetary cost of powering on node $n$ (e.g. monthly price) |
| $\ell_q \in \mathbb{Z}_{\ge 0}$ | resource cap of quota group $q$ (e.g. per-namespace GPU limit minus current usage) |
| $\gamma_w = \sum_{r\in R^{\text{gpu}}}\rho_{w,r}$ | total GPU units per replica of $w$ |
| $s^{\text{node}}_{w,n},\, s^{\text{pod}}_{a,b}$ | soft (preferred-affinity) scores, §7 |
| $\Omega$ | admission weight (auto-computed dominating constant, §5) |
| $\hat\rho_{w,r}$ | **effective (usage-adjusted) per-replica demand** used for packing when prediction is enabled (§9.1); defaults to $\rho_{w,r}$ |
| $\phi\in[0,1]$ | usage floor ratio — a workload retains $\ge\phi$ of its request (§9.1) |
| $\tau,\ \varepsilon$ | memory quantile $\tau=1-\varepsilon/100$ and per-workload overflow budget $\varepsilon$ (%) (§9.1) |
| $\beta_r\ge 1$ | usage safety factor for point estimates (cpu, mem) (§9.1) |
| $\theta_r\ge 1$ | node-side overcommit ratio for compressible resources (§9.1) |
| $\hat v_w \ge 0$ | **predicted peak VRAM** (device memory) per replica of GPU workload $w$ (§9.2) |
| $M_n \ge 0$ | per-GPU device memory of node/GPU-class $n$ (§9.2) |

The **integrality and non-overcommit of GPUs is a modeling invariant**: extended resources are
integer-valued and non-overcommittable, so the aggregate capacity inequality (§4.1) prevents
overcommit — it caps total GPU units placed on a node at its capacity, and sharing is expressed
*only* by changing capacity/units (time-slicing, MIG, DRA), never by fractional allocation. Note
this is an **aggregate count** constraint, not physical device assignment: it does not pin GPU IDs,
NUMA/NVLink topology, or homogeneous GPU model within a gang unless those are pushed into $F_w$ or
into distinct resource dimensions.

$\pi_n$ is the cost charged when node $n$ is **activated by the pending batch** ($y_n=1$); nodes that
are already powered on and merely hosting running pods carry no re-activation cost here. Modeling
committed/reserved capacity with a different (e.g. zero-marginal) coefficient is a straightforward
parameterization.

---

## 3. Decision variables

$$
\begin{aligned}
x_{w,n} &\in \{0,1,\dots,g_w\} && \text{replicas of } w \text{ placed on node } n,\quad n\in F_w\\
p_w &\in \{0,1\} && \text{admission indicator: } w \text{ is fully placed}\\
y_n &\in \{0,1\} && \text{node-activation indicator: forced to } 1 \text{ when } n \text{ hosts a replica (§4.1), driven to } 0 \text{ by cost}\\
u_{w,n} &\in \{0,1\} && \text{co-location: } w \text{ uses node } n \text{ (gangs only)}\\
z^{k}_{w,d} &\in \{0,1\} && \text{presence: } w \text{ has} \ge 1 \text{ replica in domain } d\in D_k \text{ (cross anti-affinity; hostname domains are singletons)}\\
b^k_{a,b,d} &\in \{0,1\} && \text{co-placement: } a \text{ and } b \text{ both occupy domain } d\in D_k
\end{aligned}
$$

Variables $x_{w,n}$ exist **only for feasible pairs** $n\in F_w$; infeasible placements are pruned
pre-solve (§6), which is what keeps the model small.

---

## 4. Constraints

### 4.1 Node capacity (all resources, incl. GPU exclusivity)

For every node $n$ and resource $r$:

$$
\sum_{w\,:\,n\in F_w} \rho_{w,r}\, x_{w,n} \;\le\; c_{n,r}\, y_n .
$$

Because $x_{w,n}$ and $c_{n,r}$ are integers and $R^{\text{gpu}}\subseteq R$, this enforces **whole-GPU
exclusivity** for `nvidia.com/gpu`, **per-slice exclusivity** for MIG, and **per-class device counts**
for DRA — no fractional or overcommitted GPU allocation is representable. The coupling to $y_n$
*forces* $y_n=1$ whenever a replica is placed on $n$; the converse ($y_n=1$ with nothing placed) is
feasible but never optimal under positive cost, so at any cost-optimum $y_n$ coincides with the true
active indicator. (Where node cost may be zero, an explicit linkage
$y_n\le\sum_{w:n\in F_w}x_{w,n}$ would enforce the "iff" directly.)

The coefficient $\rho_{w,r}$ here is the declared request. When the **predictive (usage-adjusted)**
mode is enabled it is replaced by the effective demand $\hat\rho_{w,r}\le\rho_{w,r}$ of §9.1 for the
compressible resources ($r\in\{\text{cpu},\text{mem}\}$); GPU-class resources $r\in R^{\text{gpu}}$
always keep the true integer request (the exclusivity invariant forbids fractional/predicted GPU
demand). VRAM is handled as a *feasibility* predicate, not a capacity coefficient (§9.2).

### 4.2 Atomic gang admission (all-or-nothing latch)

$$
\sum_{n\in F_w} x_{w,n} \;=\; g_w\, p_w \qquad \forall w\in W .
$$

Either all $g_w$ replicas are placed ($p_w=1$) or none ($p_w=0$). There is no feasible partial
placement — this is the property the default scheduler lacks.

### 4.3 Single-node co-location (co-located gangs)

For gangs that require one node:

$$
x_{w,n} \le g_w\, u_{w,n}, \qquad \sum_{n\in F_w} u_{w,n} \le p_w .
$$

The second inequality forces at most one active node per admitted co-located gang.

### 4.4 Anti-affinity

**Self-spread** (rule $(w,k)\in A^{=}$, e.g. one replica per host/zone): for each domain $d\in D_k$,

$$
\sum_{n\in d\cap F_w} x_{w,n} \le 1 \qquad \forall\,(w,k)\in A^{=},\; \forall d\in D_k .
$$

**Cross-workload** (rule $(a,b,k)\in A^{\ne}$): with per-domain presence variables $z^{k}_{w,d}$
defined by $\sum_{n\in d\cap F_w} x_{w,n}\le g_w\, z^{k}_{w,d}$,

$$
z^{k}_{a,d} + z^{k}_{b,d} \le 1 \qquad \forall\,(a,b,k)\in A^{\ne},\; \forall d\in D_k .
$$

For the hostname key each domain is a single node (a singleton domain), so this reduces to the
per-node exclusion — the case implemented in-solver; for zone/rack keys the domain spans multiple
nodes. Anti-affinity of pending pods
against **already-running** pods (including the symmetric direction — a running pod's rule excluding
an incoming pod) and all label-selector / namespace-scope semantics
(`In/NotIn/Exists/DoesNotExist`, `namespaceSelector`) are resolved during the feasibility projection
(§6), so the solver sees only the resulting node exclusions.

### 4.5 Tenant / namespace quota

Quota is defined **per resource name** $r$ (whole GPU, a specific MIG profile, or a DRA class are
*not* mutually fungible), with cap $\ell_{q,r}$:

$$
\sum_{w\in W_q} \rho_{w,r}\, g_w\, p_w \;\le\; \ell_{q,r} \qquad \forall q\in Q,\; \forall r\in R^{\text{gpu}} .
$$

The coefficient $\rho_{w,r} g_w$ is the whole gang's demand for resource $r$ (per-replica $\times$ gang
size), times its admission bool — an exact integer. Quota groups only reference workloads that own an
admission variable $p_w$, so the offline (non-partial-admission) path, which has no $p_w$, is
unaffected.

> **Implemented default.** The current shadow implementation applies a single aggregate GPU cap that
> counts **each GPU-class unit as 1** (whole GPU and every MIG slice alike) — a deliberately
> conservative policy, not a claim that a `mig-1g.5gb` slice equals a whole GPU. Per-resource caps
> ($\ell_{q,r}$ above) and profile-weighted conversions are the documented refinement.

### 4.6 Feasibility projection

For $n \notin F_w$, $x_{w,n}$ simply does not exist. $F_w$ is computed in §6 and encodes
node-selector, taints/tolerations, required node affinity (OR of `nodeSelectorTerm`s, `matchFields`),
required pod anti-affinity exclusions, volume/topology (PVC zone) constraints, residual-capacity
fit, MIG/DRA availability, and GPU-resource matching.

---

## 5. Objective — admission $\gg$ (cost + shaping) $\gg$ soft

The scheduler optimizes a **two-phase** objective. Phase 1 folds the admission and cost/shaping tiers
into one weighted objective via an auto-computed dominating weight $\Omega$; the soft-preference tier
is a **second solve** (§ below).

**Phase 1 — maximize admission value, then minimize a cost/shaping objective:**

$$
\min\;\; -\,\Omega \sum_{w\in W} v_w\, p_w \;+\; \underbrace{\sum_{n\in N}\pi_n\, y_n \;+\; \alpha\sum_n y_n \;+\; \textstyle\sum(\text{slack, churn})}_{\text{cost/shaping objective } C(\mathbf{y},\mathbf{x})} .
$$

$v_w$ is the **admission value** of workload $w$. The default is $v_w=1$ (maximize admitted
*workload count*); the `gpu-gang-aware` profile sets $v_w$ from GPU units ($\gamma_w g_w$), priority,
business value, or gang-completion. $\Omega$ is an $i128$-guarded bound chosen to strictly dominate
the maximum magnitude of $C$, so **admission is lexicographically first**. Note that the second tier
is the **blended objective $C$ (fleet cost plus active-node/slack/churn shaping), not fleet cost in
isolation** — so shaping terms *can* trade off against raw cost within tier 2 (a true per-term
lexicographic order would require nested solves; see the note below).

**Phase 2 — among Phase-1-optimal, admission-fixed solutions, maximize soft score:**
run only when Phase 1 is proven **optimal**. Pin each $p_w$ and the exact Phase-1 objective value
(admission $+\,C$), then

$$
\min\;\; -\!\!\sum_{w,n} s^{\text{node}}_{w,n}\,x_{w,n} \;-\!\!\sum_{(a,b,k)\in P}\;\sum_{d\in D_k} \omega_{a,b}\, b^k_{a,b,d},
$$

subject to the co-placement linkage $b^k_{a,b,d} \le \sum_{n\in d\cap F_a}x_{a,n}$ and
$b^k_{a,b,d}\le\sum_{n\in d\cap F_b}x_{b,n}$ (an upper-bounded reward maximized in the objective, so
at optimum $b^k_{a,b,d}=1$ iff both $a$ and $b$ occupy domain $d$). Because the **full Phase-1
objective value and the admitted set are pinned**, the soft pass **cannot change admission or the
Phase-1 (cost + shaping) objective value** — it only reorders placements within that optimal face.
(If a guarantee on *raw* fleet cost specifically is desired, pin $\sum_n \pi_n y_n$ rather than the
blended value.) The pinned Phase-1 value is recomputed in $i128$ rather than read from the reported
`f64`, which is unreliable once $\Omega \sim 10^{15}$ exceeds $2^{53}$.

*Modeling note.* Weighted-domination is a standard way to encode lexicographic priorities, but the
very large integer coefficient $\Omega\approx 10^{15}$ **weakens CP-SAT's bounds and search** (and
must be $i128$-guarded against overflow) — so proving optimality on large **flat singleton** batches
is slow even though a good incumbent is found in milliseconds. Two cleaner alternatives an OR
reviewer would consider: a **true lexicographic solve** (maximize $\sum p_w$ to optimality, fix it,
then minimize cost) instead of a single dominating weight; or a preprocessing bound on $\sum p_w$.
The current split (hard weights for admission+cost, a *separate* second solve for the soft tier)
keeps the small soft coefficients from interacting with $\Omega$ but does not by itself fix the
admission/cost proof time — that is noted as open work (§13).

---

## 6. Pre-solve feasibility projection

Rather than encode every scheduler predicate as constraints, ksolver **projects them into $F_w$**
before building the model. This is both a fidelity choice (mirror the real Filter phase) and a
size reduction (variables only for feasible $(w,n)$). The projection composes:

- **Static predicates:** `nodeSelector`, node taints vs pod tolerations, required node affinity as
  an **OR of `nodeSelectorTerm`s** (each an AND of `matchExpressions` over labels plus `matchFields`
  over `metadata.name`), and volume topology (PVC → PV zone).
- **Residual-capacity fit:** for **spreadable** workloads the filter uses the **per-replica** request
  (removing a node that cannot hold one replica is optimality-preserving); the **whole-gang** request
  is used only for **co-located** gangs (which must fit one node). Using whole-gang fit on a
  spreadable gang would wrongly prune valid multi-node placements.
- **Anti-affinity exclusion:** remove nodes whose topology domain already holds a matching pod
  (forward and symmetric, hostname exact + zone/rack domain), with full label-selector and
  namespace-scope semantics.
- **GPU availability:** MIG slices as distinct resources; DRA per-class availability (§8.3).
- **VRAM fit (predictive):** when a predicted peak VRAM $\hat v_w>0$ is present, nodes whose per-GPU
  device memory $M_n<\hat v_w$ are removed from $F_w$ (§9.2).

> **Optimality of the projection.** Removing a $(w,n)$ pair that is *provably infeasible* under the
> modeled predicates is an **equivalence transformation** — it cannot remove the true optimum
> *relative to the modeled scheduler predicates*. This is distinct from the **heuristic candidate
> cap** of §10 (bounding $|F_w|$ on very wide fleets), which solves a *restricted* problem and carries
> **no global-optimality guarantee** — the two must not be conflated. Any predicate we do not model
> (e.g. arbitrary CEL) is a fidelity gap, not an optimality gap.

A separate **conformance harness** validates $F_w$ against the *real* kube-scheduler Filter phase
(via kube-scheduler-simulator), per (pod, node) pair, reporting a confusion matrix; constructs we
intentionally don't model are bucketed as expected divergence.

---

## 7. Soft (preferred) terms

Preferred (`preferredDuringScheduling`) rules are honored as the Phase-2 tie-break, never as hard
constraints:

- **Preferred node affinity:** $s^{\text{node}}_{w,n} = \sum$ of matching term weights on node $n$.
- **Preferred pod affinity/anti-affinity:** a candidate node accrues $\pm w$ per matching *running*
  pod sharing its topology domain (per-pod accumulation, matching upstream `interpodaffinity`
  scoring), both forward and symmetric.
- **Co-placement** of two *pending* pods that prefer each other: the $b^k_{a,b,d}$ reward above —
  beyond the default kube-scheduler's one-pod scoring loop, which scores an incoming pod only against
  already-running pods (it never jointly optimizes two pending pods' mutual preference).

All soft scores are expressed on a common integer weight scale (Kubernetes `preferred` weights are
$1$–$100$; co-placement uses a comparable per-pair weight) and sum into a single per-node
coefficient, so CP-SAT handles negative (anti-affinity) contributions natively. Because this tier is
purely a tie-break *within* the cost-and-admission-optimal face (§5), the relative weighting only
orders otherwise-equivalent optima; it cannot trade off against cost or admission.

---

## 8. GPU-specific structure

### 8.1 Exclusivity and sharing
Absent explicit sharing, a GPU is **exclusive whole-unit** (§4.1). Sharing modes change the model
only through capacity/units:

| Mode | Modeling | Isolation |
|---|---|---|
| Whole GPU | `nvidia.com/gpu` integer resource | exclusive |
| MIG (mixed) | each profile a distinct integer resource `nvidia.com/mig-<p>` | HW-isolated units |
| Time-slicing | inflated `nvidia.com/gpu` capacity + a "shared, no isolation" caveat on the decision | none |
| DRA | synthetic per-class integer resource (§8.3) | driver-dependent |

**Modeling assumption (MIG).** Treating each MIG profile as an independent integer resource is exact
*only when the slice inventory is already materialized/fixed* (the device plugin advertises a fixed
set of `nvidia.com/mig-<p>` slices per node). It does **not** model dynamic re-partitioning of a
physical GPU into a different profile mix at schedule time — that is a per-GPU configuration/packing
sub-problem outside this scalar model.

### 8.2 Time-slicing disclosure
Time-sliced nodes (`nvidia.com/gpu.sharing-strategy=time-slicing`, or `replicas>1` absent the label;
MPS is *not* flagged) are schedulable but every placement carries a caveat — "fits" $\ne$ isolated
performance. This is disclosure, not a constraint.

### 8.3 DRA (Dynamic Resource Allocation) — scalar approximation
DRA is fundamentally a **matching/assignment** problem (claims $\leftrightarrow$ devices via CEL
selectors), not a scalar resource. ksolver's F3a approximation **collapses** it onto the integer
resource path: for each $(\text{node},\text{DeviceClass})$ it counts *unallocated matching* devices
(inventory from `ResourceSlice`s, minus allocations from `ResourceClaim.status`, at the newest pool
generation, node-scoped), exposing a synthetic resource $\text{dra.ksolver}/\langle\text{class}\rangle$;
each pod's claim demand becomes an integer request. A **limited CEL-subset evaluator** handles
attribute-/driver-equality selectors; anything beyond that marks the class *unevaluable* and is
excluded and caveated (fail-safe, never over-count). Because the scalar collapse is optimistic on
overlapping classes / request selectors, those cases are disclosed, the real binder refuses DRA
pods (ksolver does not allocate claims), and **DRA feasibility from this model is treated as
advisory, not guaranteed**. Exact per-device assignment (device-assignment variables + selector
matching, i.e. a matching sub-model) is the documented F3b extension.

---

## 9. Predictive demand: usage-adjusted requests and VRAM-aware bin-packing

Everything above packs against the **declared request** $\rho_{w,r}$. But a Kubernetes request is a
*contract*, not a *measurement*: batch and training pods routinely over-request CPU/RAM for padding,
so a node whose requests are "full" can sit at low real utilization, and — critically — the request
carries **no GPU-memory information at all** (GPUs are requested as whole integer units). Packing on
requests therefore *understates* achievable density (idle GPUs behind over-requested CPU/RAM) while
*ignoring* the one GPU dimension that can silently make a placement fail at runtime (VRAM). This
section adds the predictability component: a **usage-adjusted effective demand** that feeds the
capacity/quota constraints, and a **VRAM feasibility predicate** that feeds the projection. Both are
opt-in. Their risk postures differ, though, and the section is careful to keep them distinct: the VRAM
gate is *conservative* (a pre-solve feasibility filter that only ever removes candidate nodes), while
usage-adjustment is *density-increasing* — bounded (P1/P2, the quantile, the audit loop) but, by
design, more aggressive than request-based packing.

### 9.1 Usage-adjusted effective demand

For each workload $w$ and **compressible** resource $r\in\{\text{cpu},\text{mem}\}$ define an
effective per-replica demand

$$
\hat\rho_{w,r} \;=\; \min\!\Big(\rho_{w,r},\;\; \max\big(\phi\,\rho_{w,r},\;\; \hat u_{w,r},\;\; 1\big)\Big),
$$

with floor ratio $\phi$ (`usage_request_floor_ratio`, default $0.5$) and a usage estimate
$\hat u_{w,r}$ (below). Two structural properties matter to a reviewer, and both are enforced
invariants of the implementation:

- **(P1) Downward-only — prediction never inflates demand.** The outer $\min(\rho_{w,r},\cdot)$
  guarantees $\hat\rho_{w,r}\le\rho_{w,r}$: an under-requested workload (measured usage above its
  request) is still packed at its request, never above — the model uses evidence only to **reclaim**
  request headroom, never to raise a workload above its declared request. Note the direction of the
  risk trade: $\hat\rho\le\rho$ makes packing **denser and therefore more aggressive** than
  request-based packing, not safer — it is *conservative relative to trusting under-stated requests*
  (it never over-commits a node beyond declared requests), but it is **less protective than
  request-based packing whenever the prediction is low**. The floor (P2), the VRAM gate (§9.2), and
  the audit loop (§9.3) are what bound that added aggressiveness.
- **(P2) Floored.** $\hat\rho_{w,r}\ge\min(\rho_{w,r},\phi\rho_{w,r})=\phi\rho_{w,r}$, so even a
  transiently-idle workload retains a fraction $\phi$ of its request. This bounds the damage of an
  underestimate drawn from a short or unrepresentative observation window.

**Usage estimate.** The two compressible resources are treated according to their failure mode:

- **Memory** is heavy-tailed and its overflow is *fatal* (OOM-kill), so the estimate is a
  **historical high quantile** of the observed memory series $\{m_1,\dots,m_T\}$:
  $$
  \hat u_{w,\text{mem}} = Q_{\tau}(\{m_t\}),\qquad \tau = 1-\tfrac{\varepsilon}{100},
  $$
  where $\varepsilon=$ `max_memory_overflow_probability_percent` (default $1$, i.e. $\tau=0.99$) and
  $Q_\tau$ is the nearest-rank quantile at **zero-based** sorted index $\lceil (T-1)\tau\rceil$
  (equivalently one-based rank $\lceil (T-1)\tau\rceil+1$). With no history the fallback is a point
  estimate: observed memory $\times\,\beta_{\text{mem}}$ (`memory_usage_safety_factor`, default $2.0$).
- **CPU** is compressible and its overflow is *throttling, not death*, so a point estimate suffices:
  observed CPU $\times\,\beta_{\text{cpu}}$ (`cpu_usage_safety_factor`, default $1.5$), clamped by the
  same floor/cap. If observed CPU usage is $\le 0$ (no usage signal for the workload), **no adjustment
  is applied** and the request $\rho_{w,\text{cpu}}$ is retained.

**Empirical-quantile (approximate chance-constraint) reading.** Taking $\hat u_{w,\text{mem}}=Q_\tau$
makes the packed memory bound an **empirical high quantile** of the *observed* series, so historical
exceedance is bounded *in-sample*: when $Q_\tau\le\rho_{w,\text{mem}}$ the adjusted demand is at least
that quantile, and the historical over-run frequency is
$\Pr_{\text{emp}}[\text{mem}_w>\hat u_{w,\text{mem}}]\approx 1-\tau=\varepsilon/100$ (up to the
nearest-rank convention). Two caveats keep this from being a genuine chance constraint. (i) It is an
**order statistic of past samples**, not a distributional guarantee: absent i.i.d./stationarity
assumptions it carries no finite-sample or out-of-sample coverage bound. (ii) The outer cap
$\min(\rho,\cdot)$ **voids even the in-sample bound whenever $Q_\tau>\rho_{w,\text{mem}}$** — an
under-requested workload is held at $\rho$ (P1), i.e. *below* its own quantile. The point-estimate
fallback ($\times\beta$) is a heuristic safety margin with no quantile interpretation at all. The
framing is also deliberately *not* a joint (whole-node) guarantee — correlated spikes across
co-packed workloads are not modeled; the floor $\phi$ and the per-node **headroom reserve** already
folded into $c_{n,r}$ are the (blunt) safeguards against that residual risk.

**Where it enters the model.** When enabled, $\hat\rho_{w,r}$ replaces $\rho_{w,r}$ in the capacity
constraint (§4.1), the quota constraint (§4.5), and the residual-capacity fit of the projection (§6).
**GPU-class resources $r\in R^{\text{gpu}}$ are excluded**: they are integer, non-compressible, and
non-overcommittable, so their coefficient is always the true request — predicting a *fractional* GPU
demand would violate the exclusivity invariant of §4.1. Activation is **guarded**: usage-adjustment
applies only when explicitly enabled *and* usage data is present; with the flag on but **no usage data
at all**, the model reverts to $\rho$ and emits a warning (no silent optimism). Note the two fallbacks
are distinct: *no usage data* $\Rightarrow$ request $\rho$; *usage present but no memory history*
$\Rightarrow$ the point-estimate bound observed memory $\times\,\beta_{\text{mem}}$ (not $\rho$).

**Supply-side vs. demand-side shaping.** Prediction adjusts the **demand** side ($\hat\rho$). The
**supply** side is shaped independently: residual capacity $c_{n,r}$ already nets out the DaemonSet
reserve and a headroom percentage, and an optional overcommit ratio $\theta_r\ge1$ scales the
compressible dimensions ($c_{n,r}\!\leftarrow\!\theta_r\,c_{n,r}$). The two are complementary knobs —
prediction *tightens the demand estimate from evidence*, overcommit is *an operator's explicit bet on
the supply side* — and GPU dimensions take neither.

### 9.2 VRAM-aware GPU feasibility

Whole-GPU (and MIG) requests state *how many* GPUs a replica needs, not *how much device memory* each
will consume — yet a job whose peak VRAM exceeds a GPU's capacity cannot run there regardless of GPU
count. ksolver carries a **predicted peak VRAM** $\hat v_w$ (`predicted_peak_vram_bytes`) per GPU
workload, and each GPU node/class $n$ advertises **per-GPU device memory** $M_n$. The projection (§6)
adds a memory-fit predicate to the feasible set:

$$
F_w \;\leftarrow\; \{\, n \in F_w : \hat v_w \le M_n \,\}\qquad (\text{when } \hat v_w > 0).
$$

A $40$ GiB-predicted training job is therefore filtered off $24$ GB L4 nodes and admitted only on
nodes whose per-GPU memory is $\ge 40$ GiB — **before** the solver runs, so no candidate placement can
violate the **predicted** VRAM-fit predicate, and the branch never enters the search. (Runtime device
memory can still be exceeded if the predictor *under*-estimates $\hat v_w$; that residual is a
predictor-quality risk, surfaced by the audit loop of §9.3, not a modeling gap here.) When
$\hat v_w=0$ (no prediction) the predicate is inert. Because the check is **per GPU** (a replica
occupies whole GPUs under the §4.1 exclusivity invariant), it composes cleanly with gangs: a
VRAM-inadequate node is simply absent from $F_w$, and
the atomic-gang latch (§4.2) then places the *entire* gang on VRAM-adequate nodes or admits none of
it. VRAM is thus a **feasibility** dimension, not a packable scalar capacity — it gates candidate
nodes rather than consuming a resource budget.

### 9.3 Prediction audit and the feedback loop

Bin-packing on predicted rather than requested demand is only defensible if the predictions stay
calibrated, so predicted values are **audited against realized usage**: ksolver records
predicted-vs-actual peak VRAM and resource usage (the VRAM prediction proof / prediction-audit
metrics) so that drift is observable rather than latent. The intended control loop is the standard
conservative one:

The two prediction channels have **different corrective knobs** and must not be conflated:

- **Compressible CPU/memory** — under-prediction (realized $>\hat\rho$, or an observed CPU-throttle /
  OOM-kill) $\Rightarrow$ widen the margins that produce $\hat\rho$: raise the safety factor $\beta$,
  raise the floor $\phi$, or lower the overflow budget $\varepsilon$ (equivalently raise the quantile
  $\tau$). Sustained over-prediction (a large, persistent $\hat\rho$-vs-actual gap) $\Rightarrow$
  tighten them to recover density.
- **VRAM** — a VRAM under-estimate is **not** fixed by $\beta/\phi/\varepsilon$ (those act only on
  the compressible request coefficient, not on $\hat v_w$). It is corrected by **updating the VRAM
  predictor itself** — or by applying a separate VRAM safety margin to $\hat v_w$ before the §9.2 fit —
  driven by the predicted-vs-actual peak-VRAM audit.

The quantile $\tau$ / floor $\phi$ / safety factor $\beta$ (compressible) and the VRAM predictor /
margin (device memory) are the exposed **risk dials**, and the
audit metrics are the signal that keeps the dial honest. This is the component that makes the
efficiency gains of §11 real rather than optimistic: prediction is what converts "the node's requests
are full" into "the node's GPUs are actually busy," raising the *useful-GPU / active-node* ratio the
empirical comparison measures — while (P1), the VRAM gate, the per-workload empirical-quantile bound,
and the audit loop **bound and surface** the resulting risk of runtime OOMs or misplaced jobs (they
reduce it, but — as the §9.1/§9.2 caveats note — cannot eliminate it when a predictor underestimates).

---

## 10. Complexity, scale, and solution method

**Complexity.** The problem contains bin packing (pack GPU requests into fixed-capacity nodes),
multiple-knapsack, and generalized assignment as special cases, so it is **NP-hard**; the decision
version (does an admission of all $W$ exist under capacity + anti-affinity?) is NP-complete. There is
no polynomial guarantee; CP-SAT gives optimality *when it proves it* within the budget and a feasible
incumbent otherwise.

**Model size.**

$$
|x|=\sum_{w}|F_w|,\quad |p|=|W|,\quad |y|=|N|,\quad |u|=\sum_{w\in\text{gangs}}|F_w|,
$$
$$
|z|=O\!\big(|A^{\ne}|\cdot \max_k|D_k|\big),\quad |b|=O\!\Big(|P|\sum_k|D_k|\Big),
$$
with $O(|N|\,|R|)$ capacity rows plus $O(|W|)$ latch rows. The feasibility projection (§6) is what
keeps $\sum_w|F_w|$ far below $|W|\,|N|$ in practice.

**Symmetry.** Identical nodes (equal capacity + labels + cost) and identical replicas within a gang
induce large solution symmetry, which can inflate CP-SAT proof time. The implemented mitigation
groups a set of interchangeable physical nodes into one **symmetry-class column** of multiplicity
$\text{count}$ (`group_pending_input_by_node_symmetry`), solves, then **expands the grouped
assignment back to physical nodes** (`expand_grouped_solution_to_physical`). This is valid only
because (i) the grouped nodes are genuinely interchangeable and (ii) **per-unit capacity is
retained** — the group is $\text{count}$ identical unit-capacity nodes, *not* a single pooled-capacity
node (pooling would incorrectly permit an indivisible item to straddle units). Gang replicas are
interchangeable by construction. Explicit lexicographic symmetry-breaking on equal-cost nodes is a
further candidate.

**Anytime + reporting.** The scheduler accepts the best incumbent within a time budget; each decision
records **solver status** (Optimal / Feasible / Infeasible), **solve time**, and — for an OR
consumer — should expose the **incumbent objective, best bound, and optimality gap** (CP-SAT provides
all three). A returned schedule with status *Feasible* is explicitly *not* claimed optimal.

**Parallelism / pruning.** Worker count is tiered by model size and capped by cores (a prior
heuristic that throttled the wide shadow path to one worker was the main slowdown until corrected).
The optional per-workload $|F_w|$ cap trades optimality for width on very large fleets (§6) and has a
documented widening fallback.

**Observed timings (internal scenarios; not a controlled benchmark).** On a synthetic
100-node $\times$ 8-GPU fleet at a 10 s budget, structured **gang** models ($\le 125$ workloads) reached proven
optimal in roughly 1–3 s, whereas flat **singleton** batches admitted everything in milliseconds but
did not prove cost-optimality within the budget (returned as Feasible). These are directional
observations on one machine/solver build, not a reproducible benchmark — a controlled table
(scenarios, hardware, solver version, seeds, statuses, gaps) is future work.

---

## 11. Empirical comparison methodology (ksolver vs. real kube-scheduler)

To quantify the win, a scenario suite runs identical GPU workloads through **both** the real
kube-scheduler (via kube-scheduler-simulator, both `LeastAllocated`/spread and `MostAllocated`/
bin-packing configs) and ksolver, measuring per scenario:

- **admitted useful GPU** (and full vs. partial gangs),
- **active GPU nodes** and **stranded GPU on active nodes** (fragmentation),
- **GPU utilization** $=\dfrac{\text{used GPU}}{\text{GPU capacity of active nodes}}$,
- **fleet cost** $=\sum_{n:\,\text{active}}\pi_n$.

Scenarios are ranked by ksolver's efficiency delta versus the *best* kube baseline (the harder,
bin-packing one), flagging cases that are *significantly* better (e.g. $\ge$ 15 % cheaper, an extra
completed gang, or a materially higher packing density). The expected wins are exactly the
structural cases of §1: small-job fragmentation, mixed-size packing, gang atomicity, and
consolidation for scale-down.

---

## 12. Fidelity and limitations

- **Shadow by default:** binds nothing unless real binding is explicitly enabled; a no-mutation
  guard isolates the (opt-in) binder as the single mutation site.
- **Projection vs. constraints:** predicates are projected into $F_w$, validated against the real
  Filter phase by the conformance harness; unmodeled constructs (some CEL, priority/preemption,
  `DoNotSchedule` topology spread) are bucketed/caveated rather than silently ignored.
- **DRA** is a scalar approximation (§8.3); exact assignment is future work.
- **Preemption / priority-driven eviction** is out of scope (the model admits, it does not evict).
- **Predictive demand (§9)** is opt-in and only as good as its inputs: it degrades to request-based
  packing when usage/VRAM data is absent (P1 bounds it to declared requests), bounds memory overflow
  only *in-sample* via an empirical high quantile (individual, not joint, and void when a workload is
  under-requested), and depends on the audit loop (§9.3) to catch predictor drift — an uncalibrated
  predictor is a fidelity risk, mitigated but not eliminated by the floor $\phi$ and headroom reserve.

---

## 13. Summary for review

The contribution is to treat GPU placement as a **batch constrained-optimization** problem with (i)
an **atomic gang admission** primitive absent from greedy schedulers, (ii) a prioritized
**admission $\gg$ (cost + shaping) $\gg$ preference** objective with a numerically careful two-phase
realization (and a noted path to a strict per-term lexicographic order),
(iii) a **feasibility projection** that keeps the model small while remaining conformant to the real
scheduler's Filter phase, (iv) principled GPU extensions (MIG, time-slicing disclosure, a
fail-safe DRA scalar approximation), and (v) a **predictive demand model** (§9) that packs against a
downward-only, floored, quantile-based *usage-adjusted* demand — a per-workload empirical high-quantile
bound on memory — plus a **VRAM-fit feasibility gate**, so density is driven by evidence of real GPU
use rather than by over-stated requests, with an audit loop guarding calibration. The open questions best suited to an OR review are: a lighter
admission encoding to speed optimality proofs on flat singleton batches; exact DRA device-assignment
(F3b) as a matching sub-model; and incorporating preemption/priority as a bilevel or
rolling-horizon extension.
