#!/usr/bin/env bash
set -euo pipefail

cmd="${1:-start}"
count="${2:-4}"
base_port="${3:-12120}"
cache_dir="${4:-/tmp/ksolver-kss-cache}"
wait_timeout_seconds="${5:-60}"

prefix="${KSOLVER_KSS_POOL_PREFIX:-ksolver-kss}"
network="${KSOLVER_KSS_POOL_NETWORK:-${prefix}-network}"
cluster_image="${KSOLVER_KSS_CLUSTER_IMAGE:-registry.k8s.io/kwok/cluster:v0.6.0-k8s.v1.30.2}"
server_image="${KSOLVER_KSS_SERVER_IMAGE:-simulator-server}"
scheduler_image="${KSOLVER_KSS_SCHEDULER_IMAGE:-simulator-scheduler}"
state_root="${KSOLVER_KSS_POOL_STATE_DIR:-/tmp/ksolver-kss-pool}"

usage() {
  echo "usage: $0 {start|stop|status|urls|ready-urls|require-ready-urls|wait-ready-urls|preflight} [count] [base_port] [cache_dir] [wait_timeout_seconds]" >&2
}

require_non_negative_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "${name} must be a non-negative integer, got '${value}'" >&2
    return 2
  fi
}

require_positive_integer() {
  local name="$1"
  local value="$2"
  require_non_negative_integer "$name" "$value" || return 2
  if (( value == 0 )); then
    echo "${name} must be greater than 0, got '${value}'" >&2
    return 2
  fi
}

require_pool_dimensions() {
  require_positive_integer "count" "$count"
  require_non_negative_integer "base_port" "$base_port"
}

have() {
  command -v "$1" >/dev/null 2>&1
}

require_docker() {
  if ! have docker; then
    echo "docker is required to manage the kube-scheduler-simulator pool" >&2
    return 1
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "docker is installed but the daemon is not reachable" >&2
    return 1
  fi
}

require_image() {
  local image="$1"
  local role="$2"
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "missing ${role} image: ${image}" >&2
    echo "set KSOLVER_KSS_${role}_IMAGE or build/pull the image before starting the pool" >&2
    return 1
  fi
}

preflight() {
  require_docker
  require_image "$server_image" SERVER
  require_image "$scheduler_image" SCHEDULER
  if ! have curl; then
    echo "curl is not installed; status will not be able to probe /api/v1/export" >&2
  fi
  echo "KSS pool preflight ok"
  echo "cluster image:   $cluster_image"
  echo "server image:    $server_image"
  echo "scheduler image: $scheduler_image"
}

urls() {
  local out=()
  for i in $(seq 0 $((count - 1))); do
    out+=("http://127.0.0.1:$((base_port + i))")
  done
  local IFS=,
  echo "${out[*]}"
}

write_config_dir() {
  local config_dir="$1"
  local cluster="$2"
  local etcd_port="$3"
  rm -rf "$config_dir"
  mkdir -p "$config_dir"
  cat >"${config_dir}/kubeconfig.yaml" <<YAML
apiVersion: v1
kind: Config
clusters:
- cluster:
    server: http://${cluster}:3131
  name: simulator
contexts:
- context:
    cluster: simulator
  name: simulator
current-context: simulator
YAML
  cat >"${config_dir}/scheduler.yaml" <<YAML
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
clientConnection:
  kubeconfig: /config/kubeconfig.yaml
leaderElection:
  leaderElect: false
profiles:
- schedulerName: default-scheduler
YAML
  cat >"${config_dir}/config.yaml" <<YAML
apiVersion: kube-scheduler-simulator-config/v1alpha1
kind: SimulatorConfiguration
port: 1212
etcdURL: "http://${cluster}:${etcd_port}"
corsAllowedOriginList:
- "http://localhost:3000"
kubeConfig: "/kubeconfig.yaml"
kubeApiServerUrl: "http://${cluster}:3131"
kubeSchedulerConfigPath: ""
externalImportEnabled: false
resourceSyncEnabled: false
replayEnabled: false
recordFilePath: "/record.jsonl"
YAML
}

discover_etcd_port() {
  local cluster="$1"
  for _ in $(seq 1 30); do
    local ports
    ports="$(docker exec "$cluster" sh -lc 'netstat -ltnp 2>/dev/null | awk '"'"'/etcd/ { n=split($4,a,":"); print a[n] }'"'"'' 2>/dev/null | sort -n | uniq || true)"
    for port in $ports; do
      if docker exec "$cluster" sh -lc "wget -qO- --timeout=2 http://127.0.0.1:${port}/version >/dev/null 2>&1"; then
        echo "$port"
        return 0
      fi
    done
    sleep 1
  done
  echo "failed to discover etcd port for $cluster" >&2
  return 1
}

start_pool() {
  preflight
  mkdir -p "$cache_dir"
  docker network create "$network" >/dev/null 2>&1 || true
  for i in $(seq 0 $((count - 1))); do
    local port=$((base_port + i))
    local cluster="${prefix}-${i}-cluster"
    local server="${prefix}-${i}-server"
    local scheduler="${prefix}-${i}-scheduler"
    local config_dir="${state_root}/${prefix}-${i}"

    docker rm -f "$scheduler" "$server" "$cluster" >/dev/null 2>&1 || true

    # Two KWOK apiserver fixes are required for the live simulator import/reset cycle to work
    # (both verified empirically; the entrypoint forwards these container args to
    # `kwokctl create cluster`):
    #
    #  1. --kube-admission=false: this KWOK cluster image runs the apiserver but NOT a
    #     kube-controller-manager, so no per-namespace "default" ServiceAccount is ever created.
    #     With admission enabled (the default), the ServiceAccount admission plugin rejects every
    #     imported pod ("serviceaccount \"default\" not found") -> /api/v1/import 500s. Disabling
    #     admission lets pods import without a default SA (pod create 500 -> 201 with this flag).
    #
    #  2. --extra-args kube-apiserver=etcd-prefix=/kube-scheduler-simulator: KWOK's apiserver
    #     defaults to storing objects under etcd prefix /registry, but the simulator's reset.go
    #     deletes+restores prefix /kube-scheduler-simulator. Mismatched -> PUT /api/v1/reset
    #     returns 202 but never drains imported objects, so every batch times out in the reset
    #     phase. Overriding the apiserver's etcd-prefix to match reset.go makes reset actually
    #     drain. (pflag: this appends a second --etcd-prefix that wins over KWOK's default.)
    docker run -d \
      --name "$cluster" \
      --network "$network" \
      --network-alias "$cluster" \
      -e KWOK_KUBE_APISERVER_PORT=3131 \
      -e ETCD_PORT=2379 \
      "$cluster_image" \
      --kube-admission=false \
      --extra-args kube-apiserver=etcd-prefix=/kube-scheduler-simulator >/dev/null

    local etcd_port
    etcd_port="$(discover_etcd_port "$cluster")"
    write_config_dir "$config_dir" "$cluster" "$etcd_port"

    docker run -d \
      --name "$server" \
      --network "$network" \
      -p "127.0.0.1:${port}:1212" \
      -e "KUBE_SCHEDULER_SIMULATOR_ETCD_URL=http://${cluster}:${etcd_port}" \
      -e "KUBE_APISERVER_URL=http://${cluster}:3131" \
      -e PORT=1212 \
      -v "${config_dir}/config.yaml:/config.yaml:ro" \
      -v "${config_dir}/kubeconfig.yaml:/kubeconfig.yaml:ro" \
      -v "${config_dir}:/config:rw" \
      -v /var/run/docker.sock:/var/run/docker.sock \
      "$server_image" /simulator >/dev/null

    docker run -d \
      --name "$scheduler" \
      --network "$network" \
      -e KUBECONFIG=/config/kubeconfig.yaml \
      -v "${config_dir}:/config:rw" \
      "$scheduler_image" /scheduler --config /config/scheduler.yaml --master "http://${cluster}:3131" >/dev/null
  done

  echo "KSS pool started: $(urls)"
  echo "Cache dir: $cache_dir"
  echo
  echo "export KSOLVER_GPU_SCENARIO_SIMULATOR_POOL=$(urls)"
  echo "export KSOLVER_GPU_SCENARIO_SIMULATOR_CACHE_DIR=$cache_dir"
  echo
  echo "cargo run -p ksolver --features rust-cp-sat -- gpu-scenarios \\"
  echo "  --simulator-pool \"$(urls)\" \\"
  echo "  --simulator-cache-dir \"$cache_dir\" \\"
  echo "  --refresh-simulator-cache \\"
  echo "  --simulator-max-live-baselines all \\"
  echo "  --simulator-timeout-ms 20000 \\"
  echo "  --simulator-progress \\"
  echo "  --json > /tmp/ksolver-kss-pool-report.json"
}

container_status() {
  local name="$1"
  local status
  status="$(docker inspect -f '{{.State.Status}}' "$name" 2>/dev/null | tr -d '\r\n')"
  if [[ -n "$status" ]]; then
    echo "$status"
  else
    echo "missing"
  fi
}

server_url() {
  local server="$1"
  local fallback_port="$2"
  local published
  published="$(docker port "$server" 1212/tcp 2>/dev/null | head -n 1 | tr -d '\r')"
  if [[ -n "$published" ]]; then
    local host="${published%:*}"
    local port="${published##*:}"
    if [[ "$host" == "0.0.0.0" || "$host" == "::" || "$host" == "[::]" ]]; then
      host="127.0.0.1"
    fi
    echo "http://${host}:${port}"
  else
    echo "http://127.0.0.1:${fallback_port}"
  fi
}

simulator_probe_status() {
  local url="$1"
  if ! have curl; then
    echo "unknown-no-curl"
    return 0
  fi
  if curl -fsS --max-time 2 "${url}/api/v1/export" >/dev/null 2>&1; then
    echo "ready"
  else
    echo "not-ready"
  fi
}

ready_urls_csv() {
  local ready_urls=()
  for i in $(seq 0 $((count - 1))); do
    local port=$((base_port + i))
    local server="${prefix}-${i}-server"
    local url
    url="$(server_url "$server" "$port")"
    if [[ "$(simulator_probe_status "$url")" == "ready" ]]; then
      ready_urls+=("$url")
    fi
  done
  local IFS=,
  echo "${ready_urls[*]}"
}

require_ready_urls_csv() {
  local ready_pool
  ready_pool="$(ready_urls_csv)"
  if [[ -z "$ready_pool" ]]; then
    echo "no ready kube-scheduler-simulator endpoints passed /api/v1/export" >&2
    return 2
  fi
  echo "$ready_pool"
}

wait_ready_urls_csv() {
  local deadline=$((SECONDS + wait_timeout_seconds))
  local ready_pool
  while true; do
    ready_pool="$(ready_urls_csv)"
    if [[ -n "$ready_pool" ]]; then
      echo "$ready_pool"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "no ready kube-scheduler-simulator endpoints passed /api/v1/export within ${wait_timeout_seconds}s" >&2
      echo "run: ${0} status ${count} ${base_port} ${cache_dir}" >&2
      return 2
    fi
    sleep 1
  done
}

status_pool() {
  require_docker
  local ready_urls=()
  printf "%-4s %-30s %-12s %-12s %-12s %-22s %s\n" \
    "IDX" "URL" "CLUSTER" "SERVER" "SCHEDULER" "EXPORT_PROBE" "CONTAINERS"
  for i in $(seq 0 $((count - 1))); do
    local port=$((base_port + i))
    local cluster="${prefix}-${i}-cluster"
    local server="${prefix}-${i}-server"
    local scheduler="${prefix}-${i}-scheduler"
    local url
    local probe
    url="$(server_url "$server" "$port")"
    probe="$(simulator_probe_status "$url")"
    if [[ "$probe" == "ready" ]]; then
      ready_urls+=("$url")
    fi
    printf "%-4s %-30s %-12s %-12s %-12s %-22s %s,%s,%s\n" \
      "$i" \
      "$url" \
      "$(container_status "$cluster")" \
      "$(container_status "$server")" \
      "$(container_status "$scheduler")" \
      "$probe" \
      "$cluster" "$server" "$scheduler"
  done
  echo
  if (( ${#ready_urls[@]} > 0 )); then
    local ready_pool
    ready_pool="$(IFS=,; echo "${ready_urls[*]}")"
    echo "Ready simulator endpoints: ${ready_pool}"
    echo "export KSOLVER_GPU_SCENARIO_SIMULATOR_POOL=${ready_pool}"
    echo "export KSOLVER_GPU_SCENARIO_SIMULATOR_CACHE_DIR=${cache_dir}"
    echo
    echo "cargo run -p ksolver --features rust-cp-sat -- gpu-scenarios \\"
    echo "  --simulator-pool \"${ready_pool}\" \\"
    echo "  --simulator-cache-dir \"${cache_dir}\" \\"
    echo "  --refresh-simulator-cache \\"
    echo "  --simulator-max-live-baselines all \\"
    echo "  --simulator-timeout-ms 20000 \\"
    echo "  --simulator-progress \\"
    echo "  --json > /tmp/ksolver-kss-pool-report.json"
  else
    echo "No ready simulator endpoints. Run '${0} preflight' and inspect Docker logs for ${prefix}-*-server."
  fi
}

stop_pool() {
  require_docker
  for i in $(seq 0 $((count - 1))); do
    docker rm -f \
      "${prefix}-${i}-scheduler" \
      "${prefix}-${i}-server" \
      "${prefix}-${i}-cluster" >/dev/null 2>&1 || true
  done
  echo "KSS pool stopped for prefix $prefix"
}

case "$cmd" in
  start)
    require_pool_dimensions
    start_pool
    ;;
  stop)
    require_pool_dimensions
    stop_pool
    ;;
  status)
    require_pool_dimensions
    status_pool
    ;;
  urls)
    require_pool_dimensions
    urls
    ;;
  preflight) preflight ;;
  ready-urls)
    require_pool_dimensions
    require_docker
    ready_urls_csv
    ;;
  require-ready-urls)
    require_pool_dimensions
    require_docker
    require_ready_urls_csv
    ;;
  wait-ready-urls)
    require_pool_dimensions
    require_non_negative_integer "wait_timeout_seconds" "$wait_timeout_seconds"
    require_docker
    wait_ready_urls_csv
    ;;
  *)
    usage
    exit 2
    ;;
esac
