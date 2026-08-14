#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")" && pwd)
OUT=${1:?usage: CONFIRM_CONTEXT=<context> EXPECT=affected|fixed $0 <output-directory>}
KUBECTL_BIN=${KUBECTL_BIN:-oc}
EXPECT=${EXPECT:-}
CONFIRM_CONTEXT=${CONFIRM_CONTEXT:-}
COUNT=${COUNT:-500}
CONCURRENCY=${CONCURRENCY:-10}
DRY_RUNS=${DRY_RUNS:-20}
MIN_ADMITTED=${MIN_ADMITTED:-}
RUN_TIMEOUT_SECONDS=${RUN_TIMEOUT_SECONDS:-600}
SAMPLE_INTERVAL=${SAMPLE_INTERVAL:-5}
QUIESCE_SECONDS=${QUIESCE_SECONDS:-15}
CLEANUP_NAMESPACE=${CLEANUP_NAMESPACE:-true}

PIPELINES_NAMESPACE=${PIPELINES_NAMESPACE:-openshift-pipelines}
RESOLVERS_NAMESPACE=${RESOLVERS_NAMESPACE:-$PIPELINES_NAMESPACE}
CONTROLLER_DEPLOYMENT=${CONTROLLER_DEPLOYMENT:-tekton-pipelines-controller}
RESOLVER_DEPLOYMENT=${RESOLVER_DEPLOYMENT:-tekton-pipelines-remote-resolvers}
WEBHOOK_DEPLOYMENT=${WEBHOOK_DEPLOYMENT:-tekton-pipelines-webhook}
EXPECTED_CONTROLLER_REPLICAS=${EXPECTED_CONTROLLER_REPLICAS:-10}
EXPECTED_RESOLVER_REPLICAS=${EXPECTED_RESOLVER_REPLICAS:-10}
EXPECTED_WEBHOOK_REPLICAS=${EXPECTED_WEBHOOK_REPLICAS:-5}

case "$EXPECT" in affected|fixed) ;; *) echo "EXPECT must be affected or fixed" >&2; exit 2;; esac
for value in "$COUNT" "$CONCURRENCY" "$DRY_RUNS" "$RUN_TIMEOUT_SECONDS" "$SAMPLE_INTERVAL" "$QUIESCE_SECONDS"; do
  [[ "$value" =~ ^[0-9]+$ ]] || { echo "numeric settings must be non-negative integers" >&2; exit 2; }
done
[[ "$CLEANUP_NAMESPACE" == true || "$CLEANUP_NAMESPACE" == false ]] || {
  echo "CLEANUP_NAMESPACE must be true or false" >&2
  exit 2
}
[[ -n "$MIN_ADMITTED" ]] || MIN_ADMITTED=$((COUNT > 10 ? COUNT - 10 : COUNT))
[[ "$MIN_ADMITTED" =~ ^[0-9]+$ ]] || { echo "MIN_ADMITTED must be an integer" >&2; exit 2; }
(( COUNT > 0 && CONCURRENCY > 0 && DRY_RUNS > 0 && MIN_ADMITTED > 0 )) || {
  echo "COUNT, CONCURRENCY, DRY_RUNS, and MIN_ADMITTED must be positive" >&2
  exit 2
}
(( MIN_ADMITTED <= COUNT )) || { echo "MIN_ADMITTED cannot exceed COUNT" >&2; exit 2; }

for tool in "$KUBECTL_BIN" jq python3 xargs; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 2; }
done
if command -v sha256sum >/dev/null; then
  HASH=(sha256sum)
elif command -v shasum >/dev/null; then
  HASH=(shasum -a 256)
else
  echo "missing required tool: sha256sum or shasum" >&2
  exit 2
fi

current_context=$($KUBECTL_BIN config current-context)
[[ -n "$CONFIRM_CONTEXT" ]] || {
  echo "Refusing to run without CONFIRM_CONTEXT=$current_context" >&2
  exit 2
}
[[ "$CONFIRM_CONTEXT" == "$current_context" ]] || {
  echo "context mismatch: current=$current_context confirmed=$CONFIRM_CONTEXT" >&2
  exit 2
}

if [[ -e "$OUT" ]] && [[ -n "$(find "$OUT" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
  echo "output directory is not empty: $OUT" >&2
  exit 2
fi
mkdir -p "$OUT" "$OUT/input/pipelineruns" "$OUT/logs/controller" "$OUT/logs/resolver" "$OUT/logs/webhook"
OUT=$(cd "$OUT" && pwd)

RUN_ID=${RUN_ID:-$(date -u +%Y%m%d%H%M%S)-$$}
[[ "$RUN_ID" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || {
  echo "RUN_ID must be a lowercase Kubernetes label value" >&2
  exit 2
}
(( ${#RUN_ID} <= 40 )) || { echo "RUN_ID must be at most 40 characters" >&2; exit 2; }
NAMESPACE=${NAMESPACE:-resolver-cache-qe-$RUN_ID}
[[ "$NAMESPACE" != "$PIPELINES_NAMESPACE" && "$NAMESPACE" != "$RESOLVERS_NAMESPACE" ]] || {
  echo "refusing to use a product namespace as the workload namespace" >&2
  exit 2
}
[[ "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#NAMESPACE} -le 63 ]] || {
  echo "NAMESPACE must be a valid DNS label of at most 63 characters" >&2
  exit 2
}
K=("$KUBECTL_BIN")
namespace_created=false
collectors_stopped=true
watch_pid=
sampler_pid=

stop_collectors() {
  $collectors_stopped && return
  local watch_alive=false sampler_alive=false
  kill -0 "$watch_pid" 2>/dev/null && watch_alive=true
  kill -0 "$sampler_pid" 2>/dev/null && sampler_alive=true
  $watch_alive && kill "$watch_pid" 2>/dev/null || true
  $sampler_alive && kill "$sampler_pid" 2>/dev/null || true
  wait "$watch_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  if $watch_alive; then
    printf 'stopped-by-runner\n' > "$OUT/rr-watch-status.txt"
  else
    printf 'exited-early\n' > "$OUT/rr-watch-status.txt"
  fi
  collectors_stopped=true
}

on_exit() {
  local rc=$?
  stop_collectors
  if (( rc != 0 )) && $namespace_created; then
    echo "Run failed or was invalid; retained namespace $NAMESPACE" >&2
    echo "Evidence: $OUT" >&2
  fi
}
trap on_exit EXIT
trap 'exit 130' INT TERM

selector_for_deployment() {
  local namespace=$1 deployment=$2
  "${K[@]}" get deployment -n "$namespace" "$deployment" -o json \
    | jq -r '.spec.selector.matchLabels | to_entries | map("\(.key)=\(.value)") | join(",")'
}

capture_pods() {
  local component=$1 namespace=$2 selector=$3 suffix=$4
  "${K[@]}" get pods -n "$namespace" -l "$selector" -o json > "$OUT/${component}-pods-${suffix}.json"
}

capture_deployment() {
  local component=$1 namespace=$2 deployment=$3 suffix=$4
  "${K[@]}" get deployment -n "$namespace" "$deployment" -o json > "$OUT/${component}-deployment-${suffix}.json"
}

ready_count() {
  local namespace=$1 deployment=$2
  "${K[@]}" get deployment -n "$namespace" "$deployment" -o json \
    | jq -r '(.status.readyReplicas // 0) | tostring'
}

desired_count() {
  local namespace=$1 deployment=$2
  "${K[@]}" get deployment -n "$namespace" "$deployment" -o json \
    | jq -r '.spec.replicas | tostring'
}

for target in \
  "$PIPELINES_NAMESPACE/$CONTROLLER_DEPLOYMENT" \
  "$RESOLVERS_NAMESPACE/$RESOLVER_DEPLOYMENT" \
  "$PIPELINES_NAMESPACE/$WEBHOOK_DEPLOYMENT"; do
  namespace=${target%%/*}
  deployment=${target#*/}
  "${K[@]}" rollout status -n "$namespace" "deployment/$deployment" --timeout=300s >/dev/null
done

controller_selector=$(selector_for_deployment "$PIPELINES_NAMESPACE" "$CONTROLLER_DEPLOYMENT")
resolver_selector=$(selector_for_deployment "$RESOLVERS_NAMESPACE" "$RESOLVER_DEPLOYMENT")
webhook_selector=$(selector_for_deployment "$PIPELINES_NAMESPACE" "$WEBHOOK_DEPLOYMENT")

controller_desired=$(desired_count "$PIPELINES_NAMESPACE" "$CONTROLLER_DEPLOYMENT")
controller_ready=$(ready_count "$PIPELINES_NAMESPACE" "$CONTROLLER_DEPLOYMENT")
resolver_desired=$(desired_count "$RESOLVERS_NAMESPACE" "$RESOLVER_DEPLOYMENT")
resolver_ready=$(ready_count "$RESOLVERS_NAMESPACE" "$RESOLVER_DEPLOYMENT")
webhook_desired=$(desired_count "$PIPELINES_NAMESPACE" "$WEBHOOK_DEPLOYMENT")
webhook_ready=$(ready_count "$PIPELINES_NAMESPACE" "$WEBHOOK_DEPLOYMENT")
all_ready=false
if [[ "$controller_desired" == "$EXPECTED_CONTROLLER_REPLICAS" && "$controller_ready" == "$EXPECTED_CONTROLLER_REPLICAS" \
   && "$resolver_desired" == "$EXPECTED_RESOLVER_REPLICAS" && "$resolver_ready" == "$EXPECTED_RESOLVER_REPLICAS" \
   && "$webhook_desired" == "$EXPECTED_WEBHOOK_REPLICAS" && "$webhook_ready" == "$EXPECTED_WEBHOOK_REPLICAS" ]]; then
  all_ready=true
fi

jq -n \
  --argjson allReady "$all_ready" \
  --argjson controllerDesired "$controller_desired" --argjson controllerReady "$controller_ready" \
  --argjson resolverDesired "$resolver_desired" --argjson resolverReady "$resolver_ready" \
  --argjson webhookDesired "$webhook_desired" --argjson webhookReady "$webhook_ready" \
  '{allReady:$allReady, controller:{desired:$controllerDesired,ready:$controllerReady}, resolver:{desired:$resolverDesired,ready:$resolverReady}, webhook:{desired:$webhookDesired,ready:$webhookReady}}' \
  > "$OUT/preflight.json"
$all_ready || { cat "$OUT/preflight.json" >&2; echo "expected replicas are not ready" >&2; exit 1; }

"${K[@]}" version -o yaml > "$OUT/client-server-version.yaml" 2>&1 || "${K[@]}" version > "$OUT/client-server-version.txt" 2>&1
capture_deployment controller "$PIPELINES_NAMESPACE" "$CONTROLLER_DEPLOYMENT" before
capture_deployment resolver "$RESOLVERS_NAMESPACE" "$RESOLVER_DEPLOYMENT" before
capture_deployment webhook "$PIPELINES_NAMESPACE" "$WEBHOOK_DEPLOYMENT" before
capture_pods controller "$PIPELINES_NAMESPACE" "$controller_selector" before
capture_pods resolver "$RESOLVERS_NAMESPACE" "$resolver_selector" before
capture_pods webhook "$PIPELINES_NAMESPACE" "$webhook_selector" before

if "${K[@]}" get namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "refusing to reuse existing namespace: $NAMESPACE" >&2
  exit 2
fi
"${K[@]}" create namespace "$NAMESPACE" >/dev/null
namespace_created=true

cat > "$OUT/input/task.yaml" <<YAML
apiVersion: tekton.dev/v1
kind: Task
metadata:
  name: resolved-noop
  namespace: $NAMESPACE
  labels:
    qe-run: $RUN_ID
spec:
  steps:
    - name: noop
      image: mirror.gcr.io/busybox:1.36
      command: ["/bin/true"]
YAML
"${K[@]}" apply -f "$OUT/input/task.yaml" >/dev/null

width=${#COUNT}
(( width < 3 )) && width=3
for ((i=1; i<=COUNT; i++)); do
  printf -v suffix "%0${width}d" "$i"
  cat > "$OUT/input/pipelineruns/$suffix.yaml" <<YAML
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  name: resolver-cache-$RUN_ID-$suffix
  namespace: $NAMESPACE
  labels:
    qe-run: $RUN_ID
    qe-scenario: resolver-cache-amplification
spec:
  pipelineSpec:
    tasks:
      - name: remote-task
        when:
          - input: skip
            operator: in
            values: [run]
        taskRef:
          resolver: cluster
          params:
            - name: kind
              value: task
            - name: name
              value: resolved-noop
            - name: namespace
              value: $NAMESPACE
            - name: cache
              value: always
YAML
done
"${HASH[@]}" "$OUT/input/task.yaml" "$OUT/input/pipelineruns/"*.yaml > "$OUT/input/SHA256SUMS"

: > "$OUT/dry-run-results.tsv"
for ((i=1; i<=DRY_RUNS; i++)); do
  if "${K[@]}" create --dry-run=server -f "$OUT/input/pipelineruns/$(printf "%0${width}d" 1).yaml" \
      -o name > "$OUT/dry-run-$i.out" 2> "$OUT/dry-run-$i.err"; then
    printf '%s\tPASS\n' "$i" >> "$OUT/dry-run-results.tsv"
  else
    printf '%s\tFAIL\n' "$i" >> "$OUT/dry-run-results.tsv"
  fi
done
if [[ $(grep -c $'\tPASS$' "$OUT/dry-run-results.tsv") -ne "$DRY_RUNS" ]]; then
  echo "admission dry-run preflight failed" >&2
  exit 1
fi

start_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
jq -n \
  --arg runId "$RUN_ID" --arg namespace "$NAMESPACE" --arg context "$current_context" \
  --arg expectation "$EXPECT" --arg startTime "$start_time" \
  --argjson attempted "$COUNT" --argjson minimumAdmitted "$MIN_ADMITTED" --argjson dryRuns "$DRY_RUNS" \
  '{runId:$runId,namespace:$namespace,context:$context,expectation:$expectation,startTime:$startTime,attempted:$attempted,minimumAdmitted:$minimumAdmitted,dryRuns:$dryRuns}' \
  > "$OUT/metadata.json"

"${K[@]}" get resolutionrequests.resolution.tekton.dev -n "$NAMESPACE" \
  --watch-only --output-watch-events --request-timeout=0 -o json \
  2> "$OUT/rr-watch.err" | jq --unbuffered -c . > "$OUT/rr-watch.jsonl" &
# $! is jq. Stopping it closes the pipe so the API watch exits on SIGPIPE.
watch_pid=$!

(
  printf 'timestamp\tpipelineRuns\tprTrue\tprFalse\tprUnknown\tresolving\tresolutionRequests\trrTrue\trrFalse\trrUnknown\n'
  while true; do
    now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    prjson=$("${K[@]}" get pipelineruns -n "$NAMESPACE" -l "qe-run=$RUN_ID" -o json 2>/dev/null || printf '{"items":[]}')
    rrjson=$("${K[@]}" get resolutionrequests -n "$NAMESPACE" -o json 2>/dev/null || printf '{"items":[]}')
    pr_total=$(jq '.items|length' <<<"$prjson")
    pr_true=$(jq '[.items[]|select(any(.status.conditions[]?;.type=="Succeeded" and .status=="True"))]|length' <<<"$prjson")
    pr_false=$(jq '[.items[]|select(any(.status.conditions[]?;.type=="Succeeded" and .status=="False"))]|length' <<<"$prjson")
    pr_unknown=$((pr_total-pr_true-pr_false))
    resolving=$(jq '[.items[]|select(any(.status.conditions[]?;.type=="Succeeded" and .reason=="ResolvingTaskRef"))]|length' <<<"$prjson")
    rr_total=$(jq --arg prefix "resolver-cache-$RUN_ID-" '[.items[]|select(any(.metadata.ownerReferences[]?;.kind=="PipelineRun" and (.name|startswith($prefix))))]|length' <<<"$rrjson")
    rr_true=$(jq --arg prefix "resolver-cache-$RUN_ID-" '[.items[]|select(any(.metadata.ownerReferences[]?;.kind=="PipelineRun" and (.name|startswith($prefix))))|select(any(.status.conditions[]?;.type=="Succeeded" and .status=="True"))]|length' <<<"$rrjson")
    rr_false=$(jq --arg prefix "resolver-cache-$RUN_ID-" '[.items[]|select(any(.metadata.ownerReferences[]?;.kind=="PipelineRun" and (.name|startswith($prefix))))|select(any(.status.conditions[]?;.type=="Succeeded" and .status=="False"))]|length' <<<"$rrjson")
    rr_unknown=$((rr_total-rr_true-rr_false))
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$now" "$pr_total" "$pr_true" "$pr_false" "$pr_unknown" "$resolving" "$rr_total" "$rr_true" "$rr_false" "$rr_unknown"
    sleep "$SAMPLE_INTERVAL"
  done
) > "$OUT/samples.tsv" 2> "$OUT/sampler.err" &
sampler_pid=$!
collectors_stopped=false
sleep 2
kill -0 "$watch_pid" 2>/dev/null || { echo "ResolutionRequest watch failed to start" >&2; exit 1; }
kill -0 "$sampler_pid" 2>/dev/null || { echo "sampler failed to start" >&2; exit 1; }

set +e
find "$OUT/input/pipelineruns" -name '*.yaml' -print0 \
  | xargs -0 -n1 -P "$CONCURRENCY" "$KUBECTL_BIN" create -f \
      > "$OUT/create.out" 2> "$OUT/create.err"
create_rc=$?
set -e

admitted=$("${K[@]}" get pipelineruns -n "$NAMESPACE" -l "qe-run=$RUN_ID" -o json | jq '.items|length')
deadline=$(( $(date +%s) + RUN_TIMEOUT_SECONDS ))
timed_out=true
while (( $(date +%s) <= deadline )); do
  state=$("${K[@]}" get pipelineruns -n "$NAMESPACE" -l "qe-run=$RUN_ID" -o json \
    | jq '[.items|length, ([.items[]|select(any(.status.conditions[]?;.type=="Succeeded" and (.status=="True" or .status=="False")))]|length)] | @tsv' -r)
  read -r total terminal <<< "$state"
  if (( total >= MIN_ADMITTED && terminal == total )); then
    timed_out=false
    break
  fi
  sleep 5
done
sleep "$QUIESCE_SECONDS"
stop_collectors

"${K[@]}" get pipelineruns -n "$NAMESPACE" -l "qe-run=$RUN_ID" -o json > "$OUT/pipelineruns.json"
"${K[@]}" get resolutionrequests -n "$NAMESPACE" -o json > "$OUT/resolutionrequests.json"
"${K[@]}" get taskruns -n "$NAMESPACE" -o json > "$OUT/taskruns.json"
"${K[@]}" get events -n "$NAMESPACE" -o json > "$OUT/events.json"
capture_deployment controller "$PIPELINES_NAMESPACE" "$CONTROLLER_DEPLOYMENT" after
capture_deployment resolver "$RESOLVERS_NAMESPACE" "$RESOLVER_DEPLOYMENT" after
capture_deployment webhook "$PIPELINES_NAMESPACE" "$WEBHOOK_DEPLOYMENT" after
capture_pods controller "$PIPELINES_NAMESPACE" "$controller_selector" after
capture_pods resolver "$RESOLVERS_NAMESPACE" "$resolver_selector" after
capture_pods webhook "$PIPELINES_NAMESPACE" "$webhook_selector" after

jq -n --argjson createExitCode "$create_rc" --argjson admitted "$admitted" --argjson timedOut "$timed_out" \
  '{createExitCode:$createExitCode,admitted:$admitted,timedOut:$timedOut}' > "$OUT/run-state.json"

collect_logs() {
  local component=$1 namespace=$2 snapshot=$3
  jq -r '.items[] | .metadata.name as $pod | .spec.containers[].name | [$pod,.] | @tsv' "$snapshot" \
    | while IFS=$'\t' read -r pod container; do
        safe_container=${container//\//_}
        "${K[@]}" logs -n "$namespace" "$pod" -c "$container" --since-time="$start_time" \
          > "$OUT/logs/$component/${pod}__${safe_container}.log" \
          2> "$OUT/logs/$component/${pod}__${safe_container}.err" || true
      done
}
collect_logs controller "$PIPELINES_NAMESPACE" "$OUT/controller-pods-after.json"
collect_logs resolver "$RESOLVERS_NAMESPACE" "$OUT/resolver-pods-after.json"
collect_logs webhook "$PIPELINES_NAMESPACE" "$OUT/webhook-pods-after.json"

set +e
python3 "$ROOT/analyze.py" "$OUT" --expect "$EXPECT" --output "$OUT/summary.json"
analyze_rc=$?
set -e
cat "$OUT/summary.json"

if (( analyze_rc == 0 )) && [[ "$CLEANUP_NAMESPACE" == true ]]; then
  "${K[@]}" delete namespace "$NAMESPACE" --wait=true --timeout=300s >/dev/null
  namespace_created=false
fi

if (( analyze_rc != 0 )); then
  exit "$analyze_rc"
fi

echo "Evidence: $OUT"
