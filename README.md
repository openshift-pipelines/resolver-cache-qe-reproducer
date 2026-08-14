# ResolutionRequest cache amplification QE reproducer

This package validates resolver behavior under concurrent, cached remote resolution. It is intentionally generic and contains no incident-specific data.

## What this covers

The load test exercises three independent fixes together:

1. Only the resolver replica owning a leader-election bucket processes a `ResolutionRequest` ([pipeline#10480](https://github.com/tektoncd/pipeline/pull/10480)).
2. A request whose `status.data` is already populated is not resolved again ([pipeline#10114](https://github.com/tektoncd/pipeline/pull/10114)).
3. The lifecycle controller does not erase resolver-owned status fields ([pipeline#10487](https://github.com/tektoncd/pipeline/pull/10487)).

The missed-enqueue fallback in [pipeline#10429](https://github.com/tektoncd/pipeline/pull/10429) is a separate liveness test and is not claimed by this reproducer.

## Required A/B builds

Run the same workload against:

- **Affected build:** the supported build being qualified before the fixes.
- **Fixed build:** a signed product candidate from the supported release branch containing #10114, #10480, and #10487.

Do not use the historical engineering-only images as release evidence. Record the actual image IDs from both runs; `run.sh` captures them automatically.

At the time this handoff was prepared, upstream `release-v1.9.x` contained all four fixes, but no published `v1.9.x` tag contained them. QE therefore needs a signed candidate supplied through the normal product build process.

## Why this workload is isolated

Each PipelineRun resolves one shared Task through the cluster resolver with `cache: always`. Its `when` expression is always false, so Tekton creates a `ResolutionRequest` but no TaskRun or workload pod. Resolver, controller, webhook, and API-server behavior are measured without scheduling or image-pull noise.

## Prerequisites

- A disposable or approved test cluster.
- `oc` (or set `KUBECTL_BIN=kubectl`), `jq`, and Python 3.
- Permission to create/delete a namespace and to read PipelineRuns, ResolutionRequests, deployments, pods, logs, and events.
- Cluster resolver enabled.
- Resolver/controller replicas configured through the supported product mechanism before running the script.

Recommended qualification profile:

```yaml
spec:
  pipeline:
    performance:
      disable-ha: false
      replicas: 10
      buckets: 10
      threads-per-controller: 32
      kube-api-qps: 50
      kube-api-burst: 50
    options:
      horizontalPodAutoscalers:
        tekton-pipelines-webhook:
          spec:
            minReplicas: 5
            maxReplicas: 5
```

The script verifies the expected replica counts but does not mutate product configuration or image references.

## Run

Use a fresh namespace for each phase. The script creates one automatically and removes it only after a successful analysis. A failed run keeps the namespace for inspection.

```bash
git clone https://github.com/openshift-pipelines/resolver-cache-qe-reproducer.git
cd resolver-cache-qe-reproducer

context="$(oc config current-context)"

CONFIRM_CONTEXT="$context" \
EXPECT=affected \
COUNT=500 \
EXPECTED_CONTROLLER_REPLICAS=10 \
EXPECTED_RESOLVER_REPLICAS=10 \
EXPECTED_WEBHOOK_REPLICAS=5 \
./run.sh artifacts/affected
```

Install the fixed signed candidate, wait for all product rollouts, and run the identical workload:

```bash
context="$(oc config current-context)"

CONFIRM_CONTEXT="$context" \
EXPECT=fixed \
COUNT=500 \
EXPECTED_CONTROLLER_REPLICAS=10 \
EXPECTED_RESOLVER_REPLICAS=10 \
EXPECTED_WEBHOOK_REPLICAS=5 \
./run.sh artifacts/fixed
```

Compare the normalized results:

```bash
python3 analyze.py --compare \
  artifacts/affected/summary.json \
  artifacts/fixed/summary.json \
  --output artifacts/comparison.json
```

For a quicker mechanism smoke, use `COUNT=50`. Keep `COUNT=500` for qualification.

## Pass criteria

### Affected-build reproduction

`EXPECT=affected` reports `REPRODUCED` when:

- the run is valid and all admitted runs finish;
- no TaskRuns are created;
- at least one ResolutionRequest is processed by more than one resolver pod; and
- cache operations exceed admitted PipelineRuns.

The earlier engineering run observed every one of ten resolver pods processing each request, but exact latency and operation counts are not hard-coded as pass criteria.

### Fixed-build gate

`EXPECT=fixed` passes only when:

- all attempted PipelineRuns are admitted and succeed;
- every ResolutionRequest succeeds with final `status.data`;
- no TaskRuns are created;
- every ResolutionRequest is processed by exactly one resolver pod;
- no watch transition clears previously populated `status.data`;
- no request regresses after first reaching `Succeeded=True`;
- no `ResolutionRequest` `InternalError` Events occur; and
- cache stores plus retrieves are at most three per admitted run.

### A/B comparison

The comparison passes when both runs use the same cluster context, replica profile, and attempted load, the fixed run passes its correctness gate, and relative to the affected run:

- normalized resolver operations are at most 25%; and
- normalized distinct ResolutionRequest watch events are at most 45%.

Correctness invariants—one owner and zero status-data clears—must not be relaxed because of cluster variance.

## Optional upstream smoke test

The Pipeline repository already contains a smaller four-replica bundle-cache E2E test:

```bash
go test -v -count=1 -tags=e2e -timeout=20m ./test \
  -run '^TestBundleResolverCacheWithFourResolverReplicas$'
```

Run it from source matching the installed build. The affected source expects four registry fetches; source containing #10480 expects one. This is the shortest deterministic ownership smoke, but it does not replace the 500-run status-transition and performance gate.

## Evidence produced

Each run directory contains:

- generated Task and PipelineRun manifests;
- create stdout/stderr and dry-run results;
- raw ResolutionRequest watch events;
- five-second PipelineRun/ResolutionRequest samples;
- final PipelineRun, ResolutionRequest, TaskRun, and Kubernetes Event snapshots;
- controller, resolver, and webhook logs;
- pre/post deployment and pod snapshots with image IDs;
- `summary.json` with the verdict and normalized measurements.

An independent reviewer should be able to rerun `analyze.py` without cluster access.

## Validity rules

A phase is invalid if expected replicas are not Ready, the admission dry-run fails, pod UIDs/restart counts change during the measured interval, watch evidence is empty, resolver logs cannot be correlated to every request, or fewer than `MIN_ADMITTED` requests are admitted.

Defaults:

- `DRY_RUNS=20`
- `MIN_ADMITTED=COUNT-10`
- `QUIESCE_SECONDS=15`
- `RUN_TIMEOUT=600s`
- successful runs delete their namespace; set `CLEANUP_NAMESPACE=false` to retain it

## Cleanup

On success, the test namespace is deleted unless `CLEANUP_NAMESPACE=false`. On failure, it is retained and printed. Manual cleanup:

```bash
oc delete namespace <namespace-from-run-output>
```

The runner never changes TektonConfig, HPA settings, product images, or replica counts, so QE must restore those separately if they changed them during environment setup.

## Interpretation boundary

This proves the resolver amplification and status-clobber mechanisms on the tested builds. It does not by itself prove attribution for any specific production incident. Do not attach internal case notes or raw customer artifacts to this package.
