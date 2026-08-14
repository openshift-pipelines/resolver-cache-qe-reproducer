#!/usr/bin/env python3
"""Analyze or compare ResolutionRequest cache-amplification QE runs."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import sys
from collections import Counter
from pathlib import Path


def read_json(path: Path):
    with path.open() as f:
        return json.load(f)


def condition(obj):
    return next(
        (c for c in obj.get("status", {}).get("conditions", []) if c.get("type") == "Succeeded"),
        {},
    )


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def parse_time(value):
    if not value:
        return None
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def pod_state(path: Path):
    state = {}
    for pod in read_json(path).get("items", []):
        state[pod["metadata"]["name"]] = {
            "uid": pod["metadata"].get("uid"),
            "restarts": sum(
                status.get("restartCount", 0)
                for status in pod.get("status", {}).get("containerStatuses", [])
            ),
        }
    return state


def parse_log_line(line):
    start = line.find("{")
    if start < 0:
        return None
    try:
        return json.loads(line[start:])
    except json.JSONDecodeError:
        return None


def owner_name(rr, kind="PipelineRun"):
    return next(
        (
            owner.get("name")
            for owner in rr.get("metadata", {}).get("ownerReferences", [])
            if owner.get("kind") == kind
        ),
        None,
    )


def analyze_run(run_dir: Path, expect: str):
    metadata = read_json(run_dir / "metadata.json")
    run_id = metadata["runId"]
    attempted = int(metadata["attempted"])
    minimum_admitted = int(metadata["minimumAdmitted"])

    prs = [
        item
        for item in read_json(run_dir / "pipelineruns.json").get("items", [])
        if item.get("metadata", {}).get("labels", {}).get("qe-run") == run_id
    ]
    pr_names = {item["metadata"]["name"] for item in prs}
    rrs = [
        item
        for item in read_json(run_dir / "resolutionrequests.json").get("items", [])
        if owner_name(item) in pr_names
    ]
    rr_by_uid = {item["metadata"]["uid"]: item for item in rrs}
    rr_names = {item["metadata"]["name"] for item in rrs}
    taskruns = read_json(run_dir / "taskruns.json").get("items", [])
    events = read_json(run_dir / "events.json").get("items", [])
    rr_internal_error_events = sum(
        int(item.get("count") or item.get("series", {}).get("count") or 1)
        for item in events
        if item.get("involvedObject", {}).get("kind") == "ResolutionRequest"
        and item.get("involvedObject", {}).get("name") in rr_names
        and item.get("reason") == "InternalError"
    )

    pr_statuses = Counter(condition(item).get("status", "None") for item in prs)
    rr_statuses = Counter(condition(item).get("status", "None") for item in rrs)
    final_rr_data = sum(bool(item.get("status", {}).get("data")) for item in rrs)

    watch_sequences = {uid: [] for uid in rr_by_uid}
    seen_watch_versions = set()
    watch_parse_errors = 0
    with (run_dir / "rr-watch.jsonl").open(errors="replace") as stream:
        for line in stream:
            try:
                event = json.loads(line)
                obj = event["object"]
                uid = obj["metadata"]["uid"]
                rv = obj["metadata"].get("resourceVersion")
            except (json.JSONDecodeError, KeyError, TypeError):
                watch_parse_errors += 1
                continue
            if uid not in watch_sequences or (uid, rv) in seen_watch_versions:
                continue
            seen_watch_versions.add((uid, rv))
            watch_sequences[uid].append(
                {
                    "data": len(obj.get("status", {}).get("data", "")),
                    "succeeded": condition(obj).get("status", "None"),
                }
            )

    data_clear_transitions = 0
    requests_with_data_clear = 0
    terminal_regressions = 0
    for events in watch_sequences.values():
        clears = 0
        regressed = False
        previous_data = None
        seen_true = False
        for event in events:
            if previous_data is not None and previous_data > 0 and event["data"] == 0:
                clears += 1
            previous_data = event["data"]
            if event["succeeded"] == "True":
                seen_true = True
            elif seen_true:
                regressed = True
        data_clear_transitions += clears
        requests_with_data_clear += clears > 0
        terminal_regressions += regressed

    log_stats = {
        name: {"stores": 0, "retrieves": 0, "reconciles": 0, "pods": set()}
        for name in rr_names
    }
    resolver_log_parse_errors = 0
    resolver_error_files = []
    for error_path in sorted((run_dir / "logs" / "resolver").glob("*.err")):
        if error_path.stat().st_size:
            resolver_error_files.append(error_path.name)
    for log_path in sorted((run_dir / "logs" / "resolver").glob("*.log")):
        pod = log_path.name.split("__", 1)[0]
        with log_path.open(errors="replace") as stream:
            for line in stream:
                record = parse_log_line(line)
                if record is None:
                    if "{" in line:
                        resolver_log_parse_errors += 1
                    continue
                key = str(record.get("knative.dev/key") or record.get("key") or "")
                name = key.rsplit("/", 1)[-1]
                if name not in log_stats:
                    continue
                message = record.get("message", record.get("msg", "")) or ""
                relevant = False
                if message == "Adding to cache":
                    log_stats[name]["stores"] += 1
                    relevant = True
                elif message == "Cache hit":
                    log_stats[name]["retrieves"] += 1
                    relevant = True
                elif message == "Reconcile succeeded":
                    log_stats[name]["reconciles"] += 1
                    relevant = True
                if relevant:
                    log_stats[name]["pods"].add(pod)

    stores = sum(item["stores"] for item in log_stats.values())
    retrieves = sum(item["retrieves"] for item in log_stats.values())
    operations = stores + retrieves
    reconciles = sum(item["reconciles"] for item in log_stats.values())
    pod_counts = [len(log_stats[name]["pods"]) for name in sorted(rr_names)]
    requests_with_operation = sum(
        item["stores"] + item["retrieves"] > 0 for item in log_stats.values()
    )
    uncorrelated_requests = sum(count == 0 for count in pod_counts)
    multi_resolver_requests = sum(count > 1 for count in pod_counts)

    watch_counts = [len(watch_sequences[uid]) for uid in rr_by_uid]
    watch_missing_requests = sum(count == 0 for count in watch_counts)

    durations = {"pipeline": [], "resolutionRequest": [], "rrToPipeline": []}
    rr_by_owner = {owner_name(rr): rr for rr in rrs}
    for pr in prs:
        pr_created = parse_time(pr["metadata"].get("creationTimestamp"))
        pr_done = parse_time(pr.get("status", {}).get("completionTime")) or parse_time(
            condition(pr).get("lastTransitionTime")
        )
        if pr_created and pr_done:
            durations["pipeline"].append((pr_done - pr_created).total_seconds())
        rr = rr_by_owner.get(pr["metadata"]["name"])
        if not rr:
            continue
        rr_created = parse_time(rr["metadata"].get("creationTimestamp"))
        rr_done = parse_time(condition(rr).get("lastTransitionTime"))
        if rr_created and rr_done:
            durations["resolutionRequest"].append((rr_done - rr_created).total_seconds())
        if rr_done and pr_done:
            durations["rrToPipeline"].append((pr_done - rr_done).total_seconds())

    peak_resolving = 0
    samples_path = run_dir / "samples.tsv"
    if samples_path.exists():
        with samples_path.open() as stream:
            for row in csv.DictReader(stream, delimiter="\t"):
                try:
                    peak_resolving = max(peak_resolving, int(row.get("resolving", 0)))
                except ValueError:
                    pass

    stable_components = {}
    for component in ("controller", "resolver", "webhook"):
        before = pod_state(run_dir / f"{component}-pods-before.json")
        after = pod_state(run_dir / f"{component}-pods-after.json")
        stable_components[component] = before == after and bool(before)

    with (run_dir / "dry-run-results.tsv").open() as stream:
        dry_run_lines = [
            line.rstrip("\n").split("\t") for line in stream if line.strip()
        ]
    dry_run_passes = sum(len(row) >= 2 and row[1] == "PASS" for row in dry_run_lines)
    dry_runs_expected = int(metadata["dryRuns"])
    watch_status = (run_dir / "rr-watch-status.txt").read_text().strip()
    run_state = read_json(run_dir / "run-state.json")
    preflight = read_json(run_dir / "preflight.json")
    create_error_lines = [
        line.strip()
        for line in (run_dir / "create.err").read_text(errors="replace").splitlines()
        if line.strip()
    ]

    admitted = len(prs)
    all_admitted_terminal = pr_statuses["True"] + pr_statuses["False"] == admitted
    validity_checks = {
        "preflightReady": bool(preflight.get("allReady")),
        "dryRunsPassed": dry_run_passes == dry_runs_expected,
        "minimumAdmissionMet": admitted >= minimum_admitted,
        "allAdmittedTerminal": all_admitted_terminal and not run_state.get("timedOut"),
        "stableProductPods": all(stable_components.values()),
        "watchStoppedByRunner": watch_status == "stopped-by-runner",
        "watchParsed": watch_parse_errors == 0,
        "watchCoveredEveryRequest": bool(rrs) and watch_missing_requests == 0,
        "resolverLogsCollected": not resolver_error_files,
        "resolverLogsCoveredEveryRequest": bool(rrs) and uncorrelated_requests == 0,
        # Singleflight followers share the first result and therefore may not
        # emit a per-request cache store/retrieve log. Reconcile ownership and
        # status transitions remain observable for every request.
        "cacheOperationsObserved": operations > 0,
    }
    valid = all(validity_checks.values())

    fixed_checks = {
        "allAttemptsAdmitted": admitted == attempted,
        "allPipelineRunsSucceeded": pr_statuses["True"] == admitted,
        "oneResolutionRequestPerPipelineRun": len(rrs) == admitted,
        "allResolutionRequestsSucceeded": rr_statuses["True"] == len(rrs),
        "allResolutionRequestsRetainData": final_rr_data == len(rrs),
        "noTaskRunsCreated": len(taskruns) == 0,
        "exactlyOneResolverPerRequest": bool(pod_counts)
        and min(pod_counts) == max(pod_counts) == 1,
        "noStatusDataClears": data_clear_transitions == 0,
        "noTerminalRegressions": terminal_regressions == 0,
        "noResolutionRequestInternalErrors": rr_internal_error_events == 0,
        "boundedCacheOperations": admitted > 0 and 0 < operations <= admitted * 3,
    }
    affected_checks = {
        "allAdmittedPipelineRunsSucceeded": pr_statuses["True"] == admitted,
        "oneResolutionRequestPerPipelineRun": len(rrs) == admitted,
        "noTaskRunsCreated": len(taskruns) == 0,
        "multipleResolverProcessingObserved": multi_resolver_requests > 0,
        "cacheAmplificationObserved": operations > admitted,
    }

    if not valid:
        verdict = "INVALID"
    elif expect == "fixed":
        verdict = "PASS" if all(fixed_checks.values()) else "FAIL"
    else:
        verdict = "REPRODUCED" if all(affected_checks.values()) else "NOT_REPRODUCED"

    def timing(values):
        return {
            "median": percentile(values, 0.5),
            "p95": percentile(values, 0.95),
            "maximum": max(values) if values else None,
        }

    return {
        "schemaVersion": 1,
        "expectation": expect,
        "verdict": verdict,
        "valid": valid,
        "validityChecks": validity_checks,
        "fixedChecks": fixed_checks,
        "affectedChecks": affected_checks,
        "attemptedPipelineRuns": attempted,
        "admittedPipelineRuns": admitted,
        "createExitCode": run_state.get("createExitCode"),
        "createErrorLines": len(create_error_lines),
        "pipelineRunFinalStatus": dict(pr_statuses),
        "resolutionRequests": len(rrs),
        "resolutionRequestFinalStatus": dict(rr_statuses),
        "resolutionRequestsWithFinalData": final_rr_data,
        "taskRunsCreated": len(taskruns),
        "resolverCacheStores": stores,
        "resolverCacheRetrieves": retrieves,
        "resolverOperations": operations,
        "resolverSuccessfulReconciles": reconciles,
        "requestsWithCacheOperation": requests_with_operation,
        "resolverPodsPerRequest": {
            "minimum": min(pod_counts) if pod_counts else None,
            "median": percentile(pod_counts, 0.5),
            "p95": percentile(pod_counts, 0.95),
            "maximum": max(pod_counts) if pod_counts else None,
            "moreThanOne": multi_resolver_requests,
            "uncorrelated": uncorrelated_requests,
        },
        "distinctResolutionRequestWatchEvents": sum(watch_counts),
        "watchEventsPerRequest": {
            "minimum": min(watch_counts) if watch_counts else None,
            "median": percentile(watch_counts, 0.5),
            "p95": percentile(watch_counts, 0.95),
            "maximum": max(watch_counts) if watch_counts else None,
            "missing": watch_missing_requests,
        },
        "statusDataClearTransitions": data_clear_transitions,
        "requestsWithStatusDataClear": requests_with_data_clear,
        "terminalRegressions": terminal_regressions,
        "resolutionRequestInternalErrorEvents": rr_internal_error_events,
        "peakResolvingTaskRef": peak_resolving,
        "timingSeconds": {
            "pipelineRun": timing(durations["pipeline"]),
            "resolutionRequest": timing(durations["resolutionRequest"]),
            "resolutionRequestToPipelineRun": timing(durations["rrToPipeline"]),
        },
        "normalizedPerAdmitted": {
            "resolverOperations": operations / admitted if admitted else None,
            "successfulResolverReconciles": reconciles / admitted if admitted else None,
            "resolutionRequestWatchEvents": sum(watch_counts) / admitted if admitted else None,
        },
        "stableComponents": stable_components,
        "preflight": preflight,
        "watchParseErrors": watch_parse_errors,
        "resolverLogParseErrors": resolver_log_parse_errors,
        "resolverLogErrorFiles": resolver_error_files,
        "metadata": metadata,
    }


def compare_runs(affected_path: Path, fixed_path: Path):
    affected = read_json(affected_path)
    fixed = read_json(fixed_path)

    def ratio(metric):
        denominator = affected["normalizedPerAdmitted"][metric]
        numerator = fixed["normalizedPerAdmitted"][metric]
        return numerator / denominator if denominator else None

    operation_ratio = ratio("resolverOperations")
    event_ratio = ratio("resolutionRequestWatchEvents")
    affected_context = affected.get("metadata", {}).get("context")
    fixed_context = fixed.get("metadata", {}).get("context")
    affected_profile = affected.get("preflight", {})
    fixed_profile = fixed.get("preflight", {})
    checks = {
        "affectedRunReproduced": affected.get("verdict") == "REPRODUCED",
        "fixedRunPassed": fixed.get("verdict") == "PASS",
        "distinctRuns": affected.get("metadata", {}).get("runId")
        != fixed.get("metadata", {}).get("runId"),
        "sameClusterContext": bool(affected_context)
        and affected_context == fixed_context,
        "sameReplicaProfile": bool(affected_profile)
        and affected_profile == fixed_profile,
        "sameAttemptedLoad": affected.get("attemptedPipelineRuns")
        == fixed.get("attemptedPipelineRuns"),
        "operationsReducedAtLeast75Percent": operation_ratio is not None
        and operation_ratio <= 0.25,
        "watchEventsReducedAtLeast55Percent": event_ratio is not None
        and event_ratio <= 0.45,
    }
    return {
        "schemaVersion": 1,
        "verdict": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "fixedToAffectedRatios": {
            "normalizedResolverOperations": operation_ratio,
            "normalizedResolutionRequestWatchEvents": event_ratio,
        },
        "affectedSummary": str(affected_path),
        "fixedSummary": str(fixed_path),
    }


def write_result(result, output):
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if output:
        Path(output).write_text(rendered)
    else:
        sys.stdout.write(rendered)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", nargs="?", type=Path)
    parser.add_argument("--expect", choices=("affected", "fixed"))
    parser.add_argument("--compare", nargs=2, metavar=("AFFECTED", "FIXED"), type=Path)
    parser.add_argument("--output")
    args = parser.parse_args(argv)

    if args.compare:
        if args.run_dir or args.expect:
            parser.error("--compare cannot be combined with run_dir or --expect")
        result = compare_runs(*args.compare)
    else:
        if not args.run_dir or not args.expect:
            parser.error("run_dir and --expect are required unless --compare is used")
        result = analyze_run(args.run_dir, args.expect)

    write_result(result, args.output)
    return 0 if result["verdict"] in ("PASS", "REPRODUCED") else 1


if __name__ == "__main__":
    raise SystemExit(main())
