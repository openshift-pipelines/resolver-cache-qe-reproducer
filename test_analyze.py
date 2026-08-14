#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("analyze.py")
SPEC = importlib.util.spec_from_file_location("resolver_cache_analyze", MODULE_PATH)
ANALYZE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ANALYZE)


class AnalyzerTest(unittest.TestCase):
    def write_json(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value) + "\n")

    def object_condition(self, status="True", transition="2026-01-01T00:00:02Z"):
        return {
            "type": "Succeeded",
            "status": status,
            "lastTransitionTime": transition,
        }

    def make_fixture(self, root, fixed):
        run_id = "fixed" if fixed else "affected"
        attempted = 2
        self.write_json(
            root / "metadata.json",
            {
                "runId": run_id,
                "namespace": f"qe-{run_id}",
                "context": "qe-cluster",
                "attempted": attempted,
                "minimumAdmitted": attempted,
                "dryRuns": 2,
            },
        )
        self.write_json(
            root / "preflight.json",
            {
                "allReady": True,
                "controller": {"desired": 10, "ready": 10},
                "resolver": {"desired": 10, "ready": 10},
                "webhook": {"desired": 5, "ready": 5},
            },
        )
        self.write_json(
            root / "run-state.json",
            {"createExitCode": 0, "admitted": attempted, "timedOut": False},
        )
        root.joinpath("dry-run-results.tsv").write_text("1\tPASS\n2\tPASS\n")
        root.joinpath("create.err").write_text("")
        root.joinpath("rr-watch-status.txt").write_text("stopped-by-runner\n")
        root.joinpath("samples.tsv").write_text(
            "timestamp\tpipelineRuns\tprTrue\tprFalse\tprUnknown\tresolving\tresolutionRequests\trrTrue\trrFalse\trrUnknown\n"
            "2026-01-01T00:00:01Z\t2\t0\t0\t2\t2\t2\t0\t0\t2\n"
            "2026-01-01T00:00:03Z\t2\t2\t0\t0\t0\t2\t2\t0\t0\n"
        )

        prs = []
        rrs = []
        watch_events = []
        resolver_logs = {"resolver-a": [], "resolver-b": []}
        for index in range(2):
            pr_name = f"resolver-cache-{run_id}-{index}"
            rr_name = f"rr-{run_id}-{index}"
            rr_uid = f"uid-{run_id}-{index}"
            prs.append(
                {
                    "metadata": {
                        "name": pr_name,
                        "uid": f"pr-{run_id}-{index}",
                        "creationTimestamp": "2026-01-01T00:00:00Z",
                        "labels": {"qe-run": run_id},
                    },
                    "status": {
                        "completionTime": "2026-01-01T00:00:03Z",
                        "conditions": [self.object_condition()],
                    },
                }
            )
            rrs.append(
                {
                    "metadata": {
                        "name": rr_name,
                        "uid": rr_uid,
                        "creationTimestamp": "2026-01-01T00:00:00Z",
                        "ownerReferences": [{"kind": "PipelineRun", "name": pr_name}],
                    },
                    "status": {
                        "data": "resolved",
                        "conditions": [self.object_condition()],
                    },
                }
            )

            if fixed:
                states = [(1, 0, "None"), (2, 8, "True")]
                resolver_logs["resolver-a"].extend(
                    [
                        {"knative.dev/key": f"qe/{rr_name}", "message": "Adding to cache"},
                        {"knative.dev/key": f"qe/{rr_name}", "message": "Reconcile succeeded"},
                    ]
                )
            else:
                states = [
                    (1, 0, "None"),
                    (2, 8, "None"),
                    (3, 0, "None"),
                    (4, 8, "None"),
                    (5, 8, "True"),
                    (6, 8, "True"),
                ]
                for pod in resolver_logs:
                    resolver_logs[pod].extend(
                        [
                            {"knative.dev/key": f"qe/{rr_name}", "message": "Adding to cache"},
                            {"knative.dev/key": f"qe/{rr_name}", "message": "Cache hit"},
                            {"knative.dev/key": f"qe/{rr_name}", "message": "Reconcile succeeded"},
                        ]
                    )

            for rv, data_length, status in states:
                watch_events.append(
                    {
                        "type": "ADDED" if rv == 1 else "MODIFIED",
                        "object": {
                            "metadata": {
                                "name": rr_name,
                                "uid": rr_uid,
                                "resourceVersion": f"{index}{rv}",
                            },
                            "status": {
                                "data": "x" * data_length,
                                "conditions": []
                                if status == "None"
                                else [self.object_condition(status)],
                            },
                        },
                    }
                )

        self.write_json(root / "pipelineruns.json", {"items": prs})
        self.write_json(root / "resolutionrequests.json", {"items": rrs})
        self.write_json(root / "taskruns.json", {"items": []})
        self.write_json(root / "events.json", {"items": []})
        with root.joinpath("rr-watch.jsonl").open("w") as stream:
            for event in watch_events:
                stream.write(json.dumps(event) + "\n")

        logs = root / "logs" / "resolver"
        logs.mkdir(parents=True)
        for pod, records in resolver_logs.items():
            with logs.joinpath(f"{pod}__controller.log").open("w") as stream:
                for record in records:
                    stream.write(json.dumps(record) + "\n")
            logs.joinpath(f"{pod}__controller.err").write_text("")

        for component in ("controller", "resolver", "webhook"):
            pods = {
                "items": [
                    {
                        "metadata": {"name": f"{component}-a", "uid": f"{component}-uid"},
                        "status": {"containerStatuses": [{"restartCount": 0}]},
                    }
                ]
            }
            self.write_json(root / f"{component}-pods-before.json", pods)
            self.write_json(root / f"{component}-pods-after.json", pods)

    def test_affected_fixed_and_comparison_verdicts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            affected_dir = root / "affected"
            fixed_dir = root / "fixed"
            affected_dir.mkdir()
            fixed_dir.mkdir()
            self.make_fixture(affected_dir, fixed=False)
            self.make_fixture(fixed_dir, fixed=True)

            affected = ANALYZE.analyze_run(affected_dir, "affected")
            fixed = ANALYZE.analyze_run(fixed_dir, "fixed")
            self.assertEqual("REPRODUCED", affected["verdict"])
            self.assertEqual(2, affected["requestsWithStatusDataClear"])
            self.assertEqual("PASS", fixed["verdict"])
            self.assertEqual(0, fixed["statusDataClearTransitions"])
            self.assertEqual(1, fixed["resolverPodsPerRequest"]["maximum"])

            affected_path = root / "affected.json"
            fixed_path = root / "fixed.json"
            self.write_json(affected_path, affected)
            self.write_json(fixed_path, fixed)
            comparison = ANALYZE.compare_runs(affected_path, fixed_path)
            self.assertEqual("PASS", comparison["verdict"])


if __name__ == "__main__":
    unittest.main()
