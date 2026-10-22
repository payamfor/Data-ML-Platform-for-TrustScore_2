"""Focused unit tests for Gold argument construction; no AWS calls."""
import importlib.util
import os
import pathlib
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


ENV = {
    "CONTROL_DATABASE": "control",
    "CONTROL_TABLE": "silver_gold_control",
    "RELEASE_BUCKET": "release-bucket",
    "ORCHESTRATION_CLAIM_BUCKET": "claim-bucket",
    "GOLD_JOB_CONFIG_URI": "s3://config-bucket/gold/job_config.json",
    "SILVER_JOB_CONFIG_URI": "s3://config-bucket/silver/job_config.json",
}


def load_module():
    path = pathlib.Path(__file__).with_name("silver_gold_control.py")
    spec = importlib.util.spec_from_file_location("silver_gold_control_under_test", path)
    module = importlib.util.module_from_spec(spec)
    boto3 = types.ModuleType("boto3")
    boto3.client = MagicMock(return_value=MagicMock())
    botocore = types.ModuleType("botocore")
    exceptions = types.ModuleType("botocore.exceptions")
    exceptions.ClientError = type("ClientError", (Exception,), {})
    with patch.dict(os.environ, ENV, clear=False), patch.dict(
            sys.modules, {"boto3": boto3, "botocore": botocore,
                          "botocore.exceptions": exceptions}):
        spec.loader.exec_module(module)
    return module


class GoldRecoveryArgsTests(unittest.TestCase):
    def setUp(self):
        self.module = load_module()
        self.config = {
            "application": "s3://gold/pipeline-code/gold/auto/run_gold.py",
            "python_package": "s3://gold/pipeline-code/gold/auto/gold_pipeline.zip",
            "config": "s3://gold/pipeline-code/gold/auto/lotus_sandbox.yaml",
            "config_overlays": ["s3://gold/pipeline-code/gold/auto/capacity_v3.yaml"],
            "staging_root": "s3://gold/staging/lineage_auto/",
            "job": "lineage",
            "first_run_watermark_from": "1970-01-01 00:00:00",
            "spark_submit_flags": ["--deploy-mode", "cluster"],
            "spark_confs": ["spark.yarn.maxAppAttempts=1"],
            "recovery": {
                "control_finalizer_application": "s3://gold/pipeline-code/gold/auto/recovery/v1/recover_gold_run.py",
                "publish_rollback_application": "s3://gold/pipeline-code/gold/auto/recovery/v1/rollback_gold_publish.py",
                "python_package": "s3://gold/pipeline-code/gold/auto/recovery/v1/gold_recovery_tools.zip",
                "control_table": "glue_catalog.gold.gold_run_control",
                "publish_failure_policy": "rollback_then_fail",
                "snapshot_retention_days": 7,
            },
        }

    def build(self, recovery_type="finalize", stage="accounts",
              failure_kind="spark_step"):
        event = {"run_id": "gold-20261022-010203", "failed_stage": stage,
                 "recovery_type": recovery_type,
                 "failure_kind": failure_kind,
                 "failed_cluster_id": "j-FAILED123",
                 "recovery_cluster_id": "j-RECOVERY456",
                 "config_overlays": ["s3://release/extra.yaml"],
                 "overrides": ["slice.max_size=100"]}
        with patch.object(self.module, "_read_json_s3", return_value=self.config):
            return self.module.action_build_gold_failure_args(event)

    def test_builds_replacement_cluster_sidecar_command_with_original_zip(self):
        result = self.build()
        args = result["args"]
        py_files = args[args.index("--py-files") + 1]
        self.assertEqual(
            py_files,
            "s3://gold/pipeline-code/gold/auto/gold_pipeline.zip,"
            "s3://gold/pipeline-code/gold/auto/recovery/v1/gold_recovery_tools.zip",
        )
        self.assertIn("s3://gold/pipeline-code/gold/auto/recovery/v1/recover_gold_run.py", args)
        self.assertNotIn("s3://gold/pipeline-code/gold/auto/recovery/v1/rollback_gold_publish.py", args)
        self.assertNotIn("--resume-publish", args)
        self.assertNotIn("--action", args)
        for required in ("--failed-cluster-id", "j-FAILED123",
                         "--recovery-cluster-id", "j-RECOVERY456",
                         "--failed-stage", "accounts", "--failure-kind", "spark_step",
                         "--control-table", "glue_catalog.gold.gold_run_control"):
            self.assertIn(required, args)
        self.assertEqual(args.count("--config"), 3)
        self.assertIn("s3://gold/pipeline-code/gold/auto/capacity_v3.yaml", args)
        self.assertIn("s3://release/extra.yaml", args)
        self.assertIn("slice.max_size=100", args)


    def test_publish_rollback_is_publish_only_and_uses_separate_entrypoint(self):
        with self.assertRaises(ValueError):
            self.build("rollback_publish", "customers")
        result = self.build("rollback_publish", "publish")
        self.assertEqual(result["recovery_type"], "rollback_publish")
        self.assertIn(
            "s3://gold/pipeline-code/gold/auto/recovery/v1/rollback_gold_publish.py",
            result["args"],
        )
        self.assertNotIn(
            "s3://gold/pipeline-code/gold/auto/recovery/v1/recover_gold_run.py",
            result["args"],
        )

    def test_recovery_policy_is_required(self):
        self.config["recovery"]["publish_failure_policy"] = "resume"
        with self.assertRaises(ValueError):
            self.build("rollback_publish", "publish")

    def test_argument_build_failure_can_finalize_only_after_prepare(self):
        result = self.build("finalize", "accounts", "argument_build")
        self.assertIn("argument_build", result["args"])
        with self.assertRaises(ValueError):
            self.build("finalize", "prepare", "argument_build")

    def test_normal_gold_args_preserve_default_and_release_configuration(self):
        event = {
            "stage": "prepare",
            "run_id": "gold-20261022-010203",
            "first_run": False,
            "config_overlays": ["s3://release/extra.yaml"],
            "overrides": ["slice.max_size=100"],
        }
        with patch.object(self.module, "_read_json_s3", return_value=self.config):
            args = self.module.action_build_gold_args(event)["args"]
        self.assertIn("s3://gold/pipeline-code/gold/auto/capacity_v3.yaml", args)
        self.assertIn("s3://release/extra.yaml", args)
        self.assertIn("--set", args)
        self.assertIn("slice.max_size=100", args)


class SilverReplayArgsTests(unittest.TestCase):
    def setUp(self):
        self.module = load_module()
        self.config = {
            "application": "s3://silver/artifacts/run_trust_score_silver.py",
            "python_package": "s3://silver/artifacts/silver_pipeline.zip",
            "artifact_root": "s3://silver/run-artifacts/",
            "mode": "publish",
            "spark_submit_flags": ["--deploy-mode", "cluster"],
            "spark_confs": ["spark.yarn.maxAppAttempts=1"],
            "datasets": {
                "account_changes_batch": {
                    "table": "glue_catalog.silver.account_changes_batch",
                    "quarantine_table": (
                        "glue_catalog.silver.silver_quarantine_account_changes_batch"
                    ),
                    "quarantine_table_location": (
                        "s3://silver/quarantine/account_changes_batch/"
                    ),
                }
            },
        }

    def build(self, *, trigger_type="automatic", reprocess_id="", attempt=1):
        event = {
            "dataset_name": "account_changes_batch",
            "run_date": "2026-10-22",
            "folder_path": (
                "s3://bronze/bronze/account_changes_batch/2026-10-22/"
            ),
            "attempt": attempt,
            "trigger_type": trigger_type,
            "reprocess_id": reprocess_id,
        }
        with patch.object(self.module, "_read_json_s3", return_value=self.config):
            return self.module.action_build_silver_args(event)

    def test_automatic_run_id_remains_backward_compatible(self):
        result = self.build()
        self.assertEqual(
            result["run_id"], "silver-20261022-account_changes_batch-01"
        )
        self.assertIn(
            "s3://bronze/bronze/account_changes_batch/2026-10-22/",
            result["args"],
        )

    def test_manual_replay_has_distinct_stable_run_ids(self):
        first = self.build(reprocess_id="console-replay-001", trigger_type="manual")
        retry = self.build(
            reprocess_id="console-replay-001", trigger_type="manual", attempt=2
        )
        other = self.build(reprocess_id="console-replay-002", trigger_type="manual")

        self.assertIn("-replay-console-replay-001-", first["run_id"])
        self.assertTrue(first["run_id"].endswith("-01"))
        self.assertTrue(retry["run_id"].endswith("-02"))
        self.assertNotEqual(first["run_id"], retry["run_id"])
        self.assertNotEqual(first["run_id"], other["run_id"])
        self.assertEqual(first["reprocess_id"], "console-replay-001")

    def test_manual_replay_requires_identifier(self):
        with self.assertRaisesRegex(ValueError, "requires reprocess_id"):
            self.build(trigger_type="manual")

    def test_automatic_run_rejects_reprocess_identifier(self):
        with self.assertRaisesRegex(ValueError, "must not set reprocess_id"):
            self.build(reprocess_id="unexpected")


class GoldReleaseInputTests(unittest.TestCase):
    def setUp(self):
        self.module = load_module()

    def test_wrapped_eventbridge_request_resolves_release_object(self):
        event = {
            "action": "release_gate",
            "request": {
                "detail": {
                    "bucket": {"name": "release-bucket"},
                    "object": {
                        "key": "gold-release/run_date=2026-10-22/release.json"
                    },
                }
            },
        }
        self.assertEqual(
            self.module._release_object_from_event(event),
            (
                "release-bucket",
                "gold-release/run_date=2026-10-22/release.json",
            ),
        )

    def test_wrapped_manual_run_date_uses_configured_release_bucket(self):
        self.assertEqual(
            self.module._release_object_from_event(
                {
                    "action": "release_gate",
                    "request": {"run_date": "2026-10-22"},
                }
            ),
            (
                "release-bucket",
                "gold-release/run_date=2026-10-22/release.json",
            ),
        )

    def test_wrapped_manual_release_uri_uses_exact_object(self):
        self.assertEqual(
            self.module._release_object_from_event(
                {
                    "action": "release_gate",
                    "request": {
                        "release_uri": (
                            "s3://other-bucket/manual/releases/release.json"
                        )
                    },
                }
            ),
            ("other-bucket", "manual/releases/release.json"),
        )

    def test_direct_invocation_remains_supported(self):
        self.assertEqual(
            self.module._release_object_from_event({"run_date": "2026-10-22"}),
            (
                "release-bucket",
                "gold-release/run_date=2026-10-22/release.json",
            ),
        )

    def test_ambiguous_manual_request_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.module._release_object_from_event(
                {
                    "request": {
                        "run_date": "2026-10-22",
                        "release_uri": (
                            "s3://release-bucket/gold-release/"
                            "run_date=2026-10-22/release.json"
                        ),
                    }
                }
            )

    def test_invalid_manual_run_date_is_rejected(self):
        for run_date in ("20261022", "2026-02-30"):
            with self.subTest(run_date=run_date):
                with self.assertRaisesRegex(ValueError, "run_date"):
                    self.module._release_object_from_event(
                        {"request": {"run_date": run_date}}
                    )

    def test_incomplete_eventbridge_request_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "requires detail.bucket.name"):
            self.module._release_object_from_event(
                {"request": {"detail": {"bucket": {"name": "release-bucket"}}}}
            )


class ClusterReplayClaimTests(unittest.TestCase):
    def setUp(self):
        self.module = load_module()

    def conditional_conflict(self):
        error = self.module.ClientError()
        error.response = {"Error": {"Code": "PreconditionFailed"}}
        return error

    def test_terminal_claim_is_atomically_replaced_for_replay(self):
        with patch.object(
            self.module,
            "_put_cluster",
            side_effect=[self.conditional_conflict(), None],
        ) as put_cluster, patch.object(
            self.module,
            "_read_cluster",
            return_value=(
                {"owner": "old-execution", "status": "terminated"},
                "terminal-etag",
            ),
        ):
            self.assertTrue(
                self.module._claim_cluster("2026-10-22", "manual-replay")
            )

        self.assertEqual(put_cluster.call_args_list[0].kwargs, {"IfNoneMatch": "*"})
        self.assertEqual(
            put_cluster.call_args_list[1].kwargs,
            {"IfMatch": '"terminal-etag"'},
        )

    def test_active_claim_is_never_replaced(self):
        for status in ("creating", "ready"):
            with self.subTest(status=status), patch.object(
                self.module,
                "_put_cluster",
                side_effect=self.conditional_conflict(),
            ) as put_cluster, patch.object(
                self.module,
                "_read_cluster",
                return_value=(
                    {"owner": "another-execution", "status": status},
                    "active-etag",
                ),
            ):
                self.assertFalse(
                    self.module._claim_cluster("2026-10-22", "manual-replay")
                )
                self.assertEqual(put_cluster.call_count, 1)

    def test_only_one_replay_wins_terminal_claim_race(self):
        with patch.object(
            self.module,
            "_put_cluster",
            side_effect=[
                self.conditional_conflict(),
                self.conditional_conflict(),
            ],
        ), patch.object(
            self.module,
            "_read_cluster",
            side_effect=[
                (
                    {"owner": "old-execution", "status": "terminated"},
                    "terminal-etag",
                ),
                (
                    {"owner": "winning-replay", "status": "creating"},
                    "new-etag",
                ),
            ],
        ):
            self.assertFalse(
                self.module._claim_cluster("2026-10-22", "losing-replay")
            )


if __name__ == "__main__":
    unittest.main()
