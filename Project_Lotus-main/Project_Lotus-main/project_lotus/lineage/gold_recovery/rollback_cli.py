"""Rollback a failed Gold publish to its five pinned pre-run snapshots.

This command never changes ``gold_run_control``. A successful exit means the
rollback journal was written and every Gold table was independently verified
at its pinned snapshot. Control finalization is a separate Spark application.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import uuid

from .plan import RecoveryBlocked, rollback_publish


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", action="append", required=True)
    p.add_argument("--set", dest="overrides", action="append")
    p.add_argument("--staging-root", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--control-table", required=True)
    p.add_argument("--failed-cluster-id", required=True)
    p.add_argument("--recovery-cluster-id", required=True)
    p.add_argument("--failed-stage", choices=("publish",), default="publish")
    p.add_argument("--region", default="ca-central-1")
    return p


def _validate_control_table(cfg, supplied):
    configured = str(cfg.require("run_control.table"))
    if configured != supplied:
        raise RecoveryBlocked(
            "The requested control table differs from the table in the failed "
            f"run configuration ({supplied!r} != {configured!r})."
        )


def run(argv=None):
    args = parser().parse_args(argv)
    from trust_score_05.lineage.config import load_config
    from trust_score_05.lineage.spark import build_spark_session
    from trust_score_05.lineage.timezone import pin_process_timezone
    from .runtime import (
        GoldBackend, assert_no_active_gold_yarn_application, recovery_lease,
        verify_recovery_execution,
    )

    cfg = load_config(args.config, args.overrides)
    if cfg.get("run_control.mode", "iceberg") != "iceberg":
        raise RecoveryBlocked("Recovery requires Iceberg run control.")
    _validate_control_table(cfg, args.control_table)
    pin_process_timezone(cfg.get("spark.session_timezone", "America/Toronto"))
    spark = build_spark_session(cfg, "gold_publish_rollback")
    attempt = uuid.uuid4().hex
    report = {
        "run_id": args.run_id,
        "operation": "rollback_publish",
        "failed_stage": args.failed_stage,
        "started_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    backend = None
    try:
        backend = GoldBackend(spark, cfg, args.staging_root, args.run_id)
        backend.assert_identity()
        import boto3
        emr = boto3.client("emr", region_name=args.region)
        report["execution_proof"] = verify_recovery_execution(
            emr,
            failed_cluster_id=args.failed_cluster_id,
            recovery_cluster_id=args.recovery_cluster_id,
            run_id=args.run_id,
            failed_stage="publish",
            failure_kind="spark_step",
        )
        if not report["execution_proof"]["replacement_compute"]:
            report["yarn_proof"] = assert_no_active_gold_yarn_application(
                spark, cfg)

        with recovery_lease(backend.store, args.run_id):
            backend.assert_unfinished()
            plan = backend.inspect()
            report["plan_sha256"] = plan["plan_sha256"]
            report["rollback"] = rollback_publish(backend, backend.state, plan)
            if report["rollback"].get("status") != "tables_restored":
                raise RecoveryBlocked("Rollback did not reach tables_restored.")
            report["status"] = "succeeded"
            report["finished_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
            # A durable success summary is part of the hand-off contract to
            # the separate control finalizer.
            backend.store.put("recovery/rollback_summary", report)
        return report
    except Exception as exc:
        report.update(
            status="failed",
            finished_at_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
            error={"type": type(exc).__name__, "message": str(exc)},
        )
        if backend is not None:
            try:
                backend.store.put(f"recovery/attempts/rollback-{attempt}", report)
            except Exception as report_exc:
                print(
                    f"Could not persist rollback failure report; retain driver logs: {report_exc}",
                    file=sys.stderr,
                )
        raise
    finally:
        print(json.dumps(report, indent=2, default=str))
        spark.stop()


def main(argv=None):
    try:
        run(argv)
    except Exception as exc:
        print(f"GOLD_PUBLISH_ROLLBACK_STOPPED: {exc}", file=sys.stderr)
        return 2
    return 0
