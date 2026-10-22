"""Append ``failed`` only after a failed Gold run is safe to close.

The command never rolls back a Gold table. For a publish failure it requires a
successful rollback journal and re-verifies all five table heads. For an
earlier-stage failure it verifies that none of the five heads moved.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys

from .plan import TABLES, RecoveryBlocked, digest, verify_all_pinned


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", action="append", required=True)
    p.add_argument("--set", dest="overrides", action="append")
    p.add_argument("--staging-root", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--control-table", "--table", dest="control_table", required=True)
    p.add_argument("--failed-cluster-id", required=True)
    p.add_argument("--recovery-cluster-id", required=True)
    p.add_argument(
        "--failure-kind",
        choices=("spark_step", "argument_build"),
        default="spark_step",
        help="Whether a submitted Spark step failed or argument construction failed before submission.",
    )
    p.add_argument(
        "--failed-stage",
        choices=("prepare", "accounts", "customers", "validate", "publish"),
        required=True,
    )
    p.add_argument("--region", default="ca-central-1")
    return p


def _validate_control_table(cfg, supplied):
    configured = str(cfg.require("run_control.table"))
    if configured != supplied:
        raise RecoveryBlocked(
            "The requested control table differs from the table in the failed "
            f"run configuration ({supplied!r} != {configured!r})."
        )


def _require_verified_publish_rollback(backend):
    journal = backend.get_rollback()
    if not journal or journal.get("status") != "tables_restored":
        raise RecoveryBlocked(
            "Publish failure cannot be finalized until rollback completed and "
            "recorded status=tables_restored."
        )
    if journal.get("run_id") != backend.run_id:
        raise RecoveryBlocked("Rollback journal belongs to another run.")
    if journal.get("state_sha256") != digest(backend.state):
        raise RecoveryBlocked("Rollback journal does not match the frozen run state.")
    entries = journal.get("tables", {})
    if set(entries) != set(TABLES):
        raise RecoveryBlocked("Rollback journal does not cover all five Gold tables.")
    pins = backend.state["pins"]["sinks"]
    for name in TABLES:
        if entries[name].get("status") != "complete":
            raise RecoveryBlocked(f"Rollback is incomplete for {name}.")
        if entries[name].get("target") != pins[name]:
            raise RecoveryBlocked(f"Rollback target for {name} differs from its run pin.")


def _require_argument_build_failure_state(backend, failed_stage):
    """Require a completed predecessor and no target-stage/publish mutation."""
    stages = ("prepare", "accounts", "customers", "validate", "publish")
    index = stages.index(failed_stage)
    if index == 0:
        raise RecoveryBlocked(
            "A prepare argument failure has no Gold reservation to finalize."
        )
    previous = stages[index - 1]
    if backend.store.get(f"completed/{previous}") is None:
        raise RecoveryBlocked(
            f"Cannot prove {previous!r} completed before {failed_stage!r} "
            "argument construction failed."
        )
    if backend.store.get(f"completed/{failed_stage}") is not None:
        raise RecoveryBlocked(
            f"Stage {failed_stage!r} already has a completion marker; this is "
            "not an argument-build-only failure."
        )
    unexpected = [
        name for name in TABLES
        if backend.store.get(f"publish/{name}") is not None
    ]
    if unexpected:
        raise RecoveryBlocked(
            "Argument-build failure has publish journals; refusing to close "
            f"the reservation: {unexpected}"
        )


def _control_rows(spark, cfg, run_id):
    """Read the exact logical reservation before requiring staged run files."""
    from pyspark.sql import functions as F
    from trust_score_05.lineage.io.control import RunControl
    from .runtime import JOB_NAME

    control = RunControl(spark, cfg)
    control.ensure_exists()
    return [
        row.asDict()
        for row in control.read().where(
            (F.col("job_name") == F.lit(JOB_NAME))
            & (F.col("run_id") == F.lit(run_id))
        ).collect()
    ]


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
    spark = build_spark_session(cfg, "gold_control_finalizer")
    report = {
        "run_id": args.run_id,
        "operation": "finalize_failed_gold_run",
        "failed_stage": args.failed_stage,
        "started_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    try:
        import boto3
        emr = boto3.client("emr", region_name=args.region)
        report["execution_proof"] = verify_recovery_execution(
            emr,
            failed_cluster_id=args.failed_cluster_id,
            recovery_cluster_id=args.recovery_cluster_id,
            run_id=args.run_id,
            failed_stage=args.failed_stage,
            failure_kind=args.failure_kind,
        )
        if not report["execution_proof"]["replacement_compute"]:
            report["yarn_proof"] = assert_no_active_gold_yarn_application(
                spark, cfg)

        rows = _control_rows(spark, cfg, args.run_id)
        if not rows:
            # A Spark failure before RunControl.start() created no reservation
            # and therefore has nothing to close. This is a successful no-op,
            # not a manufactured failed row without a frozen watermark.
            report["status"] = "no_reservation"
            report["finished_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
            return report

        backend = GoldBackend(spark, cfg, args.staging_root, args.run_id)
        backend.assert_identity()

        with recovery_lease(backend.store, args.run_id):
            if args.failure_kind == "argument_build":
                _require_argument_build_failure_state(backend, args.failed_stage)
            elif args.failed_stage == "publish":
                _require_verified_publish_rollback(backend)
            else:
                unexpected = [
                    name for name in TABLES
                    if backend.store.get(f"publish/{name}") is not None
                ]
                if unexpected:
                    raise RecoveryBlocked(
                        "A pre-publish failure has publish journals; refusing to "
                        f"close the reservation: {unexpected}"
                    )

            # Re-read heads while holding the recovery lease. Any changed or
            # externally-written head blocks control closure.
            report["verified_heads"] = verify_all_pinned(backend, backend.state)
            backend.close_failed()
            report["status"] = "failed_record_appended"
            report["finished_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        return report
    except Exception as exc:
        report.update(
            status="failed",
            finished_at_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
            error={"type": type(exc).__name__, "message": str(exc)},
        )
        raise
    finally:
        print(json.dumps(report, indent=2, default=str))
        spark.stop()


def main(argv=None):
    try:
        run(argv)
    except Exception as exc:
        print(f"GOLD_CONTROL_FINALIZATION_STOPPED: {exc}", file=sys.stderr)
        return 2
    return 0
