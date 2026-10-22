"""Adapters to the ORIGINAL Gold ZIP, Spark and S3; no transformation changes."""
from __future__ import annotations

import datetime as dt
import json
import re
import uuid
from contextlib import contextmanager

from .plan import TABLES, RecoveryBlocked, build_plan, digest

JOB_NAME = "gold_account_customer_map"


def _workflow_tags(cluster):
    return {item.get("Key"): item.get("Value") for item in cluster.get("Tags", [])}


def assert_prior_cluster_stopped(client, cluster_id):
    if not re.fullmatch(r"j-[A-Za-z0-9]+", cluster_id or ""):
        raise RecoveryBlocked("Supply the terminated original --failed-cluster-id.")
    cluster = client.describe_cluster(ClusterId=cluster_id)["Cluster"]
    if cluster.get("Id") != cluster_id:
        raise RecoveryBlocked("EMR returned an unexpected cluster identity.")
    if _workflow_tags(cluster).get("lotus-workflow") != "silver-gold-transient":
        raise RecoveryBlocked("Failed cluster is not the tagged Silver/Gold transient cluster.")
    if cluster["Status"]["State"] not in ("TERMINATED", "TERMINATED_WITH_ERRORS"):
        raise RecoveryBlocked("Original EMR cluster is not terminated; do not recover while its application may still write.")
    return {"cluster_id": cluster_id, "state": cluster["Status"]["State"],
            "workflow": "silver-gold-transient"}


def assert_current_workflow_cluster(client, cluster_id):
    """Verify the cluster selected by the state machine is still usable.

    This is intentionally separate from ``assert_prior_cluster_stopped``:
    automatic recovery is submitted as the next step on the *same* transient
    cluster, after ``addStep.sync`` has confirmed that the failed application
    is terminal.  The workflow tag prevents a caller from pointing recovery at
    an unrelated EMR cluster.
    """
    if not re.fullmatch(r"j-[A-Za-z0-9]+", cluster_id or ""):
        raise RecoveryBlocked("Supply the active --recovery-cluster-id.")
    cluster = client.describe_cluster(ClusterId=cluster_id)["Cluster"]
    if cluster.get("Id") != cluster_id:
        raise RecoveryBlocked("EMR returned an unexpected cluster identity.")
    state = cluster["Status"]["State"]
    if state not in ("RUNNING", "WAITING"):
        raise RecoveryBlocked(f"Recovery cluster is not active (state={state}).")
    tags = _workflow_tags(cluster)
    if tags.get("lotus-workflow") != "silver-gold-transient":
        raise RecoveryBlocked("Recovery is allowed only on the tagged Silver/Gold transient cluster.")
    return {"cluster_id": cluster_id, "state": state,
            "workflow": tags["lotus-workflow"]}


def assert_failed_gold_step(client, cluster_id, run_id, failed_stage):
    """Prove the named Gold application is terminal before automation mutates.

    Step Functions can retry ``addStep.sync``, so more than one exact-name step
    can exist.  At least one must be terminal-failed and none may still be
    pending/running.  Pagination is followed because a long-lived test cluster
    may contain many Silver/Gold steps.
    """
    expected = f"gold-{failed_stage}-{run_id}"
    steps, marker = [], None
    while True:
        request = {"ClusterId": cluster_id}
        if marker:
            request["Marker"] = marker
        response = client.list_steps(**request)
        steps.extend(s for s in response.get("Steps", [])
                     if s.get("Name") == expected)
        marker = response.get("Marker")
        if not marker:
            break
    if not steps:
        raise RecoveryBlocked(f"No EMR step named {expected!r} exists on the recovery cluster.")
    states = [s.get("Status", {}).get("State") for s in steps]
    active = {"PENDING", "CANCEL_PENDING", "RUNNING"}
    failed = {"FAILED", "CANCELLED", "INTERRUPTED"}
    if any(state in active for state in states):
        raise RecoveryBlocked(f"The failed Gold stage still has an active EMR attempt: {states}.")
    if not any(state in failed for state in states):
        raise RecoveryBlocked(f"No terminal failed attempt was found for {expected!r}: {states}.")
    return {"name": expected, "attempts": len(steps), "states": states}


def assert_gold_step_not_submitted(client, cluster_id, run_id, failed_stage):
    """Prove argument construction failed before the named EMR step existed."""
    expected = f"gold-{failed_stage}-{run_id}"
    matches, marker = [], None
    while True:
        request = {"ClusterId": cluster_id}
        if marker:
            request["Marker"] = marker
        response = client.list_steps(**request)
        matches.extend(
            step for step in response.get("Steps", [])
            if step.get("Name") == expected
        )
        marker = response.get("Marker")
        if not marker:
            break
    if matches:
        states = [step.get("Status", {}).get("State") for step in matches]
        raise RecoveryBlocked(
            f"EMR step {expected!r} exists ({states}); this is not an "
            "argument-build-only failure."
        )
    return {"name": expected, "attempts": 0, "states": [], "not_submitted": True}


def verify_recovery_execution(client, *, failed_cluster_id, recovery_cluster_id,
                              run_id, failed_stage, failure_kind):
    """Separate failed-work evidence from the compute running recovery.

    Automatic recovery normally supplies the same ID twice. A reviewed manual
    recovery may use replacement compute only after the original cluster is
    terminal, which proves that its Spark/YARN application can no longer write.
    """
    recovery = assert_current_workflow_cluster(client, recovery_cluster_id)
    if failed_cluster_id == recovery_cluster_id:
        failed_cluster = recovery
    else:
        failed_cluster = assert_prior_cluster_stopped(client, failed_cluster_id)

    if failure_kind == "spark_step":
        evidence = assert_failed_gold_step(
            client, failed_cluster_id, run_id, failed_stage)
    elif failure_kind == "argument_build":
        evidence = assert_gold_step_not_submitted(
            client, failed_cluster_id, run_id, failed_stage)
    else:
        raise RecoveryBlocked(f"Unsupported recovery failure kind: {failure_kind!r}.")
    return {
        "failed_cluster": failed_cluster,
        "recovery_cluster": recovery,
        "failure_evidence": evidence,
        "replacement_compute": failed_cluster_id != recovery_cluster_id,
    }


def _active_gold_yarn_applications(reports, app_prefix):
    active = {"NEW", "NEW_SAVING", "SUBMITTED", "ACCEPTED", "RUNNING"}
    normal_prefix = f"{app_prefix}.{JOB_NAME}."
    result = []
    for report in reports:
        name = str(report.getName())
        state = str(report.getYarnApplicationState())
        if name.startswith(normal_prefix) and state in active:
            result.append({
                "application_id": str(report.getApplicationId()),
                "name": name,
                "state": state,
            })
    return result


def assert_no_active_gold_yarn_application(spark, cfg):
    """Fail closed if any normal Gold YARN application is still writable."""
    client = None
    try:
        jvm = spark._jvm
        client = jvm.org.apache.hadoop.yarn.client.api.YarnClient.createYarnClient()
        client.init(spark.sparkContext._jsc.hadoopConfiguration())
        client.start()
        active = _active_gold_yarn_applications(
            client.getApplications(),
            str(cfg.get("spark.app_name_prefix", "silver_to_gold")),
        )
    except Exception as exc:
        raise RecoveryBlocked(
            "Could not independently verify active YARN applications; "
            "recovery is blocked."
        ) from exc
    finally:
        if client is not None:
            try:
                client.stop()
            except Exception:
                pass
    if active:
        raise RecoveryBlocked(
            f"Normal Gold YARN application is still active: {active}."
        )
    return {"active_gold_applications": []}


@contextmanager
def recovery_lease(store, run_id):
    """Serialize recovery tools across the staging root; no automatic expiry.

    Legacy Gold stages do not acquire this lease. It does NOT replace operator
    maintenance or fence arbitrary external writers. Never steal a stale lease.
    """
    key = store.key.rsplit("/run_id=", 1)[0] + "/_gold_recovery.lock.json"
    owner = uuid.uuid4().hex
    payload = {"owner": owner, "run_id": run_id, "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
    try:
        store.client.put_object(Bucket=store.bucket, Key=key,
                                Body=json.dumps(payload).encode(), ContentType="application/json", IfNoneMatch="*")
    except Exception as exc:
        raise RecoveryBlocked(f"Cannot acquire recovery lease s3://{store.bucket}/{key}; inspect any existing lease, do not delete it while a recovery application is active. {exc}") from exc
    try:
        yield owner
    finally:
        current = json.loads(store.client.get_object(Bucket=store.bucket, Key=key)["Body"].read())
        if current.get("owner") != owner:
            raise RecoveryBlocked("Recovery lease ownership changed; refusing to remove it.")
        store.client.delete_object(Bucket=store.bucket, Key=key)


class GoldBackend:
    def __init__(self, spark, cfg, root, run_id):
        from trust_score_05.lineage.execution import RunStore
        from trust_score_05.lineage.io.control import RunControl
        self.spark, self.cfg = spark, cfg
        self.store = RunStore(spark, root, run_id)
        self.control = RunControl(spark, cfg)
        self.run_id = run_id
        self.state = self.store.get("run")
        if not self.state or self.state.get("run_id") != run_id:
            raise RecoveryBlocked("No matching staged run manifest. This tool does not guess recovery for monolithic runs.")
        self.control.ensure_exists()
        # Never let stale/mistyped config redirect rollback to another target.
        for name in TABLES:
            actual = cfg.require(f"sinks.{name}.table")
            pin = self.state.get("pins", {}).get("sinks", {}).get(name, {})
            if pin.get("table") != actual:
                raise RecoveryBlocked(f"{name}: configured target differs from the pinned target.")
        self.identity_errors = []
        from trust_score_05.lineage.release import CODE_SHA256
        if self.state.get("code_sha256") != CODE_SHA256:
            self.identity_errors.append("Gold ZIP identity differs from the staged run; use the ORIGINAL ZIP, not a rebuilt release.")
        if self.state.get("config_sha256") != digest(cfg.as_dict()):
            self.identity_errors.append("Resolved config differs from the staged run; restore its original YAML/overrides.")
        from trust_score_05.lineage.io.merge_sink import build_sink
        for name in TABLES:
            build_sink(name, cfg, spark).validate_existing_schema()

    def assert_identity(self):
        """Reject different Gold code/config before any recovery mutation."""
        if self.identity_errors:
            raise RecoveryBlocked(" ".join(self.identity_errors))

    def rows(self):
        from pyspark.sql import functions as F
        return [r.asDict() for r in self.control.read().where(
            (F.col("job_name") == F.lit(JOB_NAME)) & (F.col("run_id") == F.lit(self.run_id))
        ).collect()]

    def head(self, name):
        from trust_score_05.lineage.execution import current_snapshot
        table = self.state["pins"]["sinks"][name]["table"]
        return current_snapshot(self.spark, self.cfg, table)

    def snapshots(self, name):
        from trust_score_05.lineage.spark import qualified_table_name
        table = qualified_table_name(self.cfg, self.state["pins"]["sinks"][name]["table"])
        return {int(r["snapshot_id"]): int(r["parent_id"]) if r["parent_id"] is not None else None
                for r in self.spark.sql(f"SELECT snapshot_id, parent_id FROM {table}.snapshots").collect()}

    def get_rollback(self):
        return self.store.get("recovery/rollback")

    def put_rollback(self, payload):
        self.store.put("recovery/rollback", payload)

    def inspect(self):
        rows = self.rows()
        statuses = {r["status"] for r in rows}
        completed = {s: self.store.get(f"completed/{s}") for s in
                     ("prepare", "accounts", "customers", "validate", "publish")}
        journals = {t: self.store.get(f"publish/{t}") for t in TABLES}
        plan = build_plan(self.state, completed, journals,
                          {t: self.head(t) for t in TABLES}, statuses, self.get_rollback())
        extra = list(self.identity_errors)
        if len([r for r in rows if r["status"] == "running"]) != 1:
            extra.append("Expected exactly one running record for this exact job and run.")
        if len([r for r in rows if r["status"] in ("failed", "succeeded")]) > 1:
            extra.append("Duplicate terminal records need investigation.")
        plan["rollback_blockers"].extend(extra)
        plan["staging_uri"] = self.store.uri
        return plan

    def assert_unfinished(self):
        self.control.assert_no_running(JOB_NAME, except_run_id=self.run_id)
        rows = self.rows()
        if any(r["status"] in ("succeeded", "failed") for r in rows):
            raise RecoveryBlocked("The control reservation is already closed; refusing a pointer mutation.")
        if len([r for r in rows if r["status"] == "running"]) != 1:
            raise RecoveryBlocked("Expected one open control reservation.")

    def rollback(self, name, snapshot_id):
        from trust_score_05.lineage.spark import qualified_table_name
        raw = self.state["pins"]["sinks"][name]["table"]
        qualified = qualified_table_name(self.cfg, raw)
        parts = qualified.split(".")  # identifiers were validated by Gold.
        catalog, table = parts[0], ".".join(parts[1:])
        if not isinstance(snapshot_id, int) or isinstance(snapshot_id, bool) or snapshot_id <= 0:
            raise RecoveryBlocked("A positive exact snapshot ID is required.")
        self.spark.sql(
            f"CALL {catalog}.system.rollback_to_snapshot(table => '{table}', snapshot_id => {snapshot_id})"
        ).collect()
        self.spark.sql(f"REFRESH TABLE {qualified}")

    def close_failed(self):
        from trust_score_05.lineage.io.control import RunRecord, STATUS_FAILED
        rows = self.rows()
        if any(r["status"] == "succeeded" for r in rows):
            raise RecoveryBlocked("A succeeded reservation cannot be marked failed.")
        if any(r["status"] == "failed" for r in rows):
            return  # Retry after a lost finish() reply.
        self.assert_unfinished()
        running = [r for r in rows if r["status"] == "running"]
        record = RunRecord(**running[0])
        # Preserve every immutable field from the original reservation. The
        # finalizer changes only status and finished_ts; in particular it does
        # not manufacture a different watermark or slice_size.
        self.control.finish(record, STATUS_FAILED)
        if {r["status"] for r in self.rows()} != {"running", "failed"}:
            raise RecoveryBlocked("Could not verify terminal failed control record.")
