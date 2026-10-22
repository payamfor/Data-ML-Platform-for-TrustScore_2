"""Run control: where each run starts, and a record of what it did.

Every run, first or later, starts where Gold's latest successful run ended
(``watermark_to`` of the Gold run control table for the configured Gold job).
This job's own run control table is NOT read to decide that: it is an audit log.
A run inserts one ``running`` row when it starts and updates that same row (by
``run_id``) to ``succeeded`` / ``failed`` when it ends. A retry of a failed
attempt is a new row whose ``model_type`` is ``retry``.

Gold's own run control table is only READ. It is never written.

The decisions live here as plain Python over a small ``store`` object, so they
are tested without Spark; ``SparkStore`` is the real store (Iceberg tables).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ts05_features.params import ConfigError

STATUS_RUNNING, STATUS_SUCCEEDED, STATUS_FAILED = "running", "succeeded", "failed"

COLUMNS = (
    "job_name", "run_id", "watermark_from", "watermark_to", "slice_size",
    "status", "started_ts", "finished_ts", "model_type", "limit_applied",
)


MODEL_TYPE_RETRY = "retry"


class NoGoldWatermarkError(ConfigError):
    """Gold has no successful run control entry. A retry cannot fix this."""


def retry_marker(mode: str) -> str:
    """The run_id fragment that ties a ``retry`` row to the mode it retried."""
    return f"-retry-{mode}-"


def new_run_id(now: datetime, mode: str = "", retry: bool = False) -> str:
    prefix = f"ml-feature-engineering{retry_marker(mode)[:-1]}" if retry else "ml-feature-engineering"
    return f"{prefix}-{now:%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:8]}"


def plan_range(
    gold_latest: Optional[Tuple[Optional[datetime], Optional[datetime]]],
    now: datetime,
    mode: str = "scoring",
) -> Dict[str, Any]:
    """The records this run covers: after ``start`` up to ``end``.

    In training mode: covers [watermark_from, watermark_to) from Gold's latest successful run.
    In scoring mode: starts where Gold's latest successful run ended (watermark_to) up to now.
    """
    if gold_latest is None or gold_latest[1] is None:
        raise NoGoldWatermarkError(
            "ERROR: no successful entry exists in the Gold run control table for the configured "
            "gold_job_name, so there is no watermark to start from. Stopping the job; the Gold "
            "pipeline must complete a successful run first."
        )
    if mode == "training":
        if gold_latest[0] is None:
            raise NoGoldWatermarkError(
                "ERROR: latest successful Gold run has no watermark_from, so there is no start boundary."
            )
        start, end = gold_latest[0], gold_latest[1]
        if start >= end:
            raise ConfigError(
                f"Invalid Gold watermark range: watermark_from ({start}) must be before watermark_to ({end})"
            )
        return {
            "start": start,
            "end": end,
            "source": "gold run control (latest successful run: watermark_from to watermark_to)",
        }
    return {"start": gold_latest[1], "end": now, "source": "gold run control (latest successful watermark_to)"}


def nothing_new(plan: Dict[str, Any]) -> bool:
    return plan["start"] is not None and plan["start"] >= plan["end"]


class RunControl:
    """One run's row in the run control table."""

    def __init__(self, store, job_name: str, mode: str, retry: bool = False):
        self.store = store
        self.job_name = job_name
        self.mode = mode
        self.retry = retry
        self.model_type = MODEL_TYPE_RETRY if retry else mode
        self.run_id: Optional[str] = None
        self._start: Optional[datetime] = None
        self._end: Optional[datetime] = None
        self._started: Optional[datetime] = None
        self._limit_applied = False

    def plan(self, now: datetime) -> Dict[str, Any]:
        return plan_range(self.store.gold_latest(), now, mode=self.mode)

    def begin(self, start: Optional[datetime], end: datetime, limit_applied: bool, now: datetime) -> str:
        """Insert the ``running`` row (an audit record; it never decides where a run starts)."""
        self.run_id = new_run_id(now, self.mode, self.retry)
        self._start, self._end, self._started, self._limit_applied = start, end, now, limit_applied
        self.store.append(self._row(STATUS_RUNNING, 0, None, end))
        return self.run_id

    def finish(self, status: str, slice_size: int, now: datetime) -> None:
        """Update this run's row to its final status."""
        self.store.update(self._row(status, slice_size, now, self._end))

    def _row(self, status, slice_size, finished, watermark_to) -> Dict[str, Any]:
        return {
            "job_name": self.job_name, "run_id": self.run_id,
            "watermark_from": self._start, "watermark_to": watermark_to,
            "slice_size": int(slice_size), "status": status,
            "started_ts": self._started, "finished_ts": finished,
            "model_type": self.model_type, "limit_applied": self._limit_applied,
        }


class MemoryStore:
    """A list standing in for the tables (tests)."""

    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None, gold: Optional[tuple] = None):
        self.rows = list(rows or [])
        self.gold = gold

    def gold_latest(self):
        return self.gold

    def append(self, row):
        self.rows.append(dict(row))

    def update(self, row):
        for index, existing in enumerate(self.rows):
            if existing["run_id"] == row["run_id"]:
                self.rows[index] = dict(row)
                return
        raise KeyError(row["run_id"])


class SparkStore:
    """The Iceberg tables. ``table`` is this job's run control table; ``gold_table``
    is Gold's, read only."""

    def __init__(self, spark, table: str, gold_table: str, gold_job_name: str):
        self.spark, self.table = spark, table
        self.gold_table, self.gold_job_name = gold_table, gold_job_name
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        if not self.table:
            return

        try:
            exists = self.spark.catalog.tableExists(self.table)
        except Exception as exc:
            raise ConfigError(
                f"Unable to access Glue catalog table {self.table}. "
                "This usually means the EMR execution role / instance profile has no AWS credentials "
                "or lacks Glue/Lake Formation permissions. Check the EMR role, attached instance profile, "
                f"and Glue database permissions. Original error: {exc}"
            ) from exc

        if exists:
            return

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                job_name        string,
                run_id          string,
                watermark_from  timestamp,
                watermark_to    timestamp,
                slice_size      bigint,
                status          string,
                started_ts      timestamp,
                finished_ts     timestamp,
                model_type      string,
                limit_applied   boolean
            ) USING iceberg
        """
        try:
            self.spark.sql(create_sql)
        except Exception as exc:
            raise ConfigError(
                f"Unable to create Glue run control table {self.table}. Ensure the EMR job role/instance "
                f"profile can access Glue and S3, and that AWS credentials are available in the runtime. "
                f"Original error: {exc}"
            ) from exc

    def gold_latest(self):
        from pyspark.sql import functions as F

        try:
            row = (self.spark.table(self.gold_table)
                   .where((F.col("job_name") == self.gold_job_name) & (F.col("status") == STATUS_SUCCEEDED))
                   .orderBy(F.col("watermark_to").desc()).limit(1).first())
        except Exception as exc:
            raise ConfigError(
                f"Unable to read Gold run-control table {self.gold_table}. Verify that the Glue table exists "
                "and that the EMR runtime role has Glue GetTable, Lake Formation DESCRIBE/SELECT, and "
                f"read access to its S3 location. Original error: {exc}"
            ) from exc
        return (row["watermark_from"], row["watermark_to"]) if row else None

    def _row_frame(self, row):
        from pyspark.sql import types as T

        S, TS = T.StringType(), T.TimestampType()
        schema = T.StructType([
            T.StructField("job_name", S), T.StructField("run_id", S),
            T.StructField("watermark_from", TS), T.StructField("watermark_to", TS),
            T.StructField("slice_size", T.LongType()), T.StructField("status", S),
            T.StructField("started_ts", TS), T.StructField("finished_ts", TS),
            T.StructField("model_type", S), T.StructField("limit_applied", T.BooleanType()),
        ])
        return self.spark.createDataFrame([tuple(row[c] for c in COLUMNS)], schema)

    def append(self, row):
        self._row_frame(row).writeTo(self.table).append()

    def update(self, row):
        # MERGE (Iceberg SQL extensions) so the timestamps go through the same
        # conversion as append and the run's single row is rewritten in place.
        view = "_feature_engineering_run_control_update"
        self._row_frame(row).createOrReplaceTempView(view)
        self.spark.sql(
            f"MERGE INTO {self.table} t USING {view} u ON t.run_id = u.run_id "
            "WHEN MATCHED THEN UPDATE SET *"
        )
