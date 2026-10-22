"""Finding the parquet, reading it safely, and assembling the two populations.

Everything downstream of here sees pandas frames with a known schema. This
module is where the object store's messiness is absorbed: two different monthly
layouts, feed-partitioned fraud prefixes whose partition values are not
enumerable from config, feature columns that exist in one month and not the
next, and a fraud feed whose timestamps are strings of inconsistent quality.

Three things happen in a fixed order, and the order is the point.

*Discovery* resolves each configured time window to exactly one ``/data/``
prefix by probing candidates (:mod:`trust_score_05.ml.paths` builds them) and
records the windows that resolved to nothing. The notebook logged those as
warnings, which meant a month silently missing from the training set looked
identical in the output to a month that was never configured.

*Reading* goes one prefix at a time. Handing Spark several
``…/time_window=…/data`` paths in one ``spark.read.parquet`` call goes wrong in
two different ways, and which one you get depends on the layouts the months
happened to resolve to. With a ``basePath`` spanning them — or reading their
common ancestor recursively — Spark's partition inference sees two directory
depths under one root and fails the read outright: "Conflicting directory
structures detected", whose own remedy is "please load them separately and then
union them". Without a ``basePath`` it does not fail at all; each path becomes
its own root, nothing beneath it is a partition directory, and the read succeeds
having *silently dropped every partition column*, so a training frame comes back
with no ``time_window`` and no record of which month a row belongs to. Reading
each prefix on its own, with its own inferred root, and combining with
``unionByName(allowMissingColumns=True)`` avoids both, and the provenance column
:data:`SOURCE_PATH_COLUMN` keeps the origin of every row recoverable after the
union has flattened the plan.

*Assembly* builds the training matrix and the per-scenario evaluation frames.
The training matrix is non-fraud only, and it has fraud customers removed from
it by an anti-join before sampling: without that, a customer who later turned
out to be fraudulent is in the population the model learns "normal" from, and
the model is being taught that fraud is normal. The notebook performed this join
but downgraded a missing fraud feed to a warning and continued, so a listing
failure produced a contaminated training set and a run that looked successful.
Here the absence of fraud data raises.

The evaluation fraud population is *fixed* across scenarios: every evaluation
window is scored against the same fraud rows rather than only the fraud that
occurred in that window. That is deliberate and was the fraud team's call —
there are too few labelled cases per month for a per-window fraud set to give a
stable metric — but it means a scenario's metrics answer "can this model
separate this month's normal traffic from known fraud", not "would it have
caught this month's fraud". :func:`load_scenario_frame` writes the scenario name
onto every row so a reader can never confuse the two.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import functools
import logging
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from trust_score_05.common.io.s3 import join_uri, list_common_prefixes, s3_exists, write_pandas
from trust_score_05.common.splitting import (
    CASE_KEY_COLUMN,
    CUSTOMER_KEY_COLUMN,
    LABEL_COLUMN,
)
from trust_score_05.ml.config import MLConfig
from trust_score_05.ml.paths import (
    fraud_source_root,
    testing_nonfraud_candidates,
    training_nonfraud_candidates,
)
from trust_score_05.ml.taxonomy import FRAUD_SOURCES, resolve_fraud_source

__all__ = [
    "CASE_KEY_COLUMN",
    "CUSTOMER_KEY_COLUMN",
    "FRAUD_META_COLUMNS",
    "FRAUD_TIMESTAMP_COLUMN",
    "LABEL_COLUMN",
    "SOURCE_PATH_COLUMN",
    "DatasetError",
    "DiscoveredPaths",
    "add_missing_and_select",
    "bounded_to_pandas",
    "discover_fraud_paths",
    "discover_testing_nonfraud_paths",
    "discover_training_nonfraud_paths",
    "resolve_configured_windows",
    "fraud_source_from_path",
    "load_fraud_customer_ids",
    "load_fraud_frame",
    "load_fraud_frame_as_pandas",
    "load_scenario_frame",
    "load_training_matrix",
    "read_parquet_path",
    "read_parquet_paths",
]

LOGGER = logging.getLogger(__name__)

FRAUD_TIMESTAMP_COLUMN = "fraud_timestamp"
SOURCE_PATH_COLUMN = "__source_parquet_path"

FRAUD_META_COLUMNS: Tuple[str, ...] = (
    CUSTOMER_KEY_COLUMN,
    "reference_id",
    "industry",
    "partner",
    "fraud_type",
    FRAUD_TIMESTAMP_COLUMN,
    "fraud_source",
    "source",
    "source_name",
    "fraud_event_key",
    CASE_KEY_COLUMN,
)


class DatasetError(RuntimeError):
    """A dataset could not be discovered, read, or assembled as configured."""


# --------------------------------------------------------------------------
# discovery
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class DiscoveredPaths:
    found: Dict[str, str] = field(default_factory=dict)
    missing: Tuple[str, ...] = ()
    probed: Dict[str, Tuple[str, ...]] = field(default_factory=dict)

    def paths(self) -> List[str]:
        return [self.found[window] for window in self.found]

    def require(self, what: str) -> "DiscoveredPaths":
        if not self.found:
            raise DatasetError(
                f"no {what} prefixes exist for any configured window; probed "
                f"{sum(len(v) for v in self.probed.values())} candidates across "
                f"{len(self.probed)} windows"
            )
        return self


def _discover(windows: Sequence[str], candidates_for) -> DiscoveredPaths:
    found: Dict[str, str] = {}
    missing: List[str] = []
    probed: Dict[str, Tuple[str, ...]] = {}
    for window in windows:
        candidates = tuple(candidates_for(window))
        probed[window] = candidates
        for candidate in candidates:
            if s3_exists(candidate):
                found[window] = candidate
                break
        else:
            missing.append(window)
            LOGGER.warning("no data prefix for window %s; probed %s", window, list(candidates))
    return DiscoveredPaths(found=found, missing=tuple(missing), probed=probed)


def _available_time_windows(root: str, candidates_for, label: str) -> Tuple[str, ...]:
    """Find readable ``time_window=`` partitions below a configured S3 root."""
    if not root:
        raise DatasetError(f"{label}_root is not configured; cannot discover time windows")

    windows = set()
    for partition_root in (root, join_uri(root, "non-fraud")):
        for prefix in list_common_prefixes(partition_root):
            segment = prefix.rstrip("/").rsplit("/", 1)[-1]
            if not segment.startswith("time_window="):
                continue
            window = segment[len("time_window="):]
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", window):
                windows.add(window)

    readable = tuple(
        window
        for window in sorted(windows)
        if any(s3_exists(candidate) for candidate in candidates_for(window))
    )
    if not readable:
        raise DatasetError(
            f"no readable time_window= partitions discovered under {root} or "
            f"{join_uri(root, 'non-fraud')} for {label}"
        )
    return readable


def resolve_configured_windows(cfg: MLConfig) -> MLConfig:
    """Discover date windows only when the corresponding config list is empty.

    Evaluation windows take precedence when a date exists in both roots, so an
    automatically discovered training set cannot include its own test dates.
    """
    training_is_discovered = not cfg.training_months
    testing_is_discovered = not cfg.testing_windows
    training_months = cfg.training_months or _available_time_windows(
        cfg.training_root,
        lambda window: training_nonfraud_candidates(cfg, window),
        "training",
    )
    testing_windows = cfg.testing_windows or _available_time_windows(
        cfg.testing_root,
        lambda window: testing_nonfraud_candidates(cfg, window),
        "testing",
    )

    overlap = set(training_months) & set(testing_windows)
    if overlap and training_is_discovered:
        LOGGER.warning(
            "excluding auto-discovered training windows also present in testing: %s",
            sorted(overlap),
        )
        training_months = tuple(window for window in training_months if window not in overlap)
    elif overlap and testing_is_discovered:
        LOGGER.warning(
            "excluding auto-discovered testing windows also configured for training: %s",
            sorted(overlap),
        )
        testing_windows = tuple(window for window in testing_windows if window not in overlap)

    if not training_months:
        raise DatasetError(
            "no training windows remain after excluding windows reserved for testing; "
            "check that the S3 training and testing roots contain distinct time_window partitions"
        )
    if not testing_windows:
        raise DatasetError(
            "no testing windows remain after excluding windows reserved for training; "
            "check that the S3 training and testing roots contain distinct time_window partitions"
        )

    return cfg.with_overrides(
        training_months=training_months,
        testing_windows=testing_windows,
    )


def discover_training_nonfraud_paths(cfg: MLConfig) -> DiscoveredPaths:
    return _discover(
        cfg.training_months,
        lambda window: training_nonfraud_candidates(cfg, window),
    )


def discover_testing_nonfraud_paths(cfg: MLConfig) -> DiscoveredPaths:
    return _discover(
        cfg.testing_windows,
        lambda window: testing_nonfraud_candidates(cfg, window),
    )


def discover_fraud_paths(cfg: MLConfig) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for source in cfg.fraud_sources:
        root = fraud_source_root(cfg, source)
        data_prefixes = []
        for partition in list_common_prefixes(root):
            candidate = join_uri(partition, "data") + "/"
            if s3_exists(candidate):
                data_prefixes.append(candidate)
            else:
                LOGGER.info("fraud partition has no data/ prefix, skipping: %s", partition)
        if not data_prefixes:
            LOGGER.warning("fraud feed %s has no partitions with data: %s", source, root)
        out[source] = data_prefixes
    return out


_FRAUD_SEGMENT_PATTERN = re.compile(r"/fraud/([^/]+)/")


def fraud_source_from_path(path: str) -> Optional[str]:
    match = _FRAUD_SEGMENT_PATTERN.search(str(path))
    if match is None:
        return None
    return resolve_fraud_source(match.group(1))


# --------------------------------------------------------------------------
# reading
# --------------------------------------------------------------------------

_PARTITION_MARKERS = ("/time_window=", "/dt=")


def _partition_base_path(path: str) -> Optional[str]:
    text = str(path)
    positions = [text.index(marker) for marker in _PARTITION_MARKERS if marker in text]
    if not positions:
        return None
    return text[: min(positions)].rstrip("/")


def read_parquet_path(spark: SparkSession, path: str) -> DataFrame:
    base_path = _partition_base_path(path)
    if not base_path:
        return spark.read.parquet(path)
    try:
        return spark.read.option("basePath", base_path).parquet(path)
    except Exception as first_error:
        LOGGER.warning(
            "parquet read failed with basePath=%s, retrying without it: path=%s error=%s",
            base_path,
            path,
            first_error,
        )
        return spark.read.parquet(path)


def read_parquet_paths(
    spark: SparkSession,
    paths: Sequence[str],
    source_column: str = SOURCE_PATH_COLUMN,
) -> DataFrame:
    unique = list(dict.fromkeys([p for p in paths if p]))
    if not unique:
        raise DatasetError("no parquet paths supplied")
    frames = [
        read_parquet_path(spark, path).withColumn(source_column, F.lit(path)) for path in unique
    ]
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame, allowMissingColumns=True)
    return out


def add_missing_and_select(
    frame: DataFrame,
    features: Sequence[str],
    meta_columns: Sequence[str] = (),
) -> DataFrame:
    out = frame
    existing = set(out.columns)
    projection = []
    for column in meta_columns:
        if column in existing:
            projection.append(F.col(column).cast("string").alias(column))
        else:
            projection.append(F.lit(None).cast("string").alias(column))
    for column in features:
        if column in existing:
            projection.append(F.col(column).cast("double").alias(column))
        else:
            projection.append(F.lit(None).cast("double").alias(column))
    return out.select(*projection)


def bounded_to_pandas(
    frame: DataFrame,
    max_rows: int = 0,
    seed: int = 42,
    context: str = "data",
) -> pd.DataFrame:
    total = frame.count()
    if not max_rows or total <= max_rows:
        LOGGER.info("%s: collecting %d rows", context, total)
        return frame.toPandas()

    fraction = min(1.0, (max_rows / total) * 1.10 + (10.0 / max(total, 1)))
    LOGGER.info(
        "%s: %d rows, sampling fraction %.6f then taking %d",
        context,
        total,
        fraction,
        max_rows,
    )
    sampled = frame.sample(withReplacement=False, fraction=fraction, seed=seed)
    return sampled.orderBy(F.rand(seed)).limit(int(max_rows)).toPandas()


# --------------------------------------------------------------------------
# the fraud population
# --------------------------------------------------------------------------

def _source_from_path_expression():
    segment = F.lower(
        F.regexp_extract(F.col(SOURCE_PATH_COLUMN), _FRAUD_SEGMENT_PATTERN.pattern, 1)
    )
    expression = None
    for source in FRAUD_SOURCES:
        condition = segment.contains(source)
        for other in FRAUD_SOURCES:
            if other != source:
                condition = condition & ~segment.contains(other)
        if expression is None:
            expression = F.when(condition, F.lit(source))
        else:
            expression = expression.when(condition, F.lit(source))
    return expression


def load_fraud_customer_ids(spark: SparkSession, cfg: MLConfig) -> DataFrame:
    paths = [p for prefixes in discover_fraud_paths(cfg).values() for p in prefixes]
    if not paths:
        raise DatasetError(
            "no fraud data prefixes were discovered, so fraud customers cannot be "
            "removed from the training population; refusing to train on a "
            "possibly contaminated non-fraud sample"
        )
    frame = read_parquet_paths(spark, paths)
    if CUSTOMER_KEY_COLUMN not in frame.columns:
        raise DatasetError(
            f"fraud data has no {CUSTOMER_KEY_COLUMN} column; cannot enforce "
            "no-leakage training"
        )
    return (
        frame.select(F.col(CUSTOMER_KEY_COLUMN).cast("string").alias(CUSTOMER_KEY_COLUMN))
        .where(F.col(CUSTOMER_KEY_COLUMN).isNotNull())
        .distinct()
    )


def load_fraud_frame(
    spark: SparkSession,
    cfg: MLConfig,
    features: Sequence[str],
    run_prefix: str,
) -> Tuple[DataFrame, Dict[str, Any]]:
    paths = [p for prefixes in discover_fraud_paths(cfg).values() for p in prefixes]
    if not paths:
        raise DatasetError("no fraud data prefixes were discovered for evaluation")

    frame = read_parquet_paths(spark, paths)
    if FRAUD_TIMESTAMP_COLUMN not in frame.columns:
        raise DatasetError(
            f"fraud data has no {FRAUD_TIMESTAMP_COLUMN} column; the evaluation "
            "population cannot be time-bounded without it"
        )
    if CUSTOMER_KEY_COLUMN not in frame.columns:
        raise DatasetError(f"fraud data has no {CUSTOMER_KEY_COLUMN} column")

    from_path = _source_from_path_expression()
    if "fraud_source" in frame.columns:
        source_column = F.coalesce(F.col("fraud_source").cast("string"), from_path)
    else:
        source_column = from_path
    frame = frame.withColumn("fraud_source", source_column)

    parsed = frame.withColumn(
        "_fraud_timestamp_parsed", F.to_timestamp(F.col(FRAUD_TIMESTAMP_COLUMN))
    )
    manifest: Dict[str, Any] = {"rows_in": parsed.count()}

    unparseable = parsed.where(
        F.col(FRAUD_TIMESTAMP_COLUMN).isNotNull() & F.col("_fraud_timestamp_parsed").isNull()
    )
    bad_count = unparseable.count()
    manifest["rows_unparseable_timestamp"] = bad_count
    if bad_count:
        report_uri = join_uri(run_prefix, "reports", "quarantined_bad_fraud_timestamps.parquet")
        unparseable.drop("_fraud_timestamp_parsed").write.mode("overwrite").parquet(report_uri)
        message = (
            f"{bad_count:,} fraud rows have an unparseable {FRAUD_TIMESTAMP_COLUMN}; "
            f"quarantined to {report_uri}"
        )
        if not cfg.quarantine_bad_fraud_timestamps:
            raise DatasetError(message)
        LOGGER.warning(message)
        manifest["quarantine_uri"] = report_uri

    manifest["rows_null_timestamp"] = parsed.where(
        F.col(FRAUD_TIMESTAMP_COLUMN).isNull()
    ).count()

    kept = parsed.where(
        F.col("_fraud_timestamp_parsed") >= F.to_timestamp(F.lit(cfg.fraud_min_timestamp))
    )
    manifest["fraud_min_timestamp"] = cfg.fraud_min_timestamp
    manifest["rows_kept"] = kept.count()
    manifest["rows_before_min_timestamp"] = (
        manifest["rows_in"]
        - manifest["rows_kept"]
        - manifest["rows_unparseable_timestamp"]
        - manifest["rows_null_timestamp"]
    )
    if not manifest["rows_kept"]:
        raise DatasetError(
            f"no fraud rows have a {FRAUD_TIMESTAMP_COLUMN} at or after "
            f"{cfg.fraud_min_timestamp}; there is nothing to evaluate against"
        )

    meta = list(FRAUD_META_COLUMNS) + [SOURCE_PATH_COLUMN]
    out = add_missing_and_select(kept, features, meta_columns=meta).withColumn(
        LABEL_COLUMN, F.lit(1).cast("int")
    )
    return out, manifest


def load_fraud_frame_as_pandas(
    spark: SparkSession,
    cfg: MLConfig,
    features: Sequence[str],
    run_prefix: str,
) -> pd.DataFrame:
    """Collect the fraud evaluation population to Pandas once for scenario caching."""
    fraud_spark, _ = load_fraud_frame(spark, cfg, features, run_prefix)
    return bounded_to_pandas(fraud_spark, max_rows=0, seed=cfg.seed, context="evaluation fraud population")


# --------------------------------------------------------------------------
# the training population
# --------------------------------------------------------------------------

def load_training_matrix(
    spark: SparkSession,
    cfg: MLConfig,
    features: Sequence[str],
    run_prefix: str,
) -> Tuple[pd.DataFrame, List[str], pd.DataFrame]:
    discovered = discover_training_nonfraud_paths(cfg).require("training non-fraud")
    rows_per_month = cfg.train_rows_per_month
    fraud_ids = load_fraud_customer_ids(spark, cfg)

    frames: List[pd.DataFrame] = []
    manifest_rows: List[Dict[str, Any]] = []
    for window, path in discovered.found.items():
        frame = read_parquet_path(spark, path)
        if CUSTOMER_KEY_COLUMN not in frame.columns:
            raise DatasetError(f"training prefix has no {CUSTOMER_KEY_COLUMN}: {path}")

        before = frame.count()
        deduped = frame.withColumn(
            CUSTOMER_KEY_COLUMN, F.col(CUSTOMER_KEY_COLUMN).cast("string")
        ).join(fraud_ids, on=CUSTOMER_KEY_COLUMN, how="left_anti")
        after = deduped.count()

        missing = [name for name in features if name not in frame.columns]
        if missing and cfg.strict_missing_feature_check:
            raise DatasetError(
                f"training prefix {path} is missing {len(missing)} requested "
                f"features, first 20: {missing[:20]}"
            )
        if missing:
            LOGGER.warning(
                "training prefix %s is missing %d requested features; they will be "
                "all-null for this month",
                path,
                len(missing),
            )

        projected = add_missing_and_select(
            deduped, features, meta_columns=[CUSTOMER_KEY_COLUMN]
        )
        month = bounded_to_pandas(
            projected,
            max_rows=rows_per_month,
            seed=cfg.seed,
            context=f"training sample {window}",
        )
        month["time_window"] = window
        frames.append(month)
        manifest_rows.append(
            {
                "time_window": window,
                "path": path,
                "rows_before_fraud_removal": before,
                "rows_after_fraud_removal": after,
                "fraud_customers_removed": before - after,
                "rows_sampled": len(month),
                "rows_requested": rows_per_month,
                "missing_feature_count": len(missing),
            }
        )

    for window in discovered.missing:
        manifest_rows.append(
            {
                "time_window": window,
                "path": "",
                "rows_before_fraud_removal": 0,
                "rows_after_fraud_removal": 0,
                "fraud_customers_removed": 0,
                "rows_sampled": 0,
                "rows_requested": rows_per_month,
                "missing_feature_count": len(features),
            }
        )

    manifest = pd.DataFrame(manifest_rows)
    write_pandas(manifest, join_uri(run_prefix, "reports", "training_manifest.csv"))

    training = pd.concat(frames, ignore_index=True)
    present = [
        name
        for name in features
        if name in training.columns and training[name].notna().any()
    ]
    if not present:
        raise DatasetError(
            f"none of the {len(features)} requested features has a non-null value "
            "anywhere in the training sample"
        )
    return training, present, manifest


# --------------------------------------------------------------------------
# the evaluation populations
# --------------------------------------------------------------------------

@functools.lru_cache(maxsize=32)
def _read_cached_nonfraud_pandas(
    nonfraud_path: str,
    max_rows: int,
    seed: int,
    scenario: str,
) -> pd.DataFrame:
    """Read and downsample scenario non-fraud frame once per path."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    meta = list(FRAUD_META_COLUMNS) + [SOURCE_PATH_COLUMN]
    nonfraud = read_parquet_path(spark, nonfraud_path).withColumn(
        SOURCE_PATH_COLUMN, F.lit(nonfraud_path)
    )
    # We do missing selection on the fly, but downsample non-fraud once
    return bounded_to_pandas(
        nonfraud,
        max_rows=max_rows,
        seed=seed,
        context=f"scenario {scenario} non-fraud",
    )


def load_scenario_frame(
    spark: SparkSession,
    cfg: MLConfig,
    scenario: str,
    nonfraud_path: str,
    fraud_frame: Any,
    features: Sequence[str],
) -> pd.DataFrame:
    meta = list(FRAUD_META_COLUMNS) + [SOURCE_PATH_COLUMN]
    
    # Check if we already have nonfraud frame or read via Spark
    nonfraud = read_parquet_path(spark, nonfraud_path).withColumn(
        SOURCE_PATH_COLUMN, F.lit(nonfraud_path)
    )
    nonfraud = add_missing_and_select(nonfraud, features, meta_columns=meta).withColumn(
        LABEL_COLUMN, F.lit(0).cast("int")
    )

    negatives = bounded_to_pandas(
        nonfraud,
        max_rows=cfg.max_eval_rows_per_scenario,
        seed=cfg.seed,
        context=f"scenario {scenario} non-fraud",
    )
    
    if isinstance(fraud_frame, pd.DataFrame):
        positives = fraud_frame.copy()
    else:
        positives = bounded_to_pandas(
            fraud_frame, max_rows=0, seed=cfg.seed, context=f"scenario {scenario} fraud"
        )
        
    frame = pd.concat([negatives, positives], ignore_index=True)
    
    # Avoid pandas frame fragmentation warning at line 845
    return frame.assign(scenario=scenario)