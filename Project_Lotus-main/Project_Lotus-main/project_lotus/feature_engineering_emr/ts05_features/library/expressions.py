"""Small Spark helpers shared by every feature calculator.

Nothing here is specific to a feature family. The functions fall into three
groups: frame hygiene (duplicate columns, missing columns, safe joins), window
predicates (was this event inside the *n*-day lookback), and arithmetic that has
to not divide by zero.

They are private to the package — the public surface is
:mod:`ts05_features.library` — but they are named without a leading underscore
because the calculators import them by name and underscored imports read badly.
"""

from __future__ import annotations

from functools import reduce
import re
from typing import Dict, Iterable, List, Optional, Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ts05_features.library.config import (
    OPEN_INTERVAL_END,
    OPEN_INTERVAL_START,
    FeatureConfig,
)

__all__ = [
    "align_columns",
    "closed_lineage_intervals",
    "combine_conditions",
    "dedupe_columns",
    "entropy_over_dimension",
    "feature_base",
    "feature_columns",
    "feature_token",
    "filtered_events",
    "has_col",
    "lookback_condition",
    "next_event_window",
    "safe_divide",
    "safe_join",
]


def has_col(df: DataFrame, col_name: str) -> bool:
    """Whether ``df`` has a column called ``col_name``."""
    return col_name in df.columns


def dedupe_columns(df: DataFrame) -> DataFrame:
    """Drop repeated column *names*, keeping the leftmost of each.

    Spark permits two columns with the same name after a join, and then refuses
    to resolve either of them by name. Every frame that has been through a join
    on aliased sides goes through this before it is used positionally.
    """
    if len(df.columns) == len(set(df.columns)):
        return df

    temp_names = [f"__col_{index}" for index in range(len(df.columns))]
    first_temp_by_name: Dict[str, str] = {}
    for original_name, temp_name in zip(df.columns, temp_names):
        first_temp_by_name.setdefault(original_name, temp_name)

    return df.toDF(*temp_names).select(
        *(
            F.col(temp_name).alias(original_name)
            for original_name, temp_name in first_temp_by_name.items()
        )
    )


def combine_conditions(conditions: Sequence[Column]) -> Column:
    """AND a non-empty sequence of predicates together."""
    if not conditions:
        raise ValueError("At least one join or filter condition is required.")
    return reduce(lambda left, right: left & right, conditions)


def safe_divide(numerator: Column, denominator: Column) -> Column:
    """``numerator / denominator``, or null when the denominator is zero.

    Null rather than zero, and this matters downstream. A ratio whose
    denominator is zero is undefined, not small: an entity with no events in the
    thirty-day window has no seven-over-thirty ratio, and imputing zero would
    put it at the quiet end of a scale it is not on at all. The preprocessing
    stage decides what to do with the null, and it can only decide if the null
    survives to it.
    """
    return numerator / F.when(denominator != 0, denominator)


def closed_lineage_intervals(lineage_df: DataFrame, cfg: FeatureConfig) -> DataFrame:
    """Close the open ends of the lineage intervals and cast them to timestamps.

    The lineage tables write NULL for "still open" at the near end and, for a
    handful of inferred rows, at the far end too. Every feature below compares
    an event timestamp against these bounds, and a comparison against NULL is
    null rather than true, so an open interval would quietly contain no events.
    """
    lineage_df = dedupe_columns(lineage_df)
    return (
        lineage_df.withColumn(
            cfg.from_ts_col,
            F.coalesce(
                F.col(cfg.from_ts_col).cast("timestamp"),
                F.to_timestamp(F.lit(OPEN_INTERVAL_START)),
            ),
        )
        .withColumn(
            cfg.to_ts_col,
            F.coalesce(
                F.col(cfg.to_ts_col).cast("timestamp"),
                F.to_timestamp(F.lit(OPEN_INTERVAL_END)),
            ),
        )
    )


def align_columns(
    df: DataFrame,
    columns: Sequence[str],
    cfg: FeatureConfig,
) -> DataFrame:
    """Add any of ``columns`` that ``df`` lacks, typed and null.

    Used before a ``unionByName`` across event frames from different feeds: the
    carrier feeds have no partner metadata and the EnStream feed has no MNO
    change type, and the union needs one schema.
    """
    result = df
    timestamp_cols = {
        cfg.event_ts_col,
        cfg.from_ts_col,
        cfg.to_ts_col,
        cfg.reference_ts_col,
    }
    for col_name in columns:
        if has_col(result, col_name):
            continue
        if col_name in timestamp_cols:
            result = result.withColumn(col_name, F.lit(None).cast("timestamp"))
        elif col_name == cfg.event_weight_col or col_name.startswith(
            f"{cfg.event_weight_col}_"
        ):
            result = result.withColumn(col_name, F.lit(None).cast("double"))
        else:
            result = result.withColumn(col_name, F.lit(None).cast("string"))
    return result


def safe_join(
    base_df: DataFrame,
    feature_dfs: Iterable[Optional[DataFrame]],
    join_keys: Sequence[str],
) -> DataFrame:
    """Left-join feature frames onto ``base_df`` without colliding column names.

    Two properties matter and both are load-bearing.

    The join is a *left* join from the base. The base is the set of references
    being described, and a reference with no events of some family must survive
    with nulls in that family's columns rather than disappear. Every calculator
    that groups events produces rows only for entities that had events, so
    without the left join the output would be an inner join of every family and
    would contain only the entities that did everything.

    A column already present on the accumulated result is skipped rather than
    joined and suffixed. Two calculators producing the same name means the same
    feature computed twice, and the first one wins.
    """
    result = dedupe_columns(base_df)
    join_key_list = list(join_keys)

    for feature_df in feature_dfs:
        if feature_df is None:
            continue
        feature_df = dedupe_columns(feature_df)
        missing_keys = [key for key in join_key_list if not has_col(feature_df, key)]
        if missing_keys:
            raise ValueError(
                "A feature frame is missing join keys "
                f"{missing_keys}; it has {feature_df.columns}."
            )
        new_feature_cols = [
            col_name
            for col_name in feature_df.columns
            if col_name not in join_key_list and col_name not in result.columns
        ]
        if not new_feature_cols:
            continue
        result = result.join(
            feature_df.select(*join_key_list, *new_feature_cols),
            join_key_list,
            "left",
        )
    return result


def _lookback_condition_days(days: int, cfg: FeatureConfig) -> Column:
    """Whole calendar days: the event fell on one of the last ``days`` dates."""
    lag_days = F.datediff(
        F.to_date(F.col(cfg.reference_ts_col)), F.to_date(F.col(cfg.event_ts_col))
    )
    return lag_days.between(0, days - 1)


def _lookback_condition_hours(hours: int, cfg: FeatureConfig) -> Column:
    """Exact hours: the event fell within ``hours`` before the reference."""
    return (F.col(cfg.event_ts_col) <= F.col(cfg.reference_ts_col)) & (
        F.col(cfg.event_ts_col)
        > F.col(cfg.reference_ts_col) - F.expr(f"INTERVAL {hours} HOURS")
    )


def lookback_condition(days: int, cfg: FeatureConfig) -> Column:
    """Whether an event row falls inside the ``days``-day lookback window.

    Under the v4.0 default (``lookback_semantics="interval"``) every window is
    the half-open range ``(reference_ts - days*24h, reference_ts]``. A window
    labelled *N* days then covers exactly *N* x 24 hours, whatever time of day
    the reference sits at, and the short windows need no special case: one day
    is twenty-four hours and three days is seventy-two, which is precisely what
    ``lookback_hour_windows`` was carving out by hand. That field is therefore
    unread in this mode and is kept only for the legacy path.

    ``lookback_semantics="calendar"`` restores the earlier split -- exact hours
    for the windows named in ``lookback_hour_windows``, calendar dates for the
    rest. See ``_lookback_condition_calendar_days`` for the day it loses
    against a midnight reference. Use it only to reproduce pre-v4.0 output.
    """
    if cfg.lookback_semantics == "interval":
        return _lookback_condition_hours(days * 24, cfg)
    if days in tuple(cfg.lookback_hour_windows):
        return _lookback_condition_hours(days * 24, cfg)
    return _lookback_condition_calendar_days(days, cfg)


def filtered_events(
    scoped_events_df: DataFrame,
    event_family: str,
    cfg: FeatureConfig,
) -> DataFrame:
    """The rows of ``scoped_events_df`` belonging to one event family."""
    return scoped_events_df.filter(F.col(cfg.event_family_col) == event_family)


def feature_base(scoped_events_df: DataFrame, entity_cols: Sequence[str]) -> DataFrame:
    """The distinct entities present in a scoped event frame.

    This is the left side every family's features are joined onto, so that an
    entity with no events of the family still appears with nulls.
    """
    return scoped_events_df.select(*list(entity_cols)).distinct()


def feature_token(value: str) -> str:
    """Turn an event type into something that can live in a column name.

    ``STATUS_CHANGE-C`` becomes ``status_change_c``. The hyphen is the reason
    this exists: a hyphen in a column name has to be back-quoted in every SQL
    expression that touches it, and one place that forgets is a runtime error.
    """
    return re.sub(r"[^0-9a-zA-Z]+", "_", value).strip("_").lower()


def next_event_window(
    entity_cols: Sequence[str],
    target_event_type: str,
    cfg: FeatureConfig,
) -> Window:
    """A window over which "the next ``target_event_type``" can be taken.

    Rows are ordered by timestamp *descending*, so the frame
    ``[unboundedPreceding, currentRow]`` covers every event at or after the
    current row's timestamp. The minimum target timestamp in that frame is
    therefore the *next* target event.

    The tie-break is the subtle part, and the notebook library had it backwards.
    At equal timestamps, a target event must sort *before* the source event, so
    that the source's frame includes it and the transition is recorded as taking
    zero days. Ordering the target flag ascending — as the previous code did —
    puts the source first, so a target event at the same instant falls outside
    the frame. The transition is then either missed entirely or, if a later
    target exists, reported at that later gap instead. Simultaneous events are
    not an edge case here: a device change and a SIM change arriving in the same
    carrier batch share a timestamp, and that pair is one of the sharpest
    signals in the feature set.
    """
    is_target = (
        F.when(F.col(cfg.event_type_col) == target_event_type, F.lit(1))
        .otherwise(F.lit(0))
        .desc()
    )
    return (
        Window.partitionBy(*list(entity_cols))
        .orderBy(F.col(cfg.event_ts_col).desc(), is_target)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )


def entropy_over_dimension(
    filtered_df: DataFrame,
    group_cols: Sequence[str],
    dimension_col: str,
    feature_name: str,
) -> DataFrame:
    """Shannon entropy, in bits, of ``dimension_col`` within each group.

    Zero when every event landed in one bucket, rising with how evenly the
    events are spread across buckets. Buckets with zero probability contribute
    nothing, which is the usual convention and avoids ``log2(0)``.
    """
    counts = filtered_df.groupBy(*list(group_cols), dimension_col).agg(
        F.count("*").alias("__bucket_cnt")
    )
    totals = counts.groupBy(*list(group_cols)).agg(
        F.sum("__bucket_cnt").alias("__total_cnt")
    )
    probabilities = counts.join(totals, list(group_cols), "inner").withColumn(
        "__p", F.col("__bucket_cnt") / F.col("__total_cnt")
    )
    return probabilities.groupBy(*list(group_cols)).agg(
        (
            -F.sum(
                F.when(F.col("__p") > 0, F.col("__p") * F.log2(F.col("__p"))).otherwise(
                    F.lit(0.0)
                )
            )
        ).alias(feature_name)
    )


def feature_columns(df: DataFrame, entity_cols: Sequence[str]) -> List[str]:
    """The columns of ``df`` that are features rather than keys."""
    keys = set(entity_cols)
    return [col_name for col_name in df.columns if col_name not in keys]


# --- added from the v5 notebook (cell 10) ---

def _lookback_condition_calendar_days(days: int, cfg: FeatureConfig) -> Column:
    """Whole calendar days: the event fell on one of the last ``days`` dates.

    Pre-v4.0 semantics, kept only so ``lookback_semantics="calendar"`` can
    reproduce old output. It is off by one day against this pipeline's
    references, and the reason is worth stating because it is invisible in the
    output.

    ``lag_days`` runs from 0 (the reference's own date) to ``days - 1``. Every
    reference this pipeline builds sits at ``00:00:00`` --
    ``USE_WINDOW_END_AS_REFERENCE_TS`` is False, so the reference is the first
    instant of the window -- and ``attach_events_to_scope`` has already dropped
    every event after the reference. So ``lag_days == 0`` can only match an
    event at exactly midnight: the reference date contributes no observable
    time, and a window labelled 7d covers six days of data, 30d covers
    twenty-nine and 90d covers eighty-nine. It is consistent across rows, so it
    does not corrupt a model, but the column names overstate the window and the
    cross-window ratios are really 6/29 and 29/89.
    """
    lag_days = F.datediff(
        F.to_date(F.col(cfg.reference_ts_col)), F.to_date(F.col(cfg.event_ts_col))
    )
    return lag_days.between(0, days - 1)


def active_at_reference_conditions(
    from_ts: Column,
    to_ts: Column,
    reference_ts: Column,
) -> List[Column]:
    """Whether a lineage interval covers a reference moment.

    "Active at ``reference_ts``" means one thing everywhere in this package and
    it is defined here, once, because three places test it and a fourth relies
    on the answers agreeing. If the reference builder and the snapshot builder
    disagreed by so much as a boundary, a customer could be given a reference
    row and then produce no snapshot rows -- a row that costs a join and yields
    no output -- or the reverse.

    The comparison is on dates, not timestamps, and it is closed at both ends.
    A reference moment is trusted to the day and not to the second: an interval
    that opened at nine in the morning still covers a reference recorded for
    that day. Closing the upper bound has the same motivation from the other
    side, which does mean a number changing hands on the reference date matches
    the interval that ended and the one that began; ``build_reference_df_from_
    msisdns`` ranks those and takes the later start.

    Intervals left open by the source are closed to 1900/9999 by
    ``closed_lineage_intervals`` before this is applied, so a still-open
    interval reads as active rather than as NULL.
    """
    return [
        F.to_date(from_ts) <= F.to_date(reference_ts),
        F.to_date(to_ts) >= F.to_date(reference_ts),
    ]
