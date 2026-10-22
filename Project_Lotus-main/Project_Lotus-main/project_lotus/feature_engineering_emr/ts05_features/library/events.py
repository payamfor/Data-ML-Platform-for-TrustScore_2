"""Normalisation: three feeds in, one canonical event table out.

Features are computed over a single table with a uniform shape — who, when,
what family, what weight — and this module is what produces it. Three kinds of
row go in:

*Carrier change records* arrive as ``account_changes_batch`` rows keyed on a
phone number hash. They are filtered to the change types each carrier has
agreed to send, mapped to a family, and attributed to the account and customer
that held the number at the moment of the change.

*Derived lineage events* are not sent by anyone. A phone-number change and an
MNO port are both visible as a discontinuity in the lineage table itself, and
are reconstructed from it.

*EnStream API calls* are our own traffic: a partner asking us about a phone
number. They arrive keyed on a different hash than the lineage uses, so they are
bridged through a mapping table first, and they carry partner metadata that no
carrier event has.

Every function here takes the lineage frame and closes its open intervals
first, via :func:`~ts05_features.library.expressions.closed_lineage_intervals`.
That is idempotent, so passing an already-closed frame costs a projection and
changes nothing.
"""

from __future__ import annotations

from functools import reduce
from typing import List, Optional, Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ts05_features.library.config import (
    AC_EVENT_TYPE_ALIASES,
    AC_EVENT_TYPES_IGNORED,
    CANONICAL_EVENT_FAMILY,
    EVENT_METADATA_COLS,
    EVENT_TYPE_BY_FAMILY,
    INTERVAL_END_GRACE_HOURS,
    INTERVAL_ENDING_FAMILIES,
    SUPPORTED_CHANGE_TYPES,
    FeatureConfig,
)
from ts05_features.library.expressions import (
    align_columns,
    closed_lineage_intervals,
    combine_conditions,
    dedupe_columns,
    has_col,
)

__all__ = [
    "build_changes_table",
    "build_mno_port_events",
    "build_phone_number_change_events",
    "normalize_account_change_events",
    "normalize_enstream_events",
    "preprocess_enstream_events",
]


def _canonical_family_map() -> Column:
    """A Spark map from raw change type to canonical family."""
    items: List[Column] = []
    for raw_value, canonical_value in CANONICAL_EVENT_FAMILY.items():
        items.extend((F.lit(raw_value), F.lit(canonical_value)))
    return F.create_map(*items)


def _dedupe_account_changes(account_changes_df: DataFrame) -> DataFrame:
    """Collapse repeated deliveries of the same carrier change record.

    A carrier redelivering yesterday's file is normal, and two identical
    ``(change_key, change_type, mno, timestamp)`` rows are the same real-world
    event however many times they arrive.
    """
    dedupe_keys = [
        col_name
        for col_name in ("change_key", "change_type", "mno", "timestamp")
        if has_col(account_changes_df, col_name)
    ]
    if not dedupe_keys:
        return account_changes_df
    return account_changes_df.dropDuplicates(dedupe_keys)


def _supported_account_changes(account_changes_df: DataFrame) -> DataFrame:
    """Keep only (carrier, change type) pairs we have a definition for."""
    predicates = [
        (F.col("mno") == mno) & F.col("change_type").isin(*change_types)
        for mno, change_types in SUPPORTED_CHANGE_TYPES.items()
    ]
    return account_changes_df.filter(reduce(lambda left, right: left | right, predicates))


def normalize_account_change_events(
    account_changes_df: DataFrame,
    lineage_df: DataFrame,
    cfg: FeatureConfig = FeatureConfig(),
) -> DataFrame:
    """Attribute carrier change records to the account and customer of the day.

    A change record names a phone number and a moment. Which account and which
    customer that number belonged to at that moment is a question only the
    lineage can answer, and the answer changes over the number's life — which is
    the entire reason the lineage exists.

    The interval test is half-open, ``[from_ts, to_ts)``. That is correct here
    and is *not* correct everywhere: the Gold serving table had to close its
    upper bound because an event landing exactly on ``to_ts`` is the event that
    ended the interval. The difference is that a carrier change record does not
    end a lineage interval — the lineage interval ends because of a
    cancellation or a number change derived elsewhere — so a device change at
    the instant an interval closes belongs to the interval that is opening, not
    the one that closed.
    """
    lineage = closed_lineage_intervals(lineage_df, cfg)
    changes = account_changes_df
    if AC_EVENT_TYPES_IGNORED:
        # Named exclusions, applied before anything else reads the column. See
        # AC_EVENT_TYPES_IGNORED for why each one is here; they would be
        # dropped by the event_type filter below in any case, but naming them
        # is what separates "we decided not to carry this" from "nobody noticed
        # this value existed".
        changes = changes.filter(
            ~F.col("ac_event_type").isin(*AC_EVENT_TYPES_IGNORED)
        )

    changes = (changes
        # Aliases resolve first, so `event_family` and `event_type` are both
        # derived from the canonical spelling rather than the source's.
        .withColumn("__ac_event_type", _resolved_ac_event_type())
        .withColumn(cfg.msisdn_col, F.col("phone_number_AC_hash"))
        .withColumn(cfg.event_ts_col, F.col("ac_event_ts").cast("timestamp"))
        .withColumn(cfg.event_family_col, F.col("__ac_event_type"))
        .withColumn(cfg.event_type_col, _raw_event_type_map()[F.col("__ac_event_type")])
        .withColumn(cfg.event_source_col, F.lit("account_change"))
        .withColumn(cfg.event_weight_col, F.lit(1.0))
        # Drop change types we have no definition for. `_raw_event_type_map()`
        # returns NULL for any resolved `ac_event_type` outside
        # CANONICAL_EVENT_FAMILY -- that is, anything neither canonical already
        # nor covered by AC_EVENT_TYPE_ALIASES --
        # and such a row is worse than useless downstream: `event_family`
        # carries the unrecognised string, so it matches no family filter, and
        # `event_type` is NULL, so it matches nothing in
        # STATE_TRANSITION_EVENT_TYPES either. It contributes to no feature and
        # inflates the canonical table and every dedupe over it. This is the
        # guarantee the deleted `_supported_account_changes` was meant to give,
        # expressed against the schema this pipeline is actually fed, and it
        # costs a projection rather than a shuffle.
        .filter(F.col(cfg.event_type_col).isNotNull())
    )

    is_interval_ending = F.col(cfg.event_family_col).isin(*INTERVAL_ENDING_FAMILIES)

    # Two attributions over disjoint slices rather than one join plus a
    # post-filter: the grace window and the boundary ranking apply to about 5%
    # of the feed, and ranking the whole of it would shuffle every row to
    # resolve a case only cancellations can be in.
    attributed = _attribute_changes_to_lineage(
        changes.filter(~is_interval_ending), lineage, cfg, grace_hours=0
    ).unionByName(
        _attribute_changes_to_lineage(
            changes.filter(is_interval_ending),
            lineage,
            cfg,
            grace_hours=int(INTERVAL_END_GRACE_HOURS),
        )
    )
    return attributed


def build_phone_number_change_events(
    lineage_df: DataFrame,
    cfg: FeatureConfig = FeatureConfig(),
) -> DataFrame:
    """Derive a phone-number change from a discontinuity in the lineage.

    Within one account on one carrier, ordered by interval start, a row whose
    MSISDN differs from the previous row's is the account having changed its
    number. The event is stamped at the *start* of the new interval, which is
    the moment the new number took effect.

    This is deliberately the opposite choice from the defect recorded as finding
    1.1 against the previous Gold code, where a derived number change was
    stamped at the moment the *old* number began. Here there is no ambiguity to
    get wrong: ``from_ts`` of the arriving row is when the change happened.
    """
    lineage = closed_lineage_intervals(lineage_df, cfg)
    order_window = Window.partitionBy(
        cfg.customer_col, cfg.account_col, cfg.mno_col
    ).orderBy(F.col(cfg.from_ts_col))

    derived = (
        lineage.withColumn("__prev_msisdn", F.lag(cfg.msisdn_col).over(order_window))
        .filter(
            F.col("__prev_msisdn").isNotNull()
            & (F.col("__prev_msisdn") != F.col(cfg.msisdn_col))
        )
        .withColumn(cfg.event_ts_col, F.col(cfg.from_ts_col))
        .withColumn(
            cfg.event_family_col,
            F.lit(CANONICAL_EVENT_FAMILY["PHONE_NUMBER_CHANGE"]),
        )
        .withColumn(cfg.event_type_col, F.lit("PHONE_NUMBER_CHANGE"))
        .withColumn(cfg.event_source_col, F.lit("lineage"))
        .withColumn(cfg.event_weight_col, F.lit(1.0))
    )

    return derived.select(
        cfg.customer_col,
        cfg.account_col,
        cfg.mno_col,
        cfg.msisdn_col,
        cfg.event_ts_col,
        cfg.event_family_col,
        cfg.event_type_col,
        cfg.event_source_col,
        cfg.event_weight_col,
    )


def build_mno_port_events(
    lineage_df: DataFrame,
    cfg: FeatureConfig = FeatureConfig(),
) -> DataFrame:
    """Derive an MNO port from the same number appearing on a different carrier.

    Partitioned by customer and MSISDN rather than by account, because a port
    is precisely the event that ends one account and begins another — grouping
    by account would put the two sides in different partitions and see nothing.
    The number is required to be unchanged across the transition, which is what
    separates a port from a number change that happened to coincide with one.

    Note what is *not* here: a window bounding how far apart the two sides may
    be. The previous Gold code required the carriers' events to be within ten
    days of each other and thereby discarded every port where the losing
    carrier went quiet for a month first — finding 1.5, the defect with the
    largest blast radius in that review. A change of carrier for an unchanged
    number is directly observable and needs no window to believe.
    """
    lineage = closed_lineage_intervals(lineage_df, cfg)
    order_window = Window.partitionBy(cfg.customer_col, cfg.msisdn_col).orderBy(
        F.col(cfg.from_ts_col)
    )

    derived = (
        lineage.withColumn("__prev_mno", F.lag(cfg.mno_col).over(order_window))
        .withColumn("__prev_msisdn", F.lag(cfg.msisdn_col).over(order_window))
        .filter(
            F.col("__prev_mno").isNotNull()
            & (F.col("__prev_mno") != F.col(cfg.mno_col))
            & F.col("__prev_msisdn").isNotNull()
            & (F.col("__prev_msisdn") == F.col(cfg.msisdn_col))
        )
        .withColumn(cfg.event_ts_col, F.col(cfg.from_ts_col))
        .withColumn(cfg.event_family_col, F.lit(CANONICAL_EVENT_FAMILY["MNO_PORT"]))
        .withColumn(cfg.event_type_col, F.lit("MNO_PORT"))
        .withColumn(cfg.event_source_col, F.lit("lineage"))
        .withColumn(cfg.event_weight_col, F.lit(1.0))
    )

    return derived.select(
        cfg.customer_col,
        cfg.account_col,
        cfg.msisdn_col,
        cfg.mno_col,
        cfg.event_ts_col,
        cfg.event_family_col,
        cfg.event_type_col,
        cfg.event_source_col,
        cfg.event_weight_col,
    )


def preprocess_enstream_events(
    enstream_df: DataFrame,
    hash_mapping_df: DataFrame,
    hash_mapping_ac_col: str,
    hash_mapping_at_col: str,
    enstream_msisdn_col: str,
    cfg: FeatureConfig = FeatureConfig(),
) -> DataFrame:
    """Bridge EnStream API calls onto the hash the lineage is keyed on."""
    dedupe_cols = [
        col_name
        for col_name in (
            "service_provider_id",
            "partner_id",
            cfg.mno_col,
            "brand",
            "partner_name",
            "service_provider_name",
            "industry",
            enstream_msisdn_col,
            "api_timestamp",
            "api_type",
        )
        if has_col(enstream_df, col_name)
    ]
    calls = enstream_df.dropDuplicates(dedupe_cols) if dedupe_cols else enstream_df
    if has_col(calls, cfg.msisdn_col):
        # audit_trail_services_3 already has its own unrelated `msisdn` column.
        # withColumnRenamed(hash_mapping_ac_col, cfg.msisdn_col) below does not
        # overwrite an existing column of that name -- it silently produces two
        # columns both called `msisdn`, and downstream dedupe_columns keeps
        # whichever sorts first (this one), discarding the correctly-bridged
        # hash_mapping_df value entirely. Dropping it here is the fix.
        calls = calls.drop(cfg.msisdn_col)
    calls = calls.withColumn(
        cfg.event_ts_col, F.col("api_timestamp").cast("timestamp")
    ).withColumn("use_case", F.col("api_type"))

    bridge_condition = [
        F.col(f"call.{enstream_msisdn_col}") == F.col(f"map.{hash_mapping_at_col}")
    ]

    return (
        calls.alias("call")
        .join(
            dedupe_columns(hash_mapping_df).alias("map"),
            combine_conditions(bridge_condition),
            "inner",
        )
        .withColumnRenamed(hash_mapping_ac_col, cfg.msisdn_col)
        .withColumn(
            cfg.event_family_col,
            F.lit(CANONICAL_EVENT_FAMILY["ENSTREAM_API_CALL"]),
        )
        .withColumn(cfg.event_type_col, F.lit("ENSTREAM_API_CALL"))
        .withColumn(cfg.event_source_col, F.lit("enstream"))
        .withColumn(cfg.event_weight_col, F.lit(1.0))
    )


def normalize_enstream_events(
    enstream_df: DataFrame,
    lineage_df: DataFrame,
    cfg: FeatureConfig = FeatureConfig(),
) -> DataFrame:
    """Attribute preprocessed EnStream calls to an account and a customer.

    The carrier is part of the join, not only the number and the timestamp. An
    EnStream call records which carrier answered it, and a call answered by Bell
    cannot belong to a lineage interval on Telus even if the timestamps line up
    — that combination means the port boundary is in the wrong place and the
    right response is to attribute nothing rather than to guess.

    The partner metadata columns are carried through, and they are the reason
    the EnStream family has diversity and novelty features that no carrier
    family has.
    """
    lineage = closed_lineage_intervals(lineage_df, cfg)
    calls = dedupe_columns(enstream_df)

    join_conditions = [
        F.col(f"call.{cfg.msisdn_col}") == F.col(f"lin.{cfg.msisdn_col}"),
        F.col(f"call.{cfg.event_ts_col}") >= F.col(f"lin.{cfg.from_ts_col}"),
        F.col(f"call.{cfg.event_ts_col}") < F.col(f"lin.{cfg.to_ts_col}"),
        F.col(f"call.{cfg.mno_col}") == F.col(f"lin.{cfg.mno_col}"),
    ]

    selected_cols = [
        F.col(f"lin.{cfg.customer_col}").alias(cfg.customer_col),
        F.col(f"lin.{cfg.account_col}").alias(cfg.account_col),
        F.col(f"lin.{cfg.msisdn_col}").alias(cfg.msisdn_col),
        F.col(f"lin.{cfg.mno_col}").alias(cfg.mno_col),
        F.col(f"call.{cfg.event_ts_col}").alias(cfg.event_ts_col),
        F.col(f"call.{cfg.event_family_col}").alias(cfg.event_family_col),
        F.col(f"call.{cfg.event_type_col}").alias(cfg.event_type_col),
        F.col(f"call.{cfg.event_source_col}").alias(cfg.event_source_col),
        F.col(f"call.{cfg.event_weight_col}").alias(cfg.event_weight_col),
    ]
    selected_cols.extend(
        F.col(f"call.{col_name}").alias(col_name)
        for col_name in EVENT_METADATA_COLS
        if has_col(calls, col_name)
    )

    normalized = (
        lineage.alias("lin")
        .join(calls.alias("call"), combine_conditions(join_conditions), "inner")
        .select(*selected_cols)
    )

    # Which partner columns survived depends on what the feed carried, so the
    # dedupe key is filtered against the frame rather than assumed.
    dedupe_keys = [
        key for key in cfg.event_dedupe_keys() if has_col(normalized, key)
    ]
    return normalized.dropDuplicates(dedupe_keys)


def build_changes_table(
    account_changes_df: DataFrame,
    lineage_df: DataFrame,
    normalized_enstream_events_df: Optional[DataFrame] = None,
    extra_event_frames: Optional[Sequence[DataFrame]] = None,
    cfg: FeatureConfig = FeatureConfig(),
) -> DataFrame:
    """Union every event feed into the one canonical table features read.

    The frames are aligned to a common schema first — the union of their columns
    plus everything ``FeatureConfig.event_base_columns`` requires — so that a
    feed missing partner metadata contributes typed nulls rather than shifting
    the union.

    ``extra_event_frames`` is the seam for a feed that does not exist yet. Any
    frame carrying the base columns can be appended without touching this
    function.
    """
    lineage = closed_lineage_intervals(lineage_df, cfg)

    change_frames: List[DataFrame] = [
        normalize_account_change_events(account_changes_df, lineage, cfg),
        build_phone_number_change_events(lineage, cfg),
        build_mno_port_events(lineage, cfg),
    ]
    if normalized_enstream_events_df is not None:
        change_frames.append(dedupe_columns(normalized_enstream_events_df))
    if extra_event_frames:
        change_frames.extend(dedupe_columns(frame) for frame in extra_event_frames)

    all_cols: List[str] = []
    for frame in change_frames:
        for col_name in frame.columns:
            if col_name not in all_cols:
                all_cols.append(col_name)
    for col_name in cfg.event_base_columns():
        if col_name not in all_cols:
            all_cols.append(col_name)

    aligned = [
        align_columns(frame, all_cols, cfg).select(*all_cols) for frame in change_frames
    ]
    combined = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        aligned[1:],
        aligned[0],
    )

    dedupe_keys = [
        col_name for col_name in cfg.event_dedupe_keys() if has_col(combined, col_name)
    ]
    return combined.dropDuplicates(dedupe_keys)


# --- added from the v5 notebook (cell 14) ---

def _ac_event_type_alias_map() -> Column:
    """A Spark map from an aliased ``ac_event_type`` to its canonical family."""
    items: List[Column] = []
    for raw_value, canonical_value in AC_EVENT_TYPE_ALIASES.items():
        items.extend((F.lit(raw_value), F.lit(canonical_value)))
    return F.create_map(*items)


def _raw_event_type_map() -> Column:
    """A Spark map from canonical family back to the raw event type.

    account_changes_canonical_events stores `ac_event_type` as the canonical
    family already (e.g. "device_change"), not the raw carrier type
    ("DEVICE_CHANGE") -- confirmed against the live table. `event_family_col`
    can take that value as-is, but `event_type_col` cannot: it is what
    calculate_state_transition_features filters against
    STATE_TRANSITION_EVENT_TYPES, which is spelled in the raw, uppercase,
    hyphenated form ("DEVICE_CHANGE", "STATUS_CHANGE-A", ...). Reusing the
    canonical value for event_type_col means that filter's `isin(...)` never
    matches an account-change row, so every state-transition pair sourced
    from this feed -- everything except the lineage-derived and EnStream
    types, which get their raw literal set directly -- silently returns zero
    matching rows and every transition column is null. The mapping the
    canonical family value came from (CANONICAL_EVENT_FAMILY) is one-to-one,
    so it inverts cleanly via EVENT_TYPE_BY_FAMILY.
    """
    items: List[Column] = []
    for canonical_value, raw_value in EVENT_TYPE_BY_FAMILY.items():
        items.extend((F.lit(canonical_value), F.lit(raw_value)))
    return F.create_map(*items)


def _resolved_ac_event_type() -> Column:
    """``ac_event_type`` with any known alias replaced by its canonical family.

    ``coalesce`` rather than a plain lookup: the map returns NULL for the
    values that are already canonical, which is every value that is not an
    alias, so the original has to survive.
    """
    raw = F.col("ac_event_type")
    if not AC_EVENT_TYPE_ALIASES:
        return raw
    return F.coalesce(_ac_event_type_alias_map()[raw], raw)


def _attribute_changes_to_lineage(
    changes: DataFrame,
    lineage: DataFrame,
    cfg: FeatureConfig,
    grace_hours: int = 0,
) -> DataFrame:
    """Join change records to the lineage interval they belong to.

    ``grace_hours == 0`` is the ordinary case: the interval test is half-open,
    ``[from_ts, to_ts)``, so a change is attributed to the interval it happened
    inside and a change landing exactly on ``to_ts`` belongs to the interval
    opening rather than the one that closed.

    ``grace_hours > 0`` is for the families in ``INTERVAL_ENDING_FAMILIES``,
    whose event is the thing that ends the interval. The upper bound becomes
    ``to_ts + grace_hours``, inclusive, so a cancellation stamped at or shortly
    after the moment the line closed is still attributed to the line it closed.

    That widening reintroduces an ambiguity the strict bound existed to avoid:
    where one interval ends and another begins, an event in the grace window
    satisfies both. So the candidates are ranked and one is kept. The interval
    whose ``to_ts`` is nearest the event wins, because the event is what ended
    it -- an interval still running long after the cancellation is not the one
    cancelled. Ties go to the later ``from_ts``, matching how
    ``build_reference_df_from_msisdns`` breaks the same kind of tie.

    Ranking is skipped entirely when ``grace_hours`` is 0, since a half-open
    interval test cannot match two intervals of the same line.
    """
    upper_bound = (
        F.col(f"chg.{cfg.event_ts_col}") < F.col(f"lin.{cfg.to_ts_col}")
        if grace_hours <= 0
        else F.col(f"chg.{cfg.event_ts_col}")
        <= F.col(f"lin.{cfg.to_ts_col}") + F.expr(f"INTERVAL {grace_hours} HOURS")
    )

    join_condition = [
        F.col(f"chg.{cfg.msisdn_col}") == F.col(f"lin.{cfg.msisdn_col}"),
        F.col("chg.mno") == F.col(f"lin.{cfg.mno_col}"),
        F.col(f"chg.{cfg.event_ts_col}") >= F.col(f"lin.{cfg.from_ts_col}"),
        upper_bound,
    ]

    joined = lineage.alias("lin").join(
        changes.alias("chg"), combine_conditions(join_condition), "inner"
    )

    if grace_hours > 0:
        # Seconds between the interval's end and the event; the interval that
        # ended closest to it is the one the event ended.
        closeness = F.abs(
            F.col(f"lin.{cfg.to_ts_col}").cast("long")
            - F.col(f"chg.{cfg.event_ts_col}").cast("long")
        )
        # The change record's own identity, so each source row picks exactly one
        # interval. Two source rows sharing all four are the same delivery twice
        # and collapse in the dropDuplicates below either way.
        candidate_window = Window.partitionBy(
            F.col(f"chg.{cfg.msisdn_col}"),
            F.col("chg.mno"),
            F.col(f"chg.{cfg.event_ts_col}"),
            F.col(f"chg.{cfg.event_type_col}"),
        ).orderBy(closeness.asc(), F.col(f"lin.{cfg.from_ts_col}").desc())
        joined = joined.withColumn(
            "__interval_rank", F.row_number().over(candidate_window)
        ).filter(F.col("__interval_rank") == 1)

    return (
        joined.select(
            F.col(f"lin.{cfg.customer_col}").alias(cfg.customer_col),
            F.col(f"lin.{cfg.account_col}").alias(cfg.account_col),
            F.col(f"lin.{cfg.msisdn_col}").alias(cfg.msisdn_col),
            F.col(f"lin.{cfg.mno_col}").alias(cfg.mno_col),
            F.col(f"chg.{cfg.event_ts_col}").alias(cfg.event_ts_col),
            F.col(f"chg.{cfg.event_family_col}").alias(cfg.event_family_col),
            F.col(f"chg.{cfg.event_type_col}").alias(cfg.event_type_col),
            F.col(f"chg.{cfg.event_source_col}").alias(cfg.event_source_col),
            F.col(f"chg.{cfg.event_weight_col}").alias(cfg.event_weight_col),
        )
        .dropDuplicates(
            [
                cfg.customer_col,
                cfg.account_col,
                cfg.msisdn_col,
                cfg.mno_col,
                cfg.event_ts_col,
                cfg.event_type_col,
                cfg.event_source_col,
            ]
        )
    )
