"""The shared Spark helpers, and finding 1.5 where it actually lives.

Every calculator is built out of these, so a defect here is a defect in nine
feature families at once. Two of them are worth the whole module on their own:
:func:`next_event_window` is finding 1.5 — the inverted tie-break that lost every
simultaneous event pair — and :func:`safe_divide` is finding 3.1, the decision
that an undefined ratio stays null instead of being filled with zero.

The window helper is exercised the way the calculators use it: a ``min`` of the
target timestamp taken *over* the window, because the frame boundary is the thing
under test and a window with no aggregate over it asserts nothing.
"""

from __future__ import annotations

import math

import pytest

pyspark = pytest.importorskip("pyspark", reason="the feature helpers are Spark code")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402
from tests.features.conftest import REFERENCE_TS, ts  # noqa: E402

from ts05_features.library.config import (  # noqa: E402
    OPEN_INTERVAL_END,
    OPEN_INTERVAL_START,
    FeatureConfig,
)
from ts05_features.library.expressions import (  # noqa: E402
    align_columns,
    closed_lineage_intervals,
    combine_conditions,
    dedupe_columns,
    entropy_over_dimension,
    feature_base,
    feature_columns,
    feature_token,
    filtered_events,
    has_col,
    lookback_condition,
    next_event_window,
    safe_divide,
    safe_join,
)

pytestmark = pytest.mark.spark


# --------------------------------------------------------------------------
# frame hygiene
# --------------------------------------------------------------------------


def test_a_duplicated_column_name_resolves_to_the_leftmost_one(spark):
    """Spark allows two columns named the same and then resolves neither.

    A join between two aliased sides that both carry ``mno`` produces exactly
    this, and the next ``F.col("mno")`` fails with ``AMBIGUOUS_REFERENCE``
    rather than picking one. Leftmost wins because in every join in this package
    the left side is the authoritative one — the lineage, or the scope snapshot.
    """
    frame = spark.createDataFrame([("a", "b")], ["mno", "other"]).select(
        F.col("mno"), F.col("other"), F.lit("z").alias("mno")
    )
    assert frame.columns == ["mno", "other", "mno"]

    deduped = dedupe_columns(frame)

    assert deduped.columns == ["mno", "other"]
    assert deduped.collect()[0]["mno"] == "a"


def test_a_frame_with_no_duplicates_is_returned_untouched(spark):
    """Identity, not a projection. The helper runs on every frame that enters a
    join and an unconditional ``toDF`` would add a stage per call."""
    frame = spark.createDataFrame([("a", "b")], ["one", "two"])
    assert dedupe_columns(frame) is frame


def test_combining_no_conditions_is_an_error_rather_than_a_true_literal(spark):
    """Because the callers build the list conditionally.

    ``combine_conditions([...])`` over an empty list reducing to ``lit(True)``
    would turn a join whose conditions all got filtered out into a cross join,
    and a cross join of a scope snapshot with an event table does not fail — it
    just never finishes.
    """
    with pytest.raises(ValueError, match="At least one join or filter condition"):
        combine_conditions([])


def test_conditions_are_anded_and_not_ored(spark):
    frame = spark.createDataFrame([(1, 1), (1, 2), (2, 2)], ["left", "right"])
    condition = combine_conditions([F.col("left") == 1, F.col("right") == 2])
    assert frame.filter(condition).count() == 1


def test_has_col_and_feature_columns_agree_on_what_is_a_key(spark):
    frame = spark.createDataFrame([("r1", "C1", 1.0)], ["reference_id", "customer_id", "x"])
    assert has_col(frame, "customer_id")
    assert not has_col(frame, "acct_id")
    assert feature_columns(frame, ["reference_id", "customer_id"]) == ["x"]
    assert feature_columns(frame, ["reference_id", "customer_id", "x"]) == []


def test_feature_columns_keeps_the_frames_own_order(spark):
    """The projection order of an assembled frame is its column order, and the
    CSV exports downstream are positional."""
    frame = spark.createDataFrame([("r1", 1.0, 2.0, 3.0)], ["reference_id", "c", "a", "b"])
    assert feature_columns(frame, ["reference_id"]) == ["c", "a", "b"]


# --------------------------------------------------------------------------
# finding 3.1: an undefined ratio is null, not zero
# --------------------------------------------------------------------------


def test_dividing_by_zero_gives_null_and_not_zero(spark):
    """Finding 3.1, and the reason it is a finding rather than a nicety.

    An entity with no events in the thirty-day window has no seven-over-thirty
    ratio. Filling zero puts it at the *quiet* end of a scale it is not on at
    all — indistinguishable from an entity with plenty of thirty-day activity
    and none in the last week, which is the opposite behaviour. The null has to
    survive to preprocessing for preprocessing to be able to decide.
    """
    frame = spark.createDataFrame(
        [(7.0, 30.0), (7.0, 0.0), (0.0, 0.0)], ["numerator", "denominator"]
    )
    rows = frame.select(
        safe_divide(F.col("numerator"), F.col("denominator")).alias("ratio")
    ).collect()

    assert rows[0]["ratio"] == pytest.approx(7.0 / 30.0)
    assert rows[1]["ratio"] is None
    assert rows[2]["ratio"] is None


def test_a_null_denominator_also_gives_null(spark):
    """Which is a different case from zero and reaches the same answer.

    A null denominator means the count frame had no row for this entity at all,
    and inventing a ratio for it would be worse than inventing one for zero.
    """
    frame = spark.createDataFrame(
        [(1.0, None)], T.StructType(
            [
                T.StructField("numerator", T.DoubleType()),
                T.StructField("denominator", T.DoubleType()),
            ]
        )
    )
    assert frame.select(
        safe_divide(F.col("numerator"), F.col("denominator")).alias("r")
    ).collect()[0]["r"] is None


# --------------------------------------------------------------------------
# lineage intervals
# --------------------------------------------------------------------------


def test_an_open_interval_is_closed_at_both_ends(spark, cfg, lineage_df):
    """A NULL bound makes every comparison against it null, which is not true.

    An interval with ``to_ts IS NULL`` is the ordinary "still current" row, and
    ``event_ts < NULL`` is null rather than true — so before this function every
    current assignment contained no events whatsoever. That is the failure mode
    with the widest reach in the whole package, because "still current" is most
    of the table.
    """
    closed = closed_lineage_intervals(lineage_df, cfg)

    assert closed.filter(F.col(cfg.from_ts_col).isNull()).count() == 0
    assert closed.filter(F.col(cfg.to_ts_col).isNull()).count() == 0

    sentinel = closed.filter(F.col("customer_id") == "C1").collect()[0]
    assert sentinel[cfg.to_ts_col] == ts(OPEN_INTERVAL_END)
    assert sentinel[cfg.from_ts_col] == ts("2022-01-01")


def test_a_real_bound_is_not_overwritten_by_the_sentinel(spark, cfg, lineage_df):
    """C4's assignment ended in March 2024, and it has to keep saying so.

    Otherwise the recycled number P6 would resolve to both C4 and C5 forever and
    the fraud population would attribute events to whichever the join reached
    first.
    """
    closed = closed_lineage_intervals(lineage_df, cfg)
    row = closed.filter(F.col("customer_id") == "C4").collect()[0]
    assert row[cfg.to_ts_col] == ts("2024-03-01")


def test_closing_an_already_closed_frame_changes_nothing(spark, cfg, lineage_df):
    """Idempotent, which every caller relies on.

    ``build_changes_table`` closes the frame and then hands it to three
    functions that each close it again. If the second application replaced the
    far sentinel with anything else, the derived-event functions would see
    different intervals than the normalisation did.
    """
    once = closed_lineage_intervals(lineage_df, cfg)
    twice = closed_lineage_intervals(once, cfg)
    assert sorted(once.collect(), key=repr) == sorted(twice.collect(), key=repr)


def test_a_null_near_bound_becomes_the_early_sentinel_not_the_late_one(spark, cfg):
    """The two sentinels are not interchangeable and the near one is rarer.

    A handful of inferred lineage rows have no known start. Substituting the
    *far* sentinel there would put the interval start after every real event and
    the assignment would contain nothing — the same bug as the open upper bound,
    inverted.
    """
    schema = T.StructType(
        [
            T.StructField("customer_id", T.StringType()),
            T.StructField("from_ts", T.TimestampType()),
            T.StructField("to_ts", T.TimestampType()),
        ]
    )
    frame = spark.createDataFrame([("C9", None, None)], schema)
    row = closed_lineage_intervals(frame, cfg).collect()[0]
    assert row["from_ts"] == ts(OPEN_INTERVAL_START)
    assert row["to_ts"] == ts(OPEN_INTERVAL_END)
    assert row["from_ts"] < row["to_ts"]


# --------------------------------------------------------------------------
# aligning feeds for the union
# --------------------------------------------------------------------------


def test_a_missing_column_is_added_with_the_type_its_name_implies(spark, cfg):
    """Typed nulls, because ``unionByName`` needs one schema and a string null
    where a double belongs makes the weighted sum a string concatenation.

    The three cases are the three the feeds actually differ on: the carrier
    feeds have no partner metadata, an EnStream frame has no reference
    timestamp until it is scoped, and a per-window weight column only exists
    after the weighting stage.
    """
    frame = spark.createDataFrame([("C1",)], ["customer_id"])
    wanted = [
        "customer_id",
        cfg.event_ts_col,
        cfg.event_weight_col,
        "event_weight_30d",
        "industry",
    ]

    aligned = align_columns(frame, wanted, cfg)
    dtypes = dict(aligned.dtypes)

    assert dtypes[cfg.event_ts_col] == "timestamp"
    assert dtypes[cfg.event_weight_col] == "double"
    assert dtypes["event_weight_30d"] == "double"
    assert dtypes["industry"] == "string"
    assert aligned.collect()[0]["industry"] is None


def test_a_column_that_is_already_present_keeps_its_own_type(spark, cfg):
    """Alignment adds; it never re-types. A feed that typed ``event_weight`` as
    a float has to keep the values it shipped."""
    frame = spark.createDataFrame([("C1", 2.5)], ["customer_id", "event_weight"])
    aligned = align_columns(frame, ["customer_id", "event_weight"], cfg)
    assert aligned.collect()[0]["event_weight"] == 2.5
    assert aligned.columns == ["customer_id", "event_weight"]


# --------------------------------------------------------------------------
# the left join every family depends on
# --------------------------------------------------------------------------


def test_an_entity_with_no_features_keeps_its_row_with_nulls(spark):
    """The property that makes a silent customer a row of nulls, not an absence.

    Every calculator groups events, so it produces rows only for entities that
    had events of that family. Nine families inner-joined would leave only the
    entities that did all nine things — which in the real population is close to
    nobody, and in a test fixture is nobody at all.
    """
    base = spark.createDataFrame([("r1", "C1"), ("r2", "C2")], ["reference_id", "customer_id"])
    features = spark.createDataFrame([("r1", "C1", 4)], ["reference_id", "customer_id", "cnt"])

    joined = safe_join(base, [features], ["reference_id", "customer_id"])
    values = {row["customer_id"]: row["cnt"] for row in joined.collect()}

    assert values == {"C1": 4, "C2": None}


def test_a_second_frame_claiming_the_same_column_is_skipped_not_suffixed(spark):
    """First one wins, and no ``cnt`` / ``cnt`` pair appears.

    This is load-bearing for the EnStream asymmetry: the generic frequency
    calculator and the partner-weighted one produce identical column names on
    purpose, and which one runs is decided by ``assemble`` rather than by the
    join. Suffixing instead would give the feature table two columns whose names
    differ by a suffix nothing downstream knows about.
    """
    base = spark.createDataFrame([("r1",)], ["reference_id"])
    first = spark.createDataFrame([("r1", 1)], ["reference_id", "cnt"])
    second = spark.createDataFrame([("r1", 99)], ["reference_id", "cnt"])

    joined = safe_join(base, [first, second], ["reference_id"])

    assert joined.columns == ["reference_id", "cnt"]
    assert joined.collect()[0]["cnt"] == 1


def test_a_frame_contributing_nothing_new_adds_no_join_at_all(spark):
    """Not merely no columns — no join. A left join on a key adds a shuffle, and
    the assembly calls this once per family per scope."""
    base = spark.createDataFrame([("r1", 1)], ["reference_id", "cnt"])
    duplicate = spark.createDataFrame([("r1", 1), ("r1", 2)], ["reference_id", "cnt"])

    joined = safe_join(base, [duplicate], ["reference_id"])

    assert joined.count() == 1


def test_none_entries_are_skipped_so_a_caller_can_pass_a_sparse_list(spark):
    """``assemble`` builds its list with conditional members."""
    base = spark.createDataFrame([("r1",)], ["reference_id"])
    features = spark.createDataFrame([("r1", 3)], ["reference_id", "cnt"])
    assert safe_join(base, [None, features, None], ["reference_id"]).collect()[0]["cnt"] == 3


def test_a_feature_frame_missing_a_join_key_is_refused_by_name(spark):
    """Rather than joining on what it does have.

    Spark would happily join on the subset of keys present and produce a
    cartesian product within each group. The message names the missing keys and
    lists what the frame has, because the usual cause is a calculator that was
    handed the wrong scope's ``entity_cols``.
    """
    base = spark.createDataFrame([("r1", "C1")], ["reference_id", "customer_id"])
    features = spark.createDataFrame([("C1", 1)], ["customer_id", "cnt"])

    with pytest.raises(ValueError, match="missing join keys"):
        safe_join(base, [features], ["reference_id", "customer_id"])


# --------------------------------------------------------------------------
# lookback semantics
# --------------------------------------------------------------------------


def _in_window(spark, cfg, days, event_ts):
    frame = spark.createDataFrame(
        [(ts(REFERENCE_TS), ts(event_ts))],
        T.StructType(
            [
                T.StructField(cfg.reference_ts_col, T.TimestampType()),
                T.StructField(cfg.event_ts_col, T.TimestampType()),
            ]
        ),
    )
    return frame.select(lookback_condition(days, cfg).alias("hit")).collect()[0]["hit"]


def test_a_one_day_window_is_twenty_four_hours_and_not_the_calendar_day(spark, cfg):
    """The hour/day split, asserted at the boundary where it is visible.

    The reference is noon on 1 June. An event at 6pm on 31 May is on the
    previous calendar date but eleven hours before the reference; the one-day
    window is an hour window, so it counts. On calendar-day semantics it would
    not, and for a one-day window that boundary effect is the whole window.
    """
    assert cfg.lookback_hour_windows == (1, 3)
    assert _in_window(spark, cfg, 1, "2024-05-31 18:00:00") is True
    assert _in_window(spark, cfg, 1, "2024-05-31 11:00:00") is False


def test_a_thirty_day_window_is_calendar_days_and_includes_the_whole_first_date(
    spark, cfg
):
    """Because thirty days is what the fraud team reasons in, to the date.

    An event at 1am on 3 May is twenty-nine calendar dates back, so it is in the
    thirty-day window even though it is more than twenty-nine and a half
    twenty-four-hour periods before noon on 1 June.
    """
    assert _in_window(spark, cfg, 30, "2024-05-03 01:00:00") is True
    assert _in_window(spark, cfg, 30, "2024-05-02 23:00:00") is False


def test_a_future_event_is_in_no_window(spark, cfg):
    """Leakage, in one line. The scope join bounds this too, but a window
    condition that admitted the future would let it back in wherever the join
    bound is relaxed — which ``prune_event_window=False`` does."""
    for days in cfg.lookback_days:
        assert _in_window(spark, cfg, days, "2024-06-02 12:00:00") is False


def test_the_hour_window_is_exclusive_at_its_far_edge(spark, cfg):
    """``(reference - 24h, reference]``. Half-open, so an event exactly
    twenty-four hours old belongs to the window that is opening, not the one
    that closed — the same convention the lineage interval join uses."""
    assert _in_window(spark, cfg, 1, "2024-06-01 12:00:00") is True
    assert _in_window(spark, cfg, 1, "2024-05-31 12:00:01") is True
    assert _in_window(spark, cfg, 1, "2024-05-31 12:00:00") is False


def test_narrowing_the_hour_windows_makes_every_window_calendar_based(spark, narrow_cfg):
    """The documented escape hatch, asserted so it stays one.

    ``lookback_hour_windows=()`` gives uniform day semantics at the cost of no
    longer matching the features the models were trained on. The suite's
    ``narrow_cfg`` uses it, so the tests that assert per-window columns do not
    have to reason about two kinds of boundary.
    """
    assert _in_window(spark, narrow_cfg, 3, "2024-05-30 01:00:00") is True
    assert _in_window(spark, narrow_cfg, 3, "2024-05-29 23:00:00") is False


# --------------------------------------------------------------------------
# family filtering and the feature base
# --------------------------------------------------------------------------


def test_filtering_to_a_family_that_matches_nothing_gives_an_empty_frame(spark, cfg):
    """Which is finding 1.1's failure mode, seen from the helper's side.

    The helper is right; the danger was always the *argument*. An empty frame
    grouped and left-joined produces a full complement of null columns and no
    error, so the only defence is that no caller can construct a family name
    that matches nothing — which is what ``test_config.py`` pins.
    """
    frame = spark.createDataFrame([("C1", "sim_change")], ["customer_id", "event_family"])
    assert filtered_events(frame, "sim_change", cfg).count() == 1
    assert filtered_events(frame, "account_cancellationphone_number_change", cfg).count() == 0


def test_the_feature_base_is_the_distinct_entities_and_nothing_else(spark):
    """One row per entity however many events it had, and no event columns.

    The base is the left side of every family's join. If it carried event
    columns, the join would fan out; if it were not distinct, an entity with
    forty events would get forty identical feature rows.
    """
    frame = spark.createDataFrame(
        [("r1", "C1", 1), ("r1", "C1", 2), ("r2", "C2", 3)],
        ["reference_id", "customer_id", "event"],
    )
    base = feature_base(frame, ["reference_id", "customer_id"])
    assert base.columns == ["reference_id", "customer_id"]
    assert base.count() == 2


# --------------------------------------------------------------------------
# column-name tokens
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("STATUS_CHANGE-C", "status_change_c"),
        ("STATUS_CHANGE-AA", "status_change_aa"),
        ("DEVICE_CHANGE", "device_change"),
        ("ENSTREAM_API_CALL", "enstream_api_call"),
        ("MNO_PORT", "mno_port"),
    ],
)
def test_an_event_type_becomes_a_usable_column_name_fragment(raw, expected):
    """The hyphen is the whole reason this function exists.

    A hyphen in a Spark column name has to be back-quoted in every SQL
    expression that names it, and the transition features build column names by
    string concatenation in four places. One that forgets is an
    ``AnalysisException`` at runtime, on a column that only exists for the
    status families.
    """
    assert feature_token(raw) == expected


def test_two_types_differing_only_in_punctuation_still_differ_as_tokens():
    """Otherwise finding 1.2 would come back as a silent column collision.

    ``STATUS_CHANGE-C`` and ``STATUS_CHANGE_C`` both tokenise to
    ``status_change_c``. That is fine — and it is *why* the misspelling has to be
    caught in the config, where it is a filter that matches nothing, rather than
    here, where it looks identical to the correct spelling.
    """
    assert feature_token("STATUS_CHANGE-C") == feature_token("STATUS_CHANGE_C")
    assert feature_token("STATUS_CHANGE-C") != feature_token("STATUS_CHANGE-S")


def test_leading_and_trailing_punctuation_does_not_survive():
    """A token ending in an underscore concatenates into a doubled separator,
    and column names are the interface the reports and plots read."""
    assert feature_token("-DEVICE_CHANGE-") == "device_change"
    assert feature_token("a  b") == "a_b"


# --------------------------------------------------------------------------
# finding 1.5: the tie-break that lost every simultaneous pair
# --------------------------------------------------------------------------


def _next_target_ts(spark, cfg, rows, target):
    """Reproduce exactly what ``calculate_state_transition_features`` does.

    The window is only meaningful under an aggregate, and the aggregate is what
    makes the frame boundary observable, so the helper takes the same ``min`` the
    calculator takes.
    """
    frame = spark.createDataFrame(
        [(customer, ts(event_ts), event_type) for customer, event_ts, event_type in rows],
        T.StructType(
            [
                T.StructField("customer_id", T.StringType()),
                T.StructField(cfg.event_ts_col, T.TimestampType()),
                T.StructField(cfg.event_type_col, T.StringType()),
            ]
        ),
    )
    enriched = frame.withColumn(
        "__next",
        F.min(
            F.when(F.col(cfg.event_type_col) == target, F.col(cfg.event_ts_col))
        ).over(next_event_window(["customer_id"], target, cfg)),
    )
    return {
        (row["customer_id"], row[cfg.event_type_col], row[cfg.event_ts_col]): row["__next"]
        for row in enriched.collect()
    }


def test_a_target_sharing_a_timestamp_with_its_source_is_a_zero_day_transition(
    spark, cfg
):
    """Finding 1.5, and the reason it is not an edge case.

    A device change and a SIM change arriving in the same carrier batch share a
    timestamp. That pair — new device, new SIM, same second — is one of the
    sharpest signals in the feature set, and it is exactly the pair the
    notebook's ascending tie-break dropped: with the source sorted first, the
    simultaneous target falls outside the source's frame and the transition is
    either missed or reported at a later target's much longer gap.
    """
    rows = [
        ("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
        ("C1", "2024-05-01 09:00:00", "SIM_CHANGE"),
    ]
    next_ts = _next_target_ts(spark, cfg, rows, "SIM_CHANGE")
    source_key = ("C1", "DEVICE_CHANGE", ts("2024-05-01 09:00:00"))

    assert next_ts[source_key] == ts("2024-05-01 09:00:00")


def test_a_simultaneous_target_wins_over_a_later_one(spark, cfg):
    """The precise damage of the inverted flag, rather than merely its presence.

    With the tie-break backwards this source still gets a next-target — the one
    a fortnight later — so the transition is not null and no assertion on
    nullity would catch it. The value is simply wrong, and wrong in the
    direction that makes a takeover look leisurely.
    """
    rows = [
        ("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
        ("C1", "2024-05-01 09:00:00", "SIM_CHANGE"),
        ("C1", "2024-05-15 09:00:00", "SIM_CHANGE"),
    ]
    next_ts = _next_target_ts(spark, cfg, rows, "SIM_CHANGE")
    source_key = ("C1", "DEVICE_CHANGE", ts("2024-05-01 09:00:00"))

    assert next_ts[source_key] == ts("2024-05-01 09:00:00")
    assert next_ts[source_key] != ts("2024-05-15 09:00:00")


def test_the_next_target_is_the_soonest_one_after_the_source(spark, cfg):
    """The ordinary case, which the descending order has to keep getting right.

    Reversing the timestamp order to fix the tie-break would break this: the
    frame would then look backwards and report the *previous* target, turning
    every transition into a negative gap.
    """
    rows = [
        ("C1", "2024-04-01 09:00:00", "SIM_CHANGE"),
        ("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
        ("C1", "2024-05-04 09:00:00", "SIM_CHANGE"),
        ("C1", "2024-05-20 09:00:00", "SIM_CHANGE"),
    ]
    next_ts = _next_target_ts(spark, cfg, rows, "SIM_CHANGE")
    assert next_ts[("C1", "DEVICE_CHANGE", ts("2024-05-01 09:00:00"))] == ts(
        "2024-05-04 09:00:00"
    )


def test_a_source_with_no_subsequent_target_gets_null_and_not_a_prior_one(spark, cfg):
    """A transition that never completed contributes nothing to the mean.

    The alternative — falling back to the most recent earlier target — would
    make the transition features symmetric in time, which is to say meaningless.
    """
    rows = [
        ("C1", "2024-04-01 09:00:00", "SIM_CHANGE"),
        ("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
    ]
    next_ts = _next_target_ts(spark, cfg, rows, "SIM_CHANGE")
    assert next_ts[("C1", "DEVICE_CHANGE", ts("2024-05-01 09:00:00"))] is None


def test_the_window_does_not_reach_across_entities(spark, cfg):
    """One customer's SIM change is not the next event after another's device
    change, however close the timestamps are."""
    rows = [
        ("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
        ("C2", "2024-05-01 10:00:00", "SIM_CHANGE"),
    ]
    next_ts = _next_target_ts(spark, cfg, rows, "SIM_CHANGE")
    assert next_ts[("C1", "DEVICE_CHANGE", ts("2024-05-01 09:00:00"))] is None


# --------------------------------------------------------------------------
# entropy
# --------------------------------------------------------------------------


def test_all_events_in_one_bucket_gives_zero_entropy(spark):
    """Zero, not null. An entity whose events all happen at the same hour is
    maximally predictable, which is a real and low value — the shape a scheduled
    job produces."""
    frame = spark.createDataFrame(
        [("C1", 9), ("C1", 9), ("C1", 9)], ["customer_id", "__hour"]
    )
    result = entropy_over_dimension(frame, ["customer_id"], "__hour", "entropy")
    assert result.collect()[0]["entropy"] == pytest.approx(0.0)


def test_an_even_spread_over_four_buckets_gives_exactly_two_bits(spark):
    """In bits, so the value is checkable by hand: log2(4) == 2.

    Asserting the unit matters because the same quantity in nats is 1.386, and a
    feature that silently changed base would shift every entropy column by a
    constant factor — invisible in a distribution plot and visible in a model
    that had been fitted on the other scale.
    """
    frame = spark.createDataFrame(
        [("C1", bucket) for bucket in (1, 2, 3, 4)], ["customer_id", "__hour"]
    )
    result = entropy_over_dimension(frame, ["customer_id"], "__hour", "entropy")
    assert result.collect()[0]["entropy"] == pytest.approx(2.0)


def test_an_uneven_spread_lands_between_the_two_extremes(spark):
    """Three events, two in one bucket: -(2/3)log2(2/3) - (1/3)log2(1/3)."""
    frame = spark.createDataFrame(
        [("C1", 1), ("C1", 1), ("C1", 2)], ["customer_id", "__hour"]
    )
    expected = -(2 / 3) * math.log2(2 / 3) - (1 / 3) * math.log2(1 / 3)
    result = entropy_over_dimension(frame, ["customer_id"], "__hour", "entropy")
    assert result.collect()[0]["entropy"] == pytest.approx(expected)


def test_entropy_is_computed_per_entity_and_not_pooled(spark):
    """The pooled version is a population statistic and not a feature at all,
    and it is the mistake a missing ``groupBy`` key produces."""
    frame = spark.createDataFrame(
        [("C1", 1), ("C1", 1), ("C2", 1), ("C2", 2)], ["customer_id", "__hour"]
    )
    result = entropy_over_dimension(frame, ["customer_id"], "__hour", "entropy")
    values = {row["customer_id"]: row["entropy"] for row in result.collect()}
    assert values["C1"] == pytest.approx(0.0)
    assert values["C2"] == pytest.approx(1.0)


def test_a_null_bucket_is_its_own_bucket_rather_than_being_dropped(spark):
    """Because "we do not know when this happened" is not the same fact as "this
    happened at the same time as everything else", and ``groupBy`` keeps nulls."""
    schema = T.StructType(
        [
            T.StructField("customer_id", T.StringType()),
            T.StructField("__hour", T.LongType()),
        ]
    )
    frame = spark.createDataFrame([("C1", 9), ("C1", None)], schema)
    result = entropy_over_dimension(frame, ["customer_id"], "__hour", "entropy")
    assert result.collect()[0]["entropy"] == pytest.approx(1.0)


def test_the_feature_config_defaults_are_what_the_helpers_read(spark):
    """A calculator called without ``cfg`` must behave as one called with the
    production config — the notebook's inconsistent forwarding is finding 1.7,
    and a default that differed from ``FeatureConfig()`` would reintroduce it."""
    assert lookback_condition(1, FeatureConfig()) is not None
    assert FeatureConfig() == FeatureConfig()
