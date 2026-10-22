"""The eleven calculators, asserted by value rather than by shape.

Every one of these functions turns a frame of events into a frame of numbers,
and the numbers are the deliverable. So the tests here mostly compute the
expected value by hand in the docstring and then assert it — three events two
days apart give an interval mean of two, not "an interval mean". A test that
only checked the column existed would have passed against every defect the
findings document records, because not one of them raised and not one of them
produced a missing column.

Three properties recur and are worth stating once.

*Nulls are answers.* An entity with one event has no inter-arrival gap and an
entity with two has no standard deviation of gaps. Finding 3.1 is that the
notebook filled those with zero, which places the entity at the quiet end of a
scale it is not on. Several tests below assert ``is None`` where a reader might
expect ``== 0``, and that is the point of them.

*The windows are configuration.* Finding 1.7 was calculators that fell back to
their default ``cfg`` when a caller narrowed the windows, and burstness ratios
hardcoded as 1/3, 7/30 and 30/90 while the counts came from config. The tests
that care about windows use ``narrow_cfg`` and assert that the *production*
window columns are absent, since a function ignoring its argument produces a
superset rather than an error.

*The prefix comes from ``scope_prefix``.* Finding 1.6 was one wrapper passing
the scope name where the prefix belonged, invisible at the MSISDN scope because
there the two strings agree. The stability tests check all three scopes.

The scoped event frame these take is the output of ``attach_events_to_scope``,
and it is built here from literals rather than by running the snapshot
pipeline. That is deliberate: a calculator's arithmetic is what is under test,
and deriving the input would mean an assertion about a mean gap could fail
because of an interval join.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark", reason="the calculators are Spark code")

from pyspark.sql import types as T  # noqa: E402
from tests.features.conftest import (  # noqa: E402
    FAR_FUTURE,
    REFERENCE_TS,
    collect_features,
    columns_matching,
    ts,
)

from ts05_features.library.calculators import (  # noqa: E402
    calculate_burstness_features,
    calculate_cumulative_event_slope,
    calculate_event_diversity_features,
    calculate_event_frequency_features,
    calculate_event_recency_features,
    calculate_periodicity_features,
    calculate_recency_weighted_intensity,
    calculate_stability_features,
    calculate_state_transition_features,
    calculate_temporal_entropy_features,
    calculate_time_to_k_events,
)
from ts05_features.library.config import (  # noqa: E402
    CANONICAL_EVENT_FAMILY,
    ENSTREAM_DIMENSION_COLS,
    SCOPES,
    FeatureConfig,
    scope_prefix,
)
from ts05_features.library.snapshots import (  # noqa: E402
    build_reference_df_at_timestamp,
    build_scope_snapshots,
)

pytestmark = pytest.mark.spark

#: The entity key at the customer scope: ``cfg.entity_cols_for("customer")``.
#: Written out rather than derived so that a change to the key shows up here.
ENTITY_COLS = ("reference_id", "customer_id")

DEVICE = CANONICAL_EVENT_FAMILY["DEVICE_CHANGE"]
SIM = CANONICAL_EVENT_FAMILY["SIM_CHANGE"]
CANCELLATION = CANONICAL_EVENT_FAMILY["STATUS_CHANGE-C"]


# --------------------------------------------------------------------------
# the scoped event frame
# --------------------------------------------------------------------------

SCOPED_FIELDS = (
    ("reference_id", T.StringType()),
    ("customer_id", T.StringType()),
    ("acct_id", T.StringType()),
    ("msisdn", T.StringType()),
    ("mno", T.StringType()),
    ("reference_ts", T.TimestampType()),
    ("from_ts", T.TimestampType()),
    ("to_ts", T.TimestampType()),
    ("event_ts", T.TimestampType()),
    ("event_family", T.StringType()),
    ("event_type", T.StringType()),
    ("event_source", T.StringType()),
    ("event_weight", T.DoubleType()),
    ("industry", T.StringType()),
    ("service_provider_name", T.StringType()),
    ("use_case", T.StringType()),
)


def scoped_row(
    customer,
    event_ts,
    event_type="DEVICE_CHANGE",
    *,
    reference_id=None,
    reference_ts=REFERENCE_TS,
    account="A1",
    msisdn="P1",
    mno="BELL",
    weight=1.0,
    industry=None,
    service_provider_name=None,
    use_case=None,
    from_ts="2020-01-01",
    to_ts=FAR_FUTURE,
):
    """One row of a scoped event frame, as ``attach_events_to_scope`` returns it.

    ``event_ts=None`` with ``event_type=None`` is the silent entity: a scope row
    that matched no event and survived the left join as nulls. It is not an
    invalid row, it is the row the whole left join exists to preserve.
    """
    family = CANONICAL_EVENT_FAMILY[event_type] if event_type else None
    return (
        reference_id or f"{customer}_REF",
        customer,
        account,
        msisdn,
        mno,
        ts(reference_ts),
        ts(from_ts),
        ts(to_ts),
        ts(event_ts) if event_ts else None,
        family,
        event_type,
        "carrier",
        None if weight is None else float(weight),
        industry,
        service_provider_name,
        use_case,
    )


def scoped_frame(spark, rows):
    schema = T.StructType(
        [T.StructField(name, kind, True) for name, kind in SCOPED_FIELDS]
    )
    return spark.createDataFrame(list(rows), schema)


def by_customer(df):
    """``{customer_id: {column: value}}`` for the customer-scope entity key."""
    return {
        key[1]: values for key, values in collect_features(df, ENTITY_COLS).items()
    }


# --------------------------------------------------------------------------
# calculate_event_frequency_features
# --------------------------------------------------------------------------

def test_the_counts_are_per_window_and_the_windows_come_from_the_config(
    spark, narrow_cfg
):
    """Three events at 1, 10 and 60 days back give 1 in three days and 2 in thirty.

    ``narrow_cfg`` has windows of three and thirty days and no hour windows, so
    both are calendar-day windows and the boundaries are the ones
    ``test_expressions.py`` pins. What is asserted here is the arithmetic on top
    of them, and — separately — that the *production* windows are absent. A
    calculator that ignored its ``cfg`` and used the defaults would still count
    correctly in the three-day column, which is why the negative half matters:
    finding 1.7 produced a superset of the requested columns, not an error.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row("C1", "2024-05-22 09:00:00"),
            scoped_row("C1", "2024-04-02 09:00:00"),
        ],
    )
    got = calculate_event_frequency_features(
        events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
    )
    row = by_customer(got)["C1"]
    assert row["cust_device_change_cnt_3d"] == 1
    assert row["cust_device_change_cnt_30d"] == 2
    assert columns_matching(got, "cnt_") == [
        "cust_device_change_cnt_30d",
        "cust_device_change_cnt_3d",
        "cust_device_change_weighted_cnt_30d",
        "cust_device_change_weighted_cnt_3d",
    ]


def test_the_any_flag_is_a_flag_and_not_a_count(spark, narrow_cfg):
    """Three events in the window still give one.

    Kept as a separate feature from the count because for a rare family the
    distinction between nothing and something is the whole signal, and a count
    of three and a count of thirty are nearly the same fact where zero and one
    are not.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row("C1", "2024-05-31 10:00:00"),
            scoped_row("C1", "2024-05-31 11:00:00"),
        ],
    )
    row = by_customer(
        calculate_event_frequency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_cnt_3d"] == 3
    assert row["cust_device_change_any_3d"] == 1


def test_the_weighted_count_sums_the_weights_and_the_plain_count_does_not(
    spark, narrow_cfg
):
    """Weights of 2.0 and 0.5 give a weighted count of 2.5 and a plain count of 2.

    The two columns exist side by side because the weight means different things
    for different feeds — a partner-volume correction for EnStream and a plain
    one elsewhere, which is finding 3.2 — so a reader who does not trust the
    weighting can use the unweighted column.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", weight=2.0),
            scoped_row("C1", "2024-05-31 10:00:00", weight=0.5),
        ],
    )
    row = by_customer(
        calculate_event_frequency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_cnt_3d"] == 2
    assert row["cust_device_change_weighted_cnt_3d"] == pytest.approx(2.5)


def test_a_null_weight_weighs_as_one_rather_than_erasing_the_event(
    spark, narrow_cfg
):
    """A null weight must not remove the event from the sum.

    ``sum`` ignores nulls, so an unweighted row in an otherwise weighted feed
    would silently not be counted at all in the weighted column. The coalesce is
    what prevents that, and the same reasoning is why
    ``with_partner_volume_weights`` coalesces a missing partner weight to one.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", weight=None),
            scoped_row("C1", "2024-05-31 10:00:00", weight=3.0),
        ],
    )
    row = by_customer(
        calculate_event_frequency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_weighted_cnt_3d"] == pytest.approx(4.0)


def test_a_frame_with_no_weight_column_weighs_every_event_as_one(
    spark, narrow_cfg
):
    """The same function serves a feed that has no weight column at all."""
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row("C1", "2024-05-31 10:00:00"),
        ],
    ).drop("event_weight")
    row = by_customer(
        calculate_event_frequency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_weighted_cnt_3d"] == pytest.approx(2.0)
    assert row["cust_device_change_cnt_3d"] == 2


def test_an_entity_with_no_events_of_the_family_survives_the_left_join(
    spark, narrow_cfg
):
    """C2 did only SIM changes and still has a device-change row, full of nulls.

    This is what ``feature_base`` and the left join in ``safe_join`` are for.
    Without them the output of each family would contain only the entities that
    did that family, and joining nine families would leave the intersection —
    the entities that did everything.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C2", "2024-05-31 09:00:00", "SIM_CHANGE"),
        ],
    )
    got = by_customer(
        calculate_event_frequency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )
    assert sorted(got) == ["C1", "C2"]
    assert got["C2"]["cust_device_change_cnt_3d"] is None


def test_the_silent_entity_survives_with_nulls(spark, narrow_cfg):
    """A scope row that matched no event at all is still a row.

    C6 in the shared population is exactly this: present in the lineage and
    absent from every feed. Its feature row must exist, because the scored
    population is the reference population and not the eventful subset of it.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row("C6", None, None),
        ],
    )
    got = by_customer(
        calculate_event_frequency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )
    assert sorted(got) == ["C1", "C6"]
    assert got["C6"]["cust_device_change_cnt_3d"] is None


def test_zero_and_null_mean_different_things_in_a_count_column(
    spark, narrow_cfg
):
    """Zero is "had some, none lately"; null is "never had any".

    C1's only device change is two hundred days old, so it is in the family and
    outside every window and its count is 0. C2 has no device change ever, so it
    reaches the count column through the left join and is null. Both entities had
    no device change in the last thirty days.

    This asymmetry is recorded as finding 4.5 and is pinned rather than defended.
    It follows from counting inside a ``groupBy`` of the filtered frame and then
    left-joining onto the full entity base, which is the right structure for
    every other reason. Whether the model should see 0 or null for "nothing in
    the window" is a preprocessing decision, and it can only be made
    deliberately if the two cases are distinguishable here — which they are, and
    which is why this is a finding about clarity rather than about correctness.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2023-11-14 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C2", "2024-05-31 09:00:00", "SIM_CHANGE"),
        ],
    )
    got = by_customer(
        calculate_event_frequency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )
    assert got["C1"]["cust_device_change_cnt_30d"] == 0
    assert got["C2"]["cust_device_change_cnt_30d"] is None


def test_the_family_filter_counts_only_that_family(spark, narrow_cfg):
    """One device change and two SIM changes give a device count of one.

    The filter is on ``event_family`` and not ``event_type``, which is what lets
    the six carrier status types collapse into four families while the raw types
    stay available to the transition features.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C1", "2024-05-31 10:00:00", "SIM_CHANGE"),
            scoped_row("C1", "2024-05-31 11:00:00", "SIM_CHANGE"),
        ],
    )
    got = by_customer(
        calculate_event_frequency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert got["cust_device_change_cnt_3d"] == 1


def test_the_production_config_gives_all_five_windows(spark, cfg):
    """Five windows, two of them measured in hours.

    ``narrow_cfg`` is used almost everywhere in this file for speed, so one test
    runs the config the models were actually trained against — a suite that only
    tested three-day windows would not notice a regression in the ninety-day
    ones.
    """
    events = scoped_frame(spark, [scoped_row("C1", "2024-06-01 09:00:00")])
    got = calculate_event_frequency_features(events, ENTITY_COLS, DEVICE, "cust", cfg)
    for days in cfg.lookback_days:
        assert f"cust_device_change_cnt_{days}d" in got.columns
        assert f"cust_device_change_any_{days}d" in got.columns
    row = by_customer(got)["C1"]
    assert row["cust_device_change_cnt_1d"] == 1
    assert row["cust_device_change_cnt_90d"] == 1


# --------------------------------------------------------------------------
# calculate_event_recency_features
# --------------------------------------------------------------------------

def test_days_since_last_and_first_are_calendar_days_from_the_reference(
    spark, narrow_cfg
):
    """Events on 30 May and 1 April, reference 1 June: two days and sixty-one.

    Calendar days, via ``to_date`` on both sides, so a reference at noon and an
    event at nine in the morning of the same day are zero days apart rather than
    a fraction. That matches the lookback windows' day semantics, which is the
    point — a feature saying "last event was 0 days ago" and a window saying
    "nothing in the last day" would otherwise be able to disagree.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-30 09:00:00"),
            scoped_row("C1", "2024-04-01 09:00:00"),
        ],
    )
    row = by_customer(
        calculate_event_recency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_days_since_last"] == 2
    assert row["cust_device_change_days_since_first"] == 61


def test_an_event_earlier_the_same_day_is_zero_days_ago(spark, narrow_cfg):
    """Four hours before the reference, and the answer is zero, not a fraction."""
    events = scoped_frame(spark, [scoped_row("C1", "2024-06-01 08:00:00")])
    row = by_customer(
        calculate_event_recency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_days_since_last"] == 0


def test_the_total_count_is_unwindowed(spark, narrow_cfg):
    """Three events, one of them outside every window: total 3, thirty-day 1.

    The lifetime count is the one thing the windowed counts cannot express. Note
    the caveat in the docstring: with the event scan pruned, which is the
    default, "lifetime" means the pruned window plus the buffer. That is why
    this is asserted against a frame built directly rather than through
    ``attach_events_to_scope``, which would have pruned the old event away.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row("C1", "2024-05-20 09:00:00"),
            scoped_row("C1", "2022-01-01 09:00:00"),
        ],
    )
    frequency = by_customer(
        calculate_event_frequency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    recency = by_customer(
        calculate_event_recency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert recency["cust_device_change_total_cnt"] == 3
    assert frequency["cust_device_change_cnt_30d"] == 2


def test_the_working_columns_do_not_leak_into_the_output(spark, narrow_cfg):
    """No ``__``-prefixed column survives.

    Every calculator uses double-underscore working names and drops them. One
    that did not would put a column called ``__reference_ts`` into the feature
    matrix, where the preprocessing stage selects by prefix and would either
    carry it as a feature or crash on its type.
    """
    events = scoped_frame(spark, [scoped_row("C1", "2024-05-31 09:00:00")])
    for frame in (
        calculate_event_recency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        ),
        calculate_event_frequency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        ),
        calculate_periodicity_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        ),
        calculate_burstness_features(events, ENTITY_COLS, DEVICE, "cust", narrow_cfg),
        calculate_time_to_k_events(
            events, ENTITY_COLS, DEVICE, "cust", 3, narrow_cfg
        ),
    ):
        leaked = [name for name in frame.columns if name.startswith("__")]
        assert leaked == []


# --------------------------------------------------------------------------
# calculate_periodicity_features
# --------------------------------------------------------------------------

def test_the_gaps_are_between_consecutive_events(spark, narrow_cfg):
    """Events on 1, 2 and 4 May: gaps of one and two days, mean 1.5."""
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-02 09:00:00"),
            scoped_row("C1", "2024-05-04 09:00:00"),
        ],
    )
    row = by_customer(
        calculate_periodicity_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_interval_mean"] == pytest.approx(1.5)
    assert row["cust_device_change_interval_min"] == 1
    assert row["cust_device_change_interval_max"] == 2


def test_the_coefficient_of_variation_is_the_spread_over_the_mean(
    spark, narrow_cfg
):
    """Gaps of one and two days: sample stddev sqrt(0.5), so a CV of 0.4714.

    The sample standard deviation rather than the population one, so two gaps
    give ``sqrt(((1-1.5)**2 + (2-1.5)**2) / 1)``. The CV is the feature that
    earns its place: it is scale-free, so a customer who acts monthly and one
    who acts daily are comparable on how *regular* they are rather than on how
    often.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-02 09:00:00"),
            scoped_row("C1", "2024-05-04 09:00:00"),
        ],
    )
    row = by_customer(
        calculate_periodicity_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_interval_stddev"] == pytest.approx(0.5**0.5)
    assert row["cust_device_change_interval_cv"] == pytest.approx(
        (0.5**0.5) / 1.5
    )


def test_one_event_has_no_gap_and_so_no_row(spark, narrow_cfg):
    """The entity is absent from this frame rather than present with zeros.

    Finding 3.1: one event has no inter-arrival gap, and zero is not a neutral
    filler for that — it says the entity acts constantly. Absent here means null
    after the assembly's left join, which is the honest answer. C2 has two
    events and is present, so the test distinguishes "absent" from "empty
    frame".
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C2", "2024-05-01 09:00:00"),
            scoped_row("C2", "2024-05-03 09:00:00"),
        ],
    )
    got = by_customer(
        calculate_periodicity_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )
    assert sorted(got) == ["C2"]
    assert got["C2"]["cust_device_change_interval_mean"] == pytest.approx(2.0)


def test_two_events_give_a_mean_but_no_spread_and_so_no_cv(spark, narrow_cfg):
    """One gap has a mean and no sample standard deviation, hence a null CV.

    ``stddev_samp`` of a single value is null, and ``safe_divide`` propagates
    it. The alternative — filling the standard deviation with zero — would say
    "perfectly regular" about an entity for which regularity is not yet
    defined, and perfectly regular is a meaningful and quite different claim.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-03 09:00:00"),
        ],
    )
    row = by_customer(
        calculate_periodicity_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_interval_mean"] == pytest.approx(2.0)
    assert row["cust_device_change_interval_stddev"] is None
    assert row["cust_device_change_interval_cv"] is None


def test_the_velocity_is_one_over_the_mean_gap(spark, narrow_cfg):
    """A mean gap of two days is half an event per day."""
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-03 09:00:00"),
        ],
    )
    row = by_customer(
        calculate_periodicity_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row[
        "cust_device_change_inverse_mean_interarrival_velocity"
    ] == pytest.approx(0.5)


def test_a_zero_mean_gap_gives_a_null_velocity_rather_than_an_infinity(
    spark, narrow_cfg
):
    """Two events on one day: a mean gap of zero, and a null velocity.

    Not an infinity and not a crash. Same-day events are the ordinary case for
    a carrier batch, so this is the common path rather than a pathological one,
    and ``safe_divide`` returning null is what keeps an infinity out of the
    feature matrix — where a tree model tolerates it and a scaler does not.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-01 15:00:00"),
        ],
    )
    row = by_customer(
        calculate_periodicity_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_interval_mean"] == pytest.approx(0.0)
    assert row["cust_device_change_inverse_mean_interarrival_velocity"] is None


# --------------------------------------------------------------------------
# calculate_burstness_features
# --------------------------------------------------------------------------

def test_the_borrowed_frequency_columns_are_dropped_again(spark, narrow_cfg):
    """The ratios are here; the counts they were computed from are not.

    Finding 2.3: the notebook's burstness calculator left the borrowed counts
    on its output, the assembly function computed them again, and the join kept
    whichever arrived first — so the shipped counts came from inside burstness
    with whatever config *that* call had been given, which per finding 1.7 was
    not necessarily the caller's. Dropping them means the counts in the
    assembled table come from exactly one call.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row("C1", "2024-05-20 09:00:00"),
        ],
    )
    got = calculate_burstness_features(
        events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
    )
    assert "cust_device_change_cnt_3d" not in got.columns
    assert "cust_device_change_cnt_30d" not in got.columns
    assert "cust_device_change_any_3d" not in got.columns
    assert "cust_device_change_weighted_cnt_3d" not in got.columns
    assert "cust_device_change_cnt_3d_over_30d" in got.columns


def test_the_ratios_come_from_the_config_and_not_from_a_literal(
    spark, narrow_cfg
):
    """One ratio, because ``narrow_cfg`` names one — and not the hardcoded three.

    The other half of finding 1.7. The notebook hardcoded 1/3, 7/30 and 30/90
    while taking the counts from config, so narrowing ``lookback_days`` gave an
    unresolved-column error from inside this helper rather than a message about
    the caller's own argument. ``FeatureConfig.__post_init__`` now rejects a
    ratio naming a window that ``lookback_days`` lacks, which is asserted in
    ``test_config.py``; this asserts the consequence here.
    """
    events = scoped_frame(spark, [scoped_row("C1", "2024-05-31 09:00:00")])
    got = calculate_burstness_features(
        events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
    )
    assert columns_matching(got, "_over_") == ["cust_device_change_cnt_3d_over_30d"]
    assert "cust_device_change_cnt_1d_over_3d" not in got.columns
    assert "cust_device_change_cnt_7d_over_30d" not in got.columns
    assert "cust_device_change_cnt_30d_over_90d" not in got.columns


def test_the_production_config_gives_the_three_documented_ratios(spark, cfg):
    """1/3, 7/30 and 30/90, which is what the shipped models were fitted on."""
    events = scoped_frame(spark, [scoped_row("C1", "2024-06-01 09:00:00")])
    got = calculate_burstness_features(events, ENTITY_COLS, DEVICE, "cust", cfg)
    assert columns_matching(got, "_over_") == [
        "cust_device_change_cnt_1d_over_3d",
        "cust_device_change_cnt_30d_over_90d",
        "cust_device_change_cnt_7d_over_30d",
    ]
    assert tuple(cfg.ratio_windows) == ((1, 3), (7, 30), (30, 90))


def test_the_ratio_is_the_short_count_over_the_long_one(spark, narrow_cfg):
    """One event in three days and four in thirty: a quarter.

    Near one would mean the long window contains nothing the short one does
    not, which is to say everything happened just now. A quarter is an entity
    with a history.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row("C1", "2024-05-20 09:00:00"),
            scoped_row("C1", "2024-05-15 09:00:00"),
            scoped_row("C1", "2024-05-10 09:00:00"),
        ],
    )
    row = by_customer(
        calculate_burstness_features(events, ENTITY_COLS, DEVICE, "cust", narrow_cfg)
    )["C1"]
    assert row["cust_device_change_cnt_3d_over_30d"] == pytest.approx(0.25)


def test_a_long_window_with_nothing_in_it_gives_a_null_ratio(spark, narrow_cfg):
    """Zero over zero is undefined, and undefined is null rather than zero.

    The entity's only event is two hundred days old, so both counts are zero. A
    ratio of zero would say "nothing recent relative to a busy history", which
    is a real and different fact from "nothing in either window".
    """
    events = scoped_frame(spark, [scoped_row("C1", "2023-11-14 09:00:00")])
    row = by_customer(
        calculate_burstness_features(events, ENTITY_COLS, DEVICE, "cust", narrow_cfg)
    )["C1"]
    assert row["cust_device_change_cnt_3d_over_30d"] is None


def test_the_peak_month_is_compared_to_the_average_month(spark, narrow_cfg):
    """Three events in March and one in May: peak 3, average 2, ratio 1.5.

    Two active months, not five — the average is over the months that had
    events, so a gap month does not dilute the peak. That is the choice that
    makes the feature about concentration *within* an entity's activity rather
    than about how long the entity has existed, which the tenure features
    already cover.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-03-01 09:00:00"),
            scoped_row("C1", "2024-03-02 09:00:00"),
            scoped_row("C1", "2024-03-03 09:00:00"),
            scoped_row("C1", "2024-05-01 09:00:00"),
        ],
    )
    row = by_customer(
        calculate_burstness_features(events, ENTITY_COLS, DEVICE, "cust", narrow_cfg)
    )["C1"]
    assert row["cust_device_change_max_monthly_cnt"] == 3
    assert row["cust_device_change_avg_monthly_cnt"] == pytest.approx(2.0)
    assert row["cust_device_change_active_months"] == 2
    assert row["cust_device_change_monthly_peak_to_avg"] == pytest.approx(1.5)


# --------------------------------------------------------------------------
# calculate_temporal_entropy_features
# --------------------------------------------------------------------------

def test_the_entropy_is_zero_when_every_event_landed_in_one_bucket(
    spark, narrow_cfg
):
    """Three events at the same hour on the same day: zero on all three axes.

    Zero hour-entropy is what a scheduled job looks like, and the reason this
    family exists. Asserted on all three dimensions at once because the three
    are computed by the same helper over different derived columns, so a defect
    in the helper shows up in all of them and a defect in one derived column
    shows up in exactly one.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-01 09:10:00"),
            scoped_row("C1", "2024-05-01 09:20:00"),
        ],
    )
    row = by_customer(
        calculate_temporal_entropy_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_event_hour_entropy"] == pytest.approx(0.0)
    assert row["cust_device_change_event_day_of_week_entropy"] == pytest.approx(0.0)
    assert row["cust_device_change_event_month_entropy"] == pytest.approx(0.0)


def test_two_hours_evenly_used_give_exactly_one_bit(spark, narrow_cfg):
    """One event at nine and one at three: one bit of hour entropy, zero of the rest.

    Both events are on the same date, so the weekday and month buckets are
    single. Exactly one bit rather than "greater than zero", because the units
    matter: ``entropy_over_dimension`` uses ``log2``, and a helper that switched
    to natural log would return 0.693 here and every downstream threshold
    fitted against bits would move.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-01 15:00:00"),
        ],
    )
    row = by_customer(
        calculate_temporal_entropy_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_event_hour_entropy"] == pytest.approx(1.0)
    assert row["cust_device_change_event_day_of_week_entropy"] == pytest.approx(0.0)


def test_the_entropy_base_is_the_filtered_frame(spark, narrow_cfg):
    """An entity with no events of the family is absent, not present with zeros.

    Unlike the frequency and recency calculators, which build their base from
    the whole scoped frame, this one bases on the filtered frame — so C2, whose
    only event is a SIM change, does not appear in the device-change entropy at
    all. Harmless in the assembled table, where every family is left-joined onto
    one base and C2 picks up nulls either way, and recorded as finding 4.5
    because the inconsistency is the kind of thing that makes a reader distrust
    the null they are looking at. Pinned so that unifying it is a deliberate
    change with a failing test attached.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C2", "2024-05-01 09:00:00", "SIM_CHANGE"),
        ],
    )
    entropy = by_customer(
        calculate_temporal_entropy_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )
    frequency = by_customer(
        calculate_event_frequency_features(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )
    assert sorted(entropy) == ["C1"]
    assert sorted(frequency) == ["C1", "C2"]


# --------------------------------------------------------------------------
# calculate_time_to_k_events
# --------------------------------------------------------------------------

def test_the_span_is_the_days_the_last_k_events_took(spark, narrow_cfg):
    """Four events, k=3: the last three span 3 May to 10 May, so seven days.

    The *last* k, not the first, because the question is how fast the entity is
    moving now. The velocity is k over the span — three events in seven days —
    rather than the reciprocal of the mean gap, which the periodicity family
    already reports and which weights the gaps differently.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-03 09:00:00"),
            scoped_row("C1", "2024-05-06 09:00:00"),
            scoped_row("C1", "2024-05-10 09:00:00"),
        ],
    )
    row = by_customer(
        calculate_time_to_k_events(
            events, ENTITY_COLS, DEVICE, "cust", 3, narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_days_for_last_3_events"] == 7
    assert row["cust_device_change_k_event_velocity_3"] == pytest.approx(3.0 / 7.0)


def test_fewer_than_k_events_gives_a_null_span_and_not_a_shorter_one(
    spark, narrow_cfg
):
    """Two events with k=3: null, because "the last three" did not happen.

    Computing the span over however many events exist would put "one device
    change" on the same scale as "three device changes in two days", and the
    two are not the same shape of fact. The row still exists — the entity is in
    the frame — so the null is an answer rather than an absence.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-03 09:00:00"),
        ],
    )
    got = by_customer(
        calculate_time_to_k_events(
            events, ENTITY_COLS, DEVICE, "cust", 3, narrow_cfg
        )
    )
    assert sorted(got) == ["C1"]
    assert got["C1"]["cust_device_change_days_for_last_3_events"] is None
    assert got["C1"]["cust_device_change_k_event_velocity_3"] is None


def test_k_events_in_one_day_give_a_zero_span_and_a_null_velocity(
    spark, narrow_cfg
):
    """Three events in one batch: the span is zero and the rate is undefined.

    Zero is the right span — they really did take no days — and the velocity is
    genuinely undefined rather than infinite. This is the case a carrier batch
    produces routinely, so it is the common path.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-01 09:00:01"),
            scoped_row("C1", "2024-05-01 09:00:02"),
        ],
    )
    row = by_customer(
        calculate_time_to_k_events(
            events, ENTITY_COLS, DEVICE, "cust", 3, narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_days_for_last_3_events"] == 0
    assert row["cust_device_change_k_event_velocity_3"] is None


# --------------------------------------------------------------------------
# calculate_recency_weighted_intensity
# --------------------------------------------------------------------------

def test_an_event_today_contributes_one_and_one_half_life_ago_a_half(
    spark, narrow_cfg
):
    """Reference 1 June, events on 1 June and 25 May: 1.0 + 0.5 = 1.5.

    Seven days is the default half life, so the arithmetic is exact rather than
    approximate up to floating point. This is the windowed counts and the
    recency features folded into one number, and its virtue is that it has no
    cliff — see the two-hundred-day test below.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-06-01 09:00:00"),
            scoped_row("C1", "2024-05-25 09:00:00"),
        ],
    )
    row = by_customer(
        calculate_recency_weighted_intensity(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row[
        "cust_device_change_recency_weighted_intensity"
    ] == pytest.approx(1.5)


def test_nothing_falls_out_because_there_is_no_window(spark, narrow_cfg):
    """A two-hundred-day-old event contributes a tiny amount, not nothing.

    Every count feature in the package has a cliff at ninety days: an event
    matters on day ninety and does not on day ninety-one. This feature is the
    one that does not, so a positive contribution from an event far outside
    every window is the property rather than a rounding artefact. Asserted as
    strictly greater than zero and smaller than any window count could be.
    """
    events = scoped_frame(spark, [scoped_row("C1", "2023-11-14 09:00:00")])
    value = by_customer(
        calculate_recency_weighted_intensity(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]["cust_device_change_recency_weighted_intensity"]
    assert value > 0.0
    assert value < 1e-6


def test_the_half_life_can_be_overridden_per_call(spark, narrow_cfg):
    """A fourteen-day half life makes a fourteen-day-old event weigh a half.

    The override exists because the right half life is an empirical question
    and the config default is one answer to it. Asserted against the same event
    weighing a quarter under the seven-day default, so the test fails if the
    argument is accepted and ignored.
    """
    events = scoped_frame(spark, [scoped_row("C1", "2024-05-18 09:00:00")])
    overridden = by_customer(
        calculate_recency_weighted_intensity(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg, half_life_days=14.0
        )
    )["C1"]["cust_device_change_recency_weighted_intensity"]
    default = by_customer(
        calculate_recency_weighted_intensity(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]["cust_device_change_recency_weighted_intensity"]
    assert overridden == pytest.approx(0.5)
    assert default == pytest.approx(0.25)


@pytest.mark.parametrize("bad", [0.0, -1.0])
def test_a_non_positive_half_life_is_rejected_at_the_call(spark, narrow_cfg, bad):
    """Zero would divide by zero and negative would grow the weight with age.

    Raised rather than clamped, and raised before any Spark work, because a
    negative half life produces a plausible column — an intensity that rewards
    old events — and finding-style silence is exactly what this package is
    trying not to repeat.
    """
    events = scoped_frame(spark, [scoped_row("C1", "2024-05-31 09:00:00")])
    with pytest.raises(ValueError, match="half_life_days must be positive"):
        calculate_recency_weighted_intensity(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg, half_life_days=bad
        )


# --------------------------------------------------------------------------
# calculate_cumulative_event_slope
# --------------------------------------------------------------------------

def test_the_slope_is_the_rate_of_the_running_total(spark, narrow_cfg):
    """Three events on consecutive days: a cumulative count rising by one a day.

    The running total against elapsed days is a straight line here, so the
    fitted slope is exactly one. Asserted exactly because a defect in the
    elapsed-day baseline — using the reference rather than the entity's first
    event — would not change the sign or the rough magnitude, only the value.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-02 09:00:00"),
            scoped_row("C1", "2024-05-03 09:00:00"),
        ],
    )
    row = by_customer(
        calculate_cumulative_event_slope(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_cumulative_event_slope"] == pytest.approx(1.0)


def test_a_faster_entity_has_a_steeper_slope(spark, narrow_cfg):
    """Three events in two days against three in twenty: 1.0 against 0.1.

    Both entities have the same count, so this is the feature saying something
    no count says. The two are asserted against each other as well as by value,
    since the ordering is what the model uses.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C1", "2024-05-02 09:00:00"),
            scoped_row("C1", "2024-05-03 09:00:00"),
            scoped_row("C2", "2024-05-01 09:00:00"),
            scoped_row("C2", "2024-05-11 09:00:00"),
            scoped_row("C2", "2024-05-21 09:00:00"),
        ],
    )
    got = by_customer(
        calculate_cumulative_event_slope(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )
    fast = got["C1"]["cust_device_change_cumulative_event_slope"]
    slow = got["C2"]["cust_device_change_cumulative_event_slope"]
    assert fast == pytest.approx(1.0)
    assert slow == pytest.approx(0.1)
    assert fast > slow


def test_one_event_gives_a_null_slope(spark, narrow_cfg):
    """A line through one point has no slope, and the answer is null.

    ``regr_slope`` returns null rather than raising, which is the behaviour
    wanted here — but it is worth pinning, because the obvious alternative
    implementations (a hand-rolled least squares over sums) return a
    zero-division or a NaN instead, and a NaN in a feature matrix survives
    every null check and breaks the scaler.
    """
    events = scoped_frame(spark, [scoped_row("C1", "2024-05-01 09:00:00")])
    row = by_customer(
        calculate_cumulative_event_slope(
            events, ENTITY_COLS, DEVICE, "cust", narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_cumulative_event_slope"] is None


# --------------------------------------------------------------------------
# calculate_state_transition_features
# --------------------------------------------------------------------------

def test_a_target_at_the_same_instant_is_a_zero_day_transition(spark, cfg):
    """Finding 1.5, asserted as the value zero.

    A device change and a SIM change arriving in one carrier batch share a
    timestamp, because the carrier stamps the batch and not the event, and that
    pair — a new device and a new SIM at the same moment — is one of the
    sharpest signals in the feature set. With the tie-break inverted, as the
    notebook had it, the simultaneous target sorts *after* the source and falls
    outside its window frame, so the transition is recorded as never having
    happened.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C1", "2024-05-01 09:00:00", "SIM_CHANGE"),
        ],
    )
    row = by_customer(
        calculate_state_transition_features(
            events, ENTITY_COLS, [("DEVICE_CHANGE", "SIM_CHANGE")], "cust", cfg
        )
    )["C1"]
    assert row["cust_device_change_to_sim_change_min_transition_days"] == 0
    assert row["cust_device_change_to_sim_change_avg_transition_days"] == (
        pytest.approx(0.0)
    )


def test_a_simultaneous_target_wins_over_a_later_one(spark, cfg):
    """The damaging half of finding 1.5: a plausible wrong number, not a null.

    A SIM change at the same instant as the device change and another a
    fortnight later. With the tie-break inverted the simultaneous one is
    invisible and the transition is reported as fourteen days — a number a
    reviewer would accept. So the assertion that matters is the inequality
    against fourteen, not the null check that a missing-transition defect would
    trip.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C1", "2024-05-01 09:00:00", "SIM_CHANGE"),
            scoped_row("C1", "2024-05-15 09:00:00", "SIM_CHANGE"),
        ],
    )
    row = by_customer(
        calculate_state_transition_features(
            events, ENTITY_COLS, [("DEVICE_CHANGE", "SIM_CHANGE")], "cust", cfg
        )
    )["C1"]
    assert row["cust_device_change_to_sim_change_min_transition_days"] == 0
    assert row["cust_device_change_to_sim_change_min_transition_days"] != 14


def test_the_ordinary_case_is_the_soonest_target_after_the_source(spark, cfg):
    """A SIM change three days after a device change, and another ten days on.

    The soonest, not the last and not the mean of both. This is the case that
    breaks if the tie-break is fixed by reversing the primary timestamp
    ordering instead — a plausible-looking alternative that makes simultaneous
    events work and everything else wrong.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C1", "2024-05-04 09:00:00", "SIM_CHANGE"),
            scoped_row("C1", "2024-05-14 09:00:00", "SIM_CHANGE"),
        ],
    )
    row = by_customer(
        calculate_state_transition_features(
            events, ENTITY_COLS, [("DEVICE_CHANGE", "SIM_CHANGE")], "cust", cfg
        )
    )["C1"]
    assert row["cust_device_change_to_sim_change_min_transition_days"] == 3


def test_a_target_before_the_source_is_not_a_transition(spark, cfg):
    """Order matters: a SIM change five days *before* is not "the next one".

    The pair is directional — after a device change, how long until a SIM
    change — and the reverse direction is its own pair with its own columns. An
    implementation that took the nearest target in either direction would give
    this entity a five-day transition and would make every pair symmetric,
    halving the information in the family.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-04-26 09:00:00", "SIM_CHANGE"),
            scoped_row("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
        ],
    )
    row = by_customer(
        calculate_state_transition_features(
            events, ENTITY_COLS, [("DEVICE_CHANGE", "SIM_CHANGE")], "cust", cfg
        )
    )["C1"]
    assert row["cust_device_change_to_sim_change_min_transition_days"] is None
    reverse = by_customer(
        calculate_state_transition_features(
            events, ENTITY_COLS, [("SIM_CHANGE", "DEVICE_CHANGE")], "cust", cfg
        )
    )["C1"]
    assert reverse["cust_sim_change_to_device_change_min_transition_days"] == 5


def test_the_cancellation_transitions_are_reachable(spark, cfg):
    """Finding 1.2, at the calculator rather than the declaration.

    ``test_config.py`` asserts the transition list spells the type
    ``STATUS_CHANGE-C`` with a hyphen. This asserts the consequence: a device
    change followed by a cancellation produces a number. That pair is the
    signature of a device being taken and the account abandoned, and under the
    misspelling the comparison was false for every row so the column was null
    for the whole population.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C1", "2024-05-03 09:00:00", "STATUS_CHANGE-C"),
        ],
    )
    got = calculate_state_transition_features(
        events, ENTITY_COLS, [("DEVICE_CHANGE", "STATUS_CHANGE-C")], "cust", cfg
    )
    column = "cust_device_change_to_status_change_c_min_transition_days"
    assert column in got.columns
    assert by_customer(got)["C1"][column] == 2


def test_no_column_name_carries_the_raw_hyphen(spark, cfg):
    """``STATUS_CHANGE-C`` becomes ``status_change_c`` in every name.

    A hyphen in a Spark column name has to be back-quoted in every SQL
    expression that touches it, and the one place that forgets is a runtime
    error a long way from here — in the preprocessing stage, most likely, which
    selects features by prefix.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C1", "2024-05-03 09:00:00", "STATUS_CHANGE-C"),
        ],
    )
    got = calculate_state_transition_features(
        events,
        ENTITY_COLS,
        [("DEVICE_CHANGE", "STATUS_CHANGE-C"), ("STATUS_CHANGE-C", "DEVICE_CHANGE")],
        "cust",
        cfg,
    )
    assert [name for name in got.columns if "-" in name] == []
    assert "cust_status_change_c_to_device_change_avg_transition_days" in got.columns


def test_the_velocity_is_null_when_the_mean_is_zero(spark, cfg):
    """A zero-day transition has an undefined velocity rather than an infinite one.

    And zero-day transitions are the common case, per the simultaneous-batch
    test above, so this is the path most entities with both event types take.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C1", "2024-05-01 09:00:00", "SIM_CHANGE"),
        ],
    )
    row = by_customer(
        calculate_state_transition_features(
            events, ENTITY_COLS, [("DEVICE_CHANGE", "SIM_CHANGE")], "cust", cfg
        )
    )["C1"]
    assert row["cust_device_change_to_sim_change_transition_velocity"] is None


def test_only_the_named_types_are_read(spark, cfg):
    """A type absent from the transition list contributes no columns.

    Narrowing the list is how a caller builds a cheaper feature set — the full
    seventy-two pairs are three columns each — so the narrowing has to actually
    narrow. The MNO port in this frame is ignored entirely.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C1", "2024-05-03 09:00:00", "SIM_CHANGE"),
            scoped_row("C1", "2024-05-05 09:00:00", "MNO_PORT"),
        ],
    )
    got = calculate_state_transition_features(
        events, ENTITY_COLS, [("DEVICE_CHANGE", "SIM_CHANGE")], "cust", cfg
    )
    assert columns_matching(got, "mno_port") == []
    assert len(got.columns) == len(ENTITY_COLS) + 3


def test_an_empty_transition_list_returns_just_the_entities(spark, cfg):
    """No pairs means no features, and the entities still come back.

    The early return keeps ``assemble_scope_features`` able to pass
    ``state_transitions=()`` without a special case, and returning the entity
    base rather than an empty frame is what keeps the assembly's left join
    correct.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00"),
            scoped_row("C2", "2024-05-01 09:00:00"),
        ],
    )
    got = calculate_state_transition_features(events, ENTITY_COLS, [], "cust", cfg)
    assert list(got.columns) == list(ENTITY_COLS)
    assert got.count() == 2


def test_the_transition_is_per_entity_and_not_pooled(spark, cfg):
    """C1's device change does not see C2's SIM change.

    The window is partitioned by the entity columns. Pooling would give every
    entity in a batch the population's shortest transition, which is a feature
    that looks discriminative in training and cannot generalise.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", "DEVICE_CHANGE"),
            scoped_row("C2", "2024-05-02 09:00:00", "SIM_CHANGE"),
        ],
    )
    got = by_customer(
        calculate_state_transition_features(
            events, ENTITY_COLS, [("DEVICE_CHANGE", "SIM_CHANGE")], "cust", cfg
        )
    )
    assert got["C1"]["cust_device_change_to_sim_change_min_transition_days"] is None


# --------------------------------------------------------------------------
# calculate_event_diversity_features
# --------------------------------------------------------------------------

def test_the_distinct_and_non_null_counts_are_different_questions(
    spark, narrow_cfg
):
    """Industries of A, A and null: one distinct, two non-null, three events.

    Both are kept because they answer different things. Distinct is breadth —
    how many industries have asked about this number — and non-null is
    coverage, which is a property of the feed rather than of the entity, and a
    diversity of one over two calls means something different from a diversity
    of one over two hundred.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", industry="finance"),
            scoped_row("C1", "2024-05-02 09:00:00", industry="finance"),
            scoped_row("C1", "2024-05-03 09:00:00", industry=None),
        ],
    )
    row = by_customer(
        calculate_event_diversity_features(
            events, ENTITY_COLS, DEVICE, "cust", ["industry"], narrow_cfg
        )
    )["C1"]
    assert row["cust_device_change_industry_distinct_cnt"] == 1
    assert row["cust_device_change_industry_non_null_cnt"] == 2


def test_a_dimension_absent_from_the_frame_is_skipped_not_nulled(
    spark, narrow_cfg
):
    """No column at all, because the absence is a fact about the feed.

    A column of nulls under a name like
    ``cust_device_change_partner_type_distinct_cnt`` would pass through feature
    selection — where a constant column is dropped by a variance filter if
    there is one and carried if there is not — and end up in a shipped feature
    list describing nothing. That is precisely the shape of finding 1.1's third
    and most expensive consequence.
    """
    events = scoped_frame(
        spark, [scoped_row("C1", "2024-05-01 09:00:00", industry="finance")]
    ).drop("service_provider_name")
    got = calculate_event_diversity_features(
        events,
        ENTITY_COLS,
        DEVICE,
        "cust",
        list(ENSTREAM_DIMENSION_COLS),
        narrow_cfg,
    )
    assert columns_matching(got, "service_provider_name") == []
    assert "cust_device_change_industry_distinct_cnt" in got.columns
    assert "cust_device_change_use_case_distinct_cnt" in got.columns


# --------------------------------------------------------------------------
# calculate_stability_features
# --------------------------------------------------------------------------

@pytest.fixture
def snapshots(spark, lineage_df, cfg):
    """The three scope snapshots and the base, at the shared reference moment."""
    references = build_reference_df_at_timestamp(lineage_df, ts(REFERENCE_TS), cfg)
    return build_scope_snapshots(lineage_df, references, cfg)


def test_the_prefix_comes_from_the_scope_and_not_from_the_scope_name(
    snapshots, cfg
):
    """Finding 1.6: every column at the customer scope starts ``cust_``.

    The notebook's EnStream wrapper passed ``scope`` where ``prefix`` belonged,
    so some columns came out ``customer_enstream_api_call_*`` while everything
    else at the same scope was ``cust_*``. The columns held correct values, which
    is why it survived — but the preprocessing stage selects features by prefix,
    so those columns were silently omitted from whatever it selected. Asserted
    at all three scopes below; here, the negative half.
    """
    got = calculate_stability_features(snapshots, "customer", cfg)
    features = [
        name for name in got.columns if name not in cfg.entity_cols_for("customer")
    ]
    assert features
    for name in features:
        assert name.startswith("cust_")
        assert not name.startswith("customer_")


@pytest.mark.parametrize("scope", SCOPES)
def test_every_scope_prefixes_its_columns_with_scope_prefix(snapshots, cfg, scope):
    """All three scopes, including the one where the bug was invisible.

    At the MSISDN scope the scope name and the prefix are the same string, which
    is presumably why finding 1.6 was never noticed: the only scope anyone
    checked was the one where passing the wrong argument made no difference.
    Parametrising over all three is the cheap insurance against that.
    """
    got = calculate_stability_features(snapshots, scope, cfg)
    prefix = scope_prefix(scope)
    features = [
        name for name in got.columns if name not in cfg.entity_cols_for(scope)
    ]
    assert features
    for name in features:
        assert name.startswith(f"{prefix}_")


def test_days_since_first_seen_is_the_earliest_interval_and_the_assignment_the_latest(
    snapshots, cfg
):
    """Finding 4.4: C3 has been known since June 2021 and moved carrier in February.

    Reference 1 June 2024. C3's Rogers interval opened on 2021-06-01, which is
    1096 days earlier, and its Telus interval on 2024-02-01, 121 days earlier.
    Two columns rather than one because they answer different questions — how
    long we have known this customer, and how new the arrangement it is in now
    is — and a customer of three years who added a line last week is a different
    risk from a customer of one week.

    Which is precisely what the second draft of the calculator could not tell
    you. It took both the minimum and the maximum ``from_ts`` from
    ``snapshot_scope["customer"]``, where ``build_scope_snapshots`` has already
    collapsed the intervals to one row holding ``min(from_ts)``. The maximum of
    a single value is that value, so the assignment column was a verbatim copy
    of the tenure column for every entity at every scope, and C3 reported 1096
    twice. The inequality below is load-bearing: a version that reads the
    aggregated snapshot passes the first assertion and fails the rest.
    """
    got = collect_features(
        calculate_stability_features(snapshots, "customer", cfg),
        cfg.entity_cols_for("customer"),
    )
    row = {key[1]: values for key, values in got.items()}["C3"]
    assert row["cust_days_since_first_seen"] == 1096
    assert row["cust_days_since_current_assignment_start"] == 121
    assert (
        row["cust_days_since_current_assignment_start"]
        != row["cust_days_since_first_seen"]
    )


def test_the_breadth_counts_come_from_the_base_and_not_the_scope_snapshot(
    snapshots, cfg
):
    """Finding 4.4: C3 holds two accounts and two numbers, and the columns say so.

    A draft of this calculator counted breadth from the aggregated scope
    snapshot. At the customer scope that frame has no ``acct_id`` or ``msisdn``
    column at all — they are aggregated away — so both counts were typed nulls;
    at the account and MSISDN scopes the columns are grouping keys, so counting
    distinct values within a group necessarily gave 1. Every value this family
    produced was null or the constant one, with nothing raising.

    "How many accounts does this customer hold" is the most useful thing here
    and it is only answerable from ``snapshot_scope["base"]``, the un-aggregated
    join. C3 is the row that proves it: two accounts, on two carriers, with two
    numbers. C1 holds one of each, so the test also fails if the counts are
    computed but pooled across the population.
    """
    got = collect_features(
        calculate_stability_features(snapshots, "customer", cfg),
        cfg.entity_cols_for("customer"),
    )
    rows = {key[1]: values for key, values in got.items()}
    assert rows["C3"]["cust_distinct_account_cnt"] == 2
    assert rows["C3"]["cust_distinct_msisdn_cnt"] == 2
    assert rows["C1"]["cust_distinct_account_cnt"] == 1
    assert rows["C1"]["cust_distinct_msisdn_cnt"] == 1


def test_the_account_scope_counts_the_numbers_on_the_account(snapshots, cfg):
    """One number per account in this population, and the column is a 1 not a null.

    The value is uninteresting; that it is a number is the point. Under the
    finding 4.4 behaviour ``acct_distinct_msisdn_cnt`` was null for every row,
    because the account snapshot has no ``msisdn`` column — so the feature that
    would catch an account cycling through numbers did not exist.
    """
    got = collect_features(
        calculate_stability_features(snapshots, "account", cfg),
        cfg.entity_cols_for("account"),
    )
    for values in got.values():
        assert values["acct_distinct_msisdn_cnt"] == 1
        assert values["acct_distinct_account_cnt"] == 1


def test_the_stability_features_need_no_events(snapshots, cfg):
    """C6 is in every feed's blind spot and has a full stability row.

    These are the most robust features in the set precisely because no event
    frame is involved: they survive a carrier feed going quiet, which is the
    failure mode the whole pipeline is most exposed to. C6 appears in the
    lineage and in no event feed, and its tenure is measured from May 2019.
    """
    got = collect_features(
        calculate_stability_features(snapshots, "customer", cfg),
        cfg.entity_cols_for("customer"),
    )
    row = {key[1]: values for key, values in got.items()}["C6"]
    assert row["cust_days_since_first_seen"] == 1858
    assert row["cust_distinct_account_cnt"] == 1


def test_a_missing_scope_entry_names_the_builder(snapshots, cfg):
    """The error tells the caller which function produces the thing they lack."""
    with pytest.raises(KeyError, match="build_scope_snapshots"):
        calculate_stability_features({"customer": snapshots["customer"]}, "customer", cfg)


def test_a_missing_base_entry_is_an_error_rather_than_a_column_of_nulls(
    snapshots, cfg
):
    """The breadth counts have one possible source, so its absence has to raise.

    Falling back to the scope snapshot would silently reproduce finding 4.4 —
    null and constant columns, nothing raised — which is the exact failure mode
    this package exists to stop repeating. So the fallback is an error instead.
    """
    partial = {
        scope: frame for scope, frame in snapshots.items() if scope != "base"
    }
    with pytest.raises(KeyError, match="'base'"):
        calculate_stability_features(partial, "customer", cfg)


def test_an_unsupported_scope_is_rejected(snapshots, cfg):
    """A typo in a scope name is an error and not an empty frame."""
    with pytest.raises(KeyError, match="snapshot_scope has no"):
        calculate_stability_features(snapshots, "custommer", cfg)


def test_the_stability_features_are_one_row_per_entity(snapshots, cfg):
    """Five customers at the June reference, five rows, no duplicates.

    C4's interval closed in March so it is absent from the reference
    population; the other six lineage customers minus C4 is five. Worth
    asserting because the calculator joins two aggregations onto a distinct
    base, and a join key mismatch in either would fan the frame out rather than
    fail.
    """
    got = calculate_stability_features(snapshots, "customer", cfg)
    entity_cols = list(cfg.entity_cols_for("customer"))
    assert got.count() == 5
    assert got.select(*entity_cols).distinct().count() == 5


def test_the_reference_population_is_the_expected_five_customers(snapshots):
    """A guard on the fixture the stability assertions are counted against."""
    customers = sorted(
        row["customer_id"] for row in snapshots["customer"].collect()
    )
    assert customers == ["C1", "C2", "C3", "C5", "C6"]


def test_a_default_config_is_used_when_none_is_given(spark, snapshots):
    """Each calculator defaults ``cfg``, and the default is the production one."""
    events = scoped_frame(spark, [scoped_row("C1", "2024-06-01 09:00:00")])
    frequency = calculate_event_frequency_features(
        events, ENTITY_COLS, DEVICE, "cust"
    )
    assert "cust_device_change_cnt_90d" in frequency.columns
    stability = calculate_stability_features(snapshots, "customer")
    assert "cust_days_since_first_seen" in stability.columns
    assert FeatureConfig().max_lookback_days == 90
    assert CANCELLATION == "account_cancellation"
    assert SIM == "sim_change"
