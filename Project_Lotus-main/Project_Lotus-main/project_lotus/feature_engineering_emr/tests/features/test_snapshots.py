"""References, scopes, and the event join — including finding 1.8.

Three things are settled here.

*The lineage handoff.* ``EXPECTED_LINEAGE_COLUMNS`` in ``conftest.py`` is the
phase-1/phase-2 contract written down. The assertions against it are the ones
the conftest docstring promises, and their point is that a change on either side
of that boundary fails here rather than in a cluster run three hours in.

*Finding 1.8.* ``build_reference_df`` had an ``if``/``elif`` with no ``else``, so
calling it with both routes or with neither raised ``UnboundLocalError`` from the
return statement — a name error several frames from the mistake, naming a local
variable rather than the argument that was wrong.

*The recycled number.* ``P6`` moves from ``C4`` to ``C5`` on 1 March 2024. Which
customer a fraud on that number belongs to depends on the date of the fraud, and
on the handover date itself it depends on a documented tie-break. This is the row
the fixture population exists for.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark", reason="snapshots are Spark code")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402
from tests.features.conftest import (  # noqa: E402
    EXPECTED_LINEAGE_COLUMNS,
    LINEAGE_ROWS,
    REFERENCE_TS,
    events_frame,
    make_event,
    ts,
)

from ts05_features.library.config import SCOPES, FeatureConfig  # noqa: E402
from ts05_features.library.snapshots import (  # noqa: E402
    attach_events_to_scope,
    build_reference_df,
    build_reference_df_at_timestamp,
    build_reference_df_from_msisdns,
    build_scope_snapshots,
)

pytestmark = pytest.mark.spark


# --------------------------------------------------------------------------
# the lineage contract
# --------------------------------------------------------------------------


def test_every_lineage_column_the_features_read_is_in_the_contract(cfg):
    """The conftest promise, and the direction of the check matters.

    The feature package reads six columns out of the lineage. The contract
    declares thirteen. What has to hold is that the six are a subset of the
    thirteen — not the reverse, since the extra nine are provenance and
    confidence columns that phase 2 carries but never reads.

    Checked against ``FeatureConfig`` rather than against a literal, so renaming
    a config field without updating the contract fails here.
    """
    read_by_features = (
        cfg.customer_col,
        cfg.account_col,
        cfg.msisdn_col,
        cfg.mno_col,
        cfg.from_ts_col,
        cfg.to_ts_col,
    )
    for col_name in read_by_features:
        assert col_name in EXPECTED_LINEAGE_COLUMNS


def test_the_contract_has_no_duplicate_names():
    """A duplicated name in the handoff frame is what ``dedupe_columns`` exists
    to survive, and a contract that declared one would mean the two sides
    disagreed about which column a name refers to."""
    assert len(EXPECTED_LINEAGE_COLUMNS) == len(set(EXPECTED_LINEAGE_COLUMNS))


def test_the_full_thirteen_column_frame_snapshots_the_same_as_the_narrow_one(
    spark, cfg, lineage_df
):
    """The extra contract columns are tolerated and dropped, not carried.

    The suite's fixture frame has the six columns the features read. The real
    lineage table has thirteen. A snapshot builder that projected ``*`` would
    produce a wider frame from the real table than from the fixture, and then
    every downstream ``feature_columns`` call would count ``lifecycle_uid`` as a
    feature and hand it to a model as a categorical with a million levels.
    """
    extra = [
        col_name
        for col_name in EXPECTED_LINEAGE_COLUMNS
        if col_name not in lineage_df.columns
    ]
    wide = lineage_df
    for col_name in extra:
        wide = wide.withColumn(col_name, F.lit("x"))
    assert len(wide.columns) == len(EXPECTED_LINEAGE_COLUMNS)

    references = build_reference_df_at_timestamp(wide, REFERENCE_TS, cfg)
    narrow_snapshots = build_scope_snapshots(
        lineage_df, build_reference_df_at_timestamp(lineage_df, REFERENCE_TS, cfg), cfg
    )
    wide_snapshots = build_scope_snapshots(wide, references, cfg)

    for scope in SCOPES:
        assert wide_snapshots[scope].columns == narrow_snapshots[scope].columns
        assert wide_snapshots[scope].count() == narrow_snapshots[scope].count()


# --------------------------------------------------------------------------
# finding 1.8: the dispatch with no else
# --------------------------------------------------------------------------


def test_neither_route_is_an_explicit_error_naming_both_arguments(spark, cfg, lineage_df):
    """Finding 1.8. ``UnboundLocalError: local variable 'reference_df'
    referenced before assignment`` names a local; this names the two arguments
    and says what each one is for.

    Which is the whole value of the fix: the notebook error appeared at the
    ``return``, not at the call, so the traceback pointed at the function's last
    line and told the reader nothing about which argument they had forgotten.
    """
    with pytest.raises(ValueError, match="reference_ts"):
        build_reference_df(lineage_df, None, None, cfg)
    with pytest.raises(ValueError, match="msisdn_reference_df"):
        build_reference_df(lineage_df, None, None, cfg)


def test_both_routes_together_resolves_to_the_msisdn_route(spark, cfg, lineage_df):
    """The other half of 1.8, and it is a resolution rather than an error.

    Both arguments given is a real call: a fraud list of phone numbers, all to
    be described as of one common date. The notebook's ``elif`` chain had no
    branch for it and fell through to the same ``UnboundLocalError``. The
    numbers win because the numbers are the population — the timestamp is only
    the moment they are all observed at.
    """
    lookups = spark.createDataFrame([("P6",)], ["msisdn"])

    resolved = build_reference_df(lineage_df, lookups, REFERENCE_TS, cfg)
    rows = resolved.collect()

    assert len(rows) == 1
    assert rows[0]["msisdn"] == "P6"
    assert rows[0]["customer_id"] == "C5"
    assert rows[0]["reference_ts"] == ts(REFERENCE_TS)


def test_the_timestamp_route_alone_describes_every_customer(spark, cfg, lineage_df):
    """One reference per customer the lineage knows, at one moment.

    Six customers, including ``C4`` whose only assignment had already ended.
    A reference is created for it and the *snapshot* is what drops it — the
    reference stage does not decide who was active.
    """
    references = build_reference_df(lineage_df, None, REFERENCE_TS, cfg)
    customers = {row["customer_id"] for row in references.collect()}
    assert customers == {"C1", "C2", "C3", "C4", "C5", "C6"}
    assert references.count() == 6


def test_the_msisdn_route_alone_needs_a_moment_on_every_row(spark, cfg, lineage_df):
    """Otherwise each reference has no moment and there is nothing to be as-of.

    The message says why — "each reference needs its own moment" — because the
    caller's usual mistake is a fraud list whose timestamp column is named
    something else, and knowing that a per-row timestamp is what is wanted is
    the difference between renaming a column and passing a global date.
    """
    lookups = spark.createDataFrame([("P6",)], ["msisdn"])
    with pytest.raises(ValueError, match="must be a column of msisdn_reference_df"):
        build_reference_df_from_msisdns(lineage_df, lookups, None, cfg)


# --------------------------------------------------------------------------
# the recycled number
# --------------------------------------------------------------------------


def _resolve(spark, cfg, lineage_df, msisdn, when):
    lookups = spark.createDataFrame(
        [(msisdn, ts(when))],
        T.StructType(
            [
                T.StructField("msisdn", T.StringType()),
                T.StructField("reference_ts", T.TimestampType()),
            ]
        ),
    )
    return build_reference_df_from_msisdns(lineage_df, lookups, None, cfg).collect()


def test_the_same_number_resolves_to_different_customers_on_different_dates(
    spark, cfg, lineage_df
):
    """``P6`` was ``C4``'s until March 2024 and has been ``C5``'s since.

    This is the whole reason the reference carries a moment rather than being a
    customer lookup. A fraud in February belongs to ``C4``'s behaviour and a
    fraud in June to ``C5``'s, and a pipeline that resolved the number against
    the *current* lineage would label ``C5`` with ``C4``'s fraud — which is a
    training row whose features and whose label describe two different people.
    """
    february = _resolve(spark, cfg, lineage_df, "P6", "2024-02-01 09:00:00")
    june = _resolve(spark, cfg, lineage_df, "P6", "2024-06-01 09:00:00")

    assert [row["customer_id"] for row in february] == ["C4"]
    assert [row["customer_id"] for row in june] == ["C5"]


def test_on_the_handover_date_the_later_interval_wins(spark, cfg, lineage_df):
    """Both intervals match, and exactly one row comes back.

    The bounds are closed on dates at both ends, so 1 March 2024 is inside
    ``C4``'s interval *and* inside ``C5``'s. That is deliberate — a fraud report
    carries the date reliably and the time of day much less so — and it means
    the result has to be ranked rather than filtered. Later start wins because a
    number moving to a new customer on the day of the fraud is the takeover
    pattern itself, not the assignment it replaced.
    """
    handover = _resolve(spark, cfg, lineage_df, "P6", "2024-03-01 09:00:00")

    assert len(handover) == 1
    assert handover[0]["customer_id"] == "C5"
    assert handover[0]["from_ts"] == ts("2024-03-01")


def test_a_reference_earlier_in_the_day_than_the_interval_start_still_matches(
    spark, cfg, lineage_df
):
    """Dates, not timestamps, and this is where that shows.

    ``C5``'s interval starts at midnight on 1 March, so any time on that date is
    at or after it either way. The case that separates date from timestamp
    semantics is the *upper* bound: a fraud at 9am on 1 March 2024 is after
    ``C4``'s interval ended at midnight, and on timestamp semantics ``C4`` would
    not match at all — leaving nothing to rank and no need for the tie-break the
    docstring documents.
    """
    matched = _resolve(spark, cfg, lineage_df, "P6", "2024-03-01 09:00:00")
    assert len(matched) == 1

    c4_matched = [
        row
        for row in _resolve(spark, cfg, lineage_df, "P6", "2024-02-29 09:00:00")
        if row["customer_id"] == "C4"
    ]
    assert len(c4_matched) == 1


def test_a_number_the_lineage_never_saw_resolves_to_nothing(spark, cfg, lineage_df):
    """Inner join, deliberately. A fraud on a number we cannot attribute is a
    reference we cannot build, and carrying it forward with a null customer
    would only let it be counted against whichever entity the next join
    reached."""
    assert _resolve(spark, cfg, lineage_df, "P99", REFERENCE_TS) == []


def test_the_synthetic_reference_id_distinguishes_two_moments_for_one_entity(
    spark, cfg, lineage_df
):
    """Which is what makes the same customer at two dates two training rows.

    A reference id built from the entity alone would collapse them, and the
    feature table would then have one row per customer with whichever moment's
    features the join happened to keep.
    """
    lookups = spark.createDataFrame(
        [("P6", ts("2024-06-01 12:00:00")), ("P6", ts("2024-05-01 12:00:00"))],
        T.StructType(
            [
                T.StructField("msisdn", T.StringType()),
                T.StructField("reference_ts", T.TimestampType()),
            ]
        ),
    )
    rows = build_reference_df_from_msisdns(lineage_df, lookups, None, cfg).collect()
    ids = {row["reference_id"] for row in rows}

    assert len(rows) == 2
    assert ids == {"P6_20240601120000", "P6_20240501120000"}


def test_a_caller_supplied_reference_id_is_not_overwritten(spark, cfg, lineage_df):
    """Because the fraud feed's own case identifier is what an investigation
    joins back on, and a synthetic id would sever that link."""
    lookups = spark.createDataFrame(
        [("CASE-77", "P6", ts(REFERENCE_TS))],
        T.StructType(
            [
                T.StructField("reference_id", T.StringType()),
                T.StructField("msisdn", T.StringType()),
                T.StructField("reference_ts", T.TimestampType()),
            ]
        ),
    )
    rows = build_reference_df_from_msisdns(lineage_df, lookups, None, cfg).collect()
    assert [row["reference_id"] for row in rows] == ["CASE-77"]


# --------------------------------------------------------------------------
# scope snapshots
# --------------------------------------------------------------------------


@pytest.fixture
def snapshots(spark, cfg, lineage_df):
    references = build_reference_df_at_timestamp(lineage_df, REFERENCE_TS, cfg)
    return build_scope_snapshots(lineage_df, references, cfg)


def test_the_three_scopes_and_the_base_are_all_returned(snapshots):
    """``base`` is not a scope and is not fed to any calculator.

    It is kept because it is what a data-quality check or an investigation of a
    surprising feature value needs — the un-aggregated join — and recomputing it
    means paying for the interval join a second time.
    """
    assert set(snapshots) == {"base", "customer", "account", "msisdn"}


def test_the_customer_scope_has_one_row_per_reference_not_per_assignment(snapshots):
    """``C2`` holds two accounts and ``C3`` holds two carriers, and both are one
    customer-scope row.

    A customer scope that leaked the account grain would give ``C2`` two feature
    rows for one reference, and the training table would then contain the same
    labelled example twice — which inflates every metric that counts rows.
    """
    rows = snapshots["customer"].collect()
    customers = sorted(row["customer_id"] for row in rows)
    assert customers == ["C1", "C2", "C3", "C5", "C6"]
    assert snapshots["customer"].count() == 5


def test_a_customer_whose_only_assignment_had_ended_is_not_in_any_scope(snapshots):
    """``C4``'s interval closed in March, so at the June reference it holds
    nothing.

    A reference *was* built for it — the reference stage describes every customer
    the lineage knows — and the snapshot join is what drops it. That division is
    deliberate: whether an entity was active at a moment is an interval question,
    and the interval test lives in one place.
    """
    for scope in SCOPES:
        customers = {row["customer_id"] for row in snapshots[scope].collect()}
        assert "C4" not in customers
    assert "C5" in {row["customer_id"] for row in snapshots["customer"].collect()}


def test_the_account_scope_splits_a_two_account_customer(snapshots):
    accounts = sorted(
        (row["customer_id"], row["acct_id"])
        for row in snapshots["account"].collect()
    )
    assert accounts == [
        ("C1", "A1"),
        ("C2", "A2"),
        ("C2", "A3"),
        ("C3", "A4"),
        ("C3", "A5"),
        ("C5", "A7"),
        ("C6", "A8"),
    ]


def test_the_carrier_is_part_of_the_account_grain_but_not_the_customer_grain(
    snapshots, cfg
):
    """``C3`` holds one account on ROGERS and one on TELUS.

    The carrier has to be in the account and MSISDN grains, because collapsing
    the two would attribute one carrier's events to the other's account — and
    the carrier is part of the event join key, so the attribution would be
    silently empty rather than wrong-and-loud. It is *not* in the customer
    grain, because a customer with two carriers is still one customer and
    splitting them would double the customer-scope row.
    """
    assert cfg.mno_col in snapshots["account"].columns
    assert cfg.mno_col in snapshots["msisdn"].columns
    assert cfg.mno_col not in snapshots["customer"].columns

    c3_carriers = {
        row[cfg.mno_col]
        for row in snapshots["account"].collect()
        if row["customer_id"] == "C3"
    }
    assert c3_carriers == {"ROGERS", "TELUS"}


def test_each_scope_carries_the_widest_interval_its_rows_span(snapshots, cfg):
    """Earliest start and latest end, which is what tenure is measured against.

    ``C3``'s customer row spans from its ROGERS assignment in 2021 even though
    its TELUS one began in 2024. Taking the max instead would say we had known
    ``C3`` for four months, and ``cust_days_since_first_seen`` — one of the most
    robust features in the set, since it survives a carrier feed going quiet —
    would be wrong by three years.
    """
    customer_rows = {
        row["customer_id"]: row for row in snapshots["customer"].collect()
    }
    assert customer_rows["C3"][cfg.from_ts_col] == ts("2021-06-01")
    assert customer_rows["C2"][cfg.from_ts_col] == ts("2023-03-15")
    assert customer_rows["C1"][cfg.from_ts_col] == ts("2022-01-01")


def test_the_open_end_reaches_the_snapshot_as_the_sentinel_not_as_null(snapshots, cfg):
    """Because the snapshot is what the event join's upper bound is taken from.

    A NULL ``to_ts`` here would make ``event_ts < to_ts`` null for every current
    assignment, which is most of the table.
    """
    for row in snapshots["account"].collect():
        assert row[cfg.to_ts_col] is not None
        assert row[cfg.to_ts_col] > ts("2024-06-01")


def test_each_scope_is_keyed_uniquely_by_its_entity_columns(snapshots, cfg):
    """One row per key, at every grain. A duplicate here fans out every
    subsequent feature join by the same factor."""
    for scope in SCOPES:
        frame = snapshots[scope]
        keys = list(cfg.entity_cols_for(scope))
        assert frame.select(*keys).distinct().count() == frame.count()


def test_a_reference_frame_without_a_reference_id_gets_a_synthetic_one(
    spark, cfg, lineage_df
):
    """Because a caller can legitimately hand over a bare (customer, moment)
    list, and every grouping key in the package starts with the reference id."""
    references = spark.createDataFrame(
        [("C1", ts(REFERENCE_TS))],
        T.StructType(
            [
                T.StructField("customer_id", T.StringType()),
                T.StructField("reference_ts", T.TimestampType()),
            ]
        ),
    )
    snapshots = build_scope_snapshots(lineage_df, references, cfg)
    rows = snapshots["customer"].collect()
    assert [row["reference_id"] for row in rows] == ["C1_20240601120000"]


# --------------------------------------------------------------------------
# attaching events
# --------------------------------------------------------------------------


EVENT_ROWS = (
    make_event("C1", "A1", "P1", "BELL", "2024-05-30 09:00:00", "SIM_CHANGE"),
    make_event("C1", "A1", "P1", "BELL", "2024-05-31 09:00:00", "DEVICE_CHANGE"),
    make_event("C2", "A2", "P2", "BELL", "2024-05-20 09:00:00", "SIM_CHANGE"),
    make_event("C3", "A5", "P5", "TELUS", "2024-04-01 09:00:00", "DEVICE_CHANGE"),
)


@pytest.fixture
def scoped_events(spark, cfg, snapshots):
    return attach_events_to_scope(
        snapshots, events_frame(spark, EVENT_ROWS), "customer", cfg
    )


def test_a_customer_with_no_events_at_all_keeps_a_row_of_nulls(scoped_events, cfg):
    """``C6`` is in the lineage and in no event feed. Silence is informative.

    The join is a left join for exactly this: a feature row of zeros and nulls
    says "this customer did nothing", and an absent row says nothing at all —
    and worse, it removes the customer from the population the model was
    supposed to score.
    """
    rows = [row for row in scoped_events.collect() if row["customer_id"] == "C6"]
    assert len(rows) == 1
    assert rows[0][cfg.event_ts_col] is None
    assert rows[0][cfg.event_family_col] is None
    assert rows[0][cfg.from_ts_col] == ts("2019-05-01")


def test_one_row_per_reference_and_event_pair(scoped_events):
    """``C1`` has two events, so two rows; ``C5`` and ``C6`` have none, so one
    each."""
    counts: dict = {}
    for row in scoped_events.collect():
        counts[row["customer_id"]] = counts.get(row["customer_id"], 0) + 1
    assert counts == {"C1": 2, "C2": 1, "C3": 1, "C5": 1, "C6": 1}


def test_the_scope_columns_come_from_the_scope_and_the_event_columns_from_the_event(
    scoped_events, cfg
):
    """No duplicated names, and the snapshot side is authoritative.

    Both sides carry ``customer_id``. If the projection took it from the event
    side, the silent customer's row would have a null key and would drop out of
    every subsequent ``groupBy``.
    """
    assert len(scoped_events.columns) == len(set(scoped_events.columns))
    for row in scoped_events.collect():
        assert row["customer_id"] is not None
        assert row[cfg.reference_ts_col] == ts(REFERENCE_TS)


def test_an_event_after_the_reference_moment_is_not_attached(spark, cfg, snapshots):
    """Leakage, at the join rather than at the window condition.

    Both bound it, and both have to: the window conditions bound the counted
    features and the join bounds the unwindowed ones — the lifetime count, the
    periodicity gaps, the cumulative slope. A future event that reached the
    frame would appear in every feature that has no window.
    """
    future = events_frame(
        spark,
        [make_event("C1", "A1", "P1", "BELL", "2024-06-05 09:00:00", "SIM_CHANGE")],
    )
    scoped = attach_events_to_scope(snapshots, future, "customer", cfg)
    c1 = [row for row in scoped.collect() if row["customer_id"] == "C1"]
    assert len(c1) == 1
    assert c1[0][cfg.event_ts_col] is None


def test_pruning_bounds_the_scan_at_the_widest_window_plus_the_buffer(
    spark, cfg, snapshots
):
    """Ninety days plus seven, measured back from the reference.

    The lower bound is in the *join condition*, not in a filter after it, which
    is the difference between materialising a customer's whole event history and
    materialising a quarter of it. An event from January 2024 is outside
    ninety-seven days of 1 June and is not read at all.
    """
    assert cfg.max_lookback_days + cfg.event_read_buffer_days == 97
    old = events_frame(
        spark,
        [make_event("C1", "A1", "P1", "BELL", "2024-01-01 09:00:00", "SIM_CHANGE")],
    )
    scoped = attach_events_to_scope(snapshots, old, "customer", cfg)
    c1 = [row for row in scoped.collect() if row["customer_id"] == "C1"]
    assert c1[0][cfg.event_ts_col] is None


def test_turning_pruning_off_widens_the_scan_to_the_whole_interval(
    spark, cfg, snapshots
):
    """The documented trade, asserted so it stays available.

    ``prune_event_window=False`` buys the true lifetime count — the one thing a
    windowed feature cannot express — at the cost of reading every event the
    entity ever produced. The same January event is attached.
    """
    unpruned = FeatureConfig(prune_event_window=False)
    old = events_frame(
        spark,
        [make_event("C1", "A1", "P1", "BELL", "2024-01-01 09:00:00", "SIM_CHANGE")],
    )
    scoped = attach_events_to_scope(snapshots, old, "customer", unpruned)
    c1 = [row for row in scoped.collect() if row["customer_id"] == "C1"]
    assert len(c1) == 1
    assert c1[0][cfg.event_ts_col] == ts("2024-01-01 09:00:00")


def test_an_event_before_the_scope_interval_began_is_not_attached(
    spark, cfg, snapshots
):
    """The interval start is a floor even with pruning off.

    ``C5`` took over ``P6`` in March 2024. An event on that number in 2021
    belonged to ``C4``, and attributing it to ``C5`` would give the takeover
    victim's successor the victim's history.
    """
    unpruned = FeatureConfig(prune_event_window=False)
    stale = events_frame(
        spark,
        [make_event("C5", "A7", "P6", "TELUS", "2021-07-01 09:00:00", "SIM_CHANGE")],
    )
    scoped = attach_events_to_scope(snapshots, stale, "customer", unpruned)
    c5 = [row for row in scoped.collect() if row["customer_id"] == "C5"]
    assert c5[0][cfg.event_ts_col] is None


def test_the_account_scope_join_separates_two_accounts_of_one_customer(
    spark, cfg, snapshots
):
    """``C2``'s event is on ``A2``, so ``A3`` must see nothing.

    At customer scope both accounts' events pool into one row, which is the
    point of having three scopes. At account scope they must not.
    """
    frame = events_frame(spark, EVENT_ROWS)
    scoped = attach_events_to_scope(snapshots, frame, "account", cfg)
    by_account = {
        (row["customer_id"], row["acct_id"]): row[cfg.event_ts_col]
        for row in scoped.collect()
    }
    assert by_account[("C2", "A2")] == ts("2024-05-20 09:00:00")
    assert by_account[("C2", "A3")] is None


def test_the_msisdn_scope_join_reaches_the_number(spark, cfg, snapshots):
    frame = events_frame(spark, EVENT_ROWS)
    scoped = attach_events_to_scope(snapshots, frame, "msisdn", cfg)
    by_number = {
        row["msisdn"]: row[cfg.event_ts_col] for row in scoped.collect()
    }
    assert by_number["P1"] is not None
    assert by_number["P3"] is None
    assert by_number["P7"] is None


def test_the_join_key_is_the_scope_identity_and_not_the_reference(
    spark, cfg, snapshots
):
    """Events carry no reference id, so the join cannot use one.

    Stated because it is the asymmetry a reader trips on: ``entity_cols_for``
    leads with the reference id and is the *grouping* key, while
    ``group_cols_for`` is the *join* key. Using the former to join would match
    nothing at all, since no event frame has ever had a reference id on it.
    """
    frame = events_frame(spark, EVENT_ROWS).drop("reference_id")
    scoped = attach_events_to_scope(snapshots, frame, "customer", cfg)
    assert scoped.filter(F.col(cfg.event_ts_col).isNotNull()).count() == 4


def test_an_unsupported_scope_is_refused_before_any_join(spark, cfg, snapshots):
    frame = events_frame(spark, EVENT_ROWS)
    with pytest.raises(ValueError, match="Unsupported scope"):
        attach_events_to_scope(snapshots, frame, "sim", cfg)


def test_a_missing_snapshot_entry_names_what_is_there_and_how_to_build_it(
    spark, cfg, snapshots
):
    """Because the usual cause is a hand-assembled dict with one scope in it,
    and a bare ``KeyError: 'account'`` does not suggest
    ``build_scope_snapshots``."""
    frame = events_frame(spark, EVENT_ROWS)
    partial = {"customer": snapshots["customer"]}
    with pytest.raises(KeyError, match="build_scope_snapshots"):
        attach_events_to_scope(partial, frame, "account", cfg)


def test_the_broadcast_switch_does_not_change_the_result(spark, cfg, snapshots):
    """A performance switch that changed the answer would be a correctness bug
    hiding in a config flag."""
    frame = events_frame(spark, EVENT_ROWS)
    broadcast = attach_events_to_scope(snapshots, frame, "customer", cfg)
    plain = attach_events_to_scope(
        snapshots, frame, "customer", FeatureConfig(broadcast_scope=False)
    )
    assert sorted(broadcast.collect(), key=repr) == sorted(plain.collect(), key=repr)


def test_the_fixture_population_still_contains_the_awkward_rows():
    """A guard on the fixture itself, since three findings are only visible on it.

    Two accounts on one carrier, two carriers on one customer, a recycled
    number, and a silent customer. If a future edit tidied these away, several
    tests above would keep passing while covering nothing.
    """
    customers = [row[0] for row in LINEAGE_ROWS]
    assert customers.count("C2") == 2
    assert customers.count("C3") == 2

    numbers = [(row[0], row[2]) for row in LINEAGE_ROWS]
    assert ("C4", "P6") in numbers
    assert ("C5", "P6") in numbers

    assert {row[0] for row in EVENT_ROWS} == {"C1", "C2", "C3"}
