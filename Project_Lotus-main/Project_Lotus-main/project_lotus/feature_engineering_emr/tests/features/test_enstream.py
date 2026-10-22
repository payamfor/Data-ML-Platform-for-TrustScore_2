"""The EnStream partner-traffic features, and finding 1.4 in particular.

Finding 1.4 is the largest defect in the notebook library and the reason this
module is the longest test file in the package for the smallest source module.
``with_partner_volume_weights`` computes a weight per (entity, partner) pair and
then joins it back onto the event frame. The notebook projection dropped the
partner columns and joined on the entity alone, so each event row was duplicated
once per partner pair the entity had ever produced. Three things went wrong at
once and only the first is obvious:

1. the weight attached to an event was not that event's partner's weight;
2. the row count of the scoped frame was multiplied by the entity's partner
   breadth, so the *unweighted* counts drawn from the same frame were multiplied
   too;
3. therefore the correction for partner volume was itself the single largest
   source of partner-volume bias in the table — an entity asked about by four
   partners had every count inflated fourfold.

None of that raises and none of it produces a missing column, so the tests below
assert the row count across the weight join and the exact value of the plain
counts. A test that only checked ``weighted_cnt`` existed and was a float would
have passed against all three.

The second thing settled here is finding 1.6, the prefix. Every column
``calculate_enstream_partner_only_features`` produces has to start with the
scope's *prefix* — ``cust_``, not ``customer_`` — because the notebook wrapper
took ``prefix`` as an argument and then passed ``scope`` to the three
calculators beneath it. The values were right and the names were wrong, which
made the columns invisible to the preprocessing stage's prefix-based selection.

The weights themselves are asserted by hand-computed value. With a floor of 0.25
and a ceiling of 4.0 on ``sqrt(mean_volume / own_volume)``, the interesting cases
are the symmetric two-partner split, the clamp at each end, and the degenerate
single-partner case where the mean equals the volume and the weight is exactly
one — the last being the one that pins the direction of the ratio, since an
inverted ``own / mean`` also gives one there and nowhere else.
"""

from __future__ import annotations

import math

import pytest

pyspark = pytest.importorskip("pyspark", reason="the EnStream features are Spark code")

from pyspark.sql import types as T  # noqa: E402
from tests.features.conftest import (  # noqa: E402
    FAR_FUTURE,
    REFERENCE_TS,
    collect_features,
    columns_matching,
    events_frame,
    make_event,
    ts,
)

from ts05_features.library.config import (  # noqa: E402
    CANONICAL_EVENT_FAMILY,
    ENSTREAM_DIMENSION_COLS,
    SCOPES,
    FeatureConfig,
    scope_prefix,
)
from ts05_features.library.enstream import (  # noqa: E402
    calculate_enstream_event_diversity_features,
    calculate_enstream_event_frequency_features,
    calculate_enstream_event_novelty_features,
    calculate_enstream_partner_only_features,
    calculate_enstream_scoped_events,
    with_partner_volume_weights,
)
from ts05_features.library.snapshots import (  # noqa: E402
    build_reference_df_at_timestamp,
    build_scope_snapshots,
)

pytestmark = pytest.mark.spark

ENTITY_COLS = ("reference_id", "customer_id")
ENSTREAM = CANONICAL_EVENT_FAMILY["ENSTREAM_API_CALL"]
DEVICE = CANONICAL_EVENT_FAMILY["DEVICE_CHANGE"]


# --------------------------------------------------------------------------
# the scoped EnStream frame
# --------------------------------------------------------------------------

SCOPED_FIELDS = (
    ("reference_id", T.StringType()),
    ("customer_id", T.StringType()),
    ("mno", T.StringType()),
    ("reference_ts", T.TimestampType()),
    ("from_ts", T.TimestampType()),
    ("to_ts", T.TimestampType()),
    ("event_ts", T.TimestampType()),
    ("event_family", T.StringType()),
    ("event_type", T.StringType()),
    ("event_source", T.StringType()),
    ("event_weight", T.DoubleType()),
    ("service_provider_id", T.StringType()),
    ("service_provider_name", T.StringType()),
    ("industry", T.StringType()),
    ("use_case", T.StringType()),
)

#: ``cfg.enstream_partner_cols``, written out so a change to it shows up here.
PARTNER_COLS = ("service_provider_id", "use_case")


def scoped_row(
    customer,
    event_ts,
    *,
    reference_id=None,
    reference_ts=REFERENCE_TS,
    service_provider_id="SP1",
    use_case="IDENTITY",
    service_provider_name=None,
    industry=None,
    event_type="ENSTREAM_API_CALL",
    from_ts="2020-01-01",
    to_ts=FAR_FUTURE,
):
    """One row of a scoped EnStream frame, as ``attach_events_to_scope`` returns it.

    ``service_provider_name`` defaults to ``None`` rather than to the id because
    the diversity and novelty features are computed over the *name*, and a
    default that filled it would hide a calculator reading the wrong column.
    """
    family = CANONICAL_EVENT_FAMILY[event_type] if event_type else None
    return (
        reference_id or f"{customer}_REF",
        customer,
        "BELL",
        ts(reference_ts),
        ts(from_ts),
        ts(to_ts),
        ts(event_ts) if event_ts else None,
        family,
        event_type,
        "enstream",
        1.0,
        service_provider_id,
        service_provider_name,
        industry,
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


def weight_of(row, days, cfg):
    """The weight column for one window, by the name the module builds."""
    return row[f"{cfg.event_weight_col}_{days}d"]


# --------------------------------------------------------------------------
# with_partner_volume_weights - finding 1.4
# --------------------------------------------------------------------------

def test_the_weight_join_does_not_change_the_row_count(spark, narrow_cfg):
    """Finding 1.4: four events in, four events out, whatever the partner mix.

    This is the whole defect in one assertion. C1 has two partner pairs, so the
    notebook's entity-only join produced eight rows from four — each event once
    per pair, each copy with a different weight. The count is asserted rather
    than the weights because it is the count that corrupted the *unweighted*
    features, which is the part nobody was looking for.

    Two windows in ``narrow_cfg`` means two weight joins, so a fan-out here
    compounds: the notebook shape would give sixteen rows, not eight.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-30 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-29 09:00:00", service_provider_id="SP2"),
            scoped_row("C1", "2024-05-28 09:00:00", service_provider_id="SP2"),
        ],
    )
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    assert got.count() == 4


def test_the_fan_out_would_also_multiply_the_plain_counts(spark, narrow_cfg):
    """Finding 1.4's consequence: ``cnt_30d`` is four, not eight.

    The plain count and the weighted count are aggregated from the same frame in
    ``calculate_enstream_event_frequency_features``, so the fan-out did not merely
    mis-weight — it doubled a count that has nothing to do with weighting. Four
    calls from two partners is four calls.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-30 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-29 09:00:00", service_provider_id="SP2"),
            scoped_row("C1", "2024-05-28 09:00:00", service_provider_id="SP2"),
        ],
    )
    got = calculate_enstream_event_frequency_features(
        events, ENTITY_COLS, "cust", narrow_cfg
    )
    row = by_customer(got)["C1"]
    assert row[f"cust_{ENSTREAM}_cnt_30d"] == 4


def test_the_weight_belongs_to_the_events_own_partner(spark, narrow_cfg):
    """The busy partner's rows carry the discount and the quiet partner's the boost.

    In the thirty-day window SP1 made three calls and SP2 one, so the mean is two
    and the weights are ``sqrt(2/3)`` and ``sqrt(2/1)``. Under the entity-only
    join every row would carry whichever of the two the join happened to attach,
    and with a fan-out both — so asserting per row, keyed by the partner, is what
    distinguishes a correct weight from an averaged one.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-30 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-29 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-28 09:00:00", service_provider_id="SP2"),
        ],
    )
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    weights = {
        (row["service_provider_id"], row["event_ts"]): weight_of(row, 30, narrow_cfg)
        for row in (r.asDict() for r in got.collect())
    }
    busy = [value for (sp, _), value in weights.items() if sp == "SP1"]
    quiet = [value for (sp, _), value in weights.items() if sp == "SP2"]
    assert len(busy) == 3 and len(quiet) == 1
    for value in busy:
        assert value == pytest.approx(math.sqrt(2.0 / 3.0))
    assert quiet[0] == pytest.approx(math.sqrt(2.0 / 1.0))


def test_a_single_partner_weighs_exactly_one(spark, narrow_cfg):
    """One partner means the mean is its own volume, so the ratio is one.

    This is the test that pins the *direction* of the ratio. Every other case
    here would also pass against ``sqrt(own / mean)`` with the sign of the effect
    reversed, because both forms are symmetric about one; only by pairing this
    with the busy/quiet test above does "the busy partner is discounted" become
    an assertion rather than a coincidence. It also states the degenerate answer
    the weighting has to give: with no peers to compare against, weighted counts
    equal plain counts.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row("C1", "2024-05-30 09:00:00"),
        ],
    )
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    for row in (r.asDict() for r in got.collect()):
        assert weight_of(row, 30, narrow_cfg) == pytest.approx(1.0)


def test_the_two_partner_split_is_symmetric(spark, narrow_cfg):
    """Equal volumes give equal weights of one, not of one half.

    The mean across partners, not the entity's total, is the denominator. A
    version dividing by the total would give ``sqrt(4/2)`` here and would still
    look sane; it is only when partner breadth changes between two entities that
    the two forms diverge, and the next test is that case.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-30 09:00:00", service_provider_id="SP2"),
        ],
    )
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    for row in (r.asDict() for r in got.collect()):
        assert weight_of(row, 30, narrow_cfg) == pytest.approx(1.0)


def test_the_weight_does_not_depend_on_how_many_partners_there_are(
    spark, narrow_cfg
):
    """One call each from two partners and from four both weigh one.

    Which is the property the whole family exists for. C1 was asked about by two
    partners once each, C2 by four partners once each; no partner is unusual for
    either entity, so no event is discounted for either. A denominator of the
    entity total rather than the partner mean would give C2 half of C1's weight
    purely for being more widely queried — reintroducing exactly the
    partner-breadth bias the weighting removes. Breadth is a *feature* here (the
    diversity family), not a discount.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-31 09:00:00", service_provider_id="SP2"),
            scoped_row("C2", "2024-05-31 09:00:00", service_provider_id="SP1"),
            scoped_row("C2", "2024-05-31 09:00:00", service_provider_id="SP2"),
            scoped_row("C2", "2024-05-31 09:00:00", service_provider_id="SP3"),
            scoped_row("C2", "2024-05-31 09:00:00", service_provider_id="SP4"),
        ],
    )
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    for row in (r.asDict() for r in got.collect()):
        assert weight_of(row, 30, narrow_cfg) == pytest.approx(1.0)


def test_the_weight_is_clamped_at_the_ceiling(spark, narrow_cfg):
    """A partner drowned out by its peers is boosted to 4.0 and no further.

    Seventeen calls from SP1 against one from SP2 gives a mean of nine and a
    ratio of nine for SP2, whose square root is three — under the ceiling. So the
    ceiling is provoked with a wider spread: with 1 against 200 the mean is
    100.5, the ratio is 100.5 and the root is just over ten. Clamped, because a
    weight of ten makes one call from an unusual partner outrank ten from the
    usual one, and the feature is meant to normalise volume rather than to
    invert it.
    """
    rows = [
        scoped_row(
            "C1", f"2024-05-{10 + index // 24:02d} {index % 24:02d}:00:00",
            service_provider_id="SP1",
        )
        for index in range(200)
    ]
    rows.append(scoped_row("C1", "2024-05-31 09:00:00", service_provider_id="SP2"))
    events = scoped_frame(spark, rows)
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    weights = {
        row["service_provider_id"]: weight_of(row, 30, narrow_cfg)
        for row in (r.asDict() for r in got.collect())
    }
    assert weights["SP2"] == pytest.approx(narrow_cfg.enstream_max_weight)
    assert math.sqrt(100.5) > narrow_cfg.enstream_max_weight


def test_the_weight_is_clamped_at_the_floor(spark, narrow_cfg):
    """The dominant partner bottoms out at 0.25 rather than at zero.

    Same population as the ceiling test, read from the other side: SP1's ratio is
    ``100.5 / 200``, whose square root is about 0.709 — inside the floor. So the
    floor needs the *reverse* spread, one partner against many quiet ones, which
    is what this builds: SP0 makes 200 calls while sixty other partners make one
    each, dropping the mean to about 4.3 and SP0's root to about 0.147. A floor
    rather than zero because a partner asking constantly is still asking, and a
    weight of zero would erase the event instead of discounting it.
    """
    rows = [
        scoped_row(
            "C1", f"2024-05-{10 + index // 24:02d} {index % 24:02d}:00:00",
            service_provider_id="SP0",
        )
        for index in range(200)
    ]
    rows.extend(
        scoped_row(
            "C1", "2024-05-31 09:00:00", service_provider_id=f"SP{index + 1}"
        )
        for index in range(60)
    )
    events = scoped_frame(spark, rows)
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    weights = {
        row["service_provider_id"]: weight_of(row, 30, narrow_cfg)
        for row in (r.asDict() for r in got.collect())
    }
    assert weights["SP0"] == pytest.approx(narrow_cfg.enstream_min_weight)
    assert math.sqrt(260.0 / 61.0 / 200.0) < narrow_cfg.enstream_min_weight


def test_the_use_case_is_part_of_the_partner_grain(spark, narrow_cfg):
    """One provider asking for two reasons is two partners, and the weights differ.

    ``cfg.enstream_partner_cols`` is ``(service_provider_id, use_case)``, not the
    provider alone. A bank running a login check on every session and a credit
    check once a year are different behaviours from the same company, and pooling
    them would let the login volume discount the credit check — the exact effect
    the family removes across companies, reintroduced within one. Three
    ``IDENTITY`` calls against one ``CREDIT`` gives the same 2/3 and 2/1 split as
    the two-provider case.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", use_case="IDENTITY"),
            scoped_row("C1", "2024-05-30 09:00:00", use_case="IDENTITY"),
            scoped_row("C1", "2024-05-29 09:00:00", use_case="IDENTITY"),
            scoped_row("C1", "2024-05-28 09:00:00", use_case="CREDIT"),
        ],
    )
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    weights = {}
    for row in (r.asDict() for r in got.collect()):
        weights.setdefault(row["use_case"], []).append(weight_of(row, 30, narrow_cfg))
    assert weights["IDENTITY"][0] == pytest.approx(math.sqrt(2.0 / 3.0))
    assert weights["CREDIT"] == [pytest.approx(math.sqrt(2.0 / 1.0))]


def test_the_windows_are_weighted_independently(spark, narrow_cfg):
    """A partner busy last quarter and quiet today is discounted in one window only.

    SP1 made three calls, all more than three days ago; SP2 made one, yesterday.
    In the thirty-day window SP1 is the busy partner and is discounted. In the
    three-day window SP1 made *no* calls, so the volumes are 0 and 1, the mean is
    0.5, and the denominator's ``greatest(volume, 1)`` floor keeps SP1's ratio at
    0.5 rather than dividing by zero. One weight column per window rather than
    one for all of them, because "this partner asks a lot" is a statement about a
    period.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-20 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-19 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-18 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-31 09:00:00", service_provider_id="SP2"),
        ],
    )
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    rows = [r.asDict() for r in got.collect()]
    for row in rows:
        if row["service_provider_id"] == "SP1":
            assert weight_of(row, 30, narrow_cfg) == pytest.approx(
                math.sqrt(2.0 / 3.0)
            )
            assert weight_of(row, 3, narrow_cfg) == pytest.approx(math.sqrt(0.5))


def test_there_is_one_weight_column_per_configured_window(spark, narrow_cfg, cfg):
    """``narrow_cfg`` gives two weight columns and the production config five.

    Finding 1.7 in this module: the weights are built in a loop over
    ``cfg.lookback_days``, so a caller who narrows the windows must not get the
    production set. Asserted as an exact list rather than a subset because a
    function ignoring its argument produces a superset, which no membership test
    would catch.
    """
    events = scoped_frame(spark, [scoped_row("C1", "2024-05-31 09:00:00")])
    narrow = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    assert columns_matching(narrow, "event_weight_") == [
        "event_weight_30d",
        "event_weight_3d",
    ]
    wide = with_partner_volume_weights(events, ENTITY_COLS, cfg)
    assert len(columns_matching(wide, "event_weight_")) == len(cfg.lookback_days)


def test_no_partner_metadata_gives_a_weight_of_one(spark, narrow_cfg):
    """A feed without partner columns weighs everything one rather than raising.

    The degenerate case has to be the identity, not an error: a run against
    carrier data alone, or an EnStream extract that lost its metadata, should
    produce weighted counts equal to the plain counts rather than no counts at
    all. The frame here is the scoped shape with both partner columns dropped.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row("C1", "2024-05-30 09:00:00"),
        ],
    ).drop(*PARTNER_COLS)
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    assert got.count() == 2
    for row in (r.asDict() for r in got.collect()):
        assert weight_of(row, 30, narrow_cfg) == pytest.approx(1.0)


def test_one_missing_partner_column_still_grains_on_the_other(spark, narrow_cfg):
    """With ``use_case`` gone the grain is the provider alone, not the entity.

    ``partner_cols`` is filtered by ``has_col``, so a partially populated feed
    degrades to the columns it has rather than to the entity-only join that was
    finding 1.4. Three SP1 calls against one SP2 gives the 2/3 and 2/1 split even
    though the use case is absent — and the row count is still four.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-30 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-29 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-28 09:00:00", service_provider_id="SP2"),
        ],
    ).drop("use_case")
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    assert got.count() == 4
    weights = {
        row["service_provider_id"]: weight_of(row, 30, narrow_cfg)
        for row in (r.asDict() for r in got.collect())
    }
    assert weights["SP1"] == pytest.approx(math.sqrt(2.0 / 3.0))
    assert weights["SP2"] == pytest.approx(math.sqrt(2.0 / 1.0))


def test_the_weights_are_per_entity_and_not_pooled(spark, narrow_cfg):
    """C1's busy partner does not discount C2's calls.

    The weight is computed for an (entity, partner) pair against *that entity's*
    partner mean, because the question is whether this partner is unusual for
    this number — not whether it is a high-volume company overall. SP1 makes
    three calls about C1 and one about C2; C2's single SP1 call weighs one,
    because for C2 there is nothing to compare it against.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-30 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-29 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-28 09:00:00", service_provider_id="SP2"),
            scoped_row("C2", "2024-05-31 09:00:00", service_provider_id="SP1"),
        ],
    )
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    weights = {}
    for row in (r.asDict() for r in got.collect()):
        weights.setdefault(row["customer_id"], {})[row["service_provider_id"]] = (
            weight_of(row, 30, narrow_cfg)
        )
    assert weights["C1"]["SP1"] == pytest.approx(math.sqrt(2.0 / 3.0))
    assert weights["C2"]["SP1"] == pytest.approx(1.0)


def test_the_silent_entity_keeps_its_row_through_the_weight_join(
    spark, narrow_cfg
):
    """A left-join miss survives with a weight of one rather than a null.

    ``attach_events_to_scope`` preserves references with no events as rows of
    nulls, and this frame is its output, so the null-partner row reaches the
    weight join. It has to come out the other side — the whole point of the left
    join upstream is that silence is a feature row of zeros — and the
    ``coalesce`` to 1.0 is what stops a null weight poisoning the weighted sum.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row(
                "C2", None, service_provider_id=None, use_case=None, event_type=None
            ),
        ],
    )
    got = with_partner_volume_weights(events, ENTITY_COLS, narrow_cfg)
    assert got.count() == 2
    rows = {row["customer_id"]: row for row in (r.asDict() for r in got.collect())}
    assert weight_of(rows["C2"], 30, narrow_cfg) == pytest.approx(1.0)


# --------------------------------------------------------------------------
# calculate_enstream_event_frequency_features
# --------------------------------------------------------------------------

def test_the_weighted_count_is_the_sum_of_the_weights(spark, narrow_cfg):
    """Three SP1 calls and one SP2 sum to ``3*sqrt(2/3) + sqrt(2)``.

    Which is about 3.86 against a plain count of 4 — the busy partner discounted,
    the quiet one amplified, and the total pulled below the raw count because
    three of the four calls came from the partner that always calls. Asserted as
    the arithmetic rather than as an inequality so that a weight applied from the
    wrong window, or averaged instead of summed, fails here.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-30 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-29 09:00:00", service_provider_id="SP1"),
            scoped_row("C1", "2024-05-28 09:00:00", service_provider_id="SP2"),
        ],
    )
    got = calculate_enstream_event_frequency_features(
        events, ENTITY_COLS, "cust", narrow_cfg
    )
    row = by_customer(got)["C1"]
    expected = 3 * math.sqrt(2.0 / 3.0) + math.sqrt(2.0)
    assert row[f"cust_{ENSTREAM}_weighted_cnt_30d"] == pytest.approx(expected)
    assert row[f"cust_{ENSTREAM}_cnt_30d"] == 4


def test_the_column_names_are_the_generic_frequency_names(spark, narrow_cfg):
    """The same three names per window that the generic calculator would emit.

    Deliberate, and asserted because it is load-bearing. ``assemble.py`` skips the
    generic frequency calculator for this family precisely so that these fill the
    slot; if the names diverged, ``join_feature_frames`` would not be resolving a
    collision, it would be adding a second namespace, and every model selecting
    ``cust_enstream_api_call_cnt_30d`` would find nothing.
    """
    events = scoped_frame(spark, [scoped_row("C1", "2024-05-31 09:00:00")])
    got = calculate_enstream_event_frequency_features(
        events, ENTITY_COLS, "cust", narrow_cfg
    )
    assert columns_matching(got, ENSTREAM) == [
        f"cust_{ENSTREAM}_any_30d",
        f"cust_{ENSTREAM}_any_3d",
        f"cust_{ENSTREAM}_cnt_30d",
        f"cust_{ENSTREAM}_cnt_3d",
        f"cust_{ENSTREAM}_weighted_cnt_30d",
        f"cust_{ENSTREAM}_weighted_cnt_3d",
    ]


def test_a_carrier_event_in_the_frame_is_not_counted(spark, narrow_cfg):
    """The family filter runs after the weighting and before the counts.

    A scoped frame built from the canonical table carries every family, so the
    filter is what makes these EnStream features. The device change here is
    inside the window and must not appear in the count — but it is in the frame
    when the weights are computed, and a weight join that fanned out on it would
    still corrupt the EnStream rows.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row(
                "C1", "2024-05-30 09:00:00", event_type="DEVICE_CHANGE",
                service_provider_id=None, use_case=None,
            ),
        ],
    )
    got = calculate_enstream_event_frequency_features(
        events, ENTITY_COLS, "cust", narrow_cfg
    )
    row = by_customer(got)["C1"]
    assert row[f"cust_{ENSTREAM}_cnt_30d"] == 1


def test_an_entity_with_no_enstream_events_keeps_its_row(spark, narrow_cfg):
    """C2 only ever had a device change and is still a row, with nulls.

    The base comes from the whole scoped frame rather than from the filtered one,
    so an entity that appears only under another family survives with nulls
    instead of vanishing. Nulls and not zeros: the count columns come from the
    filtered aggregate, and C2 contributed no group to it.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row(
                "C2", "2024-05-31 09:00:00", event_type="DEVICE_CHANGE",
                service_provider_id=None, use_case=None,
            ),
        ],
    )
    got = calculate_enstream_event_frequency_features(
        events, ENTITY_COLS, "cust", narrow_cfg
    )
    rows = by_customer(got)
    assert set(rows) == {"C1", "C2"}
    assert rows["C2"][f"cust_{ENSTREAM}_cnt_30d"] is None


def test_the_any_flag_is_a_flag(spark, narrow_cfg):
    """Four calls in the window give one, and none give zero.

    Kept because for a number nobody normally asks about, the distinction between
    no calls and one call is the entire signal, while four and forty are nearly
    the same fact.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00"),
            scoped_row("C1", "2024-05-31 10:00:00"),
            scoped_row("C1", "2024-05-31 11:00:00"),
            scoped_row("C1", "2024-05-31 12:00:00"),
            scoped_row("C2", "2024-01-01 09:00:00"),
        ],
    )
    got = calculate_enstream_event_frequency_features(
        events, ENTITY_COLS, "cust", narrow_cfg
    )
    rows = by_customer(got)
    assert rows["C1"][f"cust_{ENSTREAM}_any_30d"] == 1
    assert rows["C2"][f"cust_{ENSTREAM}_any_30d"] == 0
    assert rows["C2"][f"cust_{ENSTREAM}_cnt_30d"] == 0


# --------------------------------------------------------------------------
# calculate_enstream_event_diversity_features
# --------------------------------------------------------------------------

def test_the_diversity_dimensions_are_the_partner_metadata(spark, cfg):
    """Two industries, two providers and two use cases, counted per dimension.

    The dimensions are ``industry``, ``service_provider_name`` and ``use_case`` —
    the name, not the id, for the provider, because the id is the join key and
    the name is what a reviewer reads. Breadth of who is asking is among the
    strongest signals in the set: one bank for two years, then four lenders in a
    week, is an application spree.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row(
                "C1", "2024-05-31 09:00:00", industry="BANKING",
                service_provider_name="BankCo", use_case="IDENTITY",
            ),
            scoped_row(
                "C1", "2024-05-30 09:00:00", industry="LENDING",
                service_provider_name="LendCo", use_case="CREDIT",
            ),
        ],
    )
    got = calculate_enstream_event_diversity_features(
        events, ENTITY_COLS, "cust", ENSTREAM_DIMENSION_COLS, cfg
    )
    row = by_customer(got)["C1"]
    for dimension in ENSTREAM_DIMENSION_COLS:
        assert row[f"cust_{ENSTREAM}_{dimension}_distinct_cnt"] == 2


def test_diversity_ignores_the_other_families(spark, cfg):
    """A device change with an industry attached does not widen the breadth.

    Carrier events carry nulls in the metadata columns in production, so this is
    a synthetic row — but the filter is what guarantees it, and a filter applied
    to the wrong column would let a genuinely mixed frame inflate the counts.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-31 09:00:00", industry="BANKING"),
            scoped_row(
                "C1", "2024-05-30 09:00:00", event_type="DEVICE_CHANGE",
                industry="LENDING",
            ),
        ],
    )
    got = calculate_enstream_event_diversity_features(
        events, ENTITY_COLS, "cust", ENSTREAM_DIMENSION_COLS, cfg
    )
    row = by_customer(got)["C1"]
    assert row[f"cust_{ENSTREAM}_industry_distinct_cnt"] == 1


# --------------------------------------------------------------------------
# calculate_enstream_event_novelty_features
# --------------------------------------------------------------------------

def test_the_novelty_spread_is_the_gap_between_first_and_last_new_industry(
    spark, cfg
):
    """Banking first seen on the 1st and lending on the 21st: a spread of twenty.

    Diversity says two industries. Novelty says they arrived twenty days apart,
    which separates this entity from one that acquired both on the same
    afternoon. The spread is computed between the *first sightings*, so the
    second banking call on the 25th does not move it — that call is not news.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", industry="BANKING"),
            scoped_row("C1", "2024-05-21 09:00:00", industry="LENDING"),
            scoped_row("C1", "2024-05-25 09:00:00", industry="BANKING"),
        ],
    )
    got = calculate_enstream_event_novelty_features(
        events, ENTITY_COLS, "cust", ("industry",), cfg
    )
    row = by_customer(got)["C1"]
    assert row["cust_new_industry_spread_days"] == 20


def test_the_days_since_the_last_new_industry_is_measured_to_the_reference(
    spark, cfg
):
    """Lending first appeared on 21 May and the reference is 1 June: eleven days.

    Measured from the reference rather than from the entity's last event, because
    the question is how recently the breadth widened as of scoring time — an
    entity whose partner mix last changed eleven days ago is in a different state
    from one whose mix last changed two years ago, whatever either has done since.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", industry="BANKING"),
            scoped_row("C1", "2024-05-21 09:00:00", industry="LENDING"),
            scoped_row("C1", "2024-05-25 09:00:00", industry="BANKING"),
        ],
    )
    got = calculate_enstream_event_novelty_features(
        events, ENTITY_COLS, "cust", ("industry",), cfg
    )
    row = by_customer(got)["C1"]
    assert row["cust_days_since_last_new_industry"] == 11


def test_the_novelty_fraction_is_the_share_of_calls_that_were_first_sightings(
    spark, cfg
):
    """Two industries across four calls: two first sightings, so one half.

    High means almost every call is from somebody new, which is the shape of an
    entity being shopped around rather than one with a settled set of
    counterparties. Four calls and two industries is exactly 0.5, and the
    denominator is the entity's calls in that dimension rather than all its
    calls — a distinction that only shows when the dimension is null on some
    rows, which the next test covers.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", industry="BANKING"),
            scoped_row("C1", "2024-05-10 09:00:00", industry="BANKING"),
            scoped_row("C1", "2024-05-21 09:00:00", industry="LENDING"),
            scoped_row("C1", "2024-05-25 09:00:00", industry="LENDING"),
        ],
    )
    got = calculate_enstream_event_novelty_features(
        events, ENTITY_COLS, "cust", ("industry",), cfg
    )
    row = by_customer(got)["C1"]
    assert row["cust_new_industry_event_cnt"] == 2
    assert row["cust_industry_event_cnt"] == 4
    assert row["cust_new_industry_event_frac"] == pytest.approx(0.5)


def test_one_industry_seen_once_has_a_spread_of_zero_and_a_fraction_of_one(
    spark, cfg
):
    """A single call is its own first sighting: spread 0, fraction 1.0.

    Zero and not null here, unlike the interval features: the spread between the
    earliest and latest first-sighting of a set of one is genuinely zero, because
    both ends are the same event. And every call this entity made was news, so
    the fraction is one — which is why the fraction has to be read alongside the
    count, not instead of it.
    """
    events = scoped_frame(spark, [scoped_row("C1", "2024-05-01 09:00:00",
                                             industry="BANKING")])
    got = calculate_enstream_event_novelty_features(
        events, ENTITY_COLS, "cust", ("industry",), cfg
    )
    row = by_customer(got)["C1"]
    assert row["cust_new_industry_spread_days"] == 0
    assert row["cust_new_industry_event_frac"] == pytest.approx(1.0)


def test_an_absent_dimension_is_skipped_rather_than_nulled(spark, cfg):
    """A frame without ``industry`` produces no industry columns at all.

    ``has_col`` guards each dimension, so a feed that never carried one degrades
    to the dimensions it has. Skipped rather than emitted as nulls because a null
    column reaches the Parquet write and fails there, a long way from the cause.
    """
    events = scoped_frame(
        spark, [scoped_row("C1", "2024-05-01 09:00:00", use_case="IDENTITY")]
    ).drop("industry")
    got = calculate_enstream_event_novelty_features(
        events, ENTITY_COLS, "cust", ENSTREAM_DIMENSION_COLS, cfg
    )
    assert columns_matching(got, "industry") == []
    assert columns_matching(got, "use_case") != []


def test_the_novelty_features_are_per_entity(spark, cfg):
    """C2's first sighting of lending is its own, not inherited from C1.

    The grouping is by the entity key plus the reference timestamp, and the
    reference timestamp is dropped before the join back — so a version that left
    it in the key would still pass a single-entity test and produce duplicate
    rows here.
    """
    events = scoped_frame(
        spark,
        [
            scoped_row("C1", "2024-05-01 09:00:00", industry="BANKING"),
            scoped_row("C1", "2024-05-21 09:00:00", industry="LENDING"),
            scoped_row("C2", "2024-05-31 09:00:00", industry="LENDING"),
        ],
    )
    got = calculate_enstream_event_novelty_features(
        events, ENTITY_COLS, "cust", ("industry",), cfg
    )
    rows = by_customer(got)
    assert got.count() == 2
    assert rows["C1"]["cust_new_industry_spread_days"] == 20
    assert rows["C2"]["cust_new_industry_spread_days"] == 0
    assert rows["C2"]["cust_days_since_last_new_industry"] == 1


# --------------------------------------------------------------------------
# calculate_enstream_scoped_events and the wrapper
# --------------------------------------------------------------------------

@pytest.fixture
def snapshots(spark, lineage_df, cfg):
    """The three scope snapshots and the base, at the shared reference moment."""
    references = build_reference_df_at_timestamp(lineage_df, ts(REFERENCE_TS), cfg)
    return build_scope_snapshots(lineage_df, references, cfg)


@pytest.fixture
def enstream_events(spark):
    """Normalised EnStream events for C1 and C3, with partner metadata.

    Attributed by MSISDN and carrier in production; here the customer and account
    are written directly because ``attach_events_to_scope`` joins on the scope
    key, and what is under test in this section is the wrapper rather than the
    attribution — which ``test_events.py`` settles.
    """
    return events_frame(
        spark,
        [
            make_event(
                "C1", "A1", "P1", "BELL", "2024-05-31 09:00:00",
                "ENSTREAM_API_CALL", event_source="enstream",
                service_provider_id="SP1", service_provider_name="BankCo",
                industry="BANKING", use_case="IDENTITY",
            ),
            make_event(
                "C1", "A1", "P1", "BELL", "2024-05-30 09:00:00",
                "ENSTREAM_API_CALL", event_source="enstream",
                service_provider_id="SP1", service_provider_name="BankCo",
                industry="BANKING", use_case="IDENTITY",
            ),
            make_event(
                "C1", "A1", "P1", "BELL", "2024-05-29 09:00:00",
                "ENSTREAM_API_CALL", event_source="enstream",
                service_provider_id="SP2", service_provider_name="LendCo",
                industry="LENDING", use_case="CREDIT",
            ),
            make_event(
                "C3", "A5", "P5", "TELUS", "2024-05-31 09:00:00",
                "ENSTREAM_API_CALL", event_source="enstream",
                service_provider_id="SP1", service_provider_name="BankCo",
                industry="BANKING", use_case="IDENTITY",
            ),
        ],
    )


def test_the_scoped_events_keep_the_partner_metadata(
    snapshots, enstream_events, cfg
):
    """The metadata survives the attach, which is why the feed is joined twice.

    ``build_changes_table`` unions the feeds into one table and
    ``attach_events_to_scope`` is run on it once for the carrier families — but
    the weighting needs the feed at partner grain, so ``enstream.py`` joins the
    normalised feed a second time rather than reading the union. The columns the
    second pass exists for have to be present on its output.
    """
    got = calculate_enstream_scoped_events(
        snapshots, "customer", enstream_events, cfg
    )
    for col_name in (*PARTNER_COLS, *ENSTREAM_DIMENSION_COLS):
        assert col_name in got.columns


def test_the_silent_customers_survive_the_attach(snapshots, enstream_events, cfg):
    """Five customers in the June population and five in the scoped frame's keys.

    C6 is in no feed at all and C2 and C5 are in no EnStream feed; all three have
    to reach the feature calculators, because a customer nobody has asked about
    is a feature row of zeros rather than a missing row, and "nobody has asked
    about this number" is one of the more informative things the table says.
    """
    got = calculate_enstream_scoped_events(
        snapshots, "customer", enstream_events, cfg
    )
    customers = {row["customer_id"] for row in got.select("customer_id").collect()}
    assert customers == {"C1", "C2", "C3", "C5", "C6"}


@pytest.mark.parametrize("scope", SCOPES)
def test_every_wrapper_column_starts_with_the_scope_prefix(
    snapshots, enstream_events, cfg, scope
):
    """Finding 1.6: the wrapper passes ``prefix``, not ``scope``.

    The notebook wrapper took a ``prefix`` argument and then handed ``scope`` to
    all three calculators beneath it, so the EnStream family came out under
    ``customer_enstream_api_call_*`` while every other family at the same scope
    was ``cust_*``. Nothing raised and the values were correct; the columns were
    simply invisible to the preprocessing stage, which selects features by
    prefix. Invisible at the MSISDN scope in particular, where ``scope`` and
    ``scope_prefix(scope)`` are the same string — which is why this is
    parametrised over all three rather than checked at the one scope somebody
    happened to look at.
    """
    got = calculate_enstream_partner_only_features(
        snapshots,
        scope,
        enstream_events,
        entity_cols=cfg.entity_cols_for(scope),
        prefix=scope_prefix(scope),
        cfg=cfg,
    )
    prefix = scope_prefix(scope)
    features = [
        name for name in got.columns if name not in cfg.entity_cols_for(scope)
    ]
    assert features
    for name in features:
        assert name.startswith(f"{prefix}_"), name


def test_the_wrapper_returns_one_row_per_entity(snapshots, enstream_events, cfg):
    """Five customers, five rows, five distinct keys.

    Three frames are joined onto the base here and each is grouped to one row per
    entity, so a duplicate means one of them leaked a second grouping key — the
    reference timestamp in the novelty features being the candidate. Asserted as
    both a count and a distinct count so that a fan-out cannot hide behind a
    coincidental total.
    """
    got = calculate_enstream_partner_only_features(
        snapshots,
        "customer",
        enstream_events,
        entity_cols=cfg.entity_cols_for("customer"),
        prefix="cust",
        cfg=cfg,
    )
    rows = got.collect()
    assert len(rows) == 5
    assert len({(row["reference_id"], row["customer_id"]) for row in rows}) == 5


def test_the_wrapper_composes_the_three_families(snapshots, enstream_events, cfg):
    """C1's counts, breadth and novelty all land on one row with the right values.

    Three calls in the ninety-day window from two providers, two industries and
    two use cases. Banking was first seen on 30 May — the earlier of its two
    calls — and lending on the 29th, so the novelty spread is one day, not the
    two days between the outermost calls: the spread is between *first
    sightings*, and banking's 31 May call is not news. The end-to-end assertion
    for this module: the attach, the weighting, the family filter and the three
    calculators, with the fan-out of finding 1.4 showing up as a count of six
    rather than three.
    """
    got = calculate_enstream_partner_only_features(
        snapshots,
        "customer",
        enstream_events,
        entity_cols=cfg.entity_cols_for("customer"),
        prefix="cust",
        cfg=cfg,
    )
    row = by_customer(got)["C1"]
    assert row[f"cust_{ENSTREAM}_cnt_90d"] == 3
    assert row[f"cust_{ENSTREAM}_industry_distinct_cnt"] == 2
    assert row[f"cust_{ENSTREAM}_use_case_distinct_cnt"] == 2
    assert row["cust_new_industry_spread_days"] == 1


def test_the_weighted_count_reflects_the_partner_split(
    snapshots, enstream_events, cfg
):
    """C1's two BankCo calls are discounted and its one LendCo call amplified.

    Volumes of two and one against a mean of 1.5 give ``sqrt(0.75)`` and
    ``sqrt(1.5)``, so the weighted count is ``2*sqrt(0.75) + sqrt(1.5)`` — about
    2.96 against a plain count of three. Below the plain count, because most of
    the traffic came from the partner that always calls. C3's single call weighs
    exactly one and its weighted count is exactly one, which is the control:
    the same code path with nothing to normalise against.
    """
    got = calculate_enstream_partner_only_features(
        snapshots,
        "customer",
        enstream_events,
        entity_cols=cfg.entity_cols_for("customer"),
        prefix="cust",
        cfg=cfg,
    )
    rows = by_customer(got)
    expected = 2 * math.sqrt(0.75) + math.sqrt(1.5)
    assert rows["C1"][f"cust_{ENSTREAM}_weighted_cnt_90d"] == pytest.approx(expected)
    assert rows["C3"][f"cust_{ENSTREAM}_weighted_cnt_90d"] == pytest.approx(1.0)


def test_a_customer_nobody_asked_about_is_a_row_of_nulls_and_not_absent(
    snapshots, enstream_events, cfg
):
    """Finding 4.5: C6 has a row, and every EnStream column on it is null.

    The row surviving is the contract — a number nobody has asked about is one of
    the more informative things the table says, and it says it by being present.
    What it says it *with* is nulls rather than zeros, and that is worth pinning
    because it is not obvious. ``filtered_events`` drops the left-joined null
    row, since a null ``event_family`` does not equal ``enstream_api_call``, so
    C6 contributes no group to the aggregate at all and ``safe_join`` fills every
    column from the miss side.

    Which makes the count columns asymmetric in a way a reader will trip over:
    an entity with an EnStream call outside the window gets ``cnt_90d == 0``,
    because it does form a group, while an entity with no EnStream call ever gets
    ``cnt_90d is None``. Both mean "nothing in this window" and the downstream
    imputation treats them alike, so this is recorded rather than repaired —
    unifying it is a deliberate change with this test attached, not a tidy-up.
    """
    got = calculate_enstream_partner_only_features(
        snapshots,
        "customer",
        enstream_events,
        entity_cols=cfg.entity_cols_for("customer"),
        prefix="cust",
        cfg=cfg,
    )
    rows = by_customer(got)
    assert "C6" in rows
    row = rows["C6"]
    assert row[f"cust_{ENSTREAM}_any_90d"] is None
    assert row[f"cust_{ENSTREAM}_cnt_90d"] is None
    assert row[f"cust_{ENSTREAM}_industry_distinct_cnt"] is None
    # C1 asked-about, in the same frame, so this is not a frame of all nulls.
    assert rows["C1"][f"cust_{ENSTREAM}_cnt_90d"] == 3


def test_the_wrapper_forwards_the_config(snapshots, enstream_events, narrow_cfg):
    """Finding 1.7: ``narrow_cfg`` gives two count windows and not five.

    The wrapper takes ``cfg`` and has to hand it to all three calculators and to
    the attach beneath them. Asserted negatively — the production ``cnt_90d``
    column must be *absent* — because a function that dropped the argument and
    fell back to the default would produce a superset of what was asked for, and
    every positive assertion would still pass.
    """
    got = calculate_enstream_partner_only_features(
        snapshots,
        "customer",
        enstream_events,
        entity_cols=narrow_cfg.entity_cols_for("customer"),
        prefix="cust",
        cfg=narrow_cfg,
    )
    assert columns_matching(got, f"{ENSTREAM}_cnt_") == [
        f"cust_{ENSTREAM}_cnt_30d",
        f"cust_{ENSTREAM}_cnt_3d",
    ]
    assert f"cust_{ENSTREAM}_cnt_90d" not in got.columns


def test_a_carrier_event_in_the_feed_contributes_nothing(
    snapshots, spark, cfg
):
    """A device change handed to the wrapper produces nulls, not counts.

    The wrapper is documented as taking the normalised EnStream feed, but nothing
    stops a caller passing the canonical table. The family filter is what makes
    that harmless, and asserting it here means the filter is a contract rather
    than an accident of how the feed happens to be built. Nulls rather than
    zeros for the reason in the previous test: nothing of this family survived the
    filter, so there is no group to count to zero. The rows are still five, which
    is the half of this that would break loudly.
    """
    feed = events_frame(
        spark,
        [
            make_event(
                "C1", "A1", "P1", "BELL", "2024-05-31 09:00:00", "DEVICE_CHANGE"
            ),
        ],
    )
    got = calculate_enstream_partner_only_features(
        snapshots,
        "customer",
        feed,
        entity_cols=cfg.entity_cols_for("customer"),
        prefix="cust",
        cfg=cfg,
    )
    rows = by_customer(got)
    assert len(rows) == 5
    assert rows["C1"][f"cust_{ENSTREAM}_any_90d"] is None
    assert rows["C1"][f"cust_{ENSTREAM}_cnt_90d"] is None


def test_a_missing_scope_entry_names_the_builder(snapshots, enstream_events, cfg):
    """The error tells the caller which function produces the thing they lack."""
    with pytest.raises(KeyError, match="build_scope_snapshots"):
        calculate_enstream_partner_only_features(
            {"base": snapshots["base"]},
            "customer",
            enstream_events,
            entity_cols=cfg.entity_cols_for("customer"),
            prefix="cust",
            cfg=cfg,
        )


def test_the_default_config_is_used_when_none_is_given(spark):
    """The signature's default is a usable config and not a placeholder.

    Every function in this module defaults ``cfg`` to ``FeatureConfig()``, and a
    default that did not work would only be found by a caller who omitted the
    argument. Five windows in the production config, so five weight columns.
    """
    events = scoped_frame(spark, [scoped_row("C1", "2024-05-31 09:00:00")])
    got = with_partner_volume_weights(events, ENTITY_COLS)
    assert len(columns_matching(got, "event_weight_")) == len(
        FeatureConfig().lookback_days
    )


def test_the_fixture_population_is_what_the_assertions_assume(
    snapshots, enstream_events
):
    """A guard: five customers at the reference and four events in the feed.

    Every count above is hand-computed against these two facts, so if the shared
    lineage fixture or this module's feed changes, this fails first and names the
    cause instead of leaving nine arithmetic assertions to fail obscurely.
    """
    customers = sorted(
        row["customer_id"]
        for row in snapshots["customer"].select("customer_id").collect()
    )
    assert customers == ["C1", "C2", "C3", "C5", "C6"]
    assert enstream_events.count() == 4
    assert (
        set(
            row["event_family"]
            for row in enstream_events.select("event_family").distinct().collect()
        )
        == {ENSTREAM}
    )
    assert DEVICE != ENSTREAM
