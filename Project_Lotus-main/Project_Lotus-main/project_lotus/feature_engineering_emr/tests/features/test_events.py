"""Normalisation: does an event end up attributed to the right entity?

Every feature in the package is a count, a gap or an entropy over the canonical
event table, so a defect here is not a wrong feature but a whole wrong table.
The module has three jobs and this file is organised by them: attribute a
carrier record to whoever held the number at the moment of the record, derive
the two events nobody sends, and bridge our own API traffic onto the hash the
lineage is keyed on.

Two interval conventions live side by side in this package and the tests treat
that as the load-bearing detail it is. ``normalize_account_change_events`` uses
``[from_ts, to_ts)``, half-open, because a carrier change record does not end a
lineage interval, so a device change at the instant an interval closes belongs
to the interval that is opening. The snapshot builder closes both ends, for the
opposite reason. A test that only checked the middle of an interval would pass
with either convention in place, so the handover instant itself is asserted on
directly — the fixture population exists partly to make that assertion
possible, since P6 hands over from C4 to C5 at exactly ``2024-03-01``.

The two derived builders are pinned against the review of the previous Gold
code in ``gold_base_rebuild/docs/known_issues_in_previous_code.md``, whose
finding 1.1 was a derived number change stamped at the moment the *old* number
began, and whose finding 1.5 was a port that went undetected whenever the losing
carrier fell quiet for longer than ten days. Both are asserted here by value:
the number change is stamped at the arriving interval's ``from_ts``, and a port
with a seventeen-month gap between its two sides is still a port.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark", reason="the event normalisers are Spark code")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402
from tests.features.conftest import (  # noqa: E402
    LINEAGE_ROWS,
    events_frame,
    make_event,
    ts,
)

from ts05_features.library.config import (  # noqa: E402
    CANONICAL_EVENT_FAMILY,
    EVENT_METADATA_COLS,
    FeatureConfig,
)
from ts05_features.library.events import (  # noqa: E402
    build_changes_table,
    build_mno_port_events,
    build_phone_number_change_events,
    normalize_account_change_events,
    normalize_enstream_events,
    preprocess_enstream_events,
)
from ts05_features.library.expressions import closed_lineage_intervals  # noqa: E402

pytestmark = pytest.mark.spark


# --------------------------------------------------------------------------
# local frame builders
#
# The carrier feed, the EnStream feed and the hash mapping are all inputs to
# this module and to nothing else in the suite, so their schemas live here
# rather than in the shared conftest. Each is spelled out as an explicit
# StructType because inference from a one-row list of tuples gives every
# absent value a null type and the joins then fail on the type rather than on
# the logic under test.
# --------------------------------------------------------------------------

def _lineage_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("acct_id", T.StringType(), True),
            T.StructField("msisdn", T.StringType(), True),
            T.StructField("mno", T.StringType(), True),
            T.StructField("from_ts", T.TimestampType(), True),
            T.StructField("to_ts", T.TimestampType(), True),
        ]
    )


def lineage_frame(spark, rows):
    """A lineage frame from ``(customer, account, msisdn, mno, from, to)`` rows.

    The timestamps are written as strings and parsed here, and an open end is
    written as ``None`` exactly as the Gold table writes NULL — a fixture that
    pre-filled the sentinel would make every interval assertion pass without
    ``closed_lineage_intervals`` having run.
    """
    parsed = [
        (
            customer,
            account,
            msisdn,
            mno,
            ts(from_ts) if from_ts else None,
            ts(to_ts) if to_ts else None,
        )
        for customer, account, msisdn, mno, from_ts, to_ts in rows
    ]
    return spark.createDataFrame(parsed, _lineage_schema())


def changes_frame(spark, rows):
    """A carrier ``account_changes_batch`` frame.

    Keyed on ``change_key``, which is the phone-number hash the lineage calls
    ``msisdn``. The rename happens inside the normaliser; the point of keeping
    the raw name here is that a test cannot accidentally pre-join the feed to
    the lineage by naming its key the same thing.
    """
    schema = T.StructType(
        [
            T.StructField("change_key", T.StringType(), True),
            T.StructField("change_type", T.StringType(), True),
            T.StructField("mno", T.StringType(), True),
            T.StructField("timestamp", T.TimestampType(), True),
        ]
    )
    parsed = [
        (change_key, change_type, mno, ts(timestamp))
        for change_key, change_type, mno, timestamp in rows
    ]
    return spark.createDataFrame(parsed, schema)


ENSTREAM_COLUMNS = (
    "hashed_msisdn",
    "timestamp",
    "api_type",
    "mno",
    "brand",
    "partner_id",
    "partner_name",
    "partner_type",
    "service_provider_id",
    "service_provider_name",
    "industry",
)


def enstream_call(
    hashed_msisdn,
    timestamp,
    api_type="identity_check",
    mno="TELUS",
    brand="brandA",
    partner_id="PT1",
    partner_name="Partner One",
    partner_type="bank",
    service_provider_id="SP1",
    service_provider_name="Provider One",
    industry="financial_services",
):
    """One raw EnStream call, keyed on the partner-facing hash."""
    return (
        hashed_msisdn,
        ts(timestamp),
        api_type,
        mno,
        brand,
        partner_id,
        partner_name,
        partner_type,
        service_provider_id,
        service_provider_name,
        industry,
    )


def enstream_frame(spark, rows):
    fields = [T.StructField("hashed_msisdn", T.StringType(), True)]
    fields.append(T.StructField("timestamp", T.TimestampType(), True))
    fields.extend(
        T.StructField(name, T.StringType(), True) for name in ENSTREAM_COLUMNS[2:]
    )
    return spark.createDataFrame(list(rows), T.StructType(fields))


def mapping_frame(spark, pairs):
    """The hash bridge: ``(lineage hash, partner-facing hash)`` pairs."""
    schema = T.StructType(
        [
            T.StructField("change_key_AC_hash", T.StringType(), True),
            T.StructField("change_key_AT_hash", T.StringType(), True),
        ]
    )
    return spark.createDataFrame(list(pairs), schema)


def rows_of(df):
    """``df.collect()`` as a list of dicts, for readable assertions."""
    return [row.asDict() for row in df.collect()]


# --------------------------------------------------------------------------
# normalize_account_change_events: attribution
# --------------------------------------------------------------------------

def test_a_change_record_is_attributed_to_whoever_held_the_number(
    spark, lineage_df, cfg
):
    """The same number, the same carrier, two moments, two customers.

    This is the whole reason the lineage exists and the whole reason the
    normaliser is a range join rather than a lookup. P6 belonged to C4 until
    March and to C5 afterwards, so a February device change and a June device
    change on that number are events of two different customers. Any
    implementation that joined on the number alone would attribute both to
    both.
    """
    changes = changes_frame(
        spark,
        [
            ("P6", "DEVICE_CHANGE", "TELUS", "2024-02-15 09:00:00"),
            ("P6", "DEVICE_CHANGE", "TELUS", "2024-06-01 09:00:00"),
        ],
    )
    got = normalize_account_change_events(changes, lineage_df, cfg)
    attributed = {
        (row[cfg.event_ts_col], row[cfg.customer_col], row[cfg.account_col])
        for row in rows_of(got)
    }
    assert attributed == {
        (ts("2024-02-15 09:00:00"), "C4", "A6"),
        (ts("2024-06-01 09:00:00"), "C5", "A7"),
    }


def test_a_change_on_the_handover_instant_belongs_to_the_interval_that_opens(
    spark, lineage_df, cfg
):
    """``[from_ts, to_ts)`` — half-open, and it has to be this way round here.

    C4's interval on P6 ends at ``2024-03-01`` and C5's begins at the same
    instant. A change record stamped exactly there is not the event that closed
    C4's interval — nothing in the carrier feed closes a lineage interval, that
    is done by the derived cancellation and number-change events — so it is an
    event of the account that is opening.

    Asserted as exactly one row rather than as "C5 is among them", because the
    failure mode of closing both ends is a *duplicated* event attributed to two
    customers, which no count feature would flag and which would inflate both
    customers' features at once.
    """
    changes = changes_frame(
        spark, [("P6", "SIM_CHANGE", "TELUS", "2024-03-01 00:00:00")]
    )
    got = rows_of(normalize_account_change_events(changes, lineage_df, cfg))
    assert len(got) == 1
    assert got[0][cfg.customer_col] == "C5"
    assert got[0][cfg.account_col] == "A7"


def test_a_change_at_the_exact_interval_start_is_attributed(spark, lineage_df, cfg):
    """The lower bound is inclusive, so the first instant of an interval counts.

    The mirror of the test above, and it is a separate test because the two
    bounds are separate comparisons: an implementation that made both strict
    would pass the handover test — one row, C5 — and silently drop every event
    that landed on an interval's opening instant.
    """
    changes = changes_frame(
        spark, [("P5", "DEVICE_CHANGE", "TELUS", "2024-02-01 00:00:00")]
    )
    got = rows_of(normalize_account_change_events(changes, lineage_df, cfg))
    assert len(got) == 1
    assert got[0][cfg.customer_col] == "C3"
    assert got[0][cfg.account_col] == "A5"


def test_the_open_end_of_an_interval_is_closed_before_the_join(
    spark, lineage_df, cfg
):
    """A still-current interval attributes events, rather than none at all.

    Every ``to_ts`` in the fixture except C4's is NULL, as the Gold table
    writes it. ``event_ts < NULL`` is NULL, not true, so without
    ``closed_lineage_intervals`` running first this join would attribute
    nothing to any current customer — which is to say to almost the entire
    population. Cheap to assert and catastrophic to lose.
    """
    changes = changes_frame(
        spark, [("P1", "DEVICE_CHANGE", "BELL", "2024-05-01 09:00:00")]
    )
    got = rows_of(normalize_account_change_events(changes, lineage_df, cfg))
    assert len(got) == 1
    assert got[0][cfg.customer_col] == "C1"


def test_a_change_before_the_number_was_ever_held_is_attributed_to_nobody(
    spark, lineage_df, cfg
):
    """No lineage interval covers it, so there is no answer and none is invented.

    Dropping the row is the right outcome and the only alternative worth
    considering — attributing it to the nearest interval — would put a 2019
    event on an account that opened in 2020.
    """
    changes = changes_frame(
        spark, [("P6", "DEVICE_CHANGE", "TELUS", "2019-01-01 09:00:00")]
    )
    assert normalize_account_change_events(changes, lineage_df, cfg).count() == 0


def test_the_carrier_must_match_and_not_only_the_number(spark, lineage_df, cfg):
    """A Bell record about a number that was on Telus attributes nothing.

    ``DEVICE_CHANGE`` is a supported type for Bell, so the record survives the
    support filter and is rejected by the join instead. That separation is
    deliberate: the two filters fail for different reasons and a test that used
    an unsupported type here could not tell which one had fired.
    """
    changes = changes_frame(
        spark, [("P6", "DEVICE_CHANGE", "BELL", "2024-06-01 09:00:00")]
    )
    assert normalize_account_change_events(changes, lineage_df, cfg).count() == 0


def test_an_unagreed_change_type_for_that_carrier_is_dropped(
    spark, lineage_df, cfg
):
    """Rogers does not send reactivations, so a Rogers reactivation is not real.

    Both records are in one frame and both are on the same number at the same
    instant, so the assertion is about selectivity rather than about emptiness:
    the device change survives and the reactivation does not. A filter that
    dropped everything would fail the first half.
    """
    changes = changes_frame(
        spark,
        [
            ("P4", "DEVICE_CHANGE", "ROGERS", "2024-05-01 09:00:00"),
            ("P4", "STATUS_CHANGE-A", "ROGERS", "2024-05-01 09:00:00"),
        ],
    )
    got = rows_of(normalize_account_change_events(changes, lineage_df, cfg))
    assert [row[cfg.event_type_col] for row in got] == ["DEVICE_CHANGE"]


def test_the_same_type_is_supported_for_one_carrier_and_not_another(
    spark, lineage_df, cfg
):
    """``STATUS_CHANGE-A`` is a Bell type and not a Rogers one.

    The support table is keyed on the pair, not on the type, and this is the
    test that would fail if it were flattened to a set of type names — which is
    a tempting simplification because six of the ten pairs are shared.
    """
    changes = changes_frame(
        spark,
        [
            ("P4", "STATUS_CHANGE-A", "ROGERS", "2024-05-01 09:00:00"),
            ("P1", "STATUS_CHANGE-A", "BELL", "2024-05-01 09:00:00"),
        ],
    )
    got = rows_of(normalize_account_change_events(changes, lineage_df, cfg))
    assert [row[cfg.customer_col] for row in got] == ["C1"]


def test_a_redelivered_carrier_file_is_one_event(spark, lineage_df, cfg):
    """Three identical records are one real-world change.

    Carriers redeliver files and the platform reads whatever is in the
    partition, so this is the ordinary case rather than a corruption. Left
    undeduplicated it inflates every count feature by the redelivery factor,
    which varies by carrier and by month and is therefore not even a constant
    bias.
    """
    record = ("P1", "DEVICE_CHANGE", "BELL", "2024-05-01 09:00:00")
    changes = changes_frame(spark, [record, record, record])
    assert normalize_account_change_events(changes, lineage_df, cfg).count() == 1


def test_two_different_changes_at_one_instant_are_two_events(
    spark, lineage_df, cfg
):
    """A device change and a SIM change in one batch stay two rows.

    The carrier stamps the batch rather than the event, so simultaneous
    different types are common — and that pair is one of the sharpest signals
    in the feature set, per finding 1.5. A dedupe key that omitted the change
    type would collapse it to one event and the transition feature between them
    would have nothing to measure.
    """
    changes = changes_frame(
        spark,
        [
            ("P1", "DEVICE_CHANGE", "BELL", "2024-05-01 09:00:00"),
            ("P1", "SIM_CHANGE", "BELL", "2024-05-01 09:00:00"),
        ],
    )
    got = rows_of(normalize_account_change_events(changes, lineage_df, cfg))
    assert sorted(row[cfg.event_type_col] for row in got) == [
        "DEVICE_CHANGE",
        "SIM_CHANGE",
    ]


def test_the_family_comes_from_the_mapping_and_the_raw_type_is_kept(
    spark, lineage_df, cfg
):
    """``STATUS_CHANGE-C`` maps to ``account_cancellation`` and survives as itself.

    Both halves matter and for different reasons. The family is what the
    frequency and recency features filter on; the raw type is what the
    transition features compare against, and finding 1.2 was exactly a raw type
    misspelled with an underscore so that the comparison was false for every
    row. Round-tripping the hyphenated spelling through normalisation is what
    makes the transition features reachable at all.
    """
    changes = changes_frame(
        spark, [("P1", "STATUS_CHANGE-C", "BELL", "2024-05-01 09:00:00")]
    )
    got = rows_of(normalize_account_change_events(changes, lineage_df, cfg))
    assert len(got) == 1
    assert got[0][cfg.event_family_col] == "account_cancellation"
    assert got[0][cfg.event_type_col] == "STATUS_CHANGE-C"
    assert CANONICAL_EVENT_FAMILY[got[0][cfg.event_type_col]] == (
        got[0][cfg.event_family_col]
    )


def test_a_normalised_row_names_its_feed_and_carries_a_unit_weight(
    spark, lineage_df, cfg
):
    """``event_source`` is the provenance the weight column does not record.

    Finding 3.2 is that ``event_weight`` is overloaded — a partner-volume
    correction for EnStream rows and a plain one for carrier rows, with nothing
    to tell them apart. ``event_source`` is the only thing that distinguishes
    them, so it is asserted rather than assumed.
    """
    changes = changes_frame(
        spark, [("P1", "DEVICE_CHANGE", "BELL", "2024-05-01 09:00:00")]
    )
    got = rows_of(normalize_account_change_events(changes, lineage_df, cfg))
    assert got[0][cfg.event_source_col] == "account_change"
    assert got[0][cfg.event_weight_col] == 1.0


def test_the_normalised_frame_carries_the_lineage_identity_columns(
    spark, lineage_df, cfg
):
    """Customer, account, number and carrier, all from the lineage side.

    The feed knows only the number. Everything else on the output row is the
    lineage's answer, and the scope snapshots join on the customer and account
    columns, so a missing one is not a missing feature but a missing entity.
    """
    changes = changes_frame(
        spark, [("P1", "DEVICE_CHANGE", "BELL", "2024-05-01 09:00:00")]
    )
    got = normalize_account_change_events(changes, lineage_df, cfg)
    for col_name in (
        cfg.customer_col,
        cfg.account_col,
        cfg.msisdn_col,
        cfg.mno_col,
        cfg.event_ts_col,
        cfg.event_family_col,
        cfg.event_type_col,
        cfg.event_source_col,
        cfg.event_weight_col,
    ):
        assert col_name in got.columns
    assert "change_key" not in got.columns
    assert "timestamp" not in got.columns
    assert len(got.columns) == len(set(got.columns))


# --------------------------------------------------------------------------
# build_phone_number_change_events
# --------------------------------------------------------------------------

NUMBER_CHANGE_ROWS = (
    # A9 changed its number in January 2023: the change is at the arriving
    # interval's start, not the departing one's.
    ("C7", "A9", "P8", "BELL", "2022-01-01", "2023-01-01"),
    ("C7", "A9", "P9", "BELL", "2023-01-01", None),
    # A10 has the same number across two consecutive intervals — a lineage
    # split for some other reason — and then changes it.
    ("C7", "A10", "P11", "BELL", "2022-01-01", "2022-06-01"),
    ("C7", "A10", "P11", "BELL", "2022-06-01", "2023-01-01"),
    ("C7", "A10", "P12", "BELL", "2023-01-01", None),
    # A11 changed both its number and its carrier at once.
    ("C8", "A11", "P13", "BELL", "2022-01-01", "2023-03-01"),
    ("C8", "A11", "P14", "TELUS", "2023-03-01", None),
)


def test_a_number_change_is_stamped_when_the_new_number_took_effect(spark, cfg):
    """``from_ts`` of the arriving interval, which is when the change happened.

    Deliberately the opposite of the previous Gold code's finding 1.1, where the
    derived change was stamped at the moment the *old* number began — in this
    fixture that would be ``2022-01-01``, a year early, which puts the event
    outside every lookback window the features use and so produces not a wrong
    number but a missing one.
    """
    lineage = lineage_frame(spark, NUMBER_CHANGE_ROWS)
    got = [
        row
        for row in rows_of(build_phone_number_change_events(lineage, cfg))
        if row[cfg.account_col] == "A9"
    ]
    assert len(got) == 1
    assert got[0][cfg.event_ts_col] == ts("2023-01-01")
    assert got[0][cfg.event_ts_col] != ts("2022-01-01")
    assert got[0][cfg.msisdn_col] == "P9"


def test_the_first_interval_of_an_account_is_not_a_number_change(spark, cfg):
    """An account arriving with a number has not changed it.

    The ``lag`` is null for the first row of every partition and the filter
    requires it not to be, which is the guard this asserts. Without it every
    account in the population acquires a spurious number-change event at the
    moment it opened — a feature that would correlate with account age and look
    plausible while measuring nothing.
    """
    lineage = lineage_frame(
        spark, [("C7", "A9", "P8", "BELL", "2022-01-01", None)]
    )
    assert build_phone_number_change_events(lineage, cfg).count() == 0


def test_the_same_number_across_two_intervals_is_not_a_change(spark, cfg):
    """A lineage split that kept the number is not a number change.

    A10 has three intervals and only one number change. Intervals split for
    reasons the features do not model — a plan change, a segment
    reclassification — and a builder that emitted an event per interval
    boundary would turn every one of them into a phone-number change.
    """
    lineage = lineage_frame(spark, NUMBER_CHANGE_ROWS)
    got = [
        row
        for row in rows_of(build_phone_number_change_events(lineage, cfg))
        if row[cfg.account_col] == "A10"
    ]
    assert len(got) == 1
    assert got[0][cfg.event_ts_col] == ts("2023-01-01")


def test_a_number_change_that_was_also_a_port_is_not_reported_here(spark, cfg):
    """Partitioned by carrier, so the two sides of a port do not compare.

    A11 changed number and carrier in one step. The partition includes the MNO,
    so the Bell interval and the Telus interval are in different windows and
    neither sees the other. It is not an MNO port either — the number changed,
    and ``build_mno_port_events`` requires it to be unchanged — so this
    transition is deliberately reported by neither builder. Recording it as a
    number change would attribute a carrier event to the wrong family, and the
    families are what every count feature filters on.
    """
    lineage = lineage_frame(spark, NUMBER_CHANGE_ROWS)
    got = [
        row
        for row in rows_of(build_phone_number_change_events(lineage, cfg))
        if row[cfg.account_col] == "A11"
    ]
    assert got == []
    ported = [
        row
        for row in rows_of(build_mno_port_events(lineage, cfg))
        if row[cfg.account_col] == "A11"
    ]
    assert ported == []


def test_a_derived_number_change_is_stamped_as_a_lineage_event(spark, cfg):
    """Family, raw type, feed and weight, all four asserted.

    ``phone_number_change`` is one of the two families the missing comma of
    finding 1.1 deleted from the feature set. It is derived rather than
    delivered, so nothing outside this function sets its family, and a family
    string that did not match ``CANONICAL_EVENT_FAMILY`` would produce the same
    silent all-null columns the comma did.
    """
    lineage = lineage_frame(spark, NUMBER_CHANGE_ROWS)
    got = rows_of(build_phone_number_change_events(lineage, cfg))
    assert got
    for row in got:
        assert row[cfg.event_family_col] == "phone_number_change"
        assert row[cfg.event_type_col] == "PHONE_NUMBER_CHANGE"
        assert (
            CANONICAL_EVENT_FAMILY[row[cfg.event_type_col]]
            == row[cfg.event_family_col]
        )
        assert row[cfg.event_source_col] == "lineage"
        assert row[cfg.event_weight_col] == 1.0


def test_the_shared_population_holds_no_number_changes(spark, lineage_df, cfg):
    """A negative control on the fixture the rest of the suite uses.

    No account in ``LINEAGE_ROWS`` changes its number — P6 moves between
    customers, which is a recycle and not a change — so the count is zero. This
    is worth pinning because several later tests count events attached to a
    scope from that population, and a spurious derived event would change those
    counts in a way that is hard to trace back to this builder.
    """
    assert build_phone_number_change_events(lineage_df, cfg).count() == 0


# --------------------------------------------------------------------------
# build_mno_port_events
# --------------------------------------------------------------------------

PORT_ROWS = (
    # A clean port: same customer, same number, new carrier, new account.
    ("C9", "A12", "P15", "BELL", "2022-01-01", "2023-05-01"),
    ("C9", "A13", "P15", "TELUS", "2023-05-01", None),
    # The quiet losing carrier: seventeen months between the two sides.
    ("C10", "A14", "P16", "BELL", "2020-01-01", "2021-01-01"),
    ("C10", "A15", "P16", "ROGERS", "2022-06-01", None),
    # Same carrier, new number: not a port.
    ("C11", "A16", "P17", "TELUS", "2022-01-01", "2023-01-01"),
    ("C11", "A16", "P18", "TELUS", "2023-01-01", None),
    # The same number moving between two customers: a recycle, not a port.
    ("C12", "A17", "P19", "BELL", "2021-01-01", "2023-01-01"),
    ("C13", "A18", "P19", "TELUS", "2023-01-01", None),
)


def test_a_number_appearing_on_a_new_carrier_is_a_port(spark, cfg):
    """One event, stamped at the gaining interval's start, on the new carrier.

    The gaining side is the right one to carry the event because the port is
    what opened it, and the features are computed against the account that
    exists at the reference moment.
    """
    lineage = lineage_frame(spark, PORT_ROWS)
    got = [
        row
        for row in rows_of(build_mno_port_events(lineage, cfg))
        if row[cfg.customer_col] == "C9"
    ]
    assert len(got) == 1
    assert got[0][cfg.event_ts_col] == ts("2023-05-01")
    assert got[0][cfg.mno_col] == "TELUS"
    assert got[0][cfg.account_col] == "A13"
    assert got[0][cfg.msisdn_col] == "P15"


def test_a_port_is_seen_across_accounts_because_it_ends_one_and_begins_another(
    spark, cfg
):
    """Partitioned by customer and number, not by account.

    C9's port moves the number from A12 to A13. Partitioning by account — which
    is the natural reading of "an account changed carrier" and is what the
    account-level features are grouped by everywhere else — puts the two sides
    in different windows and detects nothing at all. Asserted as two distinct
    accounts on the two sides so that the test fails if the fixture is ever
    tidied into one.
    """
    lineage = lineage_frame(spark, PORT_ROWS)
    accounts = {
        row["acct_id"]
        for row in rows_of(lineage)
        if row["customer_id"] == "C9"
    }
    assert accounts == {"A12", "A13"}
    assert build_mno_port_events(lineage, cfg).filter(
        F.col(cfg.customer_col) == "C9"
    ).count() == 1


def test_a_quiet_losing_carrier_does_not_hide_the_port(spark, cfg):
    """Seventeen months between the two sides, and it is still a port.

    This is the previous Gold code's finding 1.5, the defect with the largest
    blast radius in that review: the two carriers' events were required to be
    within ten days of each other, so every port where the losing carrier fell
    quiet first was discarded. C10's number left Bell in January 2021 and
    appeared on Rogers in June 2022. A change of carrier for an unchanged number
    is directly observable and needs no window to believe, and the gap is
    asserted explicitly so that reintroducing any window at all fails here.
    """
    lineage = lineage_frame(spark, PORT_ROWS)
    got = [
        row
        for row in rows_of(build_mno_port_events(lineage, cfg))
        if row[cfg.customer_col] == "C10"
    ]
    assert len(got) == 1
    assert got[0][cfg.event_ts_col] == ts("2022-06-01")
    gap_days = (ts("2022-06-01") - ts("2021-01-01")).days
    assert gap_days > 500


def test_a_new_number_on_the_same_carrier_is_not_a_port(spark, cfg):
    """The carrier has to change, which is what the ``!=`` on the MNO asserts."""
    lineage = lineage_frame(spark, PORT_ROWS)
    got = [
        row
        for row in rows_of(build_mno_port_events(lineage, cfg))
        if row[cfg.customer_col] == "C11"
    ]
    assert got == []


def test_a_recycled_number_moving_between_customers_is_not_a_port(spark, cfg):
    """Two customers, one number, two carriers — and no port.

    The partition is by customer, so C12's Bell interval and C13's Telus
    interval never compare. That is the correct outcome: a number released by
    one subscriber and reissued to another is a recycle, and recording it as a
    port would give the arriving customer an event that happened to somebody
    else. This is the case the shared fixture's P6 exists for, generalised to
    two carriers so that the customer partition rather than the carrier test is
    what rejects it.
    """
    lineage = lineage_frame(spark, PORT_ROWS)
    ported = {row[cfg.customer_col] for row in rows_of(build_mno_port_events(lineage, cfg))}
    assert "C12" not in ported
    assert "C13" not in ported
    assert ported == {"C9", "C10"}


def test_a_derived_port_is_stamped_as_a_lineage_event(spark, cfg):
    """Family, raw type, feed and weight."""
    lineage = lineage_frame(spark, PORT_ROWS)
    got = rows_of(build_mno_port_events(lineage, cfg))
    assert got
    for row in got:
        assert row[cfg.event_family_col] == "mno_port"
        assert row[cfg.event_type_col] == "MNO_PORT"
        assert (
            CANONICAL_EVENT_FAMILY[row[cfg.event_type_col]]
            == row[cfg.event_family_col]
        )
        assert row[cfg.event_source_col] == "lineage"
        assert row[cfg.event_weight_col] == 1.0


def test_the_shared_population_holds_no_ports(spark, lineage_df, cfg):
    """The other negative control. C3 has two carriers but two numbers.

    C3 is on Rogers with P4 and on Telus with P5, which is two subscriptions
    rather than a port, and the number test is what distinguishes them. If this
    ever returns a row, the ``__prev_msisdn == msisdn`` guard has gone and every
    multi-carrier customer in the population has acquired a fictional port.
    """
    assert build_mno_port_events(lineage_df, cfg).count() == 0


# --------------------------------------------------------------------------
# preprocess_enstream_events: the hash bridge
# --------------------------------------------------------------------------

def test_the_bridge_is_inner_so_an_unmapped_number_is_dropped(spark, cfg):
    """A call we cannot attribute is a call we do not carry forward.

    The alternative — keeping it with a null MSISDN — is worse than dropping
    it, because a null joins to nothing downstream but is counted by every
    aggregate that does not exclude it, and the totals then disagree with the
    per-entity sums.
    """
    calls = enstream_frame(
        spark,
        [
            enstream_call("AT-1", "2024-05-01 09:00:00"),
            enstream_call("AT-unknown", "2024-05-01 09:00:00"),
        ],
    )
    mapping = mapping_frame(spark, [("P6", "AT-1")])
    got = rows_of(preprocess_enstream_events(calls, mapping, cfg=cfg))
    assert len(got) == 1
    assert got[0][cfg.msisdn_col] == "P6"


def test_the_lineage_hash_replaces_the_partner_facing_one(spark, cfg):
    """The output is keyed on ``msisdn``, which is what the lineage joins on.

    The partner-facing hash is left on the frame — it is harmless and it is
    provenance — but the column the rest of the pipeline reads has to be the
    lineage's name for the number, or the attribution join finds nothing and
    the EnStream family is empty.
    """
    calls = enstream_frame(spark, [enstream_call("AT-1", "2024-05-01 09:00:00")])
    mapping = mapping_frame(spark, [("P6", "AT-1")])
    got = preprocess_enstream_events(calls, mapping, cfg=cfg)
    assert cfg.msisdn_col in got.columns
    assert "change_key_AC_hash" not in got.columns
    assert rows_of(got)[0][cfg.msisdn_col] == "P6"


def test_the_api_type_becomes_the_use_case(spark, cfg):
    """``use_case`` is a dedupe key and a weighting dimension, and it is derived.

    The feed calls it ``api_type``. It is renamed here because it appears in
    ``event_dedupe_keys`` and in ``enstream_partner_cols``, so the name has to
    settle before either is applied — and finding 1.4 is what happens when the
    weight grain and the join grain disagree.
    """
    calls = enstream_frame(
        spark,
        [enstream_call("AT-1", "2024-05-01 09:00:00", api_type="device_check")],
    )
    mapping = mapping_frame(spark, [("P6", "AT-1")])
    got = rows_of(preprocess_enstream_events(calls, mapping, cfg=cfg))
    assert got[0]["use_case"] == "device_check"
    assert "use_case" in cfg.event_dedupe_keys()


def test_two_partners_asking_at_the_same_instant_are_two_calls(spark, cfg):
    """A bank and a lender checking one number in the same second.

    Two calls, and the volume the partner weighting is computed against depends
    on them staying two. This is finding 1.4 approached from the dedupe side:
    the weight is normalised by a partner's volume, so collapsing simultaneous
    multi-partner traffic understates every partner's volume at once and moves
    every weight in the same direction.
    """
    calls = enstream_frame(
        spark,
        [
            enstream_call("AT-1", "2024-05-01 09:00:00", partner_id="PT1"),
            enstream_call("AT-1", "2024-05-01 09:00:00", partner_id="PT2"),
        ],
    )
    mapping = mapping_frame(spark, [("P6", "AT-1")])
    got = rows_of(preprocess_enstream_events(calls, mapping, cfg=cfg))
    assert sorted(row["partner_id"] for row in got) == ["PT1", "PT2"]


def test_the_same_partner_asking_twice_at_the_same_instant_is_one_call(spark, cfg):
    """A redelivered call, deduplicated before the bridge rather than after.

    Before matters: the mapping table can carry more than one partner-facing
    hash for a number, so a duplicate surviving into the join is multiplied by
    the mapping's fan-out rather than merely doubled.
    """
    call = enstream_call("AT-1", "2024-05-01 09:00:00")
    calls = enstream_frame(spark, [call, call, call])
    mapping = mapping_frame(spark, [("P6", "AT-1")])
    assert preprocess_enstream_events(calls, mapping, cfg=cfg).count() == 1


def test_the_same_partner_asking_twice_a_second_apart_is_two_calls(spark, cfg):
    """The timestamp is in the dedupe key, so repeated checking is visible.

    A partner polling a number is a signal rather than a duplicate — the
    EnStream frequency and burstness features exist to measure exactly that —
    so the key has to be the full identity of the call including its moment.
    """
    calls = enstream_frame(
        spark,
        [
            enstream_call("AT-1", "2024-05-01 09:00:00"),
            enstream_call("AT-1", "2024-05-01 09:00:01"),
        ],
    )
    mapping = mapping_frame(spark, [("P6", "AT-1")])
    assert preprocess_enstream_events(calls, mapping, cfg=cfg).count() == 2


def test_a_bridged_call_is_stamped_as_an_enstream_event(spark, cfg):
    """Family, raw type, feed and weight, plus a real timestamp.

    ``event_ts`` is cast from the feed's ``timestamp`` here; every lookback
    window in the package is interval arithmetic on that column, so a string
    surviving into it would compare lexicographically and quietly give the
    wrong window membership rather than an error.
    """
    calls = enstream_frame(spark, [enstream_call("AT-1", "2024-05-01 09:00:00")])
    mapping = mapping_frame(spark, [("P6", "AT-1")])
    got = preprocess_enstream_events(calls, mapping, cfg=cfg)
    row = rows_of(got)[0]
    assert row[cfg.event_family_col] == "enstream_api_call"
    assert row[cfg.event_type_col] == "ENSTREAM_API_CALL"
    assert row[cfg.event_source_col] == "enstream"
    assert row[cfg.event_weight_col] == 1.0
    assert row[cfg.event_ts_col] == ts("2024-05-01 09:00:00")
    assert isinstance(
        got.schema[cfg.event_ts_col].dataType, T.TimestampType
    )


# --------------------------------------------------------------------------
# normalize_enstream_events: attribution
# --------------------------------------------------------------------------

def _enstream_event(
    msisdn,
    mno,
    event_ts,
    partner_id="PT1",
    service_provider_id="SP1",
    use_case="identity_check",
):
    """A preprocessed EnStream row, with a deliberately wrong entity guess.

    ``customer_id`` and ``acct_id`` are filled with ``"GUESS"`` so that any
    test asserting on the normalised entity is asserting that the *lineage*
    answered, not that the input happened to be right.
    """
    return make_event(
        "GUESS",
        "GUESS",
        msisdn,
        mno,
        event_ts,
        "ENSTREAM_API_CALL",
        event_source="enstream",
        partner_id=partner_id,
        partner_name="Partner One",
        partner_type="bank",
        service_provider_id=service_provider_id,
        service_provider_name="Provider One",
        industry="financial_services",
        use_case=use_case,
    )


def test_a_call_is_attributed_to_the_holder_at_that_moment(spark, lineage_df, cfg):
    """The same range join as the carrier feed, and the same answer.

    P6 in February is C4's and in June is C5's. The input's own guess at the
    customer is discarded, which is asserted by the ``"GUESS"`` sentinel never
    appearing in the output.
    """
    calls = events_frame(
        spark,
        [
            _enstream_event("P6", "TELUS", "2024-02-15 09:00:00"),
            _enstream_event("P6", "TELUS", "2024-06-01 09:00:00"),
        ],
    )
    got = rows_of(normalize_enstream_events(calls, lineage_df, cfg))
    attributed = {
        (row[cfg.event_ts_col], row[cfg.customer_col], row[cfg.account_col])
        for row in got
    }
    assert attributed == {
        (ts("2024-02-15 09:00:00"), "C4", "A6"),
        (ts("2024-06-01 09:00:00"), "C5", "A7"),
    }
    assert "GUESS" not in {row[cfg.customer_col] for row in got}


def test_a_call_answered_by_the_wrong_carrier_attributes_nothing(
    spark, lineage_df, cfg
):
    """A Bell-answered call cannot belong to a Telus interval.

    That combination means the port boundary is in the wrong place, and the
    right response is to attribute nothing rather than to guess. Dropping the
    carrier from the join would attribute the call to C5 — a plausible row,
    which is the failure mode that does not get noticed.
    """
    calls = events_frame(
        spark, [_enstream_event("P6", "BELL", "2024-06-01 09:00:00")]
    )
    assert normalize_enstream_events(calls, lineage_df, cfg).count() == 0


def test_the_enstream_interval_is_half_open_as_well(spark, lineage_df, cfg):
    """A call at the handover instant belongs to the arriving account.

    The same convention as the carrier feed, asserted separately because it is
    a separate pair of comparisons in a separate function — and the failure
    mode is again a duplicated row rather than a missing one.
    """
    calls = events_frame(
        spark, [_enstream_event("P6", "TELUS", "2024-03-01 00:00:00")]
    )
    got = rows_of(normalize_enstream_events(calls, lineage_df, cfg))
    assert len(got) == 1
    assert got[0][cfg.customer_col] == "C5"


def test_the_partner_metadata_survives_normalisation(spark, lineage_df, cfg):
    """All seven metadata columns, because the EnStream-only features need them.

    Diversity, novelty and the partner-volume weighting are the three feature
    groups no carrier family has, and each reads one of these columns. A select
    list that dropped them would leave those features computing entropy over a
    single null bucket — a constant column, not an error.
    """
    calls = events_frame(
        spark, [_enstream_event("P6", "TELUS", "2024-06-01 09:00:00")]
    )
    got = normalize_enstream_events(calls, lineage_df, cfg)
    for col_name in EVENT_METADATA_COLS:
        assert col_name in got.columns
    row = rows_of(got)[0]
    assert row["partner_id"] == "PT1"
    assert row["service_provider_id"] == "SP1"
    assert row["industry"] == "financial_services"
    assert row["use_case"] == "identity_check"


def test_two_partners_at_one_instant_both_survive_the_attribution_dedupe(
    spark, lineage_df, cfg
):
    """The dedupe key carries the partner identity, so both rows live.

    The key is applied after the join here, and the join can only ever produce
    one row per call, so what this actually pins is that the key is not
    narrower than the call's identity. Narrow it to the entity and the
    timestamp — which is the obvious-looking key — and simultaneous
    multi-partner traffic collapses.
    """
    calls = events_frame(
        spark,
        [
            _enstream_event("P6", "TELUS", "2024-06-01 09:00:00", partner_id="PT1"),
            _enstream_event("P6", "TELUS", "2024-06-01 09:00:00", partner_id="PT2"),
        ],
    )
    got = rows_of(normalize_enstream_events(calls, lineage_df, cfg))
    assert sorted(row["partner_id"] for row in got) == ["PT1", "PT2"]


def test_two_use_cases_at_one_instant_are_two_calls(spark, lineage_df, cfg):
    """``use_case`` is in the key, and it is also the weighting grain.

    Tied to ``enstream_partner_cols`` rather than restated: the weights are
    computed per partner grain, so a dedupe key coarser than that grain would
    merge two rows whose weights differ, and whichever survived would carry a
    weight computed from a volume it did not contribute to.
    """
    calls = events_frame(
        spark,
        [
            _enstream_event(
                "P6", "TELUS", "2024-06-01 09:00:00", use_case="identity_check"
            ),
            _enstream_event(
                "P6", "TELUS", "2024-06-01 09:00:00", use_case="device_check"
            ),
        ],
    )
    got = rows_of(normalize_enstream_events(calls, lineage_df, cfg))
    assert sorted(row["use_case"] for row in got) == [
        "device_check",
        "identity_check",
    ]
    for col_name in cfg.enstream_partner_cols:
        assert col_name in cfg.event_dedupe_keys()


def test_the_bridge_and_the_attribution_compose(spark, lineage_df, cfg):
    """Raw feed to attributed event, both halves in one call chain.

    Each half is tested alone above; this asserts that the columns one produces
    are the columns the other reads. The two functions are the only place in
    the package where a frame is handed from one module-level function to
    another without an intervening schema alignment, so a rename in either
    would otherwise be caught only by a cluster run.
    """
    calls = enstream_frame(
        spark, [enstream_call("AT-1", "2024-06-01 09:00:00", mno="TELUS")]
    )
    mapping = mapping_frame(spark, [("P6", "AT-1")])
    bridged = preprocess_enstream_events(calls, mapping, cfg=cfg)
    got = rows_of(normalize_enstream_events(bridged, lineage_df, cfg))
    assert len(got) == 1
    assert got[0][cfg.customer_col] == "C5"
    assert got[0][cfg.account_col] == "A7"
    assert got[0][cfg.event_family_col] == "enstream_api_call"
    assert got[0]["use_case"] == "identity_check"


# --------------------------------------------------------------------------
# build_changes_table
# --------------------------------------------------------------------------

@pytest.fixture
def account_changes(spark):
    """A small carrier batch touching four of the fixture's customers."""
    return changes_frame(
        spark,
        [
            ("P1", "DEVICE_CHANGE", "BELL", "2024-05-01 09:00:00"),
            ("P1", "SIM_CHANGE", "BELL", "2024-05-01 09:00:00"),
            ("P2", "STATUS_CHANGE-C", "BELL", "2024-04-01 09:00:00"),
            ("P5", "DEVICE_CHANGE", "TELUS", "2024-03-01 09:00:00"),
        ],
    )


def test_every_feed_lands_in_one_table_with_one_schema(
    spark, account_changes, lineage_df, cfg
):
    """The union's columns are exactly the base columns, in that order.

    The carrier feed has no partner metadata and the EnStream feed has no
    derived-event provenance, so the frames are aligned to the union of their
    columns plus everything ``event_base_columns`` requires before the union
    rather than after. ``unionByName`` on unaligned frames either raises or —
    with ``allowMissingColumns`` — silently produces a frame whose column order
    depends on which feed was first.
    """
    calls = events_frame(
        spark, [_enstream_event("P6", "TELUS", "2024-06-01 09:00:00")]
    )
    got = build_changes_table(account_changes, lineage_df, calls, cfg=cfg)
    assert list(got.columns) == list(cfg.event_base_columns())
    assert len(got.columns) == len(set(got.columns))


def test_a_carrier_row_gets_typed_nulls_for_the_partner_columns(
    spark, account_changes, lineage_df, cfg
):
    """Null and string-typed, not null and void-typed.

    The alignment types an absent column rather than filling it with
    ``F.lit(None)``, which would give it Spark's null type — and a null-typed
    column cannot be grouped, compared or written to Parquet, so the failure
    would land in the feature stage or in the write rather than here.
    """
    calls = events_frame(
        spark, [_enstream_event("P6", "TELUS", "2024-06-01 09:00:00")]
    )
    got = build_changes_table(account_changes, lineage_df, calls, cfg=cfg)
    for col_name in EVENT_METADATA_COLS:
        assert isinstance(got.schema[col_name].dataType, T.StringType)
    carrier_rows = [
        row for row in rows_of(got) if row[cfg.event_source_col] == "account_change"
    ]
    assert carrier_rows
    for row in carrier_rows:
        assert row["partner_id"] is None
        assert row["use_case"] is None


def test_the_derived_events_are_included_without_being_asked_for(
    spark, account_changes, cfg
):
    """A number change and a port appear in the table with no argument for them.

    The two derived families are the ones the caller cannot supply — nobody
    sends them — so ``build_changes_table`` deriving them from the lineage it
    was given for attribution is the only place they can come from. A caller
    who forgot them would get a table that looked complete.
    """
    lineage = lineage_frame(
        spark,
        (
            ("C7", "A9", "P8", "BELL", "2022-01-01", "2023-01-01"),
            ("C7", "A9", "P9", "BELL", "2023-01-01", None),
            ("C9", "A12", "P15", "BELL", "2022-01-01", "2023-05-01"),
            ("C9", "A13", "P15", "TELUS", "2023-05-01", None),
        ),
    )
    got = build_changes_table(account_changes, lineage, cfg=cfg)
    families = {row[cfg.event_family_col] for row in rows_of(got)}
    assert "phone_number_change" in families
    assert "mno_port" in families


def test_the_enstream_feed_is_optional(spark, account_changes, lineage_df, cfg):
    """Omitting it leaves the table without that family, and nothing else.

    The EnStream feed is a separate extract on a separate schedule, so a run
    that has the carrier partition and not the EnStream one is normal. It must
    produce a smaller table rather than an error or an empty one.
    """
    without = build_changes_table(account_changes, lineage_df, cfg=cfg)
    families = {row[cfg.event_family_col] for row in rows_of(without)}
    assert "enstream_api_call" not in families
    assert without.count() == 4

    calls = events_frame(
        spark, [_enstream_event("P6", "TELUS", "2024-06-01 09:00:00")]
    )
    with_calls = build_changes_table(account_changes, lineage_df, calls, cfg=cfg)
    assert with_calls.count() == 5


def test_an_extra_feed_is_appended_without_touching_the_function(
    spark, account_changes, lineage_df, cfg
):
    """``extra_event_frames`` is the seam for a feed that does not exist yet.

    Anything carrying the base columns can be unioned in. Asserted with a row
    whose ``event_source`` is a name this package has never heard of, so the
    test cannot pass by the frame having been recognised as one of the known
    feeds.
    """
    extra = events_frame(
        spark,
        [
            make_event(
                "C6",
                "A8",
                "P7",
                "BELL",
                "2024-05-15 09:00:00",
                "DEVICE_CHANGE",
                event_source="future_feed",
            )
        ],
    )
    got = build_changes_table(
        account_changes, lineage_df, extra_event_frames=[extra], cfg=cfg
    )
    sources = {row[cfg.event_source_col] for row in rows_of(got)}
    assert "future_feed" in sources
    assert got.count() == 5


def test_a_row_delivered_by_two_feeds_is_one_row(
    spark, account_changes, lineage_df, cfg
):
    """The dedupe is applied to the union, not only within each feed.

    An extra feed replaying a carrier event exactly — same entity, same moment,
    same type, same provenance — is one event. This is the case an overlapping
    backfill produces, and every count feature is a ``sum`` over these rows.
    """
    duplicate = events_frame(
        spark,
        [
            make_event(
                "C1",
                "A1",
                "P1",
                "BELL",
                "2024-05-01 09:00:00",
                "DEVICE_CHANGE",
                event_source="account_change",
            )
        ],
    )
    got = build_changes_table(
        account_changes, lineage_df, extra_event_frames=[duplicate], cfg=cfg
    )
    assert got.count() == 4


def test_the_same_event_from_two_feeds_stays_two_rows(
    spark, account_changes, lineage_df, cfg
):
    """``event_source`` is in the dedupe key, so provenance is not collapsed.

    The mirror of the test above and the reason the key includes the feed: two
    feeds independently reporting one real change are two observations, and
    finding 3.2 is that the weight column cannot tell them apart. Collapsing
    them here would decide that question silently, in favour of whichever feed
    Spark happened to keep.
    """
    same_event_other_feed = events_frame(
        spark,
        [
            make_event(
                "C1",
                "A1",
                "P1",
                "BELL",
                "2024-05-01 09:00:00",
                "DEVICE_CHANGE",
                event_source="partner_feed",
            )
        ],
    )
    got = build_changes_table(
        account_changes,
        lineage_df,
        extra_event_frames=[same_event_other_feed],
        cfg=cfg,
    )
    assert got.count() == 5


def test_an_already_closed_lineage_is_accepted_unchanged(
    spark, account_changes, lineage_df, cfg
):
    """Idempotence, which the module docstring claims and nothing else checks.

    ``closed_lineage_intervals`` is applied by every function here, and
    ``build_changes_table`` applies it once and then passes the result to three
    functions that each apply it again. If it were not idempotent the derived
    events would be built from a doubly-coalesced frame, so the claim is
    load-bearing rather than decorative.
    """
    raw = build_changes_table(account_changes, lineage_df, cfg=cfg)
    pre_closed = build_changes_table(
        account_changes, closed_lineage_intervals(lineage_df, cfg), cfg=cfg
    )
    assert raw.count() == pre_closed.count()
    assert list(raw.columns) == list(pre_closed.columns)
    key = [cfg.customer_col, cfg.event_ts_col, cfg.event_type_col]
    assert sorted(tuple(row[k] for k in key) for row in rows_of(raw)) == sorted(
        tuple(row[k] for k in key) for row in rows_of(pre_closed)
    )


def test_the_base_columns_appear_even_when_no_feed_carried_them(
    spark, account_changes, lineage_df, cfg
):
    """A feed with no partner metadata still yields the full canonical schema.

    The EnStream frame here has been stripped to the nine core columns, which
    is what a feed in a month before the partner columns were added looks like.
    The features read the metadata columns unconditionally, so they have to
    exist and be null rather than be absent.
    """
    core = [
        cfg.customer_col,
        cfg.account_col,
        cfg.msisdn_col,
        cfg.mno_col,
        cfg.event_ts_col,
        cfg.event_family_col,
        cfg.event_type_col,
        cfg.event_source_col,
        cfg.event_weight_col,
    ]
    stripped = events_frame(
        spark, [_enstream_event("P6", "TELUS", "2024-06-01 09:00:00")]
    ).select(*core)
    assert set(EVENT_METADATA_COLS).isdisjoint(stripped.columns)

    got = build_changes_table(account_changes, lineage_df, stripped, cfg=cfg)
    assert list(got.columns) == list(cfg.event_base_columns())
    enstream_rows = [
        row for row in rows_of(got) if row[cfg.event_source_col] == "enstream"
    ]
    assert len(enstream_rows) == 1
    assert enstream_rows[0]["partner_id"] is None


def test_a_narrowed_config_is_forwarded_to_every_feed(
    spark, account_changes, lineage_df, narrow_cfg
):
    """Finding 1.7, at the normalisation layer.

    ``build_changes_table`` calls three normalisers and each takes a ``cfg``.
    The failure this guards is not a wrong window — nothing here reads
    ``lookback_days`` — but a frame built with the default column names when
    the caller configured others, which surfaces much later as an unresolved
    column in a calculator. Asserted by giving the non-default config and
    requiring the same table, since ``narrow_cfg`` differs only in its windows.
    """
    got = build_changes_table(account_changes, lineage_df, cfg=narrow_cfg)
    assert list(got.columns) == list(narrow_cfg.event_base_columns())
    assert got.count() == 4


def test_the_fixture_population_is_still_the_awkward_one(spark, lineage_df):
    """A guard on the fixture, not on the code.

    Every count in this file is derived from the shape of ``LINEAGE_ROWS`` — P6
    handing over on ``2024-03-01``, C3 holding two carriers with two numbers,
    C4's interval being the only closed one. Tidying the population would make
    the interval and port assertions pass for the wrong reason, so the shape is
    asserted where a reader will see it.
    """
    assert len(LINEAGE_ROWS) == 8
    rows = rows_of(lineage_df)
    closed = [row for row in rows if row["to_ts"] is not None]
    assert [(row["customer_id"], row["to_ts"]) for row in closed] == [
        ("C4", ts("2024-03-01"))
    ]
    recycled = sorted(row["customer_id"] for row in rows if row["msisdn"] == "P6")
    assert recycled == ["C4", "C5"]
    c3_carriers = {row["mno"] for row in rows if row["customer_id"] == "C3"}
    assert c3_carriers == {"ROGERS", "TELUS"}
    c3_numbers = {row["msisdn"] for row in rows if row["customer_id"] == "C3"}
    assert len(c3_numbers) == 2


def test_a_default_config_is_used_when_none_is_given(
    spark, account_changes, lineage_df
):
    """Every function here defaults ``cfg`` to a fresh ``FeatureConfig``.

    Worth one test because the default is a mutable-looking call in a signature
    and a reviewer's instinct is to hoist it to a module constant — which would
    share one frozen instance across calls harmlessly, but the same instinct
    applied to any non-frozen config would not be harmless.
    """
    got = build_changes_table(account_changes, lineage_df)
    assert list(got.columns) == list(FeatureConfig().event_base_columns())
    assert got.count() == 4
