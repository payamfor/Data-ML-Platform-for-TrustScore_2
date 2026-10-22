"""The assembly layer: which questions get asked, at which scope, joined how.

Nothing here computes a feature. Every assertion is about composition, and the
findings this module settles are all of that kind.

*Finding 1.1, the missing comma.* ``GENERIC_EVENT_FAMILIES`` was a literal list
in the notebook with a comma missing between ``"account_cancellation"`` and
``"phone_number_change"``, so Python concatenated them and the list held
``"account_cancellationphone_number_change"`` — a family matching no event.
Those two families therefore had no features at all, and a third block of
all-null columns appeared under the concatenated name. *Finding 1.3* is that
``account_activation`` was missing from the same literal outright. Both show up
here as a family-by-family sweep of the assembled column names, because that is
the only place the omission is visible: the calculators were all correct and
none of them raised.

*Finding 2.1, the join per family.* The notebook attached the event table to the
scope snapshot once per family, so the range join — the expensive step in the
whole pipeline — ran nine times to produce nine disjoint subsets of one result.
Asserted below by counting how many times the events frame is touched, which is
the only way a performance decision can be pinned by a test rather than by a
comment.

*The EnStream asymmetry.* ``calculate_generic_event_feature_set`` deliberately
skips frequency and time-to-*k* for ``enstream_api_call``, because
``enstream.py`` produces the same column names with partner-volume weights.
Running both would give two frames claiming one set of names and
``join_feature_frames`` would keep the first and drop the second silently — so
which one runs is a correctness question. Both halves are asserted: the generic
set does not emit them, and the assembled frame has them anyway.

*Finding 1.7, the config.* Several tests use ``narrow_cfg`` and assert the
production windows are *absent*. A function that drops its ``cfg`` argument
produces a superset of what was asked for, which no positive assertion catches.

The event families are narrowed to two or three in most tests. Assembling all
nine at three scopes with seventy-two transition pairs is a few thousand columns
and several hundred joins, which is the shape of the real job and much too slow
for a unit test; the one sweep that does span every family uses ``narrow_cfg``
and a single scope.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark", reason="the assembly layer is Spark code")

from tests.features.conftest import (  # noqa: E402
    REFERENCE_TS,
    collect_features,
    columns_matching,
    events_frame,
    make_event,
    ts,
)

from ts05_features.library.assemble import (  # noqa: E402
    assemble_all_scope_features,
    assemble_scope_features,
    calculate_generic_event_feature_set,
    feature_column_names,
    join_feature_frames,
)
from ts05_features.library.config import (  # noqa: E402
    CANONICAL_EVENT_FAMILY,
    GENERIC_EVENT_FAMILIES,
    SCOPES,
    FeatureConfig,
    scope_prefix,
)
from ts05_features.library.snapshots import (  # noqa: E402
    attach_events_to_scope,
    build_reference_df_at_timestamp,
    build_scope_snapshots,
)

pytestmark = pytest.mark.spark

ENTITY_COLS = ("reference_id", "customer_id")
DEVICE = CANONICAL_EVENT_FAMILY["DEVICE_CHANGE"]
SIM = CANONICAL_EVENT_FAMILY["SIM_CHANGE"]
CANCELLATION = CANONICAL_EVENT_FAMILY["STATUS_CHANGE-C"]
ACTIVATION = CANONICAL_EVENT_FAMILY["STATUS_CHANGE-A"]
NUMBER_CHANGE = CANONICAL_EVENT_FAMILY["PHONE_NUMBER_CHANGE"]
ENSTREAM = CANONICAL_EVENT_FAMILY["ENSTREAM_API_CALL"]

#: A cheap subset for the composition tests. Three families is enough to show a
#: family being skipped, and cheap enough that a test finishes in seconds.
THREE_FAMILIES = (DEVICE, SIM, CANCELLATION)

#: Two transition pairs rather than the seventy-two of the default, for the same
#: reason. The cancellation pair is deliberate: finding 1.2 was the type spelled
#: ``STATUS_CHANGE_C`` with an underscore, so every pair involving it was null.
TWO_TRANSITIONS = (
    ("DEVICE_CHANGE", "STATUS_CHANGE-C"),
    ("SIM_CHANGE", "DEVICE_CHANGE"),
)


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------

@pytest.fixture
def snapshots(spark, lineage_df, cfg):
    """The three scope snapshots and the base, at the shared reference moment."""
    references = build_reference_df_at_timestamp(lineage_df, ts(REFERENCE_TS), cfg)
    return build_scope_snapshots(lineage_df, references, cfg)


@pytest.fixture
def changes(spark):
    """A canonical event table covering every family for C1 and some for C3.

    One event per family for C1 so that the family sweep has something to find in
    each, and a handful for C3 so that "per entity" assertions have a second
    entity to fail against. C2, C5 and C6 are in the reference population and in
    no feed, which is what makes them the silent controls.
    """
    rows = [
        make_event("C1", "A1", "P1", "BELL", "2024-05-31 09:00:00", "DEVICE_CHANGE"),
        make_event("C1", "A1", "P1", "BELL", "2024-05-30 09:00:00", "SIM_CHANGE"),
        make_event(
            "C1", "A1", "P1", "BELL", "2024-05-29 09:00:00", "STATUS_CHANGE-A"
        ),
        make_event(
            "C1", "A1", "P1", "BELL", "2024-05-28 09:00:00", "STATUS_CHANGE-AA"
        ),
        make_event(
            "C1", "A1", "P1", "BELL", "2024-05-27 09:00:00", "STATUS_CHANGE-C"
        ),
        make_event(
            "C1", "A1", "P1", "BELL", "2024-05-26 09:00:00", "STATUS_CHANGE-S"
        ),
        make_event(
            "C1", "A1", "P1", "BELL", "2024-05-25 09:00:00", "PHONE_NUMBER_CHANGE",
            event_source="derived",
        ),
        make_event(
            "C1", "A1", "P1", "BELL", "2024-05-24 09:00:00", "MNO_PORT",
            event_source="derived",
        ),
        make_event(
            "C1", "A1", "P1", "BELL", "2024-05-23 09:00:00", "ENSTREAM_API_CALL",
            event_source="enstream", service_provider_id="SP1",
            service_provider_name="BankCo", industry="BANKING",
            use_case="IDENTITY",
        ),
        make_event("C3", "A5", "P5", "TELUS", "2024-05-20 09:00:00", "SIM_CHANGE"),
        make_event("C3", "A5", "P5", "TELUS", "2024-05-23 09:00:00", "DEVICE_CHANGE"),
    ]
    return events_frame(spark, rows)


@pytest.fixture
def enstream_events(spark):
    """The normalised EnStream feed, with the partner metadata still attached."""
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
                service_provider_id="SP2", service_provider_name="LendCo",
                industry="LENDING", use_case="CREDIT",
            ),
        ],
    )


@pytest.fixture
def scoped(snapshots, changes, cfg):
    """The events attached to the customer scope, once."""
    return attach_events_to_scope(snapshots, changes, "customer", cfg)


def by_customer(df):
    """``{customer_id: {column: value}}`` for the customer-scope entity key."""
    return {
        key[1]: values for key, values in collect_features(df, ENTITY_COLS).items()
    }


# --------------------------------------------------------------------------
# calculate_generic_event_feature_set
# --------------------------------------------------------------------------

def test_the_generic_set_runs_eight_calculators_for_an_ordinary_family(
    scoped, narrow_cfg
):
    """Recency, periodicity, burstness, entropy, intensity, slope, counts and k.

    Asserted by one representative column from each rather than by a full list,
    because the full list is what the calculator tests already pin and repeating
    it here would mean two places to edit for one change. What this test is for
    is that all eight ran and their outputs are on one row — a calculator
    returning a frame keyed differently would show up as a missing column here
    even though its own test passed.
    """
    got = calculate_generic_event_feature_set(
        scoped, ENTITY_COLS, DEVICE, "cust", narrow_cfg
    )
    for suffix in (
        "_total_cnt",
        "_days_since_last",
        "_interval_mean",
        "_max_monthly_cnt",
        "_event_hour_entropy",
        "_recency_weighted_intensity",
        "_cumulative_event_slope",
        "_cnt_30d",
        "_days_for_last_3_events",
    ):
        assert f"cust_{DEVICE}{suffix}" in got.columns, suffix


def test_the_enstream_family_gets_no_generic_counts(scoped, narrow_cfg):
    """No ``cnt_`` and no ``days_for_last_`` columns for ``enstream_api_call``.

    The one asymmetry in this module, and it is a correctness question rather
    than a performance one. ``enstream.py`` emits ``cust_enstream_api_call_cnt_30d``
    with a partner-volume weight applied; if the generic calculator emitted the
    same name too, ``join_feature_frames`` would keep whichever frame it saw
    first and drop the other without a word, so the table would carry either the
    weighted or the unweighted counts depending on a list order in
    ``assemble_scope_features``. The six family-agnostic calculators still run,
    which the positive half below asserts.

    The window columns are named rather than pattern-matched because burstness
    emits ``_cnt_3d_over_30d`` and ``_monthly_cnt_std``, which contain ``_cnt_``
    and are not frequency features — a fragment match here would fail against
    correct code.
    """
    got = calculate_generic_event_feature_set(
        scoped, ENTITY_COLS, ENSTREAM, "cust", narrow_cfg
    )
    for days in narrow_cfg.lookback_days:
        assert f"cust_{ENSTREAM}_cnt_{days}d" not in got.columns
        assert f"cust_{ENSTREAM}_weighted_cnt_{days}d" not in got.columns
        assert f"cust_{ENSTREAM}_any_{days}d" not in got.columns
    assert columns_matching(got, ENSTREAM, "_days_for_last_") == []
    assert f"cust_{ENSTREAM}_total_cnt" in got.columns
    assert f"cust_{ENSTREAM}_interval_mean" in got.columns


def test_the_k_values_come_from_the_config_and_not_from_an_argument(
    scoped, narrow_cfg, cfg
):
    """Finding 1.7: ``narrow_cfg`` gives ``k=3`` only, the default gives 3 and 5.

    The notebook took ``k_values=(3, 5)`` as a parameter of this function while
    ``cfg`` carried the windows, so the two could disagree — and then called
    ``calculate_time_to_k_events`` without forwarding ``cfg`` at all, so a caller
    who narrowed ``lookback_days`` got narrowed windows everywhere except the
    velocity features. One source for both, asserted from both ends.
    """
    narrow = calculate_generic_event_feature_set(
        scoped, ENTITY_COLS, DEVICE, "cust", narrow_cfg
    )
    assert columns_matching(narrow, "_days_for_last_") == [
        f"cust_{DEVICE}_days_for_last_3_events"
    ]
    wide = calculate_generic_event_feature_set(
        scoped, ENTITY_COLS, DEVICE, "cust", cfg
    )
    assert columns_matching(wide, "_days_for_last_") == [
        f"cust_{DEVICE}_days_for_last_3_events",
        f"cust_{DEVICE}_days_for_last_5_events",
    ]


def test_the_generic_set_is_one_row_per_entity(scoped, narrow_cfg):
    """Eight frames joined onto one base gives one row per entity, not eight.

    Five customers in the June population, five rows. Each calculator groups to
    one row per entity and ``safe_join`` left-joins on the entity key, so a
    duplicate means one of them leaked an extra grouping column — the reference
    timestamp being the usual candidate, since several of them need it.
    """
    got = calculate_generic_event_feature_set(
        scoped, ENTITY_COLS, DEVICE, "cust", narrow_cfg
    )
    rows = got.collect()
    assert len(rows) == 5
    assert len({(row["reference_id"], row["customer_id"]) for row in rows}) == 5


def test_a_family_with_no_events_is_a_row_of_nulls_and_not_an_absent_row(
    scoped, narrow_cfg
):
    """C3 never ported, and has a full ``mno_port`` block of nulls.

    The base of every calculator is the whole scoped frame rather than the
    filtered one, so an entity that has no events of a family keeps its row. That
    is the property the whole pipeline rests on: a feature matrix with a row
    missing for the customers who did nothing cannot be joined to a label table.
    """
    got = calculate_generic_event_feature_set(
        scoped, ENTITY_COLS, CANONICAL_EVENT_FAMILY["MNO_PORT"], "cust", narrow_cfg
    )
    rows = by_customer(got)
    assert "C3" in rows
    port = CANONICAL_EVENT_FAMILY["MNO_PORT"]
    assert rows["C3"][f"cust_{port}_days_since_last"] is None
    assert rows["C1"][f"cust_{port}_days_since_last"] is not None


# --------------------------------------------------------------------------
# assemble_scope_features - findings 1.1 and 1.3
# --------------------------------------------------------------------------

def test_every_canonical_family_gets_features(
    snapshots, changes, enstream_events, narrow_cfg
):
    """Findings 1.1 and 1.3: all nine families have columns, and no tenth does.

    The missing comma concatenated ``account_cancellation`` and
    ``phone_number_change`` into one name that matched no event, so those two
    families had no features and a block of nulls appeared under the
    concatenation. ``account_activation`` was absent from the literal outright.
    Both are only visible as a sweep like this one, since every calculator was
    correct and nothing raised.

    The negative half is the important one and it is why the family is derived
    from the mapping rather than written out: a column naming a family that is
    not in ``CANONICAL_EVENT_FAMILY`` is the concatenation defect, whatever it is
    concatenated from. Checked by taking the family segment of every feature name
    and requiring the set to be exactly the canonical set.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        enstream_events,
        "customer",
        state_transitions=(),
        event_families=GENERIC_EVENT_FAMILIES,
        cfg=narrow_cfg,
    )
    names = set(got.columns)
    for family in GENERIC_EVENT_FAMILIES:
        assert f"cust_{family}_total_cnt" in names, family
    # No family name outside the canonical set appears in a total-count column,
    # which is where the concatenated name showed up.
    found = {
        name[len("cust_") : -len("_total_cnt")]
        for name in names
        if name.startswith("cust_") and name.endswith("_total_cnt")
    }
    assert found == set(GENERIC_EVENT_FAMILIES)
    assert "account_cancellationphone_number_change" not in found
    assert ACTIVATION in found


def test_the_cancellation_family_and_transition_both_exist(
    snapshots, changes, narrow_cfg
):
    """Findings 1.1 and 1.2 together: cancellation has features and transitions.

    ``account_cancellation`` was the family the missing comma swallowed, and
    ``STATUS_CHANGE-C`` was the type the underscore misspelling made unmatchable,
    so cancellation was missing from the table twice over by two unrelated
    defects. It is the end of an account, which makes it the single most
    informative thing in the feed — "how long after a device change did this
    account cancel" was among the features that silently did not exist.

    C1 changed device on 31 May and cancelled on the 27th, so the pair as ordered
    has the target *before* the source and no transition; the reverse pair is the
    one with a value, and it is four days. Asserted rather than merely
    non-null, because a hyphen/underscore fix that still matched nothing would
    give a non-null column of zeros under finding 3.1's fill.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        None,
        "customer",
        state_transitions=(
            ("DEVICE_CHANGE", "STATUS_CHANGE-C"),
            ("STATUS_CHANGE-C", "DEVICE_CHANGE"),
        ),
        event_families=(DEVICE, CANCELLATION),
        cfg=narrow_cfg,
    )
    row = by_customer(got)["C1"]
    assert row[f"cust_{CANCELLATION}_total_cnt"] == 1
    assert row["cust_device_change_to_status_change_c_avg_transition_days"] is None
    assert row["cust_status_change_c_to_device_change_avg_transition_days"] == 4


def test_the_enstream_counts_are_the_weighted_ones(
    snapshots, changes, enstream_events, narrow_cfg
):
    """The assembled ``cust_enstream_api_call_cnt_30d`` comes from ``enstream.py``.

    Two calls in the feed from two different partners, each with volume one
    against a mean of one, so both weigh exactly one and the weighted count
    equals the plain count of two. Which is the point: the column *exists* and
    holds the EnStream module's answer, not the generic calculator's. The
    canonical table also carries an ``enstream_api_call`` row — a third event, on
    23 May — so if the generic frequency calculator were running for this family
    the count would be three and the weighted count would be missing or wrong.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        enstream_events,
        "customer",
        state_transitions=(),
        event_families=(DEVICE, ENSTREAM),
        cfg=narrow_cfg,
    )
    row = by_customer(got)["C1"]
    assert row[f"cust_{ENSTREAM}_cnt_30d"] == 2
    assert row[f"cust_{ENSTREAM}_weighted_cnt_30d"] == pytest.approx(2.0)


def test_no_enstream_feed_means_no_enstream_only_columns(
    snapshots, changes, narrow_cfg
):
    """``None`` skips the partner features, which is what a carrier-only run wants.

    The generic six still run for the family, because the canonical table carries
    the EnStream rows too — so a carrier-only run is not blind to EnStream
    traffic, it simply has no partner-weighted counts and no breadth. The
    distinction is asserted in both directions.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        None,
        "customer",
        state_transitions=(),
        event_families=(ENSTREAM,),
        cfg=narrow_cfg,
    )
    for days in narrow_cfg.lookback_days:
        assert f"cust_{ENSTREAM}_cnt_{days}d" not in got.columns
        assert f"cust_{ENSTREAM}_weighted_cnt_{days}d" not in got.columns
    # The diversity columns are ``cust_<family>_<dimension>_distinct_cnt`` and
    # are the EnStream module's alone; the stability breadth columns are named
    # ``cust_distinct_account_cnt`` and are unconditional, so both are checked.
    assert columns_matching(got, "_distinct_cnt") == []
    assert "cust_distinct_account_cnt" in got.columns
    assert f"cust_{ENSTREAM}_total_cnt" in got.columns


def test_the_events_are_attached_once_and_not_once_per_family(
    snapshots, changes, narrow_cfg, monkeypatch
):
    """Finding 2.1: three families, one call to ``attach_events_to_scope``.

    The range join between the snapshot and the event table is the single most
    expensive step in the job, and the notebook ran it once per family to produce
    nine disjoint subsets of one result. There is no output difference, so a
    value assertion cannot see it — the call is counted instead. Counted through
    the name ``assemble`` imported, not the one ``snapshots`` defines, because
    patching the definition would not catch a module that had bound the function
    at import time.

    The stability features take the snapshot rather than the events, and the
    EnStream features are skipped here, so one is the whole budget.
    """
    from ts05_features.library import assemble as assemble_module

    calls = []
    original = assemble_module.attach_events_to_scope

    def counting(*args, **kwargs):
        calls.append(args[2] if len(args) > 2 else kwargs.get("scope"))
        return original(*args, **kwargs)

    monkeypatch.setattr(assemble_module, "attach_events_to_scope", counting)
    assemble_scope_features(
        snapshots,
        changes,
        None,
        "customer",
        state_transitions=TWO_TRANSITIONS,
        event_families=THREE_FAMILIES,
        cfg=narrow_cfg,
    )
    assert calls == ["customer"]


def test_the_snapshot_columns_survive_the_assembly(
    snapshots, changes, narrow_cfg
):
    """The result is the snapshot plus features, not the features alone.

    ``assemble_scope_features`` joins onto the scope snapshot rather than onto a
    bare key frame, so ``reference_ts``, ``from_ts`` and ``to_ts`` come through.
    Deliberate: the reference timestamp is what makes a feature row reproducible,
    and a training table that lost it cannot be re-derived or time-split.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        None,
        "customer",
        state_transitions=(),
        event_families=(DEVICE,),
        cfg=narrow_cfg,
    )
    for col_name in ("reference_id", "customer_id", "reference_ts", "from_ts"):
        assert col_name in got.columns


def test_the_stability_features_are_there_without_being_asked_for(
    snapshots, changes, narrow_cfg
):
    """No families, no transitions, no EnStream — and still a tenure column.

    Stability is unconditional because it needs no events, which makes it the
    only part of the table that survives a feed going quiet. An empty
    ``event_families`` is a legitimate request: it is how a caller builds the
    lineage-only feature set.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        None,
        "customer",
        state_transitions=(),
        event_families=(),
        cfg=narrow_cfg,
    )
    row = by_customer(got)["C3"]
    assert row["cust_days_since_first_seen"] == 1096
    assert row["cust_distinct_account_cnt"] == 2


def test_no_transitions_means_no_transition_columns(
    snapshots, changes, narrow_cfg
):
    """An empty ``state_transitions`` skips the calculator rather than emitting nulls.

    Seventy-two pairs at three columns each is two hundred and sixteen columns of
    the default, so being able to turn them off is what makes a cheap subset
    cheap. Skipped and not nulled, for the reason the diversity dimensions are:
    an all-null column reaches the Parquet write and fails there.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        None,
        "customer",
        state_transitions=(),
        event_families=(DEVICE,),
        cfg=narrow_cfg,
    )
    assert columns_matching(got, "_transition_") == []


def test_the_transition_columns_are_named_from_the_types(
    snapshots, changes, narrow_cfg
):
    """Two pairs, six columns, and no hyphen anywhere in a column name.

    ``STATUS_CHANGE-C`` has a hyphen, which is not legal in a Parquet column
    name, so the calculator tokenises it to ``status_change_c``. Finding 1.2 was
    the notebook doing the reverse — writing the *type* with an underscore, where
    the feed and every other status type use a hyphen — so the filter matched
    nothing. The tokenisation belongs on the way out, not on the way in.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        None,
        "customer",
        state_transitions=TWO_TRANSITIONS,
        event_families=(DEVICE,),
        cfg=narrow_cfg,
    )
    assert len(columns_matching(got, "_transition_")) == 6
    for name in got.columns:
        assert "-" not in name, name


def test_a_silent_customer_gets_a_full_row(
    snapshots, changes, enstream_events, narrow_cfg
):
    """C6 is in no feed and has every column, all null or zero, none missing.

    The failure this guards against is the one that makes a pipeline quietly
    train on a biased sample: every left join in the chain has to be a left join,
    and one inner join anywhere drops the customers who did nothing — who are
    precisely the negative class. Asserted as an identical column set rather than
    as "C6 exists", because a row present with fewer columns is not possible in a
    frame, but a row absent is easy.

    Null rather than zero in the count columns, per finding 4.5: C6's left-joined
    null row is dropped by the family filter, so it forms no group in the
    aggregate and ``safe_join`` fills from the miss side. The stability columns
    are the ones that hold a real value for C6, because they read the lineage
    rather than the events — which is the reason that family exists.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        enstream_events,
        "customer",
        state_transitions=TWO_TRANSITIONS,
        event_families=THREE_FAMILIES,
        cfg=narrow_cfg,
    )
    rows = by_customer(got)
    assert set(rows) == {"C1", "C2", "C3", "C5", "C6"}
    assert set(rows["C6"]) == set(rows["C1"])
    assert rows["C6"][f"cust_{DEVICE}_total_cnt"] is None
    assert rows["C1"][f"cust_{DEVICE}_total_cnt"] == 1
    assert rows["C6"]["cust_days_since_first_seen"] == 1858
    assert rows["C6"]["cust_distinct_account_cnt"] == 1


def test_a_missing_scope_entry_names_the_builder(snapshots, changes, cfg):
    """The error tells the caller which function produces the thing they lack."""
    with pytest.raises(KeyError, match="build_scope_snapshots"):
        assemble_scope_features(
            {"base": snapshots["base"]},
            changes,
            None,
            "customer",
            state_transitions=(),
            event_families=(DEVICE,),
            cfg=cfg,
        )


@pytest.mark.parametrize("scope", SCOPES)
def test_every_feature_column_carries_the_scope_prefix(
    snapshots, changes, enstream_events, narrow_cfg, scope
):
    """Finding 1.6 at the assembly level, at all three scopes.

    Everything the assembler adds to the snapshot has to start with the scope's
    prefix, because the preprocessing stage selects features by it. The snapshot
    columns are excluded from the check — they are keys and timestamps, not
    features — and the ``msisdn`` scope is the one where the defect was invisible,
    since there the scope name and the prefix are the same string.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        enstream_events,
        scope,
        state_transitions=(),
        event_families=(DEVICE, ENSTREAM),
        cfg=narrow_cfg,
    )
    prefix = scope_prefix(scope)
    snapshot_cols = set(snapshots[scope].columns)
    features = [name for name in got.columns if name not in snapshot_cols]
    assert features
    for name in features:
        assert name.startswith(f"{prefix}_"), name


# --------------------------------------------------------------------------
# assemble_all_scope_features
# --------------------------------------------------------------------------

def test_the_three_scopes_come_back_separately(
    snapshots, changes, narrow_cfg
):
    """Three frames keyed by scope name, not one joined frame.

    Deliberately unjoined. The grains differ — one row per customer, per account,
    per number — and broadcasting a customer-scope feature across that customer's
    four numbers is the same value four times, which is a choice about what a
    training row represents. That choice belongs to whoever builds the training
    table, and it is made in the preprocessing stage.
    """
    got = assemble_all_scope_features(
        snapshots,
        changes,
        None,
        scopes=SCOPES,
        state_transitions=(),
        event_families=(DEVICE,),
        cfg=narrow_cfg,
    )
    assert set(got) == set(SCOPES)
    for scope, frame in got.items():
        assert list(frame.columns)[: len(narrow_cfg.entity_cols_for(scope))] == list(
            narrow_cfg.entity_cols_for(scope)
        )


def test_the_scopes_have_different_grains(snapshots, changes, narrow_cfg):
    """Five customer rows and seven account rows over the same population.

    C2 holds A2 and A3, C3 holds A4 on Rogers and A5 on Telus, and C1, C5 and C6
    hold one each: seven open accounts across five customers at the June
    reference. Which is the arithmetic reason the frames are not joined — the
    account frame cannot be reduced to the customer frame without deciding what
    to do with the second account, and that decision is a modelling one.
    """
    got = assemble_all_scope_features(
        snapshots,
        changes,
        None,
        scopes=("customer", "account"),
        state_transitions=(),
        event_families=(DEVICE,),
        cfg=narrow_cfg,
    )
    assert got["customer"].count() == 5
    assert got["account"].count() == 7


def test_a_subset_of_scopes_is_a_subset(snapshots, changes, narrow_cfg):
    """Asking for one scope builds one, which is how a cheap run stays cheap."""
    got = assemble_all_scope_features(
        snapshots,
        changes,
        None,
        scopes=("customer",),
        state_transitions=(),
        event_families=(DEVICE,),
        cfg=narrow_cfg,
    )
    assert set(got) == {"customer"}


# --------------------------------------------------------------------------
# join_feature_frames and feature_column_names
# --------------------------------------------------------------------------

def test_a_repeated_column_resolves_to_the_first_frame(snapshots, narrow_cfg):
    """Two frames claiming one name: the first wins and the second is dropped.

    Which is the mechanism that makes the EnStream asymmetry a correctness
    question rather than a preference. It is silent by design — a suffixed
    ``cust_device_change_cnt_30d_1`` would be worse, since nothing downstream
    would select it — and stating it here means a caller assembling their own
    subset knows the order of their list matters.
    """
    base = snapshots["customer"].select(*ENTITY_COLS).distinct()
    first = base.withColumn("cust_x", pyspark.sql.functions.lit(1))
    second = base.withColumn("cust_x", pyspark.sql.functions.lit(2))
    got = join_feature_frames(base, [first, second], ENTITY_COLS)
    assert got.columns.count("cust_x") == 1
    assert {row["cust_x"] for row in got.collect()} == {1}


def test_a_none_frame_in_the_list_is_skipped(snapshots, narrow_cfg):
    """``None`` is a legitimate entry, because a calculator may return nothing.

    The list is built by comprehension in several places and appending
    conditionally is uglier than appending ``None``, so the join has to tolerate
    it rather than the callers having to filter.
    """
    base = snapshots["customer"].select(*ENTITY_COLS).distinct()
    frame = base.withColumn("cust_x", pyspark.sql.functions.lit(1))
    got = join_feature_frames(base, [None, frame, None], ENTITY_COLS)
    assert "cust_x" in got.columns
    assert got.count() == 5


def test_the_feature_column_names_exclude_the_keys(
    snapshots, changes, narrow_cfg
):
    """The entity keys and the snapshot timestamps are not features.

    Computed from ``cfg`` rather than by pattern-matching the prefix, so that a
    snapshot column which happens to begin ``cust_`` is not mistaken for a
    feature — and, in the other direction, so that the keys are excluded by name
    rather than by hoping they do not match the pattern. A feature matrix is the
    frame minus its keys and the model stages ask for it constantly.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        None,
        "customer",
        state_transitions=(),
        event_families=(DEVICE,),
        cfg=narrow_cfg,
    )
    names = feature_column_names(got, "customer", narrow_cfg)
    for key in narrow_cfg.entity_cols_for("customer"):
        assert key not in names
    assert f"cust_{DEVICE}_total_cnt" in names
    assert len(names) == len(got.columns) - len(
        narrow_cfg.entity_cols_for("customer")
    )


def test_the_feature_column_names_keep_the_frame_order(
    snapshots, changes, narrow_cfg
):
    """The order is the frame's, because a feature matrix is positional.

    A model trained on a column order and scored against another is the kind of
    failure that produces plausible predictions rather than an error, so the
    order has to be a property of the frame and not of a set iteration.
    """
    got = assemble_scope_features(
        snapshots,
        changes,
        None,
        "customer",
        state_transitions=(),
        event_families=(DEVICE,),
        cfg=narrow_cfg,
    )
    names = feature_column_names(got, "customer", narrow_cfg)
    keys = set(narrow_cfg.entity_cols_for("customer"))
    assert names == [name for name in got.columns if name not in keys]


def test_the_default_config_is_used_when_none_is_given(snapshots, changes):
    """The signature's defaults are usable and not placeholders.

    Every entry point here defaults ``cfg`` to ``FeatureConfig()``, and a broken
    default would only be found by a caller who omitted the argument — which the
    notebooks all did. One family and no transitions keeps it cheap; what is
    under test is that the default resolves, so five windows is the assertion.
    """
    got = assemble_scope_features(
        snapshots, changes, None, "customer", state_transitions=(),
        event_families=(DEVICE,),
    )
    assert len(columns_matching(got, DEVICE, "_cnt_")) == len(
        FeatureConfig().lookback_days
    ) * 2 + len(FeatureConfig().lookback_hour_windows) * 2


def test_the_fixture_population_is_what_the_assertions_assume(
    snapshots, changes, enstream_events
):
    """A guard: five customers, eleven events over nine families, two calls.

    Every hand-computed number above rests on these, so if the shared lineage
    fixture or this module's feeds change, this fails first and names the cause.
    """
    customers = sorted(
        row["customer_id"]
        for row in snapshots["customer"].select("customer_id").collect()
    )
    assert customers == ["C1", "C2", "C3", "C5", "C6"]
    assert changes.count() == 11
    families = {
        row["event_family"]
        for row in changes.select("event_family").distinct().collect()
    }
    assert families == set(GENERIC_EVENT_FAMILIES)
    assert enstream_events.count() == 2
    assert NUMBER_CHANGE in families
