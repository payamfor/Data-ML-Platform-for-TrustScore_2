"""The constants, and the four notebook defects that lived in them.

This is the only module in the feature suite that needs no SparkSession. That is
not an accident of what is easy to test: findings 1.1, 1.2, 1.3 and 1.6 were all
defects in *declarations*, and a declaration is wrong before any data touches it.
The notebook library had no test at this level, which is exactly why a missing
comma in a list literal survived into production.
"""

from __future__ import annotations

import pytest

from ts05_features.library.config import (
    CANONICAL_EVENT_FAMILY,
    DEFAULT_STATE_TRANSITIONS,
    ENSTREAM_DIMENSION_COLS,
    EVENT_METADATA_COLS,
    EVENT_TYPE_BY_FAMILY,
    GENERIC_EVENT_FAMILIES,
    OPEN_INTERVAL_END,
    OPEN_INTERVAL_START,
    SCOPES,
    STATE_TRANSITION_EVENT_TYPES,
    SUPPORTED_CHANGE_TYPES,
    FeatureConfig,
    scope_prefix,
)

# --------------------------------------------------------------------------
# finding 1.1: the missing comma that deleted two feature families
# --------------------------------------------------------------------------


def test_no_family_name_is_the_concatenation_of_two_others():
    """The exact shape of finding 1.1, asserted as a property of the tuple.

    The notebook literal read ``..., "account_cancellation"
    "phone_number_change", ...`` with no comma, and Python's adjacent-string-
    literal concatenation turned two families into one name that matches no
    event. Nothing raised — a family filter that matches nothing produces a
    column of nulls, and a column of nulls is what a wide feature table is
    mostly made of anyway.

    Asserted as "no member is another two members joined" rather than against a
    hand-written expected list, because a hand-written list is the same kind of
    artifact that broke in the first place.
    """
    families = set(GENERIC_EVENT_FAMILIES)
    for left in families:
        for right in families:
            assert left + right not in families


def test_every_generic_family_is_the_family_of_some_real_event_type():
    """A family nothing maps to can never be filtered to a non-empty frame."""
    assert set(GENERIC_EVENT_FAMILIES) == set(CANONICAL_EVENT_FAMILY.values())
    for family in GENERIC_EVENT_FAMILIES:
        assert family in EVENT_TYPE_BY_FAMILY


def test_the_generic_families_are_derived_and_not_a_literal():
    """Nine families, one per mapped event type, with no duplicates.

    The count is asserted because it is the number that changes when a family is
    added to the mapping and forgotten somewhere else — which is the second half
    of finding 1.1, where ``account_activation`` was simply absent from the
    literal rather than eaten by the comma.
    """
    assert len(GENERIC_EVENT_FAMILIES) == 9
    assert len(set(GENERIC_EVENT_FAMILIES)) == len(GENERIC_EVENT_FAMILIES)
    assert list(GENERIC_EVENT_FAMILIES) == sorted(GENERIC_EVENT_FAMILIES)


# --------------------------------------------------------------------------
# finding 1.3: account_activation was never in the feature set
# --------------------------------------------------------------------------


def test_account_activation_is_a_family_and_gets_the_generic_feature_set():
    """Finding 1.3. Absent from the notebook's family literal entirely.

    An activation is the beginning of an account's life, and "how long ago was
    this account activated" is the kind of thing a takeover model wants most.
    Pinned separately from 1.1 because the two have different causes and a fix
    for the comma would not have brought this one back.
    """
    assert CANONICAL_EVENT_FAMILY["STATUS_CHANGE-AA"] == "account_activation"
    assert "account_activation" in GENERIC_EVENT_FAMILIES


def test_the_cancellation_and_number_change_families_both_survive():
    """The two families the missing comma actually consumed."""
    assert "account_cancellation" in GENERIC_EVENT_FAMILIES
    assert "phone_number_change" in GENERIC_EVENT_FAMILIES
    assert "account_cancellationphone_number_change" not in GENERIC_EVENT_FAMILIES


# --------------------------------------------------------------------------
# finding 1.2: STATUS_CHANGE_C, spelled with an underscore
# --------------------------------------------------------------------------


def test_the_cancellation_event_type_is_spelled_the_way_the_feed_spells_it():
    """Finding 1.2, and the reason it cost fourteen transition features.

    Every status type the carriers send is ``STATUS_CHANGE-<letter>`` with a
    hyphen. The notebook's transition-type list spelled this one
    ``STATUS_CHANGE_C``, so ``event_type.isin(...)`` matched nothing for it and
    all fourteen pairs involving a cancellation — seven in, seven out —
    produced nulls.

    Asserted against ``SUPPORTED_CHANGE_TYPES``, which is what the carrier feed
    is filtered by, so the two spellings cannot drift apart: if the feed's name
    changed, this fails rather than silently matching nothing again.
    """
    assert "STATUS_CHANGE-C" in STATE_TRANSITION_EVENT_TYPES
    assert "STATUS_CHANGE_C" not in STATE_TRANSITION_EVENT_TYPES
    for change_types in SUPPORTED_CHANGE_TYPES.values():
        assert "STATUS_CHANGE_C" not in change_types


def test_every_transition_event_type_is_a_type_something_can_emit():
    """The general form of 1.2: a transition over a type no feed sends is dead.

    ``CANONICAL_EVENT_FAMILY`` is the set of types the pipeline can ever produce
    — the carrier types, the two derived from the lineage, and the EnStream
    call. A transition type outside it is a misspelling by definition.
    """
    for event_type in STATE_TRANSITION_EVENT_TYPES:
        assert event_type in CANONICAL_EVENT_FAMILY


def test_the_default_transitions_are_every_ordered_pair_of_distinct_types():
    """Nine types, so seventy-two ordered pairs, and no self-pairs.

    A self-pair would ask "how long after a device change until the next device
    change", which is the periodicity family's question and would be a second,
    differently-computed answer to it.
    """
    assert len(DEFAULT_STATE_TRANSITIONS) == 9 * 8
    assert len(set(DEFAULT_STATE_TRANSITIONS)) == len(DEFAULT_STATE_TRANSITIONS)
    for source, target in DEFAULT_STATE_TRANSITIONS:
        assert source != target
        assert source in STATE_TRANSITION_EVENT_TYPES
        assert target in STATE_TRANSITION_EVENT_TYPES


def test_the_cancellation_transitions_are_present_in_both_directions():
    """Sixteen pairs touch a cancellation; fourteen of them were nulls."""
    into = [
        pair for pair in DEFAULT_STATE_TRANSITIONS if pair[1] == "STATUS_CHANGE-C"
    ]
    out_of = [
        pair for pair in DEFAULT_STATE_TRANSITIONS if pair[0] == "STATUS_CHANGE-C"
    ]
    assert len(into) == 8
    assert len(out_of) == 8


# --------------------------------------------------------------------------
# finding 1.6: the scope name passed where the prefix belonged
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("scope", "expected"),
    [("customer", "cust"), ("account", "acct"), ("msisdn", "msisdn")],
)
def test_the_scope_prefix_is_the_short_form_and_not_the_scope_name(scope, expected):
    """Finding 1.6. ``customer`` is not ``cust``, and that difference split a
    family across two column namespaces.

    The notebook's EnStream wrapper passed ``scope`` to calculators whose other
    callers passed ``scope_prefix(scope)``. The generic
    ``cust_enstream_api_call_cnt_30d`` columns were then never filled by
    anything, while a parallel ``customer_enstream_api_call_*`` set appeared
    that no downstream code looked for.
    """
    assert scope_prefix(scope) == expected


def test_the_customer_prefix_is_not_the_customer_scope_name():
    """Stated on its own because it is the one pair that actually differed.

    ``msisdn`` happens to be its own prefix, so a test that only covered that
    scope would pass with the defect still in place.
    """
    assert scope_prefix("customer") != "customer"


def test_an_unknown_scope_is_rejected_by_name():
    """The other half of 1.6: a typo'd scope has to raise, not fall through.

    ``KeyError`` from a dict lookup deep inside a calculator names the key and
    nothing else. This names the argument and lists the alternatives, which is
    the difference between a one-line fix and a bisect.
    """
    with pytest.raises(ValueError, match="Unsupported scope"):
        scope_prefix("subscriber")
    with pytest.raises(ValueError, match="customer, account, msisdn"):
        scope_prefix("CUSTOMER")


# --------------------------------------------------------------------------
# finding 1.7: ratio windows that outlived the lookbacks they divide
# --------------------------------------------------------------------------


def test_a_ratio_window_absent_from_the_lookbacks_is_refused_at_construction():
    """Finding 1.7, moved from a runtime failure to a construction failure.

    The notebook hardcoded the 1/3, 7/30 and 30/90 ratios inside the burstness
    calculator while taking the windows from config, so a caller who narrowed
    ``lookback_days`` got ``AnalysisException: cannot resolve
    cust_sim_change_cnt_1d`` from inside a helper three frames down. The
    complaint here names the caller's own argument instead.
    """
    with pytest.raises(ValueError, match="ratio_windows"):
        FeatureConfig(lookback_days=(3, 30), ratio_windows=((1, 3),))


def test_the_message_names_which_ratio_is_unsatisfiable():
    """Both offending windows listed, and the lookbacks that were on offer."""
    with pytest.raises(ValueError) as excinfo:
        FeatureConfig(lookback_days=(7, 30), ratio_windows=((1, 3), (7, 30)))
    message = str(excinfo.value)
    assert "1d/3d" in message
    assert "7d/30d" not in message
    assert "(7, 30)" in message


def test_the_default_ratio_windows_are_satisfiable():
    """The production config has to survive its own guard.

    Cheap, and it is the assertion that would have caught a default changed on
    one side only.
    """
    cfg = FeatureConfig()
    for numerator, denominator in cfg.ratio_windows:
        assert numerator in cfg.lookback_days
        assert denominator in cfg.lookback_days


def test_the_narrow_config_the_suite_uses_is_itself_valid(narrow_cfg):
    """Promised by ``conftest.py``'s docstring, so asserted here as it says."""
    assert tuple(narrow_cfg.lookback_days) == (3, 30)
    assert tuple(narrow_cfg.ratio_windows) == ((3, 30),)
    assert tuple(narrow_cfg.lookback_hour_windows) == ()


# --------------------------------------------------------------------------
# the rest of the config surface
# --------------------------------------------------------------------------


def test_the_scopes_nest_so_one_event_join_can_serve_all_three(cfg):
    """Each scope's key is a prefix of the next, coarsest first.

    This is the property ``attach_events_to_scope`` relies on: the same range
    join against the same event table works at every grain because the finer
    grain only adds columns to the right of the coarser one. Break it and the
    package still runs, one join per scope, three times slower.
    """
    keys = [tuple(cfg.group_cols_for(scope)) for scope in SCOPES]
    for coarser, finer in zip(keys, keys[1:]):
        assert finer[: len(coarser)] == coarser


def test_the_reference_id_leads_every_entity_key(cfg):
    """Because two references for one customer are two independent rows.

    A feature keyed on the customer alone would average a March fraud and a
    June non-fraud into one row and label it once.
    """
    for scope in SCOPES:
        assert cfg.entity_cols_for(scope)[0] == cfg.reference_id_col
        assert cfg.entity_cols_for(scope)[1:] == cfg.group_cols_for(scope)


def test_an_unknown_scope_is_refused_by_the_grouping_helpers(cfg):
    with pytest.raises(ValueError, match="Unsupported scope"):
        cfg.group_cols_for("sim")
    with pytest.raises(ValueError, match="Unsupported scope"):
        cfg.entity_cols_for("sim")


def test_the_widest_lookback_is_what_bounds_the_event_scan(cfg, narrow_cfg):
    """``max``, not the last element: ``lookback_days`` is not required sorted."""
    assert cfg.max_lookback_days == 90
    assert narrow_cfg.max_lookback_days == 30
    assert FeatureConfig(lookback_days=(30, 7, 90), ratio_windows=()).max_lookback_days == 90


def test_the_event_base_columns_cover_the_partner_metadata(cfg):
    """Because a carrier frame lacking them must union with an EnStream frame.

    ``build_changes_table`` aligns every feed to this list before the union. A
    partner column missing from it would be dropped from whichever feed had it,
    which is the diversity and novelty families gone.
    """
    base = cfg.event_base_columns()
    for col_name in EVENT_METADATA_COLS:
        assert col_name in base
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
        assert col_name in base
    assert len(base) == len(set(base))


def test_the_dedupe_key_keeps_two_partners_asking_at_the_same_instant(cfg):
    """Two calls, not one redelivered call, and the weighting depends on it.

    The partner *identity* columns are in the dedupe key precisely so that a
    bank and a lender asking about the same number in the same second stay two
    rows. Drop them and every simultaneous multi-partner call collapses, which
    understates the volume the EnStream weight is computed against.

    The descriptive columns are deliberately *not* in the key. ``partner_name``
    is a label for ``partner_id`` and ``industry`` is an attribute of the
    service provider, so including them would mean a feed that renamed a partner
    mid-file kept both spellings as two events. What must be in the key is the
    grain the weights are computed at — asserted against
    ``enstream_partner_cols`` rather than restated, since a weight grain finer
    than the dedupe grain is finding 1.4 waiting to happen from the other side.
    """
    keys = cfg.event_dedupe_keys()
    assert cfg.event_ts_col in keys
    assert cfg.event_type_col in keys
    assert "partner_id" in keys
    for col_name in cfg.enstream_partner_cols:
        assert col_name in keys
    for descriptive in ("partner_name", "partner_type", "service_provider_name"):
        assert descriptive not in keys
    assert cfg.event_weight_col not in keys
    assert cfg.event_family_col not in keys


def test_the_enstream_dimensions_are_all_metadata_the_feed_carries():
    """A diversity dimension that is not a real column is skipped, not counted.

    Which is safe, and also silent — so the naming is pinned here instead.
    """
    for dimension in ENSTREAM_DIMENSION_COLS:
        assert dimension in EVENT_METADATA_COLS


def test_the_partner_grain_the_weights_are_computed_at_is_metadata(cfg):
    """Finding 1.4's grain, asserted at the declaration.

    ``with_partner_volume_weights`` joins on the entity columns *and* these. If
    one of them were not an event column the join would silently fall back to
    the entity alone, which is the fan-out that finding 1.4 records.
    """
    for col_name in cfg.enstream_partner_cols:
        assert col_name in EVENT_METADATA_COLS


def test_the_weight_clamps_bracket_the_neutral_weight(cfg):
    """A clamp that excluded 1.0 would rescale every entity with one partner.

    A single-partner entity's volume *is* the mean, so its raw weight is exactly
    one. If the clamp range did not contain one, the commonest case in the
    population would be pushed off neutral by the normaliser.
    """
    assert cfg.enstream_min_weight < 1.0 < cfg.enstream_max_weight


def test_the_open_interval_sentinels_bracket_every_real_timestamp():
    """Both ends, and the near one matters as much as the far one.

    ``closed_lineage_intervals`` substitutes these for NULL. A near sentinel
    later than a real event would exclude that event from its own interval.
    """
    assert OPEN_INTERVAL_START < "1970-01-01"
    assert OPEN_INTERVAL_END > "2100-01-01"


def test_the_config_is_frozen_so_a_calculator_cannot_edit_a_shared_one(cfg):
    """Every calculator defaults to ``FeatureConfig()`` and many share one
    instance across a whole assembly. A mutable config would make a stage's
    windows depend on which stage ran before it."""
    with pytest.raises(Exception):
        cfg.lookback_days = (1,)


def test_the_event_read_buffer_widens_the_scan_rather_than_narrowing_it(cfg):
    """Positive, and the reason is periodicity rather than counting.

    The counts only need ``max_lookback_days``. The interval between the last
    two events can straddle the ninety-day edge, and a buffer of zero truncates
    exactly the gap nearest the reference — the one that matters most.
    """
    assert cfg.event_read_buffer_days > 0
    assert cfg.prune_event_window is True
