"""Constants and column names for the feature pipeline.

Everything in this module is data. No function here reads a DataFrame or builds a
Spark expression, which is deliberate: the values below are the ones a reviewer
from the carrier team or the fraud team needs to check, and they should be
readable without knowing any Spark.

Two of these declarations were wrong in the notebook library this package
replaces, and both were wrong in a way that produced columns full of nulls
rather than an error. They are called out in place, and the reasoning is in
``docs/feature_library_findings.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Sequence, Tuple

# The lineage tables leave an interval open by writing NULL rather than a
# sentinel. Features are computed with interval arithmetic throughout, so the
# open ends are closed with these before anything else happens. They are strings
# rather than Spark columns so that this module imports without a session.
OPEN_INTERVAL_START = "1900-01-01 00:00:00"
OPEN_INTERVAL_END = "9999-12-31 00:00:00"

# The change types each carrier actually sends. A record whose (mno,
# change_type) pair is absent from this table is dropped during normalisation
# rather than mapped to a family, because a change type we have not agreed with
# the carrier is a change type whose meaning we would be guessing at.
SUPPORTED_CHANGE_TYPES: Dict[str, Tuple[str, ...]] = {
    "BELL": (
        "DEVICE_CHANGE",
        "SIM_CHANGE",
        "STATUS_CHANGE-A",
        "STATUS_CHANGE-AA",
        "STATUS_CHANGE-C",
        "STATUS_CHANGE-S",
    ),
    "ROGERS": (
        "DEVICE_CHANGE",
        "SIM_CHANGE",
        "STATUS_CHANGE-S",
        "STATUS_CHANGE-C",
    ),
    "TELUS": (
        "DEVICE_CHANGE",
        "SIM_CHANGE",
        "STATUS_CHANGE-A",
        "STATUS_CHANGE-AA",
        "STATUS_CHANGE-C",
        "STATUS_CHANGE-S",
    ),
}

# Carrier change type, or derived event type, to the family the features are
# named after. The family is what `event_family` holds in the canonical event
# table; the raw type is kept alongside it in `event_type` because the state
# transition features are defined on raw types.
CANONICAL_EVENT_FAMILY: Dict[str, str] = {
    "DEVICE_CHANGE": "device_change",
    "SIM_CHANGE": "sim_change",
    "STATUS_CHANGE-A": "account_reactivation",
    "STATUS_CHANGE-AA": "account_activation",
    "STATUS_CHANGE-C": "account_cancellation",
    "STATUS_CHANGE-S": "account_suspension",
    "PHONE_NUMBER_CHANGE": "phone_number_change",
    "MNO_PORT": "mno_port",
    "ENSTREAM_API_CALL": "enstream_api_call",
}

# The inverse, for the state transition features, which are declared in terms of
# raw event types.
EVENT_TYPE_BY_FAMILY: Dict[str, str] = {
    family: event_type for event_type, family in CANONICAL_EVENT_FAMILY.items()
}

# Columns that ride along on an EnStream event and describe who made the call.
# Only EnStream events carry them; every other feed gets nulls, which is what
# makes the diversity and novelty features EnStream-only.
EVENT_METADATA_COLS: Tuple[str, ...] = (
    "partner_id",
    "partner_name",
    "service_provider_id",
    "service_provider_name",
    "industry",
    "use_case",
)

# The dimensions the EnStream diversity and novelty features are computed over.
ENSTREAM_DIMENSION_COLS: Tuple[str, ...] = (
    "industry",
    "service_provider_name",
    "use_case",
)

# The families that get the full generic feature set. This is every family in
# CANONICAL_EVENT_FAMILY, and it is written as a comprehension over that mapping
# rather than as a literal list for a specific reason.
#
# The notebook library wrote it as a literal, and the literal had a missing
# comma in it:
#
#     ["device_change", ..., "account_cancellation"      <- no comma
#      "phone_number_change", "mno_port", ...]
#
# Python concatenates adjacent string literals, so the list contained
# "account_cancellationphone_number_change" — a family that matches no event.
# `account_cancellation` and `phone_number_change` therefore had no features at
# all, and a third group of all-null columns appeared under the concatenated
# name. Nothing raised: the filter simply matched nothing.
#
# `account_activation` was also absent from that literal, which is a separate
# gap rather than a consequence of the comma. Deriving the tuple from the family
# mapping closes both, and means a family added to the mapping cannot be
# forgotten here.
GENERIC_EVENT_FAMILIES: Tuple[str, ...] = tuple(
    sorted(set(CANONICAL_EVENT_FAMILY.values()))
)

# The raw event types the state transition features are defined over.
#
# The notebook library spelled the last of these `STATUS_CHANGE_C`, with an
# underscore, where every other status type and the carrier feed itself use a
# hyphen. `event_type.isin(...)` then matched nothing for it, so all fourteen
# transition pairs that involve a cancellation — seven into it and seven out of
# it — produced columns of nulls. Cancellation is the end of an account, so
# "how long after a device change did this account cancel" was among the
# features that silently did not exist.
STATE_TRANSITION_EVENT_TYPES: Tuple[str, ...] = (
    "DEVICE_CHANGE",
    "SIM_CHANGE",
    "STATUS_CHANGE-A",
    "STATUS_CHANGE-AA",
    "STATUS_CHANGE-C",
    "STATUS_CHANGE-S",
    "PHONE_NUMBER_CHANGE",
    "MNO_PORT",
    "ENSTREAM_API_CALL",
)

# Every ordered pair of distinct types. A pair (source, target) asks: after a
# `source`, how long until the next `target`?
DEFAULT_STATE_TRANSITIONS: Tuple[Tuple[str, str], ...] = tuple(
    (source, target)
    for source in STATE_TRANSITION_EVENT_TYPES
    for target in STATE_TRANSITION_EVENT_TYPES
    if source != target
)

# The three grains features are built at, coarsest first.
SCOPES: Tuple[str, ...] = ("customer", "account", "msisdn")

# The prefix every feature name at a scope carries. Short, because these names
# are concatenated with a family and a window and the result has to stay
# readable in a correlation matrix.
_SCOPE_PREFIX: Dict[str, str] = {
    "customer": "cust",
    "account": "acct",
    "msisdn": "msisdn",
}


def scope_prefix(scope: str) -> str:
    """The feature-name prefix for ``scope``.

    Every feature built at a scope carries this prefix, without exception. The
    notebook library had one exception — the EnStream wrapper passed the scope
    name itself rather than the prefix — which split the EnStream family across
    ``cust_enstream_api_call_*`` and ``customer_enstream_api_call_*`` depending
    on which calculator produced the column. Going through one function makes
    that particular mistake impossible to repeat.
    """
    if scope not in _SCOPE_PREFIX:
        raise ValueError(
            f"Unsupported scope {scope!r}. Expected one of {', '.join(SCOPES)}."
        )
    return _SCOPE_PREFIX[scope]


# --- added from the v5 notebook (cell 04) ---
AC_EVENT_TYPE_ALIASES: Dict[str, str] = {
    "account_activation_number_recycle": "account_activation",
}

AC_EVENT_TYPES_IGNORED: Tuple[str, ...] = (
    "account_activation_mno_port",
    # The carrier's two sides of a number change: `_from` on the old number,
    # `_to` on the new one. Both are excluded because `phone_number_change` is
    # derived from the lineage instead, by build_phone_number_change_events,
    # and the derivation is the more accurate of the two. Carrying the carrier
    # rows as well would put the same change in the table twice under two
    # event_source values, which event_dedupe_keys() would not collapse.
    "phone_number_change_from",
    "phone_number_change_to",
    # A rate-plan change. It has no canonical family, so it was already being
    # discarded by the event_type filter; listing it here changes no row and
    # records that the omission is a decision rather than an oversight. Give it
    # a CANONICAL_EVENT_FAMILY entry, not an alias, if it ever earns features
    # of its own -- it is not a spelling of any family we already have.
    "plan_change",
)

_alias_ignore_overlap = sorted(set(AC_EVENT_TYPE_ALIASES) & set(AC_EVENT_TYPES_IGNORED))
if _alias_ignore_overlap:
    raise ValueError(
        "A change type cannot be both aliased and ignored; the ignore filter "
        "runs first and would discard it before the alias applied. Overlapping: "
        f"{', '.join(_alias_ignore_overlap)}."
    )

_alias_targets_unknown = sorted(
    set(AC_EVENT_TYPE_ALIASES.values()) - set(CANONICAL_EVENT_FAMILY.values())
)
if _alias_targets_unknown:
    raise ValueError(
        "Every AC_EVENT_TYPE_ALIASES value must be a canonical family, or the "
        "alias resolves to something that still maps to a null event_type and "
        f"gets dropped anyway. Unknown: {', '.join(_alias_targets_unknown)}."
    )

INTERVAL_ENDING_FAMILIES: Tuple[str, ...] = ("account_cancellation",)

INTERVAL_END_GRACE_HOURS: int = 24

_ending_unknown = sorted(set(INTERVAL_ENDING_FAMILIES) - set(CANONICAL_EVENT_FAMILY.values()))
if _ending_unknown:
    raise ValueError(
        "Every INTERVAL_ENDING_FAMILIES entry must be a canonical family, or "
        "the grace window is applied to a value no event ever carries. "
        f"Unknown: {', '.join(_ending_unknown)}."
    )

BURST_PERIOD_DAYS: Dict[str, float] = {"week": 7.0, "month": 30.44}

BURST_PERIOD_LABEL: Dict[str, str] = {"week": "weekly", "month": "monthly"}

BURST_PERIOD_PLURAL: Dict[str, str] = {"week": "weeks", "month": "months"}

MIN_BURST_PERIOD_BUCKETS: float = 6.0

LONG_HORIZON_ENTROPY_UNITS: Tuple[str, ...] = ("week_of_year", "month_of_year")


@dataclass(frozen=True)
class FeatureConfig:
    """Column names and windows for the whole feature pipeline.

    Every calculator takes one of these and none of them reads a global. The
    notebook library also took a config object but forwarded it inconsistently:
    several calculators called their dependencies without passing it on, so a
    caller who changed ``lookback_days`` got the new windows in some feature
    families and the defaults in others. Every call in this package forwards
    ``cfg``, and the tests assert it.

    Attributes:
        lookback_days: The rolling windows, in days, that count and any-flag
            features are computed over. Note ``lookback_semantics`` below —
            these are not all measured the same way.
        lookback_hour_windows: Windows measured in exact hours from the
            reference timestamp rather than in whole calendar days. The short
            windows are here because "in the last twenty-four hours" and "on the
            same calendar day" differ by up to a day at the boundary, and for a
            one-day window that is the whole window. The longer windows use
            calendar days, where the boundary effect is proportionally small and
            calendar alignment is what the fraud team reasons in. This split is
            inherited from the notebook library, and it is kept rather than
            unified because the trained models were fitted against it.
        lookback_semantics: How a window's lower bound is drawn.
            ``"interval"`` (default from v4.0) makes every window the
            half-open range ``(reference_ts - days*24h, reference_ts]``, so a
            window labelled *N* days covers exactly *N* x 24 hours whatever
            time of day the reference sits at. ``"calendar"`` restores the
            pre-v4.0 behaviour, where the long windows matched on calendar
            dates instead; see ``_lookback_condition_calendar_days`` for why
            that lost a day against this pipeline's midnight references. Set
            it to ``"calendar"`` only to reproduce output from before v4.0.
        burst_period: The bucket the burstness family aggregates over, one of
            ``BURST_PERIOD_DAYS``. Default ``"month"``, which is reachable
            because ``prune_event_window`` is off and the scan runs back to
            ``from_ts``: how many buckets an entity gets is now a fact about
            its tenure, not a cap imposed by the config. ``"week"`` gives
            finer resolution — a takeover burst plays out over days, and a
            monthly bucket smears it — at the cost of column names that leave
            the documented spec. ``__post_init__`` still rejects a bucket too
            wide for the scan whenever ``prune_event_window`` is on. The column
            names follow the period: ``_max_monthly_cnt`` / ``_active_months``
            versus ``_max_weekly_cnt`` / ``_active_weeks``.
        long_horizon_entropy_unit: The third temporal-entropy dimension, after
            hour-of-day and day-of-week. ``"month_of_year"`` (default) reaches
            all twelve of its buckets over a multi-year tenure;
            ``"week_of_year"`` has fifty-three and is the finer alternative.
            Both were near-degenerate under the v3.0 97-day scan, which is why
            this knob exists.
        k_event_values: The *k* values for the "how long did the last k events
            take" velocity features.
        recency_half_life_days: Half-life of the exponential decay in the
            recency-weighted intensity feature.
        enstream_min_weight: Floor on the EnStream partner volume weight.
        enstream_max_weight: Ceiling on the same.
        event_read_buffer_days: Extra days added to the widest lookback when
            pruning the event scan. A buffer covers events that are relevant to
            an interval-based feature but fall just outside the widest count
            window. Read only when ``prune_event_window`` is True, which from
            v4.0 it is not.
        prune_event_window: Whether to floor the scope/event join at
            ``reference_ts - (max_lookback_days + event_read_buffer_days)``.
            **Off from v4.0**, so the join's lower bound is the entity's own
            ``from_ts`` and the scan spans its whole tenure.

            It was on through v3.0, and the effect was easy to miss because the
            bound is ``greatest(from_ts, reference_ts - 97 days)``: the 97-day
            floor is the later of the two for any entity with more than 97 days
            of tenure, so the scan was ``min(tenure, 97 days)`` and tenure
            bound only for entities younger than that. Every windowed count was
            unaffected — the widest is 90 days — but everything unwindowed was
            silently a 97-day statistic: ``_total_cnt``, ``_days_since_first``,
            all of periodicity, burstness, cumulative slope and every state
            transition pair. ``_days_since_last`` was worse than truncated: an
            entity dormant longer than 97 days had no events in the scan at
            all, so it dropped out of the family frame and the column came back
            NULL rather than large — the dormant-then-active pattern the
            feature exists to catch.

            The cost of turning it off is real and is the reason it was on: the
            range join now materialises each entity's full event history, once
            per reference. Note ``scan_days`` is meaningless in this mode.

            Turning it back on requires a ``burst_period`` that fits inside 97
            days — ``__post_init__`` will say so.
        broadcast_scope: Whether to broadcast the scope snapshot into the event
            join. The snapshot is one row per reference and the event table is
            many rows per reference, so the snapshot is the small side.
    """

    lookback_days: Sequence[int] = (1, 3, 7, 30, 90)
    lookback_hour_windows: Sequence[int] = (1, 3)
    k_event_values: Sequence[int] = (3, 5)
    recency_half_life_days: float = 7.0

    lookback_semantics: str = "interval"
    burst_period: str = "month"
    long_horizon_entropy_unit: str = "month_of_year"

    customer_col: str = "customer_id"
    account_col: str = "acct_id"
    msisdn_col: str = "msisdn"
    mno_col: str = "mno"
    from_ts_col: str = "from_ts"
    to_ts_col: str = "to_ts"
    reference_ts_col: str = "reference_ts"
    reference_id_col: str = "reference_id"
    event_ts_col: str = "event_ts"
    event_weight_col: str = "event_weight"
    event_family_col: str = "event_family"
    event_type_col: str = "event_type"
    event_source_col: str = "event_source"

    enstream_min_weight: float = 0.25
    enstream_max_weight: float = 4.0
    enstream_partner_cols: Sequence[str] = ("service_provider_id", "use_case")

    event_read_buffer_days: int = 7
    prune_event_window: bool = False
    broadcast_scope: bool = False

    # Filled in by __post_init__ so that a caller who overrides `lookback_days`
    # does not have to keep this in step by hand.
    ratio_windows: Sequence[Tuple[int, int]] = field(default=((1, 3), (7, 30), (30, 90)))

    def __post_init__(self) -> None:
        missing = [
            f"{numerator}d/{denominator}d"
            for numerator, denominator in self.ratio_windows
            if numerator not in self.lookback_days or denominator not in self.lookback_days
        ]
        if missing:
            raise ValueError(
                "Every window named in `ratio_windows` must also appear in "
                f"`lookback_days`. Missing: {', '.join(missing)}. "
                f"lookback_days={tuple(self.lookback_days)}."
            )

        if self.lookback_semantics not in ("interval", "calendar"):
            raise ValueError(
                "lookback_semantics must be 'interval' or 'calendar', got "
                f"{self.lookback_semantics!r}."
            )

        if self.long_horizon_entropy_unit not in LONG_HORIZON_ENTROPY_UNITS:
            raise ValueError(
                "long_horizon_entropy_unit must be one of "
                f"{', '.join(LONG_HORIZON_ENTROPY_UNITS)}, got "
                f"{self.long_horizon_entropy_unit!r}."
            )

        if self.burst_period not in BURST_PERIOD_DAYS:
            raise ValueError(
                f"burst_period must be one of {', '.join(BURST_PERIOD_DAYS)}, "
                f"got {self.burst_period!r}."
            )

        # A burstness bucket wider than the event scan is not a feature, it is
        # a constant. Before v4.0 the bucket was hardcoded to a calendar month
        # while the scan was pruned to 97 days, so `_active_months` could only
        # ever be 1-4 and `_monthly_peak_to_avg` was a ratio over three or four
        # numbers. Nothing raised; the columns were simply near-degenerate.
        # `prune_event_window=False` reads the entity's whole tenure, so the
        # check does not apply there.
        if self.prune_event_window:
            reachable = self.scan_days / BURST_PERIOD_DAYS[self.burst_period]
            if reachable < MIN_BURST_PERIOD_BUCKETS:
                raise ValueError(
                    f"burst_period={self.burst_period!r} yields only "
                    f"~{reachable:.1f} buckets inside the {self.scan_days}-day "
                    f"event scan (max_lookback_days={self.max_lookback_days} + "
                    f"event_read_buffer_days={self.event_read_buffer_days}), "
                    f"below the {MIN_BURST_PERIOD_BUCKETS:g} needed for the "
                    "peak-to-average and spread statistics to mean anything. "
                    "Use a shorter burst_period, widen lookback_days, or set "
                    "prune_event_window=False."
                )

    @property
    def max_lookback_days(self) -> int:
        """The widest count window, which bounds how far back events are read."""
        return int(max(self.lookback_days))

    @property
    def scan_days(self) -> int:
        """How many days of events a pruned scope join actually reads.

        This is the real horizon of every unwindowed feature -- the burstness
        buckets, the long-horizon entropy, the periodicity gaps -- and it is
        wider than ``max_lookback_days`` by the read buffer.
        """
        return self.max_lookback_days + int(self.event_read_buffer_days)

    @property
    def scope_group_cols(self) -> Dict[str, Tuple[str, ...]]:
        """The identity columns that define each scope.

        Coarser scopes are prefixes of finer ones, which is what lets one join
        of events to a snapshot serve all three.
        """
        return {
            "customer": (self.customer_col,),
            "account": (self.customer_col, self.account_col),
            "msisdn": (self.customer_col, self.account_col, self.msisdn_col),
        }

    def group_cols_for(self, scope: str) -> Tuple[str, ...]:
        """The identity columns for ``scope``."""
        groups = self.scope_group_cols
        if scope not in groups:
            raise ValueError(
                f"Unsupported scope {scope!r}. Expected one of {', '.join(SCOPES)}."
            )
        return groups[scope]

    def entity_cols_for(self, scope: str) -> Tuple[str, ...]:
        """The full grouping key for ``scope``: the reference plus its identity.

        The reference id is first because two references for the same customer
        at different moments are different rows, and every feature is computed
        per reference.
        """
        return (self.reference_id_col, *self.group_cols_for(scope))

    def event_base_columns(self) -> Tuple[str, ...]:
        """The columns every canonical event frame is aligned to before union."""
        return (
            self.customer_col,
            self.account_col,
            self.msisdn_col,
            self.mno_col,
            self.event_ts_col,
            self.event_family_col,
            self.event_type_col,
            self.event_source_col,
            self.event_weight_col,
            *EVENT_METADATA_COLS,
        )

    def event_dedupe_keys(self) -> Tuple[str, ...]:
        """What makes two canonical event rows the same event.

        The partner columns are in here because one MSISDN can legitimately
        produce two EnStream calls at the same instant from two partners, and
        collapsing those would understate the volume that the weighting exists
        to normalise.
        """
        return (
            self.customer_col,
            self.account_col,
            self.msisdn_col,
            self.mno_col,
            self.event_ts_col,
            self.event_type_col,
            self.event_source_col,
            "partner_id",
            "service_provider_id",
            "use_case",
        )
