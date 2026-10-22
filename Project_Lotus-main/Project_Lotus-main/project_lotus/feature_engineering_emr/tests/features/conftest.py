"""Fixtures for the feature suite, and why this one cannot avoid Spark.

The ML suite runs on pandas because every module it tests takes arrays or
callables. The feature package is the opposite: window functions, interval
joins and multi-stage aggregations *are* the logic, and a fixture that mocked
them would test the mock. So this suite starts one local SparkSession for the
whole run and builds every frame from small Python literals.

The frames are deliberately awkward rather than tidy. The population has a
customer with two carriers, a customer with two accounts on one carrier, a
customer whose number changed hands, and a customer with no events at all. Each
of those exists because a feature that is right on a single-account,
single-carrier, always-eventful customer can still be wrong in production —
and three of the four defects this package fixes were only visible on the
awkward rows.

One process-level decision, made at import time for the same reason
``tests/lineage/conftest.py`` makes it: the driver timezone is pinned to ``EST``
before pyspark is imported. A naive ``datetime`` handed to ``createDataFrame``
is interpreted in the driver's zone and read back in the session's, so a
container running UTC against an EST session shifts every literal by five hours
and every timestamp assertion in the suite becomes wrong by exactly that. Pinned
at both ends, ``"2024-06-01 09:00"`` means what it says.
"""

from __future__ import annotations

import datetime as _dt
import os
from pathlib import Path
import sys
import time
from typing import Dict, List, Optional, Sequence

import pytest

SESSION_TZ = "EST"

os.environ["TZ"] = SESSION_TZ
if hasattr(time, "tzset"):
    time.tzset()

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TESTS_DIR.parent.parent

_SRC = str(REPO_ROOT)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# The feature calculators are all DataFrame API, so nothing here is pickled into
# a Python worker the way the lineage walks are. PYTHONPATH is set anyway
# because a future test that uses a UDF would otherwise fail inside a task with
# a ModuleNotFoundError a long way from its cause.
os.environ["PYTHONPATH"] = os.pathsep.join(
    p for p in (_SRC, os.environ.get("PYTHONPATH", "")) if p
)

from ts05_features.library.config import (  # noqa: E402  (after sys.path)
    EVENT_METADATA_COLS,
    FeatureConfig,
)

# --------------------------------------------------------------------------
# session
# --------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark", reason="pyspark is required for these tests")
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    session = (
        SparkSession.builder.appName("trust_score_05.feature_tests")
        .master("local[2]")
        # One shuffle partition. Every frame here is a handful of rows and the
        # calculators shuffle repeatedly, so partition scheduling would
        # otherwise dominate the runtime of the whole suite.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", SESSION_TZ)
        .config("spark.driver.host", "127.0.0.1")
        # The weighting and the transition features join a frame to a
        # derivative of itself. Spark can resolve that lineage into an
        # ambiguous self-join reference; failing on it rather than guessing is
        # what we want in a test, and the calculators are written to avoid it.
        .config("spark.sql.analyzer.failAmbiguousSelfJoin", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    _ = pyspark
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _session_timezone(request):
    """Pin the session zone per test, and only if the test asked for Spark."""
    if "spark" in request.fixturenames:
        request.getfixturevalue("spark").conf.set(
            "spark.sql.session.timeZone", SESSION_TZ
        )
    yield


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------

@pytest.fixture
def cfg() -> FeatureConfig:
    """The production config, defaults and all.

    Not a slimmed-down one. The windows, the hour/day split and the weight
    clamps are the values the models were trained against, and a suite that
    tested three-day windows would not notice a regression in the ninety-day
    ones.
    """
    return FeatureConfig()


@pytest.fixture
def narrow_cfg() -> FeatureConfig:
    """A config with two windows and uniform day semantics.

    For the tests that assert on *every* window's column, where iterating five
    windows in both hour and day flavours costs Spark stages without covering
    any additional branch. ``ratio_windows`` has to be narrowed in step or
    ``FeatureConfig.__post_init__`` rejects it — which is itself asserted in
    ``test_config.py``.
    """
    return FeatureConfig(
        lookback_days=(3, 30),
        lookback_hour_windows=(),
        ratio_windows=((3, 30),),
        k_event_values=(3,),
    )


# --------------------------------------------------------------------------
# time helpers
# --------------------------------------------------------------------------

def ts(value: str) -> _dt.datetime:
    """``"2024-06-01"`` or ``"2024-06-01 09:00"`` -> a naive datetime.

    Naive is safe because the module pins the driver zone to the session zone
    above; see the module docstring.
    """
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return _dt.datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"unrecognised timestamp literal {value!r}")


#: The moment every reference in this suite is taken at. A Saturday, chosen so
#: that the day-of-week entropy fixtures are not accidentally uniform, and far
#: enough from a month boundary that the ninety-day window does not clip.
REFERENCE_TS = "2024-06-01 12:00:00"

#: Open-interval sentinels, matching ``features.config``. Spelled out here
#: rather than imported so that a change to the constants shows up as a test
#: failure rather than as two definitions moving in lockstep.
FAR_FUTURE = "9999-12-31 00:00:00"


# --------------------------------------------------------------------------
# frame builders
# --------------------------------------------------------------------------

LINEAGE_COLUMNS = ("customer_id", "acct_id", "msisdn", "mno", "from_ts", "to_ts")

#: The columns the lineage contract hands to the feature pipeline. Asserted
#: against in ``test_snapshots.py`` so that a change on either side of the
#: phase-1/phase-2 boundary is caught here rather than in a cluster run.
EXPECTED_LINEAGE_COLUMNS = (
    "customer_id",
    "acct_id",
    "customer_seq",
    "assignment_type",
    "mno",
    "msisdn",
    "from_ts",
    "to_ts",
    "seq_order",
    "segment_type",
    "account_type",
    "lifecycle_uid",
    "source_event_id",
)


def _lineage_row(
    customer: str,
    account: str,
    msisdn: str,
    mno: str,
    from_ts: Optional[str],
    to_ts: Optional[str],
) -> tuple:
    return (
        customer,
        account,
        msisdn,
        mno,
        ts(from_ts) if from_ts else None,
        ts(to_ts) if to_ts else None,
    )


#: The population, as (customer, account, msisdn, mno, from_ts, to_ts).
#:
#: C1  one account, one number, one carrier, open interval (to_ts NULL). The
#:     ordinary case, and the one that proves the open end gets closed.
#: C2  two accounts on BELL, so the account scope has two rows for one
#:     reference and the customer scope must not double-count.
#: C3  two carriers, which is the row that makes the MNO part of the account
#:     grain load-bearing.
#: C4  a number that moved from C4 to C5 in March, so the same MSISDN resolves
#:     to different customers depending on the reference date.
#: C6  present in the lineage and absent from every event feed: the silent
#:     customer whose feature row must exist and be zeros, not missing.
LINEAGE_ROWS: Sequence[tuple] = (
    _lineage_row("C1", "A1", "P1", "BELL", "2022-01-01", None),
    _lineage_row("C2", "A2", "P2", "BELL", "2023-03-15", None),
    _lineage_row("C2", "A3", "P3", "BELL", "2023-09-01", None),
    _lineage_row("C3", "A4", "P4", "ROGERS", "2021-06-01", None),
    _lineage_row("C3", "A5", "P5", "TELUS", "2024-02-01", None),
    _lineage_row("C4", "A6", "P6", "TELUS", "2020-01-01", "2024-03-01"),
    _lineage_row("C5", "A7", "P6", "TELUS", "2024-03-01", None),
    _lineage_row("C6", "A8", "P7", "BELL", "2019-05-01", None),
)


@pytest.fixture
def lineage_df(spark):
    """The lineage frame, with the open ends left NULL as the real table has them.

    Left NULL on purpose. ``closed_lineage_intervals`` exists precisely because
    the Gold tables write NULL for "still open", and a fixture that pre-filled
    the sentinel would make every interval test pass without that function.
    """
    from pyspark.sql.types import (
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("acct_id", StringType(), True),
            StructField("msisdn", StringType(), True),
            StructField("mno", StringType(), True),
            StructField("from_ts", TimestampType(), True),
            StructField("to_ts", TimestampType(), True),
        ]
    )
    return spark.createDataFrame(list(LINEAGE_ROWS), schema)


def event_schema():
    """The canonical event schema, with partner metadata and a weight."""
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    fields = [
        StructField("customer_id", StringType(), True),
        StructField("acct_id", StringType(), True),
        StructField("msisdn", StringType(), True),
        StructField("mno", StringType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("event_family", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_source", StringType(), True),
        StructField("event_weight", DoubleType(), True),
    ]
    fields.extend(
        StructField(col_name, StringType(), True) for col_name in EVENT_METADATA_COLS
    )
    return StructType(fields)


def make_event(
    customer: str,
    account: str,
    msisdn: str,
    mno: str,
    event_ts: str,
    event_type: str,
    *,
    event_family: Optional[str] = None,
    event_source: str = "carrier",
    event_weight: float = 1.0,
    partner_id: Optional[str] = None,
    partner_name: Optional[str] = None,
    partner_type: Optional[str] = None,
    service_provider_id: Optional[str] = None,
    service_provider_name: Optional[str] = None,
    industry: Optional[str] = None,
    use_case: Optional[str] = None,
) -> tuple:
    """One canonical event row.

    ``event_family`` defaults to the mapping of ``event_type``, so a test that
    cares about the family writes only the type and cannot put the two out of
    step — which is how a family filter comes to match nothing.
    """
    from ts05_features.library.config import CANONICAL_EVENT_FAMILY

    family = event_family or CANONICAL_EVENT_FAMILY[event_type]
    return (
        customer,
        account,
        msisdn,
        mno,
        ts(event_ts),
        family,
        event_type,
        event_source,
        float(event_weight),
        partner_id,
        partner_name,
        partner_type,
        service_provider_id,
        service_provider_name,
        industry,
        use_case,
    )


def events_frame(spark, rows: Sequence[tuple]):
    """A canonical event frame from ``make_event`` rows."""
    return spark.createDataFrame(list(rows), event_schema())


def collect_features(df, entity_cols: Sequence[str]) -> Dict[tuple, dict]:
    """Collect a feature frame into ``{entity key: {column: value}}``.

    A dict keyed by the entity, because every assertion in this suite is of the
    form "this customer's such-and-such column is *n*", and reading that out of
    a list of Rows in row order makes the test depend on a shuffle ordering
    Spark does not promise.
    """
    keys = list(entity_cols)
    out: Dict[tuple, dict] = {}
    for row in df.collect():
        as_dict = row.asDict()
        key = tuple(as_dict[k] for k in keys)
        out[key] = as_dict
    return out


def columns_matching(df, *fragments: str) -> List[str]:
    """The frame's column names containing every one of ``fragments``.

    Used by the regression tests, which assert on the *shape* of the output —
    "there is a column for this family in every window" — rather than on a
    hand-written list that would have to be edited whenever a window is added.
    """
    return sorted(
        col_name
        for col_name in df.columns
        if all(fragment in col_name for fragment in fragments)
    )
