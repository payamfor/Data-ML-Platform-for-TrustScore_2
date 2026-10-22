"""Test-only: run the EMR feature job for real, under local Spark, on synthetic Gold data.

    python tests/fe_local.py --mode training [--out DIR] [--original-sagemaker-dir DIR]
    python tests/fe_local.py --mode scoring --batch-date 2024-08-15 [...]

Adapted from the client's ``sagemaker/tests/fe_local.py`` (same synthetic data,
same in-memory storage). What runs unmodified: the whole job - the v5 feature
library, every verbatim notebook function, the wide-table assembly and the
fraud labels. What is replaced: only Spark's *storage* calls (parquet/CSV write
and read, ``_path_exists``, and the job's ``_delete_path`` / ``_write_text``),
which go to an in-memory store, because open-source Spark cannot write files on
Windows without the Hadoop native binaries. On EMR the real S3 calls run.

Checks:
1. the output tree is exactly the DE layout (train/ and test/ non-fraud, test/
   fraud, test/batch_date=<d>/ for scoring) and the completion marker is the
   LAST thing written; a stale marker is removed at the start;
2. Spark's per-directory ``_SUCCESS`` is switched off;
3. when a window fails, no completion marker is written (exit code 1);
4. with ``--original-sagemaker-dir`` (the client's ``sagemaker/`` folder), the
   original SageMaker job is run on the same data and every published table
   must be identical, row for row - the proof that the move changed no logic.

``--out DIR`` exports what went in and what came out, so it can be inspected
(each table as ``part-00000.parquet`` plus a ``part-00000.csv`` copy):

    DIR/inputs/<table>/                      the 5 synthetic input tables
    DIR/mock-data/version1/train/…, test/…   the published tables, in the job's paths
    DIR/mock-data/version1/train/_SUCCESS    the completion marker
    DIR/mock-data/version1/_runs/…/summary.json
    DIR/mock-data/version1/feature_store/…   with --export-all: the 12 feature groups
    DIR/mock-data/version1/_work/…           with --export-all: scratch tables, manifests
"""

from __future__ import annotations

import argparse
import datetime as dt
import fnmatch
from functools import reduce
import importlib
import json
import os
from pathlib import Path
import random
import sys
import tempfile

HERE = Path(__file__).resolve().parent
PACKAGE_DIR = HERE.parent
sys.path[:0] = [str(PACKAGE_DIR)]

OUTPUT_ROOT = "s3://ts05-fe-test/mock-data/version1/"
ORIGINAL_ROOT = "s3://ts05-fe-test/original-sagemaker/features/"
TABLES = {
    "gold.msisdn_lifecycle_account_customer_mapping": "lineage",
    "gold.account_changes_canonical_events": "account_changes",
    "silver.audit_trail_services_3": "enstream",
    "silver.device_lookup_batch": "hash_mapping",
    "gold.fraud_labels": "fraud_labels",
}
N_CUSTOMERS, FRAUD_CUSTOMERS = 60, 12


def params(window_start: str, window_end: str, emr: bool = True,
           original_root: str = ORIGINAL_ROOT) -> dict:
    """The job's config. ``emr=False``: the original SageMaker job's config."""
    doc = _params(window_start, window_end)
    feats = doc["features"]
    if emr:
        labels = window_labels(window_start, window_end)
        # Last window to test/, the rest to train/ (the real split is config).
        feats.update(output_root=OUTPUT_ROOT,
                     training_windows=labels[:-1], testing_windows=labels[-1:])
        # As conf/sandbox.yaml: labels keyed by phone number (the PRD's label
        # schema), mapped to customers through the lineage. The original job
        # keeps customer-keyed labels, so --original-sagemaker-dir proves the
        # mapping gives the same fraud rows.
        feats["fraud_labels"] = {"table": "gold.fraud_labels",
                                 "phone_number_col": "phone_number_AC_hash",
                                 "fraud_type_col": "fraud_type",
                                 "fraud_timestamp_col": "fraud_timestamp",
                                 "fraud_source": "Phase1-v1.0"}
    else:
        feats["features_root"] = original_root
    return doc


def window_labels(window_start: str, window_end: str, days: int = 14) -> list:
    from datetime import datetime, timedelta

    cur = datetime.strptime(window_start, "%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(window_end, "%Y-%m-%d %H:%M:%S")
    out = []
    while cur < end:
        out.append(cur.strftime("%Y-%m-%d_%H-%M-%S"))
        cur += timedelta(days=days)
    return out


def _params(window_start: str, window_end: str) -> dict:
    return {"features": {
        "window_start_ts": window_start, "window_end_exclusive_ts": window_end,
        "window_size_days": 14,
        "output_version": "v5.0.bi-weekly",
        "enable_row_counts": True, "fail_fast": True,
        "output_partitions": 4, "min_output_partitions": 1,
        "sample_enabled": False,
        "inputs": {
            "lineage_table": "gold.msisdn_lifecycle_account_customer_mapping",
            "account_changes_table": "gold.account_changes_canonical_events",
            "enstream_table": "silver.audit_trail_services_3",
            "hash_mapping_table": "silver.device_lookup_batch",
        },
        "fraud_labels": {"table": "gold.fraud_labels", "customer_col": "customer_id",
                         "fraud_type_col": "fraud_type", "fraud_timestamp_col": "fraud_timestamp",
                         "fraud_source": "Phase1-v1.0"},
        # Terraform's spark_conf, sized for 60 customers on a laptop instead of
        # the notebook's 2000 shuffle partitions for a production cluster.
        "spark_conf": {"spark.sql.shuffle.partitions": "4",
                       "spark.sql.adaptive.coalescePartitions.enabled": "true",
                       "spark.ui.showConsoleProgress": "false",
                       "spark.executor.heartbeatInterval": "60s",
                       "spark.network.timeout": "900s"},
    }}


# --------------------------------------------------------------------------
# synthetic Gold / Silver inputs, in the real table schemas
# --------------------------------------------------------------------------

def make_inputs(spark):
    import datetime as dt

    from pyspark.sql import types as T

    rng = random.Random(7)
    ts = lambda s: dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")  # noqa: E731
    lineage, changes, calls, mapping, labels = [], [], [], [], []
    families = ["device_change", "sim_change", "account_suspension", "account_reactivation"]
    for i in range(N_CUSTOMERS):
        cust, acct, mno = f"c{i:03d}", f"a{i:03d}", ("ROGERS" if i % 2 else "BELL")
        numbers = [f"m{i:03d}"] + ([f"m{i:03d}b"] if i % 5 == 0 else [])
        for n, number in enumerate(numbers):
            start = ts("2022-01-01 00:00:00") + dt.timedelta(days=rng.randint(0, 400) + 30 * n)
            # Open intervals are NULL to_ts; closed_lineage_intervals closes them.
            lineage.append((cust, acct, mno, number, f"lc-{number}", start, None))
            mapping.append((number, f"t-{number}"))
            # background behaviour: a few carrier events and API calls a quarter
            for k in range(rng.randint(2, 8)):
                when = ts("2024-01-01 08:00:00") + dt.timedelta(days=rng.randint(0, 265),
                                                                 hours=rng.randint(0, 12))
                changes.append((f"ac-{number}-{k}", number, mno, rng.choice(families), when, f"co-{k}"))
            for k in range(rng.randint(1, 10)):
                when = ts("2024-01-01 09:00:00") + dt.timedelta(days=rng.randint(0, 265),
                                                                 minutes=rng.randint(0, 600))
                calls.append((f"api-{number}-{k}", f"t-{number}", mno, "PT1", "Partner One",
                              f"SP{rng.randint(1, 3)}", "BankCo", "BANKING", "identity_check",
                              "BRAND", when))
        if i < FRAUD_CUSTOMERS:
            # takeover pattern: device + SIM change and an API-call burst, days
            # before the labelled fraud event, spread over the four windows
            fraud_at = ts("2024-08-05 12:00:00") + dt.timedelta(days=4 * i)
            for k, fam in enumerate(["device_change", "sim_change", "device_change"]):
                changes.append((f"fr-{i}-{k}", numbers[0], mno, fam,
                                fraud_at - dt.timedelta(days=3 - k), f"fr-{k}"))
            for k in range(25):
                calls.append((f"fr-api-{i}-{k}", f"t-{numbers[0]}", mno, "PT9", "Partner Nine",
                              "SP9", "LendCo", "LENDING", "credit_check", "BRAND",
                              fraud_at - dt.timedelta(hours=40 - k)))
            labels.append((cust, "FA", fraud_at, numbers[0]))

    schema = lambda *cols: T.StructType([T.StructField(n, t, True) for n, t in cols])  # noqa: E731
    S, TS = T.StringType(), T.TimestampType()
    return {
        "lineage": spark.createDataFrame(lineage, schema(
            ("customer_id", S), ("acct_id", S), ("mno", S), ("phone_number_AC_hash", S),
            ("lifecycle_uid", S), ("from_ts", TS), ("to_ts", TS))),
        "account_changes": spark.createDataFrame(changes, schema(
            ("ac_event_id", S), ("phone_number_AC_hash", S), ("mno", S), ("ac_event_type", S),
            ("ac_event_ts", TS), ("correlation_id", S))),
        "enstream": spark.createDataFrame(calls, schema(
            ("record_id", S), ("phone_number_AT_hash", S), ("mno", S), ("partner_id", S),
            ("partner_name", S), ("service_provider_id", S), ("service_provider_name", S),
            ("industry", S), ("api_type", S), ("brand", S), ("api_timestamp", TS))),
        "hash_mapping": spark.createDataFrame(mapping, schema(
            ("phone_number_AC_hash", S), ("phone_number_AT_hash", S))),
        "fraud_labels": spark.createDataFrame(labels, schema(
            ("customer_id", S), ("fraud_type", S), ("fraud_timestamp", TS),
            ("phone_number_AC_hash", S))),
    }


# --------------------------------------------------------------------------
# Spark storage in memory
# --------------------------------------------------------------------------

STORE: dict = {}
#: Stand-in for the run control tables (--run-control).
from ts05_features.run_control import MemoryStore  # noqa: E402
RC_STORE = MemoryStore()
#: Small objects the job writes itself (the completion marker, the summary).
TEXT: dict = {}
#: Every write, in order - to prove the marker is written last.
WRITE_LOG: list = []


def _key(path) -> str:
    return str(path).rstrip("/")


def _write_text(path, text) -> None:
    TEXT[_key(path)] = text
    WRITE_LOG.append(_key(path))


def _delete_path(path) -> bool:
    key = _key(path)
    found = TEXT.pop(key, None) is not None
    found = (STORE.pop(key, None) is not None) or found
    return found


def install_memory_storage() -> None:
    from pyspark.sql import DataFrameReader, DataFrameWriter

    def put(writer, path):
        # persist(DISK_ONLY) + count: materialised once, on local disk through
        # Spark's own block manager (plain Java IO, so it works on Windows),
        # without holding every "written" table in the driver's heap.
        # (localCheckpoint trips a Spark internal error on some of these plans.)
        #
        # Then the lineage is CUT, as a real write-then-read cuts it: a table
        # read back from parquet is a fresh scan, not the plan that produced it.
        # Rebuilding the frame from its cached rows inside the JVM (no Python,
        # so no datetime conversion) gives it a one-step plan; without this the
        # driver accumulates every upstream plan and runs out of heap.
        from pyspark.sql import DataFrame
        from pyspark.storagelevel import StorageLevel

        key = _key(path)
        previous = STORE.get(key)
        source = writer._df
        cached = source.persist(StorageLevel.DISK_ONLY)
        cached.count()
        session = source.sparkSession
        jdf = session._jsparkSession.createDataFrame(cached._jdf.rdd(), cached._jdf.schema())
        frame = DataFrame(jdf, session).persist(StorageLevel.DISK_ONLY)
        frame.count()
        cached.unpersist()
        STORE[key] = frame
        WRITE_LOG.append(key)
        if previous is not None and previous is not frame:
            previous.unpersist()

    def get(path):
        key = _key(path)
        if "*" in key:
            hits = [df for k, df in STORE.items() if fnmatch.fnmatch(k, key)]
            if not hits:
                raise FileNotFoundError(key)
            return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), hits)
        if key not in STORE:
            raise FileNotFoundError(key)
        return STORE[key]

    DataFrameWriter.save = lambda self, path=None, *a, **k: put(self, path)
    DataFrameWriter.parquet = lambda self, path, *a, **k: put(self, path)
    DataFrameWriter.csv = lambda self, path, *a, **k: put(self, path)
    DataFrameReader.parquet = lambda self, *paths, **k: get(paths[0])
    DataFrameReader.load = lambda self, path=None, *a, **k: get(path)
    DataFrameReader.csv = lambda self, path, *a, **k: get(path)


def _long_path(path: Path) -> Path:
    """On Windows, the ``\\\\?\\`` form, which lifts the 260-character path limit.

    The feature-store paths (feature_set=…/version=…/dt=…) are long enough to
    exceed it under an ordinary output folder.
    """
    if os.name != "nt":
        return path
    text = str(path.resolve())
    return Path(text if text.startswith("\\\\?\\") else "\\\\?\\" + text)


def _write_frame(df, target: Path) -> None:
    """One table -> ``target/part-00000.parquet`` and ``target/part-00000.csv``."""
    target = _long_path(target)
    target.mkdir(parents=True, exist_ok=True)
    # Timestamps go out as strings: collecting a Spark timestamp needs a
    # Python datetime, which Windows cannot build for the open-interval
    # sentinel 9999-12-31. The ML stages never use them as features.
    exprs = [f"CAST(`{c}` AS STRING) AS `{c}`" if t == "timestamp" else f"`{c}`"
             for c, t in df.dtypes]
    frame = df.selectExpr(*exprs).toPandas()
    frame.to_parquet(target / "part-00000.parquet", index=False)
    # A CSV copy to open in Excel; parquet is what the ML side actually reads.
    frame.to_csv(target / "part-00000.csv", index=False)


def export(prefix: str, out: Path) -> list:
    """Every stored dataset under ``prefix`` -> ``out/<path after the bucket>/part-00000.{parquet,csv}``."""
    written = []
    for key, df in sorted(STORE.items()):
        if not key.startswith(_key(prefix)):
            continue
        rel = key.split("://", 1)[1].split("/", 1)[1]
        _write_frame(df, out / rel)
        written.append(rel)
    return written


def export_inputs(inputs: dict, out: Path) -> list:
    """The synthetic input tables -> ``out/inputs/<table>/part-00000.{parquet,csv}``."""
    written = []
    for table, name in TABLES.items():
        target = out / "inputs" / table
        _write_frame(inputs[name], target)
        written.append(f"inputs/{table}")
    return written


def _check(condition: bool, message: str, failures: list) -> None:
    print(("  ok    " if condition else "  FAIL  ") + message)
    if not condition:
        failures.append(message)


#: Relative tolerance for floating-point columns. The client's job itself is
#: not bit-for-bit repeatable: Spark adds doubles in task-completion order, so
#: two runs of the ORIGINAL job on the same data differ in the 15th-16th digit
#: of e.g. cust_enstream_api_call_weighted_cnt_30d (see --repeat-original).
FLOAT_RTOL = 1e-9
#: Set by _same_rows: True when the last comparison needed the tolerance.
LAST_NEEDED_TOLERANCE = False


def _same_rows(a, b, key: str = "customer_id") -> bool:
    """Same columns (in order) and the same rows.

    Exact for every column, except that double/float columns may differ by
    FLOAT_RTOL (relative) - and only when the exact comparison fails.
    """
    global LAST_NEEDED_TOLERANCE
    from pyspark.sql import functions as F

    LAST_NEEDED_TOLERANCE = False
    if a.columns != b.columns:
        print(f"        columns differ:\n          {a.columns}\n          {b.columns}")
        return False
    if a.exceptAll(b).count() == 0 and b.exceptAll(a).count() == 0:
        return True
    # Not exact: compare row by row on the key, doubles within tolerance.
    if key not in a.columns or a.count() != b.count():
        return False
    if a.select(key).distinct().count() != a.count() or b.select(key).distinct().count() != b.count():
        return False
    types = dict(a.dtypes)
    joined = a.alias("a").join(b.alias("b"), on=key, how="full_outer")
    mismatch = F.lit(False)
    for c in a.columns:
        if c == key:
            continue
        x, y = F.col(f"a.`{c}`"), F.col(f"b.`{c}`")
        if types[c] in ("double", "float"):
            close = (x.isNull() & y.isNull()) | (
                x.isNotNull() & y.isNotNull()
                & (F.abs(x - y) <= F.lit(FLOAT_RTOL) * F.greatest(F.lit(1.0), F.abs(x), F.abs(y))))
            # NaN == NaN counts as equal, as in the exact comparison.
            close = close | (F.isnan(x) & F.isnan(y))
        else:
            close = x.eqNullSafe(y)
        mismatch = mismatch | ~close
    same = joined.filter(mismatch).count() == 0
    LAST_NEEDED_TOLERANCE = same
    return same


def _label(same: bool) -> str:
    return "identical within float rounding" if same and LAST_NEEDED_TOLERANCE else "identical"


def _diff_columns(a, b, key: str = "customer_id", show: int = 3) -> list:
    """The columns whose values differ between two tables keyed by ``key``."""
    from pyspark.sql import functions as F

    if key not in a.columns or key not in b.columns:
        return ["<no key column>"]
    cols = [c for c in a.columns if c != key and c in b.columns]
    joined = a.alias("a").join(b.alias("b"), on=key, how="full_outer")
    flags = [F.sum(F.when(~F.col(f"a.`{c}`").eqNullSafe(F.col(f"b.`{c}`")), 1).otherwise(0))
             .alias(c) for c in cols]
    counts = joined.agg(*flags).collect()[0].asDict()
    differing = [c for c in cols if counts[c]]
    for c in differing:
        rows = (joined.filter(~F.col(f"a.`{c}`").eqNullSafe(F.col(f"b.`{c}`")))
                .select(key, F.col(f"a.`{c}`").cast("string").alias("a"),
                        F.col(f"b.`{c}`").cast("string").alias("b")).limit(show).collect())
        print(f"        {c}: {counts[c]} row(s) differ, e.g. "
              + "; ".join(f"{r[key]}: {r['a']} vs {r['b']}" for r in rows))
    return differing


def _run_job(module_name: str, config: dict, opts, inputs, spark):
    """Import a job module with its argv, wire it to the in-memory store, run it."""
    if "ts05_features" in module_name and getattr(opts, "run_control", False):
        config["features"]["run_control"] = {
            "enabled": True, "table": "fe_run_control", "job_name": "ml_feature_engineering",
            "gold_table": "gold_run_control", "gold_job_name": "gold_account_customer_map"}
    path = Path(tempfile.mkdtemp(prefix="ts05_fe_")) / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    sys.argv = ["feature_engineering_job.py", "--config", str(path), "--mode", opts.mode] + (
        ["--batch-date", opts.batch_date] if opts.batch_date else [])
    if "ts05_features" in module_name and not getattr(opts, "run_control", False):
        # The synthetic data is from 2024; without a range the job reads only today.
        sys.argv += ["--start-date", "2000-01-01", "--end-date", "2099-12-31"]
    if "ts05_features" in module_name and getattr(opts, "limit", None):
        sys.argv += ["--limit", str(opts.limit)]
    if module_name in sys.modules:
        # A second run of the same job: re-read its configuration from argv.
        fe = importlib.reload(sys.modules[module_name])
    else:
        fe = importlib.import_module(module_name)
    fe.read_source = lambda name: inputs[TABLES[name]]
    fe._path_exists = lambda p: _key(p) in STORE
    if hasattr(fe, "_write_text"):
        fe._write_text = _write_text
        fe._delete_path = _delete_path
    fe.spark = spark
    if "ts05_features" in module_name and getattr(opts, "run_control", False):
        # Gold's last successful run ended 2024-08-01, and this job has no run yet:
        # the first run covers everything after that, up to now.
        RC_STORE.rows.clear()
        RC_STORE.gold = (dt.datetime(2024, 7, 1), dt.datetime(2024, 8, 1))
        fe.run_control.SparkStore = lambda *a, **k: RC_STORE
    return fe, fe.main()


def check_run_control(failures, limit=None) -> None:
    rows = RC_STORE.rows
    print(f"\nrun control rows: {len(rows)}")
    for r in rows:
        print(f"  {r['status']:<9} {r['watermark_from']} .. {r['watermark_to']} slice_size={r['slice_size']}")
    _check([r["status"] for r in rows] == ["running", "succeeded"], "a running row, then a succeeded row", failures)
    if len(rows) == 2:
        running, final = rows
        _check(running["run_id"] == final["run_id"], "same run_id on both rows", failures)
        _check(final["watermark_from"] == dt.datetime(2024, 8, 1), "first run starts where Gold ended", failures)
        if limit:
            _check(final["limit_applied"] is True, "the audit row records that a limit was applied", failures)
            _check(0 < final["slice_size"] <= 3 * int(limit), f"slice_size is at most 3 x limit ({3 * int(limit)})", failures)
            return
        _check(final["slice_size"] > 0, "slice_size counts the records processed", failures)
        _check(final["finished_ts"] is not None, "finished_ts is set", failures)


def check_emr_run(fe, code, opts, spark, failures) -> None:
    from ts05_features import layout

    root = OUTPUT_ROOT
    marker = _key(layout.batch_marker_path(root, opts.mode, opts.batch_date))
    print(f"\nEMR job (mode={opts.mode}) exit code {code}")
    _check(code == 0, "exit code 0", failures)
    _check(spark._jsc.hadoopConfiguration().get(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs") == "false",
        "Spark's per-directory _SUCCESS is switched off", failures)
    _check(TEXT.get(marker) == "", f"completion marker written: {marker}", failures)
    _check("STALE" not in TEXT.values(), "the stale marker was removed", failures)
    data_writes = [k for k in WRITE_LOG if "/train/" in k or "/test/" in k]
    _check(bool(data_writes) and WRITE_LOG[-1] == marker,
           "the marker is the LAST thing written", failures)

    published = sorted(k for k in STORE if k.startswith(_key(root) + "/train/")
                       or k.startswith(_key(root) + "/test/"))
    print("  published tables:")
    for k in published:
        print(f"    {k[len(root):]}  ({STORE[k].count()} rows)")
    if opts.mode == "training":
        labels = window_labels(opts.window_start, opts.window_end)
        expected = [_key(layout.nonfraud_data_path(root, "train", l)) for l in labels[:-1]]
        expected += [_key(layout.nonfraud_data_path(root, "test", labels[-1]))]
        expected += [_key(layout.fraud_data_path(root, "Phase1-v1.0", l[:10])) for l in labels]
        _check(sorted(expected) == published, "exactly the DE train/ + test/ layout", failures)
        _check(any(k.startswith(_key(root) + "/feature_store/training/feature_set=") for k in STORE),
               "the 12 feature groups are kept in feature_store/training/", failures)
    else:
        expected = [_key(layout.scoring_data_path(root, opts.batch_date))]
        _check(expected == published, "exactly test/batch_date=<d>/data", failures)
    summary_keys = [k for k in TEXT if k.endswith("summary.json")]
    _check(len(summary_keys) == 1, "one run summary written under _runs/", failures)


def check_failure_path(fe, opts, failures) -> None:
    """A window that did not finish: no marker, exit code 1."""
    from ts05_features import layout

    marker = _key(layout.batch_marker_path(OUTPUT_ROOT, opts.mode, opts.batch_date))
    TEXT[marker] = "STALE"
    first = fe.GENERATED_WINDOWS[0]["window_id"]
    real_check = fe.window_groups_complete
    fe.run_extraction = lambda *a, **k: []  # feature groups are already in the store
    fe.read_inputs = lambda: ()
    fe.select_records = lambda *a: a
    fe.window_groups_complete = lambda w, recs: (
        ["forced_failure"] if w["window_id"] == first else [])
    try:
        code = fe.main()
    finally:
        fe.window_groups_complete = real_check
    print(f"\nforced failure of {first}: exit code {code}")
    _check(code == 1, "exit code 1 when a window is not published", failures)
    _check(marker not in TEXT, "no completion marker after a failure (and the stale one is gone)",
           failures)


def compare_with_original(fe_new, fe_old, opts, failures) -> None:
    """Every published table must equal the original SageMaker job's, row for row."""
    from ts05_features import layout

    print("\ncomparison with the original SageMaker job:")
    old_root = _key(ORIGINAL_ROOT)
    pairs = []
    if opts.mode == "training":
        labels = window_labels(opts.window_start, opts.window_end)
        for i, label in enumerate(labels):
            split = "test" if i == len(labels) - 1 else "train"
            pairs.append((f"{old_root}/ml/non-fraud/time_window={label}/data",
                          _key(layout.nonfraud_data_path(OUTPUT_ROOT, split, label))))
            pairs.append((f"{old_root}/ml/fraud/Phase1-v1.0/dt={label[:10]}/data",
                          _key(layout.fraud_data_path(OUTPUT_ROOT, "Phase1-v1.0", label[:10]))))
    else:
        pairs.append((f"{old_root}/scoring/batch_date={opts.batch_date}/data",
                      _key(layout.scoring_data_path(OUTPUT_ROOT, opts.batch_date))))
    for old, new in pairs:
        if old not in STORE or new not in STORE:
            _check(False, f"both exist: {old} / {new}", failures)
            continue
        rows = STORE[new].count()
        same = _same_rows(STORE[old], STORE[new])
        _check(same, f"{_label(same)} ({rows} rows, {len(STORE[new].columns)} cols): "
                     f"{new[len(OUTPUT_ROOT):]}", failures)
        if not same or LAST_NEEDED_TOLERANCE:
            _diff_columns(STORE[old], STORE[new])
    # The 12 feature groups themselves, window by window.
    old_groups = sorted(k for k in STORE if k.startswith(f"{old_root}/groups/{opts.mode}/feature_set="))
    group_failures = 0
    for old in old_groups:
        new = _key(OUTPUT_ROOT) + "/feature_store/" + old[len(f"{old_root}/groups/"):]
        if new not in STORE:
            _check(False, f"feature group missing: {new}", failures)
            group_failures += 1
            continue
        if not _same_rows(STORE[old], STORE[new]):
            _check(False, f"feature group differs: {new[len(OUTPUT_ROOT):]}", failures)
            _diff_columns(STORE[old], STORE[new])
            group_failures += 1
        elif LAST_NEEDED_TOLERANCE:
            print(f"        identical within float rounding: {new[len(OUTPUT_ROOT):]}")
            _diff_columns(STORE[old], STORE[new])
    _check(bool(old_groups) and not group_failures,
           f"{len(old_groups) - group_failures} of {len(old_groups)} feature-group tables identical",
           failures)


def compare_original_with_itself(root_a: str, root_b: str, opts) -> list:
    """The original job run twice on the same data: which tables differ?

    A difference here is in the client's code itself (not in the EMR move):
    a computation whose result depends on Spark's task order.
    """
    print("\noriginal SageMaker job vs ITSELF (second run, same data):")
    a_root, b_root = _key(root_a), _key(root_b)
    differing = []
    for a_key in sorted(k for k in STORE if k.startswith(a_root + "/groups/" + opts.mode + "/feature_set=")
                        or k.startswith(a_root + "/ml/") or k.startswith(a_root + "/scoring/")):
        b_key = b_root + a_key[len(a_root):]
        if b_key not in STORE:
            continue
        same = _same_rows(STORE[a_key], STORE[b_key])
        if not same or LAST_NEEDED_TOLERANCE:
            kind = "within float rounding" if same else "BEYOND float rounding"
            print(f"  DIFFERS between two runs of the original ({kind}): {a_key[len(a_root):]}")
            _diff_columns(STORE[a_key], STORE[b_key])
            differing.append(a_key[len(a_root):])
    if not differing:
        print("  every table identical between the two original runs")
    return differing


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=None,
                        help="export the input tables and the published tables here "
                             "(parquet + a CSV copy of each), plus the _SUCCESS marker and summary")
    parser.add_argument("--export-all", action="store_true",
                        help="with --out: also export the intermediate tables (the 12 feature "
                             "groups, scratch tables, manifests)")
    parser.add_argument("--mode", choices=("training", "scoring"), default="training")
    parser.add_argument("--batch-date", default=None)
    parser.add_argument("--window-start", default="2024-08-01 00:00:00")
    parser.add_argument("--window-end", default="2024-09-12 00:00:00")  # 3 windows
    parser.add_argument("--original-sagemaker-dir", default=os.environ.get("TS05_ORIGINAL_SAGEMAKER_DIR"),
                        help="the client's sagemaker/ folder; runs the original job for comparison")
    parser.add_argument("--skip-failure-check", action="store_true")
    parser.add_argument("--limit", default=None, help="pass --limit to the EMR job (max records per input)")
    parser.add_argument("--run-control", action="store_true",
                        help="run with run_control enabled (the run control tables are an in-memory stand-in)")
    parser.add_argument("--repeat-original", action="store_true",
                        help="run the original job twice and report any table that differs "
                             "between its own two runs (non-determinism in the client's code)")
    opts = parser.parse_args()
    if opts.mode == "scoring" and not opts.batch_date:
        parser.error("--mode scoring needs --batch-date")

    # Python must read plain datetimes as EST from start-up (Windows reads TZ
    # only then), so restart this script with TZ set if it is not.
    if os.environ.get("TZ") != "EST5":
        import subprocess
        env = dict(os.environ, TZ="EST5")
        return subprocess.call([sys.executable, *sys.argv], env=env)

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.master("local[4]").appName("fe_local")
             .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
             .config("spark.sql.session.timeZone", "EST")
             .config("spark.driver.memory", "5g")
             .config("spark.executor.heartbeatInterval", "60s")
             .config("spark.network.timeout", "900s")
             # bounded driver bookkeeping, as conf/lineage/dev.yaml does on EMR
             .config("spark.sql.ui.retainedExecutions", "5")
             .config("spark.ui.retainedJobs", "20")
             .config("spark.ui.retainedStages", "20")
             .config("spark.ui.retainedTasks", "1000")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    install_memory_storage()
    inputs = make_inputs(spark)
    failures: list = []

    fe_old = None
    if opts.original_sagemaker_dir:
        # The client's original job, on the same data, into its own root.
        sys.path.insert(0, str(Path(opts.original_sagemaker_dir).resolve()))
        fe_old, old_code = _run_job("feature_engineering",
                                    params(opts.window_start, opts.window_end, emr=False),
                                    opts, inputs, spark)
        print(f"original SageMaker job exit code {old_code}")
        _check(old_code == 0, "original job exit code 0", failures)
        if opts.repeat_original:
            second_root = ORIGINAL_ROOT.rstrip("/") + "-run2/"
            _, code2 = _run_job("feature_engineering",
                                params(opts.window_start, opts.window_end, emr=False,
                                       original_root=second_root),
                                opts, inputs, spark)
            print(f"original SageMaker job (second run) exit code {code2}")
            compare_original_with_itself(ORIGINAL_ROOT, second_root, opts)

    from ts05_features import layout

    TEXT[_key(layout.batch_marker_path(OUTPUT_ROOT, opts.mode, opts.batch_date))] = "STALE"
    WRITE_LOG.clear()
    fe, code = _run_job("ts05_features.feature_engineering",
                        params(opts.window_start, opts.window_end), opts, inputs, spark)
    check_emr_run(fe, code, opts, spark, failures)
    if opts.run_control:
        check_run_control(failures, opts.limit)
    if fe_old is not None:
        compare_with_original(fe, fe_old, opts, failures)

    if opts.out:
        out = Path(opts.out)
        written = export_inputs(inputs, out)
        written += export(_key(OUTPUT_ROOT) + "/train/", out) + export(_key(OUTPUT_ROOT) + "/test/", out)
        if opts.export_all:
            for sub in ("feature_store", "_work"):
                written += export(f"{_key(OUTPUT_ROOT)}/{sub}/", out)
        # The completion marker and the run summary, as the job wrote them.
        for key, text in TEXT.items():
            if key.endswith("_SUCCESS") or key.endswith("summary.json"):
                rel = key.split("://", 1)[1].split("/", 1)[1]
                target = _long_path(out / rel)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(text, encoding="utf-8")
                written.append(rel)
        print(f"\nexported to {out.resolve()}:")
        for item in written:
            print(f"  {item}")

    if not opts.skip_failure_check:
        check_failure_path(fe, opts, failures)

    spark.stop()
    print("\nRESULT: " + ("PASS" if not failures else f"FAIL ({len(failures)}): {failures}"))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
