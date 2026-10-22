"""Run control: every run starts at Gold's latest successful watermark_to.

Plain Python over an in-memory store, no Spark. The Glue/Iceberg tables
themselves can only be checked on AWS.
"""

from datetime import datetime

import pytest

from ts05_features.params import ConfigError, check_params
from ts05_features.run_control import MemoryStore, NoGoldWatermarkError, RunControl

JOB, MODE = "ml_feature_engineering", "scoring"
T = lambda day, hour=0: datetime(2026, 10, day, hour)  # noqa: E731
GOLD = (T(1), T(8, 9))  # Gold's latest successful slice: (from, to)


def make(rows=None, gold=GOLD):
    store = MemoryStore(rows, gold)
    return store, RunControl(store, JOB, MODE)


def test_run_starts_where_gold_ended_not_at_its_slice_start():
    _, rc = make()
    plan = rc.plan(T(9, 12))
    assert plan["start"] == T(8, 9) and plan["end"] == T(9, 12)


def test_training_mode_plans_full_gold_watermark_range():
    store = MemoryStore(gold=GOLD)
    rc = RunControl(store, JOB, "training")
    plan = rc.plan(T(9, 12))
    assert plan["start"] == T(1) and plan["end"] == T(8, 9)
    assert "watermark_from to watermark_to" in plan["source"]


def test_training_mode_invalid_gold_watermark_range():
    store = MemoryStore(gold=(T(10), T(5)))
    rc = RunControl(store, JOB, "training")
    with pytest.raises(ConfigError, match="watermark_from .* must be before watermark_to"):
        rc.plan(T(12))


def test_training_mode_missing_watermark_from():
    store = MemoryStore(gold=(None, T(5)))
    rc = RunControl(store, JOB, "training")
    with pytest.raises(NoGoldWatermarkError, match="no watermark_from"):
        rc.plan(T(12))


def test_no_gold_success_is_a_config_error():
    _, rc = make(gold=None)
    with pytest.raises(NoGoldWatermarkError):
        rc.plan(T(9))
    assert issubclass(NoGoldWatermarkError, ConfigError)


def test_later_runs_still_start_at_gold_not_at_their_own_watermark():
    store, rc = make()
    rc.begin(T(8, 9), T(9, 12), False, T(9, 12))
    rc.finish("succeeded", 100, T(9, 13))
    _, rc2 = make(store.rows)
    assert rc2.plan(T(10))["start"] == T(8, 9)


def test_a_run_writes_one_row_that_is_updated_to_its_final_status():
    store, rc = make()
    run_id = rc.begin(T(8, 9), T(9, 12), True, T(9, 12))
    (running,) = store.rows
    assert (running["status"], running["slice_size"], running["finished_ts"]) == ("running", 0, None)
    rc.finish("succeeded", 1234, T(9, 13))
    (final,) = store.rows
    assert final["run_id"] == run_id
    assert (final["status"], final["slice_size"], final["finished_ts"]) == ("succeeded", 1234, T(9, 13))
    assert final["watermark_from"] == T(8, 9) and final["watermark_to"] == T(9, 12)
    assert final["limit_applied"] is True and final["model_type"] == MODE


def test_a_retry_is_a_new_row_with_model_type_retry():
    store, rc = make()
    rc.begin(T(8, 9), T(9, 12), False, T(9, 12))
    rc.finish("failed", 0, T(9, 13))
    retry = RunControl(store, JOB, MODE, retry=True)
    assert retry.plan(T(9, 15))["start"] == T(8, 9)
    retry.begin(T(8, 9), T(9, 15), False, T(9, 15))
    retry.finish("succeeded", 10, T(9, 16))
    failed, retried = store.rows
    assert (failed["model_type"], failed["status"]) == (MODE, "failed")
    assert (retried["model_type"], retried["status"]) == ("retry", "succeeded")


def test_run_control_settings_are_checked_only_when_enabled():
    base = {"output_root": "s3://b/p", "inputs": {k: "t" for k in (
        "lineage_table", "account_changes_table", "enstream_table", "hash_mapping_table")}}
    assert check_params({**base, "run_control": {"enabled": False}}, "scoring") == []
    errors = check_params({**base, "run_control": {"enabled": True}}, "scoring")
    assert any("run_control.table" in e for e in errors)
    ok = {"enabled": True, "table": "t", "job_name": "j", "gold_table": "g", "gold_job_name": "gj"}
    assert check_params({**base, "run_control": ok}, "scoring") == []
