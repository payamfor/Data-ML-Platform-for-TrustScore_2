"""The output layout and the config loader - the only new logic in the package.

No Spark: these are pure path and dict computations.
"""

from __future__ import annotations

import fnmatch
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ts05_features import layout  # noqa: E402
from ts05_features.params import ConfigError, check_params, deep_merge, load_params  # noqa: E402

ROOT = "s3://lotus-sandbox-sagemaker-data/mock-data/version1/"
CONF = Path(__file__).resolve().parent.parent / "conf"

# The DE team's EventBridge patterns (trust_score_ml_v2_eventbridge.tf), with
# the default prefixes of trust_score_ml_v2_variables.tf.
TRAINING_TRIGGER = "mock-data/version1/train/*_SUCCESS"
SCORING_TRIGGER = "mock-data/version1/test/*_SUCCESS"


def _key(uri: str) -> str:
    return uri.split("://", 1)[1].split("/", 1)[1]


# --------------------------------------------------------------------------
# paths - must match what trust_score_05.ml reads
# --------------------------------------------------------------------------

def test_nonfraud_paths_match_the_ml_readers():
    assert layout.nonfraud_data_path(ROOT, "train", "2026-04-01_00-00-00") == (
        "s3://lotus-sandbox-sagemaker-data/mock-data/version1/train/non-fraud/"
        "time_window=2026-04-01_00-00-00/data"
    )
    assert layout.nonfraud_data_path(ROOT, "test", "2026-06-01_00-00-00") == (
        "s3://lotus-sandbox-sagemaker-data/mock-data/version1/test/non-fraud/"
        "time_window=2026-06-01_00-00-00/data"
    )


def test_fraud_path_matches_the_ml_reader():
    assert layout.fraud_data_path(ROOT, "NBC-v1.0", "2026-06-01") == (
        "s3://lotus-sandbox-sagemaker-data/mock-data/version1/test/fraud/NBC-v1.0/dt=2026-06-01/data"
    )


def test_scoring_path_is_a_batch_folder_under_test():
    assert layout.scoring_data_path(ROOT, "2026-10-01") == (
        "s3://lotus-sandbox-sagemaker-data/mock-data/version1/test/batch_date=2026-10-01/data"
    )


# --------------------------------------------------------------------------
# markers - exactly one per batch, and only it starts a pipeline
# --------------------------------------------------------------------------

def test_training_marker_starts_training_and_not_scoring():
    key = _key(layout.batch_marker_path(ROOT, layout.TRAINING))
    assert key == "mock-data/version1/train/_SUCCESS"
    assert fnmatch.fnmatchcase(key, TRAINING_TRIGGER)
    assert not fnmatch.fnmatchcase(key, SCORING_TRIGGER)


def test_scoring_marker_starts_scoring_and_names_the_batch():
    key = _key(layout.batch_marker_path(ROOT, layout.SCORING, "2026-10-01"))
    assert key == "mock-data/version1/test/batch_date=2026-10-01/_SUCCESS"
    assert fnmatch.fnmatchcase(key, SCORING_TRIGGER)
    assert not fnmatch.fnmatchcase(key, TRAINING_TRIGGER)


def test_scoring_marker_needs_a_batch_date():
    with pytest.raises(ValueError):
        layout.batch_marker_path(ROOT, layout.SCORING)


def test_summary_never_matches_a_trigger():
    for uri in (layout.summary_path(ROOT, layout.TRAINING, "r1"),
                layout.summary_path(ROOT, layout.SCORING, "r1", "2026-10-01")):
        key = _key(uri)
        assert not fnmatch.fnmatchcase(key, TRAINING_TRIGGER)
        assert not fnmatch.fnmatchcase(key, SCORING_TRIGGER)


# --------------------------------------------------------------------------
# routing
# --------------------------------------------------------------------------

def test_route_window():
    assert layout.route_window("a", ["a"], ["b"]) == "train"
    assert layout.route_window("b", ["a"], ["b"]) == "test"
    assert layout.route_window("c", ["a"], ["b"]) is None


def test_window_lists_valid():
    assert layout.check_window_lists(["a", "b", "c"], ["a"], ["b"]) == []


def test_window_lists_overlap_unknown_and_empty_are_reported():
    errors = layout.check_window_lists(["a", "b"], ["a", "x"], ["a"])
    assert any("both" in e and "'a'" in e for e in errors)
    assert any("not generated" in e and "'x'" in e for e in errors)
    assert len(layout.check_window_lists(["a"], [], [])) == 2


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------

def test_deep_merge_overrides_key_by_key():
    base = {"features": {"a": 1, "inputs": {"x": "1", "y": "2"}}}
    over = {"features": {"inputs": {"y": "3"}, "b": 2}}
    assert deep_merge(base, over) == {"features": {"a": 1, "b": 2, "inputs": {"x": "1", "y": "3"}}}


def test_shipped_configs_load_and_validate():
    params = load_params([str(CONF / "base.yaml"), str(CONF / "sandbox.yaml")])
    assert check_params(params, "training") == []
    assert check_params(params, "scoring") == []
    assert params["output_root"].startswith("s3://")
    assert params["data_split"]["train_percentage"] == 80
    assert params["data_split"]["test_percentage"] == 20
    assert params["data_split"]["random_seed"] == 42


def test_data_split_validation():
    from ts05_features.params import get_data_split

    base = {"output_root": "s3://b/p", "inputs": {k: "t" for k in (
        "lineage_table", "account_changes_table", "enstream_table", "hash_mapping_table")}}

    # Default values when data_split not provided
    default_split = get_data_split(base)
    assert default_split == {"train_percentage": 80.0, "test_percentage": 20.0, "random_seed": 42}

    # Custom valid values
    custom = {**base, "data_split": {"train_percentage": 70, "test_percentage": 30, "random_seed": 123}}
    assert check_params(custom, "training") == []
    assert get_data_split(custom) == {"train_percentage": 70.0, "test_percentage": 30.0, "random_seed": 123}

    # Invalid: non-positive percentage
    assert any("positive" in e for e in check_params({**base, "data_split": {"train_percentage": 0, "test_percentage": 100}}, "training"))
    assert any("positive" in e for e in check_params({**base, "data_split": {"train_percentage": -10, "test_percentage": 110}}, "training"))

    # Invalid: percentages don't sum to 100
    assert any("add up to 100" in e for e in check_params({**base, "data_split": {"train_percentage": 75, "test_percentage": 20}}, "training"))

    # Invalid: non-numeric percentage
    assert any("numbers" in e for e in check_params({**base, "data_split": {"train_percentage": "eighty", "test_percentage": 20}}, "training"))

    # Invalid: non-integer seed
    assert any("random_seed must be an integer" in e for e in check_params({**base, "data_split": {"train_percentage": 80, "test_percentage": 20, "random_seed": "abc"}}, "training"))

    # Invalid: non-dict data_split
    assert any("data_split must be a mapping" in e for e in check_params({**base, "data_split": [80, 20]}, "training"))


def test_json_pipeline_config_still_loads(tmp_path):
    path = tmp_path / "pipeline_config.json"
    path.write_text(json.dumps({"features": {"output_root": "s3://b/p/"}}), encoding="utf-8")
    assert load_params([str(path)]) == {"output_root": "s3://b/p/"}


def test_missing_settings_are_reported():
    errors = check_params({}, "training")
    assert "output_root is required" in errors
    assert "inputs.lineage_table is required" in errors


def test_missing_file_and_section_raise(tmp_path):
    with pytest.raises(ConfigError):
        load_params([str(tmp_path / "nope.yaml")])
    empty = tmp_path / "empty.yaml"
    empty.write_text("other: 1\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_params([str(empty)])
    with pytest.raises(ConfigError):
        load_params([])


# ---- start_date / end_date / limit ------------------------------------------

def test_selection_range_end_is_inclusive_and_limit_applies():
    from datetime import datetime
    from ts05_features.params import resolve_selection

    sel = resolve_selection("2024-08-01", "2024-08-03", "50", datetime(2026, 10, 8, 15, 0))
    assert sel["start"] == datetime(2024, 8, 1) and sel["end_exclusive"] == datetime(2024, 8, 4)
    assert sel["limit"] == 50


def test_selection_falls_back_to_current_date_when_either_missing():
    from datetime import datetime
    from ts05_features.params import resolve_selection

    today = datetime(2026, 10, 8, 15, 0)
    for start, end in ((None, "2024-08-03"), ("2024-08-01", None), ("", ""), (None, None)):
        sel = resolve_selection(start, end, None, today)
        assert sel["start"] == datetime(2026, 10, 8) and sel["end_exclusive"] == datetime(2026, 10, 9)
        assert sel["limit"] is None


def test_selection_rejects_bad_input():
    from datetime import datetime
    import pytest
    from ts05_features.params import ConfigError, resolve_selection

    now = datetime(2026, 10, 8)
    for args in (("2024-08-05", "2024-08-01", None), ("08/01/2024", "2024-08-02", None),
                 ("2024-08-01", "2024-08-02", "abc"), ("2024-08-01", "2024-08-02", "-1")):
        with pytest.raises(ConfigError):
            resolve_selection(*args, now)
