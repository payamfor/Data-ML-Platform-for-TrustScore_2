"""Reading the job's configuration.

The job reads one or more config files, later files overriding earlier ones
key by key (``conf/base.yaml`` then ``conf/sandbox.yaml``), and uses their
``features`` section. YAML and JSON are both accepted, so the Terraform-rendered
``pipeline_config.json`` of the SageMaker version still works unchanged.

On EMR the files arrive through ``--files`` and are opened by basename from the
container's working directory, so reading them needs neither boto3 nor S3
access - the same convention as the DE team's ``submit_emr_serverless.py``.
"""

from __future__ import annotations

import copy
import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence

REQUIRED_INPUTS = ("lineage_table", "account_changes_table", "enstream_table", "hash_mapping_table")
DEFAULT_DATA_SPLIT = {"train_percentage": 80.0, "test_percentage": 20.0, "random_seed": 42}


class ConfigError(ValueError):
    """The configuration cannot be used. The job exits with code 2."""


def read_config_file(path: str) -> Dict[str, Any]:
    if not os.path.isfile(path):
        raise ConfigError(f"config file not found: {path}")
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    if path.lower().endswith(".json"):
        doc = json.loads(text)
    else:
        import yaml  # vendored in pydeps.zip on the cluster

        doc = yaml.safe_load(text)
    if doc is None:
        return {}
    if not isinstance(doc, dict):
        raise ConfigError(f"{path}: the top level must be a mapping")
    return doc


def deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """``overlay`` on top of ``base``; nested mappings merge, everything else replaces."""
    out = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def load_params(paths: Sequence[str]) -> Dict[str, Any]:
    """The merged ``features`` section of ``paths``."""
    if not paths:
        raise ConfigError("--config is required (or set TS05_FEATURES_CONFIG)")
    merged: Dict[str, Any] = {}
    for path in paths:
        merged = deep_merge(merged, read_config_file(path))
    if "features" not in merged:
        raise ConfigError(f"none of {list(paths)} has a 'features' section")
    return merged["features"]


def _parse_selection_date(value: Any, name: str) -> datetime:
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ConfigError(f"{name} must be YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS', got {text!r}")


def resolve_selection(start_date: Any, end_date: Any, limit: Any, today: datetime) -> Dict[str, Any]:
    """Which input records to read: ``[start, end_exclusive)`` and an optional row limit.

    Both dates given: that range (a date-only ``end_date`` includes the whole day).
    Either missing: the current day, ``today``'s date. Limit empty/None/0: no limit.
    """
    blank = lambda v: v is None or str(v).strip() == ""  # noqa: E731
    if blank(start_date) or blank(end_date):
        start = today.replace(hour=0, minute=0, second=0, microsecond=0)
        end_exclusive = start + timedelta(days=1)
        source = "current date"
    else:
        start = _parse_selection_date(start_date, "start_date")
        end = _parse_selection_date(end_date, "end_date")
        end_exclusive = end + timedelta(days=1) if len(str(end_date).strip()) == 10 else end + timedelta(seconds=1)
        if end_exclusive <= start:
            raise ConfigError(f"end_date {end_date} is before start_date {start_date}")
        source = "start_date/end_date"
    if blank(limit):
        row_limit = None
    else:
        try:
            row_limit = int(str(limit).strip())
        except ValueError:
            raise ConfigError(f"limit must be a whole number, got {limit!r}") from None
        if row_limit < 0:
            raise ConfigError("limit must not be negative")
        row_limit = row_limit or None
    return {"start": start, "end_exclusive": end_exclusive, "limit": row_limit, "source": source,
            "watermark": False}


def check_params(params: Dict[str, Any], mode: str) -> List[str]:
    """Missing or malformed settings, as messages. Empty means usable."""
    errors: List[str] = []
    if not str(params.get("output_root") or "").strip():
        errors.append("output_root is required")
    inputs = params.get("inputs") or {}
    for key in REQUIRED_INPUTS:
        if not str(inputs.get(key) or "").strip():
            errors.append(f"inputs.{key} is required")
    run_control = params.get("run_control") or {}
    if run_control.get("enabled"):
        for key in ("table", "job_name", "gold_table", "gold_job_name"):
            if not str(run_control.get(key) or "").strip():
                errors.append(f"run_control.{key} is required when run_control.enabled is true")
    data_split = params.get("data_split")
    if data_split is not None:
        if not isinstance(data_split, dict):
            errors.append("data_split must be a mapping")
        else:
            train_pct = data_split.get("train_percentage", 80)
            test_pct = data_split.get("test_percentage", 20)
            seed = data_split.get("random_seed", 42)
            valid_pcts = True
            try:
                train_val = float(train_pct)
                test_val = float(test_pct)
            except (ValueError, TypeError):
                errors.append("data_split.train_percentage and test_percentage must be numbers")
                valid_pcts = False

            if valid_pcts:
                if train_val <= 0:
                    errors.append("data_split.train_percentage must be positive")
                if test_val <= 0:
                    errors.append("data_split.test_percentage must be positive")
                if round(train_val + test_val, 6) != 100.0:
                    errors.append("data_split percentages must add up to 100")

            if seed is not None:
                try:
                    int(seed)
                except (ValueError, TypeError):
                    errors.append("data_split.random_seed must be an integer")
    return errors


def get_data_split(params: Dict[str, Any]) -> Dict[str, Any]:
    """Return validated data_split configuration with defaults."""
    split = params.get("data_split") or {}
    train_pct = float(split.get("train_percentage", DEFAULT_DATA_SPLIT["train_percentage"]))
    test_pct = float(split.get("test_percentage", DEFAULT_DATA_SPLIT["test_percentage"]))
    seed = int(split.get("random_seed", DEFAULT_DATA_SPLIT["random_seed"]))
    return {
        "train_percentage": train_pct,
        "test_percentage": test_pct,
        "random_seed": seed,
    }
