from __future__ import annotations

import importlib
from pathlib import Path
import sys

from pyspark.sql.types import StringType, StructField, StructType


def test_materialize_reads_empty_parquet_with_source_schema(spark, tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "feature_engineering_job.py",
            "--config",
            str(root / "conf" / "base.yaml"),
            "--config",
            str(root / "conf" / "sandbox.yaml"),
            "--mode",
            "training",
        ],
    )
    feature_engineering = importlib.import_module("ts05_features.feature_engineering")
    monkeypatch.setattr(feature_engineering, "spark", spark)

    schema = StructType([StructField("id", StringType(), nullable=True)])
    empty = spark.createDataFrame([], schema)
    result = feature_engineering.materialize(
        empty, "empty", "test-run", base_path=str(tmp_path), partition_by=["id"]
    )

    assert result.schema == schema
    assert result.count() == 0
