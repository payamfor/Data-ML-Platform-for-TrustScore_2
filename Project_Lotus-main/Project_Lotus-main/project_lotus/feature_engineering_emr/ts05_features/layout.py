"""Where the feature job writes, and which window goes where.

This module is the contract between the feature job (DE) and the ML pipeline
(DS). It holds no feature logic and touches no Spark; every function is a pure
path or list computation so the layout can be tested on its own.

    <output_root>/
    ├── train/
    │   ├── non-fraud/time_window=<label>/data/part-*.parquet
    │   └── _SUCCESS                         training batch complete (written LAST)
    ├── test/
    │   ├── non-fraud/time_window=<label>/data/part-*.parquet
    │   ├── fraud/<fraud_source>/dt=<YYYY-MM-DD>/data/part-*.parquet
    │   └── batch_date=<YYYY-MM-DD>/
    │       ├── data/part-*.parquet          nightly scoring features
    │       └── _SUCCESS                     scoring batch complete (written LAST)
    ├── scoring_data/dt=<YYYY-MM-DD>/        optional --scoring-output-root
    │   ├── data/part-*.parquet               batch-scoring features
    │   └── _SUCCESS                          inference trigger (written LAST)
    ├── feature_store/<mode>/feature_set=…/version=…/dt=…/   the 12 feature groups
    ├── _work/                               scratch tables (safe to expire)
    └── _runs/…/summary.json                 what each run wrote

Why only one ``_SUCCESS`` per batch: the DE team's EventBridge rules start the
training state machine on ANY key matching ``<train prefix>*_SUCCESS`` and the
scoring state machine on ANY key matching ``<test prefix>*_SUCCESS``
(accounts/data-sandbox/infra/trust_score_ml_v2_eventbridge.tf). Spark's own
per-directory ``_SUCCESS`` files would therefore start one training run per
window, and scoring runs on evaluation data. The job switches them off and
writes exactly one marker per batch, after everything else.

Readers on the DS side, for reference:
* training non-fraud  ``trust_score_05.ml.paths.training_nonfraud_candidates``
  -> ``<training_root>/non-fraud/time_window=…/data/``
* testing non-fraud   ``testing_nonfraud_candidates``
  -> ``<testing_root>/non-fraud/time_window=…/data/``
* fraud               ``fraud_source_root`` + ``datasets.discover_fraud_paths``
  -> every ``<testing_root>/fraud/<source>/dt=…/data/``
* scoring             ``inference.py --input <test prefix>batch_date=…/``
  (reads every parquet file under the prefix)
"""

from __future__ import annotations

from typing import List, Optional, Sequence

TRAIN_DIR = "train"
TEST_DIR = "test"
MARKER_NAME = "_SUCCESS"

TRAINING = "training"
SCORING = "scoring"


def _root(output_root: str) -> str:
    return str(output_root).rstrip("/")


def route_window(
    label: str, training_windows: Sequence[str], testing_windows: Sequence[str]
) -> Optional[str]:
    """``"train"``, ``"test"`` or ``None`` (computed but not published as non-fraud)."""
    if label in training_windows:
        return TRAIN_DIR
    if label in testing_windows:
        return TEST_DIR
    return None


def nonfraud_data_path(output_root: str, split: str, label: str) -> str:
    return f"{_root(output_root)}/{split}/non-fraud/time_window={label}/data"


def fraud_data_path(output_root: str, fraud_source: str, dt: str) -> str:
    return f"{_root(output_root)}/{TEST_DIR}/fraud/{fraud_source}/dt={dt}/data"


def scoring_batch_prefix(output_root: str, batch_date: str) -> str:
    return f"{_root(output_root)}/{TEST_DIR}/batch_date={batch_date}"


def scoring_data_path(output_root: str, batch_date: str) -> str:
    return f"{scoring_batch_prefix(output_root, batch_date)}/data"


def batch_scoring_data_path(scoring_output_root: str, batch_date: str) -> str:
    return f"{_root(scoring_output_root)}/dt={batch_date}/data"


def batch_scoring_marker_path(scoring_output_root: str, batch_date: str) -> str:
    return f"{_root(scoring_output_root)}/dt={batch_date}/{MARKER_NAME}"


def batch_marker_path(output_root: str, mode: str, batch_date: Optional[str] = None) -> str:
    """The one completion marker of a batch."""
    if mode == SCORING:
        if not batch_date:
            raise ValueError("scoring mode needs a batch date for its marker")
        return f"{scoring_batch_prefix(output_root, batch_date)}/{MARKER_NAME}"
    return f"{_root(output_root)}/{TRAIN_DIR}/{MARKER_NAME}"


def summary_path(
    output_root: str, mode: str, run_id: str, batch_date: Optional[str] = None
) -> str:
    """Outside train/ and test/, so writing it can never start a pipeline."""
    base = f"{_root(output_root)}/_runs/{mode}"
    if mode == SCORING and batch_date:
        base = f"{base}/batch_date={batch_date}"
    return f"{base}/run_id={run_id}/summary.json"


def check_window_lists(
    generated_labels: Sequence[str],
    training_windows: Sequence[str],
    testing_windows: Sequence[str],
) -> List[str]:
    """Configuration problems with the train/test window lists, as messages.

    Empty means the lists are usable. Checked before Spark starts, so a typo
    fails in seconds instead of after hours of feature computation.
    """
    errors: List[str] = []
    if not training_windows:
        errors.append("training_windows is empty; training mode would publish no training data")
    if not testing_windows:
        errors.append("testing_windows is empty; training mode would publish no evaluation data")
    overlap = sorted(set(training_windows) & set(testing_windows))
    if overlap:
        errors.append(f"windows in both training_windows and testing_windows: {overlap}")
    generated = set(generated_labels)
    unknown = sorted((set(training_windows) | set(testing_windows)) - generated)
    if unknown:
        errors.append(
            f"windows listed but not generated by window_start_ts/window_end_exclusive_ts/"
            f"window_size_days: {unknown}; generated windows are {sorted(generated)}"
        )
    return errors
