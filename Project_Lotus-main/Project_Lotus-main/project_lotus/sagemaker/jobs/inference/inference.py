"""SageMaker Processing entry point: nightly batch scoring and drift detection.

Run once per night against that night's feature batch:

    python inference.py --input s3://.../batch/batch_date=2026-09-29/ --batch-date 2026-09-29

What it does, in order:

1. **Finds the champion.** In-process (v2): the one named by
   ``champion/current.json``. Through the live endpoint (v2.2, ``--endpoint-name``
   or ``TS05_SCORING_ENDPOINT``): the endpoint is asked which registered champion
   it serves, and that champion's document (``champion/history/run_id=``) supplies
   the drift reference.
2. **Scores the batch.** In-process: the package's ``score_population``.
   Through the endpoint: ``InvokeEndpoint`` in ordered CSV chunks (<= 5 MB,
   ``--endpoint-rows-per-request`` rows), several in flight, every response
   checked for row count, row order and the champion that produced it; if a
   deployment lands mid-batch, the whole batch is scored again once.

3. **Writes the review queue**: the top ``--top-k`` customers by score
   (one row per customer, highest-scoring record), plus every scored record.
4. **Checks drift** against the reference ``train.py`` stored with the
   champion, using the package's own statistics in ``trust_score_05.ml.drift``:
   the anomaly-score PSI and queue growth (``score_drift``) and a PSI per input
   feature (``population_stability_index``, reference-quantile bins).
5. **Recommends retraining** when the score PSI crosses ``drift.score_psi`` from
   config, or when at least ``--feature-drift-share`` of the features drifted.
   The recommendation is data, not an action: it is written as
   ``_RETRAIN_RECOMMENDED.json`` next to the scores *and* under
   ``champion/retrain_requests/``, and ``retrain_recommended`` is in the final
   summary line. The orchestrator decides whether to launch ``train.py``.

A drifted night still exits 0 - the scores are valid output of the champion that
exists. Exit 2 means the batch or the champion is unusable (missing features, no
champion); exit 1 anything else.

The batch has no labels, so drift is measured on the whole batch. That differs
from the sweep's drift report, which drops the known-fraud rows first; here
there are none to drop.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import os
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

import sm_common  # noqa: F401 - puts PROGRAM_DIR on sys.path before the package import
from sm_common import (
    CHAMPION_DIRNAME,
    CHAMPION_FILE,
    EXIT_CONFIG,
    EXIT_FAILED,
    EXIT_OK,
    champion_history_prefix,
    configure,
    container_argv,
    current_champion_uri,
    read_parquet_any,
    record_failure,
    resolve_configs,
)
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from trust_score_05.common.io.s3 import (  # noqa: E402
    join_uri,
    read_json,
    s3_exists,
    write_json,
    write_marker,
    write_pandas,
)
from trust_score_05.common.io.serialization import BundleError, load_bundle  # noqa: E402
from trust_score_05.common.splitting import CUSTOMER_KEY_COLUMN  # noqa: E402
from trust_score_05.lineage.config import ConfigError  # noqa: E402
from trust_score_05.lineage.logging_utils import get_logger, log_banner, log_mapping  # noqa: E402
from trust_score_05.ml.config import MLConfig, load_ml_config  # noqa: E402
from trust_score_05.ml.drift import (  # noqa: E402
    DriftError,
    DriftThresholds,
    population_stability_index,
    score_drift,
)
from trust_score_05.ml.evaluation import (  # noqa: E402
    _CARRIED_COLUMNS,
    LABEL_COLUMN,
    SCORE_COLUMN,
    score_population,
)
LOGGER = get_logger("ts05.sagemaker.inference")

#: A feature whose null share moves by more than this is reported, whatever its
#: PSI says - PSI is computed on the non-null values and cannot see a feed that
#: stopped populating a column.
NULL_SHIFT_REPORT = 0.10


class ScoringInputError(ValueError):
    """The batch cannot be scored by this champion. Exits 2."""


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="inference.py",
        description="Trust Score 0.5 nightly batch scoring with drift detection.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input", required=True,
                        help="The night's feature batch: a parquet file or prefix, local or s3://.")
    parser.add_argument("--batch-date", default=None,
                        help="YYYY-MM-DD label for this batch. Default: today (EST).")
    parser.add_argument("--config", action="append", default=None,
                        help="ML config overlay(s). Default: conf/ml/base.yaml + conf/ml/sandbox.yaml.")
    parser.add_argument("--set", action="append", default=None, dest="overrides",
                        metavar="KEY=VALUE")
    parser.add_argument("--champion", default=None,
                        help="A champion.json to score with. Default: champion/current.json.")
    parser.add_argument("--output-prefix", default=None,
                        help="Default: <models_root>/version_id=<v>/scoring/batch_date=<date>/")
    parser.add_argument("--batch-output-root", default=None,
                        help="When set, write batch predictions and monitoring artifacts to separate model/version/date partitions under this S3 root.")
    parser.add_argument("--top-k", type=int, default=500,
                        help="Review-queue length: the customers the fraud team works per cycle.")
    parser.add_argument("--queue-level", choices=("customer", "record"), default="customer")
    parser.add_argument("--feature-psi-threshold", type=float, default=None,
                        help="PSI above which one feature counts as drifted. Default: drift.score_psi.")
    parser.add_argument("--feature-drift-share", type=float, default=0.20,
                        help="Recommend retraining when at least this share of features drifted.")
    parser.add_argument("--log-level", default="INFO",
                        choices=("DEBUG", "INFO", "WARNING", "ERROR"))

    parser.add_argument("--endpoint-name", default=os.environ.get("TS05_SCORING_ENDPOINT") or None,
                        help="Score through this live SageMaker endpoint instead of loading the "
                             "champion here. Default: $TS05_SCORING_ENDPOINT; unset = in-process.")
    parser.add_argument("--endpoint-rows-per-request", type=int,
                        default=int(os.environ.get("TS05_ENDPOINT_ROWS_PER_REQUEST", "2000")))
    parser.add_argument("--endpoint-concurrency", type=int,
                        default=int(os.environ.get("TS05_ENDPOINT_CONCURRENCY", "4")))

    return parser


# --------------------------------------------------------------------------
# steps
# --------------------------------------------------------------------------

def load_champion(cfg: MLConfig, uri: Optional[str]) -> Dict[str, Any]:
    target = uri or current_champion_uri(cfg)
    if not s3_exists(target):
        raise ConfigError(
            f"no champion at {target}. Run train.py (mode finalize or all) first, "
            "or pass --champion."
        )
    champion = read_json(target)
    for key in ("bundle_uri", "reference_scores_uri", "reference_features_uri", "features"):
        if key not in champion:
            raise ConfigError(f"{target} is not a champion document: no {key!r}")
    champion["_source"] = target
    return champion


def check_batch(frame: pd.DataFrame, features: Sequence[str]) -> Dict[str, Any]:
    """Refuse a batch the champion cannot score, and say exactly why.

    A missing feature is fatal rather than imputed: the imputation values in the
    bundle are for *missing values* in a column the model was trained on, not
    for a column the upstream feature job stopped producing, and filling a whole
    column with its training median would score every customer on a constant.
    """
    if frame.empty:
        raise ScoringInputError("the batch has no rows")
    if CUSTOMER_KEY_COLUMN not in frame.columns:
        raise ScoringInputError(f"the batch has no {CUSTOMER_KEY_COLUMN!r} column")
    missing = [name for name in features if name not in frame.columns]
    if missing:
        raise ScoringInputError(
            f"the batch is missing {len(missing)} of the champion's {len(features)} "
            f"features, first 10: {missing[:10]}"
        )
    return {
        "rows": int(len(frame)),
        "customers": int(frame[CUSTOMER_KEY_COLUMN].nunique()),
        "extra_columns": int(len(set(frame.columns) - set(features))),
    }


def score_batch(frame: pd.DataFrame, bundle: Any, champion: Dict[str, Any],
                batch_date: str) -> pd.DataFrame:
    scored = score_population(frame, bundle.preprocessor, bundle.model)
    return rank_scores(scored, champion, batch_date, champion.get("model", bundle.model_name))


def rank_scores(scored: pd.DataFrame, champion: Dict[str, Any], batch_date: str,
                model: Optional[str]) -> pd.DataFrame:
    scored["score_rank"] = scored[SCORE_COLUMN].rank(ascending=False, method="first").astype(int)
    scored["score_percentile"] = scored[SCORE_COLUMN].rank(pct=True)
    scored["batch_date"] = batch_date
    scored["champion_run_id"] = champion["run_id"]
    scored["model"] = model
    scored["hyperparam_id"] = champion.get("hyperparam_id", "")
    return scored.sort_values("score_rank").reset_index(drop=True)


def review_queue(scored: pd.DataFrame, top_k: int, level: str) -> pd.DataFrame:
    """The ``top_k`` to hand to the review team.

    At customer level each customer appears once, at their highest-scoring
    record: a customer with forty records must not take forty queue slots. That
    is the same reasoning as ``best_config.level: customer`` in config.
    """
    if level == "customer":
        best = scored.sort_values(SCORE_COLUMN, ascending=False)
        best = best.drop_duplicates(subset=[CUSTOMER_KEY_COLUMN], keep="first")
    else:
        best = scored.sort_values(SCORE_COLUMN, ascending=False)
    queue = best.head(int(top_k)).reset_index(drop=True)
    queue.insert(0, "queue_position", np.arange(1, len(queue) + 1))
    return queue

# --------------------------------------------------------------------------
# scoring through the live endpoint (v2.2)
# --------------------------------------------------------------------------

#: InvokeEndpoint (real-time) accepts at most 6 MB per request body; chunks are
#: cut below that, and a chunk that would exceed it is halved until it fits.
MAX_REQUEST_BYTES = 5 * 1024 * 1024

#: serve.py answers a request carrying this custom attribute with the champion
#: it serves instead of scores.
DESCRIBE_ATTRIBUTE = "ts05-describe"


class EndpointError(RuntimeError):
    """The endpoint answered, but not with what was asked. Exits 1."""


class ChampionChangedError(EndpointError):
    """A deployment switched the endpoint's champion while the batch was scored."""


class EndpointScorer:
    """Scores a frame through the live endpoint: ordered chunks, several in flight."""

    def __init__(self, endpoint_name: str, rows_per_request: int = 2000,
                 concurrency: int = 4, client: Any = None):
        if client is None:
            import boto3
            from botocore.config import Config

            # Real-time invocations time out after 60 s on the SageMaker side.
            client = boto3.client("sagemaker-runtime", config=Config(
                retries={"max_attempts": 10, "mode": "adaptive"},
                connect_timeout=10, read_timeout=70))
        self.endpoint_name = endpoint_name
        self.rows_per_request = max(1, int(rows_per_request))
        self.concurrency = max(1, int(concurrency))
        self.client = client

    def _invoke(self, body: bytes, content_type: str,
                attributes: Optional[str] = None) -> Dict[str, Any]:
        request = {"EndpointName": self.endpoint_name, "Body": body,
                   "ContentType": content_type, "Accept": "application/json"}
        if attributes:
            request["CustomAttributes"] = attributes
        response = self.client.invoke_endpoint(**request)
        return json.loads(response["Body"].read())

    def describe(self) -> Dict[str, Any]:
        """The champion the endpoint serves right now."""
        served = self._invoke(b"[]", "application/json", DESCRIBE_ATTRIBUTE)
        for key in ("champion_run_id", "features"):
            if key not in served:
                raise EndpointError(f"{self.endpoint_name} did not describe its champion (no {key!r}); "
                                    "is it running the v2.2 serve.py?")
        return served

    def chunks(self, frame: pd.DataFrame) -> List[Tuple[int, int, bytes]]:
        """``(start, stop, CSV body)`` in row order, each under MAX_REQUEST_BYTES."""
        out: List[Tuple[int, int, bytes]] = []
        start, size = 0, self.rows_per_request
        while start < len(frame):
            stop = min(len(frame), start + size)
            body = frame.iloc[start:stop].to_csv(index=False).encode("utf-8")
            if len(body) > MAX_REQUEST_BYTES and stop - start > 1:
                size = max(1, (stop - start) // 2)
                continue
            out.append((start, stop, body))
            start = stop
        return out

    def score(self, frame: pd.DataFrame, features: Sequence[str]) -> Tuple[np.ndarray, str]:
        """One score per row of ``frame``, in its order, and the champion that made them."""
        payload = frame.loc[:, [CUSTOMER_KEY_COLUMN] + list(features)]
        keys = payload[CUSTOMER_KEY_COLUMN].astype(str).to_numpy()
        chunks = self.chunks(payload)

        def call(chunk: Tuple[int, int, bytes]):
            start, stop, body = chunk
            result = self._invoke(body, "text/csv")
            predictions = result.get("predictions") or []
            if len(predictions) != stop - start:
                raise EndpointError(f"rows {start}-{stop}: sent {stop - start}, "
                                    f"got {len(predictions)} predictions")
            if [str(p[CUSTOMER_KEY_COLUMN]) for p in predictions] != list(keys[start:stop]):
                raise EndpointError(f"rows {start}-{stop}: predictions are not in request order")
            return start, stop, str(result.get("champion_run_id")), [p[SCORE_COLUMN] for p in predictions]

        scores = np.full(len(payload), np.nan)
        served_by = set()
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            for start, stop, run_id, values in pool.map(call, chunks):
                scores[start:stop] = np.asarray(values, dtype=float)
                served_by.add(run_id)
        if len(served_by) != 1:
            raise ChampionChangedError(f"the batch was scored by several champions: {sorted(served_by)}")
        LOGGER.info("scored %d rows through %s in %d request(s)",
                    len(payload), self.endpoint_name, len(chunks))
        return scores, served_by.pop()


def endpoint_scored_frame(frame: pd.DataFrame, scores: np.ndarray) -> pd.DataFrame:
    """The frame ``score_population`` returns, carrying the endpoint's scores."""
    carried = [name for name in _CARRIED_COLUMNS if name in frame.columns]
    scored = frame.loc[:, carried].reset_index(drop=True).copy()
    scored[SCORE_COLUMN] = scores
    if LABEL_COLUMN not in scored.columns:
        scored[LABEL_COLUMN] = 0
    scored[LABEL_COLUMN] = scored[LABEL_COLUMN].fillna(0).astype(float).astype(int)
    return scored


def score_in_process(cfg: MLConfig, args: argparse.Namespace, batch: pd.DataFrame,
                     batch_date: str):
    champion = load_champion(cfg, args.champion)
    bundle = load_bundle(champion["bundle_uri"])
    if list(bundle.features) != list(champion["features"]):
        raise ConfigError("the champion document and its bundle disagree on the feature list")
    batch_info = check_batch(batch, bundle.features)
    return champion, score_batch(batch, bundle, champion, batch_date), batch_info


def score_via_endpoint(cfg: MLConfig, args: argparse.Namespace, batch: pd.DataFrame,
                       batch_date: str):
    if args.champion:
        raise ConfigError("--champion and --endpoint-name are exclusive: the endpoint "
                          "decides which champion scores")
    scorer = EndpointScorer(args.endpoint_name, args.endpoint_rows_per_request,
                            args.endpoint_concurrency)
    for attempt in (1, 2):
        served = scorer.describe()
        champion = load_champion(cfg, join_uri(
            champion_history_prefix(cfg, str(served["champion_run_id"])), CHAMPION_FILE))
        if list(served["features"]) != list(champion["features"]):
            raise ConfigError(f"the endpoint's feature list differs from champion "
                              f"{champion['run_id']}'s document")
        batch_info = check_batch(batch, champion["features"])
        try:
            scores, run_id = scorer.score(batch, champion["features"])
            if run_id != str(champion["run_id"]):
                raise ChampionChangedError(f"described {champion['run_id']}, scored by {run_id}")
        except ChampionChangedError as exc:
            if attempt == 2:
                raise
            LOGGER.warning("%s - a deployment finished mid-batch; scoring the whole batch again", exc)
            continue
        champion["_scored_by"] = args.endpoint_name
        scored = rank_scores(endpoint_scored_frame(batch, scores), champion, batch_date,
                             champion.get("model") or served.get("model"))
        return champion, scored, batch_info
    raise AssertionError("unreachable")


def _finite(values: pd.Series) -> np.ndarray:
    array = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    return array[np.isfinite(array)]


def feature_drift(reference: pd.DataFrame, batch: pd.DataFrame, features: Sequence[str],
                  threshold: float) -> pd.DataFrame:
    """One row per feature: PSI on reference-quantile bins, plus null-share shift."""
    rows = []
    for name in features:
        ref_all = reference[name] if name in reference.columns else pd.Series(dtype=float)
        cur_all = batch[name]
        ref_null = float(ref_all.isna().mean()) if len(ref_all) else float("nan")
        cur_null = float(cur_all.isna().mean()) if len(cur_all) else float("nan")
        row: Dict[str, Any] = {
            "feature": name,
            "reference_null_share": ref_null,
            "batch_null_share": cur_null,
            "null_share_shift": abs(cur_null - ref_null),
        }
        try:
            index = population_stability_index(_finite(ref_all), _finite(cur_all))
        except DriftError as exc:
            row.update(psi=float("nan"), status="unevaluable", note=str(exc))
        else:
            drifted = index["psi"] > threshold
            row.update(
                psi=index["psi"],
                psi_max_bin_contribution=index["max_bin_contribution"],
                status="drifted" if drifted else "ok",
                note="",
            )
        row["null_shift_flag"] = bool(
            np.isfinite(row["null_share_shift"]) and row["null_share_shift"] > NULL_SHIFT_REPORT
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values("psi", ascending=False, na_position="last")


def drift_summary(cfg: MLConfig, champion: Dict[str, Any], scored: pd.DataFrame,
                  batch: pd.DataFrame, args: argparse.Namespace):
    thresholds = DriftThresholds.from_config(cfg)
    feature_threshold = (
        float(args.feature_psi_threshold)
        if args.feature_psi_threshold is not None
        else float(thresholds.psi)
    )
    reference_scores = read_parquet_any(champion["reference_scores_uri"])[SCORE_COLUMN]
    reference_features = read_parquet_any(champion["reference_features_uri"])

    score = score_drift(reference_scores.to_numpy(dtype=float),
                        scored[SCORE_COLUMN].to_numpy(dtype=float), thresholds=thresholds)
    per_feature = feature_drift(reference_features, batch, champion["features"],
                                feature_threshold)

    evaluated = per_feature.loc[per_feature["status"] != "unevaluable"]
    n_drifted = int((evaluated["status"] == "drifted").sum())
    share = n_drifted / len(evaluated) if len(evaluated) else 0.0
    reasons: List[str] = []
    if score["psi_drift_flag"]:
        reasons.append(f"score PSI {score['psi']:.3f} > {thresholds.psi}")
    if len(evaluated) and share >= float(args.feature_drift_share):
        reasons.append(
            f"{n_drifted}/{len(evaluated)} features drifted "
            f"({share:.0%} >= {float(args.feature_drift_share):.0%})"
        )
    report = {
        "thresholds": {**thresholds.as_dict(), "feature_psi_threshold": feature_threshold,
                       "feature_drift_share": float(args.feature_drift_share)},
        "score": score,
        "features": {
            "n_features": int(len(per_feature)),
            "n_evaluated": int(len(evaluated)),
            "n_drifted": n_drifted,
            "share_drifted": share,
            "n_null_shift": int(per_feature["null_shift_flag"].sum()),
            "top_drifted": per_feature.head(10)[["feature", "psi", "status"]]
            .to_dict(orient="records"),
        },
        "retrain_recommended": bool(reasons),
        "reasons": reasons,
    }
    return report, per_feature


def batch_output_prefixes(batch_output_root: str, champion: Dict[str, Any],
                          batch_date: str) -> Tuple[str, str]:
    """Build the required prediction and monitoring partitions for batch scoring."""
    model_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(champion.get("model") or "unknown"))
    version_id = re.sub(
        r"[^A-Za-z0-9_.-]+", "_", str(champion.get("models_version") or "unknown")
    )
    partitions = [f"model={model_name}", f"version={version_id}", f"dt={batch_date}"]
    return (
        join_uri(batch_output_root, "outputs", "predictions", *partitions),
        join_uri(batch_output_root, "outputs", "monitoring", *partitions),
    )


# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------

def run(args: argparse.Namespace) -> Dict[str, Any]:
    cfg = load_ml_config(config_paths=resolve_configs(args.config),
                         overrides=list(args.overrides or []))
    # Default: today in EST (fixed UTC-5), the zone every Trust Score timestamp
    # uses (PRD). The pipeline always passes --batch-date from the marker key.
    batch_date = args.batch_date or datetime.now(timezone(timedelta(hours=-5))).strftime("%Y-%m-%d")
    output = args.output_prefix or join_uri(
        cfg.versioned_models_root, "scoring", f"batch_date={batch_date}"
    )
    log_banner(LOGGER, f"nightly scoring | batch_date={batch_date}")

    batch = read_parquet_any(args.input)
    if args.endpoint_name:
        champion, scored, batch_info = score_via_endpoint(cfg, args, batch, batch_date)
    else:
        champion, scored, batch_info = score_in_process(cfg, args, batch, batch_date)

    monitoring_output = output
    if args.batch_output_root:
        output, monitoring_output = batch_output_prefixes(
            args.batch_output_root, champion, batch_date
        )

    log_mapping(LOGGER, "champion", {
        "run_id": champion["run_id"], "model": champion.get("model"),
        "hyperparam_id": champion.get("hyperparam_id"), "features": len(champion["features"]),
        "source": champion["_source"], "scored_by": args.endpoint_name or "in-process",
    })
    log_mapping(LOGGER, "batch", batch_info)

    queue = review_queue(scored, args.top_k, args.queue_level)
    report, per_feature = drift_summary(cfg, champion, scored, batch, args)

    write_pandas(scored, join_uri(output, "scores.parquet"))
    write_pandas(queue, join_uri(output, "review_queue.csv"))
    write_pandas(per_feature, join_uri(monitoring_output, "feature_drift.csv"))
    write_json(join_uri(monitoring_output, "drift_report.json"), {
        "batch_date": batch_date, "champion_run_id": champion["run_id"], **report,
    })

    summary = {
        "status": "SUCCESS",
        "batch_date": batch_date,
        "output_prefix": output,
        "monitoring_output_prefix": monitoring_output,
        "rows_scored": int(len(scored)),
        "customers_scored": batch_info["customers"],
        "queue_length": int(len(queue)),
        "champion_run_id": champion["run_id"],
        "model": champion.get("model"),
        "score_psi": report["score"]["psi"],
        "scored_by": args.endpoint_name or "in-process",
        "queue_growth_at_train_p99": report["score"]["queue_growth_at_train_p99"],
        "features_drifted": report["features"]["n_drifted"],
        "retrain_recommended": report["retrain_recommended"],
        "reasons": report["reasons"],
    }
    if report["retrain_recommended"]:
        request = {**summary, "requested_at_utc": datetime.now(timezone.utc).isoformat()}
        write_marker(monitoring_output, "RETRAIN_RECOMMENDED", request)
        write_json(join_uri(cfg.versioned_models_root, CHAMPION_DIRNAME, "retrain_requests",
                            f"batch_date={batch_date}", "request.json"), request)
        LOGGER.warning("retraining recommended: %s", "; ".join(report["reasons"]))
    write_marker(monitoring_output, "READY", summary)

    # Handed back through the processing job's output mapping: the scoring state
    # machine reads summary.json from there to decide whether to alert/retrain.
    # Created here rather than trusted to exist, because a missing summary is
    # indistinguishable, from the orchestrator's side, from a failed night.
    local_out = os.environ.get("TS05_PROCESSING_OUTPUT", "/opt/ml/processing/output")
    if os.path.isdir(local_out) or os.path.isdir(os.path.dirname(local_out)):
        os.makedirs(local_out, exist_ok=True)
        with open(os.path.join(local_out, "summary.json"), "w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, default=str)
    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(container_argv(argv))
    configure(args.log_level)
    try:
        summary = run(args)
        log_mapping(LOGGER, "scoring summary", summary)
        # One machine-readable line at the end, for whatever tails the log.
        print("TS05_SCORING_SUMMARY " + json.dumps(summary, default=str), flush=True)
        return EXIT_OK
    except (ConfigError, ScoringInputError, BundleError) as exc:
        LOGGER.error("cannot score: %s", exc)
        record_failure(f"cannot score: {exc}")
        return EXIT_CONFIG
    except Exception as exc:  # noqa: BLE001 - container boundary
        LOGGER.error("inference.py failed: %s", exc, exc_info=True)
        record_failure(f"{type(exc).__name__}: {exc}")
        return EXIT_FAILED


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
