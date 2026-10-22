"""Small, pure helpers the Step Functions state machines call.

Step Functions cannot reformat a timestamp, do date arithmetic or pick a
``batch_date=`` segment out of an S3 key, so those three jobs live here, where
they are ordinary Python and unit-tested (``sagemaker/tests/test_pipeline_helper.py``).
Nothing here touches SageMaker: the state machines start the jobs themselves.

Actions (``event["action"]``):

``training_context``
    The run id and the three kinds of training job (prepare, N sweep slices,
    finalize), each with its job name and its hyperparameters exactly as
    ``train.py`` parses them. Records the start time for the retrain cooldown.

``scoring_context``
    From the S3 "Object Created" event of a batch's completion marker: the
    batch prefix to score, the batch date, the processing job name, the
    ``inference.py`` arguments, and where its ``summary.json`` will land.

``deploy_context``
    From the "SageMaker Model Package State Change" event of an Approved
    champion: the package ARN and version, and unique names for the SageMaker
    model and endpoint config the deploy state machine creates from it.


``retrain_gate``
    Whether a drift-triggered retrain may start now, given the cooldown.

Configuration comes from environment variables set by Terraform.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import re
from typing import Any, Dict, List, Optional

ALL_MODELS = ("kmeans,weighted_kmeans,hdbscan,isolation_forest,imf,copod,ecod,hbos,"
              "pca_reconstruction,autoencoder")
_BATCH_DATE = re.compile(r"(?:^|/)(?:batch_date|dt|date)=(\d{4}-\d{2}-\d{2})(?:/|$)")
_PACKAGE_VERSION = re.compile(r"/(\d+)$")
_JOB_NAME_UNSAFE = re.compile(r"[^a-zA-Z0-9-]+")
EPOCH = "1970-01-01T00:00:00+00:00"


# --------------------------------------------------------------------------
# small utilities
# --------------------------------------------------------------------------

def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _parse_time(value: Optional[str]) -> datetime:
    """``$$.Execution.StartTime`` ("2026-09-29T02:20:20.123Z") -> aware datetime."""
    if not value:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def job_name(*parts: str) -> str:
    """A SageMaker job name: [a-zA-Z0-9-], starting alphanumeric, <= 63 chars."""
    name = "-".join(_JOB_NAME_UNSAFE.sub("-", str(p)).strip("-") for p in parts if str(p))
    name = re.sub(r"-{2,}", "-", name).lower().strip("-")
    return name[:63].rstrip("-")


def _short_id(text: str) -> str:
    """Six hex characters that make two jobs started in the same second distinct."""
    return hashlib.sha1(str(text).encode()).hexdigest()[:6]


def slice_arms(total: int, slices: int) -> List[Dict[str, int]]:
    """``total`` arms in ``slices`` contiguous, near-equal, non-empty ranges.

    The same arithmetic as ``sagemaker/launch.py`` so a pipeline run and a manual
    launch cut the sweep identically.
    """
    slices = max(1, min(int(slices), int(total)))
    base, extra = divmod(int(total), slices)
    out, start = [], 0
    for index in range(slices):
        count = base + (1 if index < extra else 0)
        out.append({"arm_start": start, "arm_count": count})
        start += count
    return out


def _common_hyperparameters() -> Dict[str, str]:
    params = {"config": _env("CONFIG_FILES", "conf/ml/base.yaml,conf/ml/sandbox.yaml")}
    overrides = [o for o in json.loads(_env("OVERRIDES", "[]")) if str(o).strip()]
    if overrides:
        params["set"] = ";".join(str(o) for o in overrides)
    params["models"] = _env("MODELS", ALL_MODELS)
    return params


def _ssm():
    import boto3

    return boto3.client("ssm")


def _record_training_start(when: datetime) -> None:
    name = _env("LAST_TRAINING_PARAMETER")
    if name:
        _ssm().put_parameter(Name=name, Value=when.isoformat(), Type="String", Overwrite=True)


# --------------------------------------------------------------------------
# actions
# --------------------------------------------------------------------------

def training_context(event: Dict[str, Any]) -> Dict[str, Any]:
    started = _parse_time(event.get("start_time"))
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    prefix = _env("JOB_NAME_PREFIX", "ts05")
    stem = job_name(prefix, run_id, _short_id(event.get("execution_name", run_id)))
    common = _common_hyperparameters()

    models = [m for m in common["models"].split(",") if m.strip()]
    total_arms = len(models) * int(_env("ARMS_PER_MODEL", "63"))
    ranges = slice_arms(total_arms, int(_env("SLICE_COUNT", "6")))

    prepare = {"mode": "prepare", **common}
    if _env("ALLOW_FAILED_CHECKS", "false").lower() == "true":
        prepare["allow-failed-checks"] = "true"

    slices = [
        {
            "job_name": job_name(stem, f"s{index:02d}"),
            "hyperparameters": {**common, "mode": "sweep", "selection-run-id": run_id,
                                "arm-start": str(span["arm_start"]),
                                "arm-count": str(span["arm_count"])},
        }
        for index, span in enumerate(ranges, start=1)
    ]

    champion_root = _env("CHAMPION_ROOT_KEY").strip("/")
    champion_key = "/".join(p for p in (champion_root, "history", f"run_id={run_id}",
                                        "champion.json") if p)
    _record_training_start(started)
    
    return {
        "run_id": run_id,
        "total_arms": total_arms,
        "prepare": {"job_name": job_name(stem, "prep"), "hyperparameters": prepare},
        "slices": slices,
        # Finalize promotes the winning configuration automatically (writes
        # champion/current.json). MANUAL_PROMOTION=true holds it under
        # champion/history/ instead, for the PRD's sign-off (7.1.7 step 8);
        # `train.py --mode promote --run-id <run_id>` then publishes it.
        "finalize": {"job_name": job_name(stem, "fin"),
                     "hyperparameters": {**common, "mode": "finalize",
                                         "selection-run-id": run_id}},
        "champion_key": champion_key,
        "champion_uri": f"s3://{_env('OUTPUT_BUCKET')}/{champion_key}",
    }


def scoring_context(event: Dict[str, Any]) -> Dict[str, Any]:
    detail = event.get("detail") or {}
    bucket = (detail.get("bucket") or {}).get("name")
    key = (detail.get("object") or {}).get("key")
    if not bucket or not key:
        raise ValueError(f"not an S3 Object Created event: {json.dumps(event)[:500]}")

    batch_prefix = key.rsplit("/", 1)[0] + "/" if "/" in key else ""
    started = _parse_time(event.get("start_time"))
    match = _BATCH_DATE.search(batch_prefix)
    batch_date = match.group(1) if match else started.strftime("%Y-%m-%d")
    name = job_name(_env("JOB_NAME_PREFIX", "ts05"), "score", batch_date,
                    started.strftime("%H%M%S"), _short_id(event.get("execution_name", key)))

    arguments = ["--input", f"s3://{bucket}/{batch_prefix}", "--batch-date", batch_date,
                 "--top-k", _env("TOP_K", "500")]
    for path in _env("CONFIG_FILES", "conf/ml/base.yaml,conf/ml/sandbox.yaml").split(","):
        if path.strip():
            arguments += ["--config", path.strip()]
    for override in json.loads(_env("OVERRIDES", "[]")):
        if str(override).strip():
            arguments += ["--set", str(override)]

    out_bucket = _env("OUTPUT_BUCKET")
    out_prefix = _env("OUTPUT_PREFIX", "").strip("/")
    summary_prefix = "/".join(p for p in (out_prefix, "scoring-summaries",
                                          f"batch_date={batch_date}", name) if p)
    return {
        "batch_date": batch_date,
        "input_uri": f"s3://{bucket}/{batch_prefix}",
        "job_name": name,
        "container_arguments": arguments,
        "summary_s3_prefix": f"s3://{out_bucket}/{summary_prefix}/",
        "summary_bucket": out_bucket,
        "summary_key": f"{summary_prefix}/summary.json",
    }


def batch_scoring_context(event: Dict[str, Any]) -> Dict[str, Any]:
    """Build the standard scoring job with batch-specific output partitioning."""
    context = scoring_context(event)
    bucket = (event.get("detail") or {}).get("bucket", {}).get("name")
    context["container_arguments"].extend(["--batch-output-root", f"s3://{bucket}"])
    return context


def retrain_gate(event: Dict[str, Any]) -> Dict[str, Any]:
    """Allow a drift retrain only if no training started within the cooldown.

    Reads the time ``training_context`` recorded - every training run records
    it, whether new data or drift started it - so a retrain is not stacked on
    top of one that just happened for another reason.
    """
    now = _parse_time(event.get("start_time"))
    cooldown = timedelta(days=float(_env("RETRAIN_COOLDOWN_DAYS", "7")))
    name = _env("LAST_TRAINING_PARAMETER")
    last_text = EPOCH
    if name:
        try:
            last_text = _ssm().get_parameter(Name=name)["Parameter"]["Value"]
        except Exception as exc:  # noqa: BLE001 - a missing parameter means "never"
            if "ParameterNotFound" not in type(exc).__name__ + str(exc):
                raise
    last = _parse_time(last_text)
    allowed = now - last >= cooldown
    return {"allowed": bool(allowed), "last_training_start": last.isoformat(),
            "next_allowed_after": (last + cooldown).isoformat()}

def deploy_context(event: Dict[str, Any]) -> Dict[str, Any]:
    """Names for deploying one Approved model package version.

    The model and the endpoint config get a name unique to this execution
    (``ts05-live-v<version>-<id>``): both are immutable in SageMaker, and a
    re-deploy of the same version (a rollback, a re-approval) must not collide
    with the ones an earlier deployment created. They carry the job-name prefix
    the Step Functions role is scoped to.
    """
    detail = event.get("detail") or {}
    arn = detail.get("ModelPackageArn")
    if not arn:
        raise ValueError(f"not a model package event (no ModelPackageArn): {json.dumps(event)[:500]}")
    version = str(detail.get("ModelPackageVersion") or "")
    if not version:
        match = _PACKAGE_VERSION.search(arn)
        version = match.group(1) if match else "0"
    metadata = detail.get("CustomerMetadataProperties") or {}
    name = job_name(_env("JOB_NAME_PREFIX", "ts05"), "live", f"v{version}",
                    _short_id(event.get("execution_name", arn)))
    return {
        "model_package_arn": arn,
        "model_package_version": version,
        "champion_run_id": str(metadata.get("run_id", "unknown")),
        "model_name": name,
        "endpoint_config_name": name,
    }



ACTIONS = {
    "training_context": training_context,
    "scoring_context": scoring_context,
    "batch_scoring_context": batch_scoring_context,
    "deploy_context": deploy_context,
    "retrain_gate": retrain_gate,
}


def handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    action = event.get("action")
    if action not in ACTIONS:
        raise ValueError(f"unknown action {action!r}; expected one of {sorted(ACTIONS)}")
    return ACTIONS[action](event)
