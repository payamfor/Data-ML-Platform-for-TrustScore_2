"""Validate and claim automatic Bronze arrivals or manual folder replays."""

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError


CLAIM_BUCKET = os.environ["CLAIM_BUCKET"]
CLAIM_PREFIX = os.environ.get(
    "CLAIM_PREFIX", "control/bronze-arrival-claims"
).strip("/")
ENABLED_DATASETS = {
    value.strip()
    for value in os.environ.get("ENABLED_DATASETS", "").split(",")
    if value.strip()
}

s3 = boto3.client("s3")


def _manual_folder(folder_path):
    """Return the canonical folder and its dataset/date components.

    Manual executions deliberately accept only a complete dataset/date folder,
    never an individual object or a broader prefix.  This keeps a console-started
    execution inside the same landing contract as an EventBridge-triggered run.
    """
    if not isinstance(folder_path, str) or not folder_path.strip():
        raise ValueError("manual execution requires folder_path")

    parsed = urlparse(folder_path.strip())
    if parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError("folder_path must be an s3:// folder URI")
    if parsed.netloc != CLAIM_BUCKET:
        raise ValueError(
            f"folder_path must use the Bronze landing bucket {CLAIM_BUCKET!r}"
        )

    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) != 3 or parts[0] != "bronze":
        raise ValueError(
            "folder_path must be exactly "
            f"s3://{CLAIM_BUCKET}/bronze/<dataset>/YYYY-MM-DD/"
        )

    dataset, run_date = parts[1], parts[2]
    if dataset not in ENABLED_DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset}")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", run_date):
        raise ValueError("run_date in folder_path must be YYYY-MM-DD")

    return dataset, run_date, f"s3://{CLAIM_BUCKET}/bronze/{dataset}/{run_date}/"


def _automatic_folder(event):
    dataset = event["dataset_name"]
    run_date = event["run_date"]
    folder_path = event["folder_path"]

    if dataset not in ENABLED_DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset}")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", run_date):
        raise ValueError("run_date must be YYYY-MM-DD")

    expected_folder = f"s3://{CLAIM_BUCKET}/bronze/{dataset}/{run_date}/"
    if folder_path != expected_folder:
        raise ValueError(
            f"folder_path must be exactly {expected_folder!r}, got {folder_path!r}"
        )
    return dataset, run_date, folder_path


def _claim_key(trigger_type, dataset, run_date, reprocess_id):
    if trigger_type == "automatic":
        return f"{CLAIM_PREFIX}/{dataset}/{run_date}.json"

    # Use a digest in the key rather than an operator-controlled execution
    # name.  The original value remains in the body and downstream audit data.
    replay_digest = hashlib.sha256(reprocess_id.encode("utf-8")).hexdigest()[:32]
    return (
        f"{CLAIM_PREFIX}/manual/{dataset}/{run_date}/"
        f"{replay_digest}.json"
    )


def lambda_handler(event, context):
    trigger_type = str(event.get("trigger_type", "automatic")).lower()
    owner = event["owner"]
    if not owner:
        raise ValueError("owner is required")

    if trigger_type == "manual":
        dataset, run_date, folder_path = _manual_folder(event.get("folder_path"))
        reprocess_id = str(event.get("reprocess_id") or "").strip()
        if not reprocess_id:
            raise ValueError("manual execution requires reprocess_id")
        if len(reprocess_id) > 128:
            raise ValueError("reprocess_id must not exceed 128 characters")
    elif trigger_type == "automatic":
        dataset, run_date, folder_path = _automatic_folder(event)
        reprocess_id = ""
    else:
        raise ValueError("trigger_type must be 'automatic' or 'manual'")

    key = _claim_key(
        trigger_type, dataset, run_date, reprocess_id
    )
    body = {
        "dataset_name": dataset,
        "run_date": run_date,
        "folder_path": folder_path,
        "trigger_type": trigger_type,
        "reprocess_id": reprocess_id,
        "owner": owner,
        "claimed_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    try:
        s3.put_object(
            Bucket=CLAIM_BUCKET,
            Key=key,
            Body=json.dumps(body).encode("utf-8"),
            ContentType="application/json",
            IfNoneMatch="*",
        )
        return {
            "claimed": True,
            "claim_uri": f"s3://{CLAIM_BUCKET}/{key}",
            **body,
        }
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code not in (
            "PreconditionFailed",
            "ConditionalRequestConflict",
            "412",
            "409",
        ):
            raise

    # If Lambda returned no response after creating the object, Step Functions
    # retries with the same execution ID. Treat that retry as the owner rather
    # than incorrectly routing it to AlreadyScheduled.
    current = json.loads(
        s3.get_object(Bucket=CLAIM_BUCKET, Key=key)["Body"].read()
    )
    return {
        "claimed": current.get("owner") == owner,
        "claim_uri": f"s3://{CLAIM_BUCKET}/{key}",
        "dataset_name": current.get("dataset_name", dataset),
        "run_date": current.get("run_date", run_date),
        "folder_path": current.get("folder_path", folder_path),
        "trigger_type": current.get("trigger_type", trigger_type),
        "reprocess_id": current.get("reprocess_id", reprocess_id),
        "owner": current.get("owner"),
        "claimed_at_utc": current.get("claimed_at_utc"),
    }
