"""Submit the feature engineering job to an EMR Serverless application.

    python scripts/submit_emr_serverless.py training                      [--dry-run] [--wait]
    python scripts/submit_emr_serverless.py scoring --batch-date 2026-10-01

The feature-engineering counterpart of
``project_lotus/sagemaker/scripts/submit_emr_serverless.py``, with the same
conventions:

* **Configs travel with the job** as ``--files`` and are passed to the driver by
  basename, so the driver never needs boto3 to read its own config. Two configs
  with the same basename are refused.
* **Iceberg** extension and runtime jar are always added: the inputs are the
  Gold/Silver Iceberg tables in the Glue catalog.
* **Cost ceiling** per run is ``spark.dynamicAllocation.maxExecutors`` - EMR
  Serverless bills per vCPU-second actually used.

Environment (or flags):
    TS05_EMRS_APPLICATION_ID   the EMR Serverless application (Spark, emr-7.x)
    TS05_EMRS_ROLE_ARN         the job execution role
    TS05_FE_ARTIFACT_ROOT      s3://.../ts05_features/  (what scripts/package.sh uploaded)
    TS05_EMRS_LOG_URI          optional, s3:// prefix for driver/executor logs
    TS05_RUN_ID                optional, forwarded to the driver (resume: set
                               resume_run_id in the config instead)
    AWS_REGION                 default ca-central-1

Default configs: ``conf/base.yaml`` + ``conf/<environment>.yaml`` under the
artifact root. Always read the ``--config`` lines of a ``--dry-run`` first.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Sequence

MODES = ("training", "scoring")
ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
ICEBERG_JAR = "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"
DEFAULT_REGION = "ca-central-1"
TERMINAL = ("SUCCESS", "FAILED", "CANCELLED")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _required(value: Optional[str], flag: str, env: str, dry_run: bool) -> str:
    if value:
        return value
    if dry_run:
        return f"<{env}>"
    raise SystemExit(f"{flag} (or {env}) is required")


def config_files(args: argparse.Namespace, root: str) -> List[str]:
    if args.config_files:
        return [c.strip() for c in args.config_files.split(",") if c.strip()]
    return [f"{root}/conf/base.yaml", f"{root}/conf/{args.environment}.yaml"]


def build_request(args: argparse.Namespace) -> Dict[str, Any]:
    if args.mode == "scoring" and not (args.batch_date and _DATE.match(args.batch_date)):
        raise SystemExit("scoring needs --batch-date YYYY-MM-DD")
    root = _required(args.artifact_root, "--artifact-root", "TS05_FE_ARTIFACT_ROOT",
                     args.dry_run).rstrip("/")

    files: List[str] = []
    config_args: List[str] = []
    seen: set = set()
    for cfg in config_files(args, root):
        if cfg.startswith(("s3://", "s3a://")):
            name = cfg.rsplit("/", 1)[-1]
            if name in seen:
                raise SystemExit(f"two config files are both named {name}; they would "
                                 "overwrite each other when localised. Rename one.")
            seen.add(name)
            files.append(cfg)
            config_args += ["--config", name]
        else:
            config_args += ["--config", cfg]

    conf: Dict[str, str] = {
        # EST wall-clock time, as every Trust Score job (the notebook sets it too).
        "spark.sql.session.timeZone": "EST",
        "spark.driver.cores": str(args.driver_cores),
        "spark.driver.memory": args.driver_memory,
        "spark.executor.cores": str(args.executor_cores),
        "spark.executor.memory": args.executor_memory,
        "spark.executor.disk": args.executor_disk,
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.maxExecutors": str(args.max_executors),
        "spark.task.maxFailures": "8",
        "spark.stage.maxConsecutiveAttempts": "8",
        "spark.sql.extensions": ICEBERG_EXTENSIONS,
        "spark.jars": ICEBERG_JAR,
        # No per-directory _SUCCESS (the job also sets this itself; here it is in
        # place before the session exists).
        "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
        # Python reads plain datetimes as EST on the driver and the executors
        # (PRD: every timestamp is EST). The job also pins this itself.
        "spark.emr-serverless.driverEnv.TZ": "EST5",
        "spark.executorEnv.TZ": "EST5",
    }
    run_id = args.run_id or os.environ.get("TS05_RUN_ID")
    if run_id:
        conf["spark.emr-serverless.driverEnv.TS05_RUN_ID"] = run_id
    for pair in args.spark_conf or []:
        key, _, value = pair.partition("=")
        conf[key.strip()] = value.strip()  # caller's settings win

    params = [f"--py-files {root}/ts05_features.zip,{root}/pydeps.zip"]
    if files:
        params.append("--files " + ",".join(files))
    params += [f"--conf {key}={value}" for key, value in conf.items()]

    job_args = config_args + ["--mode", args.mode]
    if args.mode == "scoring":
        job_args += ["--batch-date", args.batch_date]
    for flag, value in (("--start-date", args.start_date), ("--end-date", args.end_date),
                        ("--limit", args.limit)):
        if value is not None:
            job_args += [flag, str(value)]
    if run_id:
        job_args += ["--run-id", run_id]

    suffix = f".{args.batch_date}" if args.mode == "scoring" else ""
    request: Dict[str, Any] = {
        "applicationId": _required(args.application_id, "--application-id",
                                   "TS05_EMRS_APPLICATION_ID", args.dry_run),
        "executionRoleArn": _required(args.role_arn, "--role-arn", "TS05_EMRS_ROLE_ARN",
                                      args.dry_run),
        "name": f"ts05_features.{args.mode}{suffix}.{datetime.now(timezone.utc):%Y%m%dT%H%M%S}",
        "jobDriver": {"sparkSubmit": {
            "entryPoint": f"{root}/feature_engineering_job.py",
            "entryPointArguments": job_args,
            "sparkSubmitParameters": " ".join(params),
        }},
        "executionTimeoutMinutes": int(args.timeout_minutes),
        "tags": {"project": "trust-score-05", "environment": args.environment,
                 "job": f"feature-engineering-{args.mode}"},
    }
    if args.log_uri:
        request["configurationOverrides"] = {"monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": args.log_uri}}}
    return request


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("mode", choices=MODES)
    parser.add_argument("--batch-date", default=None, help="scoring: YYYY-MM-DD")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD; with --end-date, read only this range")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD, inclusive; if either date is missing the current date is read")
    parser.add_argument("--limit", type=int, default=None, help="max records per event input; omit for all")
    parser.add_argument("--application-id", default=os.environ.get("TS05_EMRS_APPLICATION_ID"))
    parser.add_argument("--role-arn", default=os.environ.get("TS05_EMRS_ROLE_ARN"))
    parser.add_argument("--artifact-root", default=os.environ.get("TS05_FE_ARTIFACT_ROOT"))
    parser.add_argument("--log-uri", default=os.environ.get("TS05_EMRS_LOG_URI"))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", DEFAULT_REGION))
    parser.add_argument("--environment", default="sandbox",
                        help="Which conf/<environment>.yaml the default configs name.")
    parser.add_argument("--config-files", default=None,
                        help="Comma-separated; overrides the base + <environment> default.")
    parser.add_argument("--run-id", default=None)
    # Sized after the notebook's cluster (18 executors x 18 GB); training walks
    # every window, so it gets the larger default ceiling.
    parser.add_argument("--driver-cores", type=int, default=4)
    parser.add_argument("--driver-memory", default="16g")
    parser.add_argument("--executor-cores", type=int, default=4)
    parser.add_argument("--executor-memory", default="18g")
    parser.add_argument("--executor-disk", default="200g",
                        help="Shuffle spill space per executor (the notebook's failures were "
                             "out-of-disk and lost shuffle blocks).")
    parser.add_argument("--max-executors", type=int, default=None,
                        help="The cost ceiling of one run. Default 20 (training), 8 (scoring).")
    parser.add_argument("--timeout-minutes", type=int, default=None,
                        help="Default 1440 (training), 240 (scoring).")
    parser.add_argument("--spark-conf", action="append", default=None, metavar="KEY=VALUE")
    parser.add_argument("--dry-run", action="store_true", help="Print the request; send nothing.")
    parser.add_argument("--wait", action="store_true",
                        help="Poll until the run ends; exit 1 unless it SUCCEEDED.")
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    args = build_parser().parse_args(argv)
    if args.max_executors is None:
        args.max_executors = 20 if args.mode == "training" else 8
    if args.timeout_minutes is None:
        args.timeout_minutes = 1440 if args.mode == "training" else 240
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    request = build_request(args)
    if args.dry_run:
        print(json.dumps(request, indent=2))
        return 0

    import boto3

    client = boto3.client("emr-serverless", region_name=args.region)
    response = client.start_job_run(**request)
    run_id = response["jobRunId"]
    print(f"started {request['name']} as job run {run_id}")
    if not args.wait:
        return 0
    while True:
        run = client.get_job_run(applicationId=request["applicationId"], jobRunId=run_id)["jobRun"]
        state = run["state"]
        if state in TERMINAL:
            break
        time.sleep(30)
    print(f"{run_id}: {state} {run.get('stateDetails', '')}")
    return 0 if state == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
