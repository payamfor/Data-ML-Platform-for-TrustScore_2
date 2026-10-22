# Lotus sandbox pipeline run handbook

## Before deploying

Apply the `accounts/data-sandbox/security` Terraform root before
`accounts/data-sandbox/infra`, reviewing each plan. The optional
Terraform-managed `daily_data_pipeline` cluster is disabled by default
(`create_persistent_emr_cluster = false`). Set that variable to `true` when
the maintenance/cleanup cluster is needed. When created, it uses one
On-Demand `m5.xlarge` primary, one On-Demand `m5.xlarge` core node, no task
nodes, step concurrency 1, and a one-hour idle termination policy. It is
sized for small cleanup jobs, not a 24g Gold driver. No cleanup steps are
automatically submitted by this Terraform resource.

The separate Step Functions-created Silver/Gold cluster remains enabled and
retains one On-Demand `m5.xlarge` primary, two On-Demand `r5.2xlarge` core
nodes, and two On-Demand `r5.2xlarge` task nodes. Its capacity is independent
of the small Terraform-managed cluster. YARN node labels constrain its
ApplicationMaster/driver to the core fleet; it also has a one-hour idle
termination policy.
Terraform validation does not check actual AWS
permissions, S3 artifacts, or job execution.

Required artifacts in S3:

- `s3://lotus-sandbox-bronze-landing-data/artifacts/silver/run_trust_score_silver.py`
  from `project_lotus/run_trust_score_silver.py`.
- `s3://lotus-sandbox-bronze-landing-data/artifacts/silver/silver_pipeline.zip`
  with the importable `silver_pipeline` package at the ZIP root.
- `run_gold.py`, `gold_pipeline.zip`, and `lotus_sandbox.yaml` under
  `s3://lotus-sandbox-gold-curated-data/pipeline-code/gold/auto/`.

The Gold source artifacts are **not in this checkout**. Supply and verify
them before an end-to-end run. Confirm EMR runtime S3, Glue, and Lake
Formation grants as well.

## Manual Gold capacity test

For a future manual Gold capacity test, first restore full-sized cluster
capacity and set `create_persistent_emr_cluster = true`; the current
maintenance-sized configuration cannot run the 24g Gold driver. Use the
`emr_cluster_id` output. The Terraform-managed cluster is separate from the
daily cluster registered by Silver. Gold's `ReadCluster` still uses the daily
S3 claim and does not automatically select the Terraform-managed cluster.

After verifying the V3 artifacts and the intended YAML, generate all five
steps using `accounts/data-sandbox/infra/build_gold_test_steps.ps1`:

```powershell
./build_gold_test_steps.ps1 -RunId <gold-test-run-id> -ArtifactRoot <verified-v3-s3-prefix> -ConfigUri <verified-job-yaml-s3-uri>
```

The emitted JSON uses the SAME explicit run ID for every stage, cluster deploy
mode, 24g driver heap and 4g overhead, and omits first-run flags for an
incremental test. Submit these steps with the Gold job runtime role. Use a
new run ID only if there is no unresolved Gold run; a failed staged run must
be recovered deliberately under its existing code/config contract.

Manual steps use `CANCEL_AND_WAIT`, so a failed stage cancels later queued
steps. They do not trigger Step Functions cleanup. The manual cluster stays
available for log inspection and has a one-hour idle termination fallback.
The cluster and task fleet retain `ignore_changes = all`: these fleet/config
defaults apply to a NEW cluster, not an already running one.

The repo Gold config seeds 24g/4g, but `gold_job_config.tf` deliberately
ignores subsequent source/etag changes. For AUTOMATED runs, update the live
`pipeline-code/gold/auto/job_config.json` memory flags and verified V3 artifact
URIs, then inspect `BuildArgs_prepare.Payload.args` before submission. The
manual helper supplies 24g/4g explicitly, even when its input config is older.

After the test, set `create_persistent_emr_cluster = false` when only daily
Step Functions clusters should be created. To use Spot task workers on new
transient clusters, set `emr_task_target_on_demand_capacity = 0` and
`emr_task_target_spot_capacity = 2`.

## Gold failure cleanup

Gold retries a failed EMR stage once, then enters `TerminateOnError` directly.
After EMR confirms termination, `MarkClusterTerminatedOnError` closes the
daily claim and `StageFailed` sends the failure notification. Cleanup and
notifications preserve the run context. Claim closure failure still reaches
the failure notification; notification failure still reaches the terminal
Fail state. Cleanup API calls have two additional retries, separate from the
single job retry. `TerminationFailed` reports exhausted cleanup retries and
ends with `GoldClusterTerminationFailed` rather than claiming successful cleanup.
Inspect execution history for these states and the original stage error.
The termination role only permits clusters tagged
`lotus-workflow=silver-gold-transient`; the manually created test cluster is
not automatically registered or tagged as a daily workflow cluster.
An execution stopped by an operator, or terminated by `States.Runtime`,
does not run the normal catch/cleanup path. The daily cluster's default
one-hour idle timeout remains its fallback. This is not a limit on active
Spark runtime. Step Functions EMR status polling can delay observation of
a failed step, so termination is not necessarily instantaneous.

Silver retries a failed job once with a fresh run ID. If the second job fails,
or its retry arguments cannot be built, the workflow records the failure and
terminates the shared cluster. Recording errors/timeouts also route to
termination. Any other Silver jobs on that cluster are affected; Gate 2 does
not release Gold while a required dataset is failed.

## Automatic flow

The landing bucket is `s3://lotus-sandbox-bronze-landing-data/`. The S3
EventBridge rule matches `.parquet` objects directly under these enabled
independent dated folders:

```text
bronze/account_changes_batch/YYYY-MM-DD/*.parquet
bronze/device_lookup_batch/YYYY-MM-DD/*.parquet
```

The first object for a dataset/date starts its own 240-second (four-minute) wait; later
objects do not restart it. Afterward the Bronze validator scans the full
folder. PASS starts that dataset's Silver execution. The first passing
Silver execution for the date creates the transient EMR cluster; other
Silver executions reuse it. Silver permits one retry with a new run ID.
Gate 2 releases Gold only when both enabled Silver datasets succeed. Gold
runs `prepare`, `accounts`, `customers`, `validate`, `publish` sequentially,
with one automatic retry per failed stage. The cluster terminates after
successful publish or exhausted Gold retry, and after an exhausted Silver
retry. The default one-hour idle timeout is a fallback if explicit cleanup
is not reached. An idle gap of more than an hour between datasets can
terminate the cluster before the next arrival. The S3 claim is not
automatically closed by EMR's idle policy; check its status and design
recovery before attempting to reuse a terminated cluster.

Tiny JSON objects under `control/bronze-arrival-claims/` provide the atomic
first-arrival claim. Objects under `control/pipeline-claims/cluster/` retain
the shared daily EMR cluster ID/status. There
is no orchestration Iceberg table and there are **no DynamoDB tables**. The
Bronze validation and Silver-to-Gold control tables remain Iceberg because
they hold actual gate results. A claimed arrival date is not retriggered by
later files.

## Config objects, parameters, and Spark submit

Terraform seeds these JSON objects once. Future applies deliberately ignore
content changes: inspect/edit the **live S3 object** for the next run, and
keep the reviewed value in source control.

| Job | Live JSON config | Repo seed |
| --- | --- | --- |
| Silver | `s3://lotus-sandbox-silver-conformed-data/pipeline-code/silver/job_config.json` | `accounts/data-sandbox/infra/silver_job_config.tf` |
| Gold | `s3://lotus-sandbox-gold-curated-data/pipeline-code/gold/auto/job_config.json` | `accounts/data-sandbox/infra/gold_job_config.json` |

The control Lambda reads the applicable JSON before each Silver attempt or
Gold stage, builds the complete `spark-submit` argument array, and the state
machine passes it to the EMR `command-runner.jar` step. The builder is
`project_lotus/lambdas/silver_gold_control/silver_gold_control.py`.

Silver's config has `application`, `python_package`, `artifact_root`,
`mode`, `spark_submit_flags`, `spark_confs`, and per-dataset `table`,
`quarantine_table`, `quarantine_table_location`. The workflow adds
`--dataset`, `--run-date`, `--run-id`, and `--bronze-path`. The application
is the existing `run_trust_score_silver.py`, importing
`silver_pipeline.runner`. Example for one account batch:

```text
spark-submit --master yarn --deploy-mode cluster \
  --py-files s3://lotus-sandbox-bronze-landing-data/artifacts/silver/silver_pipeline.zip \
  s3://lotus-sandbox-bronze-landing-data/artifacts/silver/run_trust_score_silver.py \
  --dataset account_changes_batch --mode publish --run-date 2026-08-16 \
  --run-id <unique-attempt-id> \
  --bronze-path s3://lotus-sandbox-bronze-landing-data/bronze/account_changes_batch/2026-08-16/ \
  --artifact-root s3://lotus-sandbox-silver-conformed-data/run-artifacts/ \
  --table glue_catalog.lotus_sandbox_silver_conformed.account_changes_batch \
  --quarantine-table glue_catalog.lotus_sandbox_silver_conformed.silver_quarantine_account_changes_batch \
  --quarantine-table-location s3://lotus-sandbox-silver-conformed-data/quarantine/iceberg/silver_quarantine_account_changes_batch/
```

Gold's JSON supplies `application`, `python_package`, `config` (the Gold
job YAML, distinct from this JSON), `job`, `staging_root`,
`spark_submit_flags`, `spark_confs`, `first_run_watermark_from`. The
workflow adds the common `--run-id` and each `--stage`. On a first run,
`prepare` alone gets `--first-run --watermark-from <configured timestamp>`.

```text
spark-submit --deploy-mode cluster [configured memory/cores and --conf flags] \
  --py-files s3://lotus-sandbox-gold-curated-data/pipeline-code/gold/auto/gold_pipeline.zip \
  s3://lotus-sandbox-gold-curated-data/pipeline-code/gold/auto/run_gold.py \
  --config s3://lotus-sandbox-gold-curated-data/pipeline-code/gold/auto/lotus_sandbox.yaml \
  --job lineage --run-id <gold-run-id> \
  --staging-root s3://lotus-sandbox-gold-curated-data/staging/lineage_auto/ \
  --stage prepare
```

## Running and checking

1. Verify job artifacts, live JSON configs, table grants, EMR network and
   roles. Land Parquet files in the enabled dated folders. Normal operation
   requires no manual state-machine start. A test object in a production
   date starts that date's four-minute clock.
2. Monitor `lotus-sandbox-bronze-arrival` per folder, then
   `lotus-sandbox-trust-score-bronze-silver`, then Gold and EMR steps.
   CloudWatch `/aws/vendedlogs/states/...` and Lambda logs show failures.
3. Inspect the JSON claim objects for arrival and cluster status
   (`creating` → `ready` → `terminated`) and query
   `control.silver_gold_control` for the two Silver outcomes. Gold's release
   object is at `s3://lotus-sandbox-silver-conformed-data/gold-release/run_date=YYYY-MM-DD/release.json`.

For a controlled folder replay, start `lotus-sandbox-bronze-arrival` manually
with a new, meaningful Step Functions execution name and this input:

```json
{
  "folder_path": "s3://lotus-sandbox-bronze-landing-data/bronze/account_changes_batch/2026-08-16/"
}
```

The state machine validates the exact bucket/dataset/date shape, uses its
execution name as the replay identity, skips the automatic four-minute wait,
and sends the folder to the same schema validator. The validator enumerates
every `.parquet` object below that prefix and Silver reads the complete folder.
A new execution name permits an intentional later replay; retrying the same
execution remains idempotent. Silver inserts unseen record IDs, skips exact
replays, and quarantines conflicting payloads for an existing record ID. It
does not overwrite or delete an existing Silver row. Do **not** delete arrival
claim objects or edit Iceberg state to rerun a date.

After a successful replay, Silver leaves the shared transient cluster running.
If that date's `release.json` already exists, S3 does not emit a second object-
creation event, so Gold must be started manually before the cluster's one-hour
idle timeout. Start `lotus-sandbox-trust-score-gold` with exactly one of these
inputs:

```json
{
  "run_date": "2026-08-16"
}
```

or:

```json
{
  "release_uri": "s3://lotus-sandbox-silver-conformed-data/gold-release/run_date=2026-08-16/release.json"
}
```

The Gold release gate reads the existing immutable marker, rechecks the current
Silver control state, creates a new Gold run ID, and requires the cluster claim
for the same date to still be `ready`. Gold then reuses that cluster and
terminates it after successful publish or after its failure/recovery path. Do
not supply both manual fields, do not delete or overwrite `release.json`, and do
not start Gold after the EMR idle policy has already terminated the cluster.
The normal EventBridge S3-event input remains unchanged.
