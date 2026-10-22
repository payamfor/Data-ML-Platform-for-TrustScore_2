# Gold failure recovery

Recovery is a failure-only branch. It is not a sixth normal Gold stage and it
does not change any Gold transformation or capacity YAML.

## State-machine contract

For `prepare`, `accounts`, `customers`, or `validate` failure after the normal
retry:

1. Run `recover_gold_run.py` on the existing healthy EMR cluster.
2. Verify the failed Spark step is terminal and every Gold table still matches
   its pinned pre-run snapshot.
3. Append a terminal `failed` row to `gold_run_control`.
4. Terminate the cluster and send the normal Gold failure alert.

If `BuildArgs_prepare` fails, no Gold application or reservation exists, so the
workflow terminates and alerts without running the finalizer. If argument
construction fails for `accounts`, `customers`, `validate`, or `publish`, the
workflow uses a separate `argument_build` finalizer path. That path proves the
target EMR step was never submitted, requires the preceding stage marker,
verifies all five Gold heads are still pinned, and only then closes the running
reservation.

For `publish` failure after the normal retry:

1. Confirm the failed Spark step is terminal and no longer writing.
2. Run `rollback_gold_publish.py`.
3. Read the frozen run state, publish journals and current Iceberg snapshots.
4. Roll back attributable changes in reverse publication order.
5. Verify all five Gold tables match their pinned pre-run snapshots.
6. Only after rollback exits successfully, run `recover_gold_run.py` with the
   same `run_id`.
7. Append a terminal `failed` row, terminate the cluster and alert.

If rollback, verification, or finalization fails, the finalizer is not run (or
does not append), the reservation remains unresolved, the cluster is terminated
when possible, and the Gold SNS topic receives a P1 recovery-required alert.

## Separation of responsibility

| Program | Allowed mutation | Forbidden mutation |
| --- | --- | --- |
| `rollback_gold_publish.py` | Restore Gold table snapshots and write rollback audit files under the run staging prefix | Any change to `gold_run_control` |
| `recover_gold_run.py` | Append the terminal `failed` control row after re-verification | Rollback or modification of a Gold business table |

The finalizer copies `job_name`, `run_id`, `watermark_from`, `watermark_to`,
`slice_size`, and `started_ts` from the original running reservation. It changes
only `status` and `finished_ts`. Since `RunControl.last_watermark()` reads only
successful rows, the failed window is not consumed. A later execution must use
a new run ID and will start from the last successful watermark.

## Safety checks

Automatic publish rollback exits non-zero unless:

- the exact `gold-publish-<run_id>` EMR step is terminal-failed and no matching
  attempt remains active;
- when recovery uses the original active cluster, YARN reports no active normal
  `gold_account_customer_map.*` application (the recovery application itself is
  excluded);
- the cluster is the active tagged Silver/Gold workflow cluster;
- the run has one unresolved control reservation;
- each changed snapshot equals that run's completed publish journal head;
- every pinned snapshot still exists and is an ancestor of the current head;
- no current head is unexpected; and
- final verification finds all five heads at their exact pins.

A `writing` journal with a changed head is not attributable and fails closed.
Likewise, a changed table with no retained pre-run snapshot is not automatically
recoverable. There is no force-rollback switch.

The rollback and finalizer each acquire the staging-root recovery lease. The
finalizer rechecks the rollback journal and table heads after acquiring its own
lease. The lease serializes these tools, but it is not global fencing for an
external Athena or Spark writer; Gold must remain single-writer operationally.

Both entrypoints also compare the current Gold ZIP identity and fully resolved
configuration identity with the frozen run manifest. A mismatch stops recovery
before table rollback or control finalization.

## Failed cluster and recovery cluster

Recovery commands take two explicit IDs:

- `--failed-cluster-id` identifies the cluster that ran (or was about to run)
  the failed Gold stage.
- `--recovery-cluster-id` identifies the active tagged cluster on which the
  recovery Spark application is submitted.

The automatic Step Functions branch supplies the original cluster for both.
For reviewed recovery after cluster loss, create suitable replacement compute,
submit the command there, retain the original failed cluster ID, and use the
replacement as the recovery cluster ID. The command permits this only when the
original cluster is `TERMINATED` or `TERMINATED_WITH_ERRORS`; it then checks the
failed step (or proves the argument-failure step was never submitted) on the
original cluster. The replacement cluster must be active and tagged
`lotus-workflow=silver-gold-transient`.

## Artifacts and configuration

Terraform packages and uploads:

| Artifact | S3 key |
| --- | --- |
| Recovery Python package | `pipeline-code/gold/v3/recovery/v1/gold_recovery_tools.zip` |
| Publish rollback entrypoint | `pipeline-code/gold/v3/recovery/v1/rollback_gold_publish.py` |
| Control finalizer entrypoint | `pipeline-code/gold/v3/recovery/v1/recover_gold_run.py` |

`gold_job_config.json` contains the nested `recovery` object with those paths,
the exact control table, `rollback_then_fail` policy, and the default
`capacity_v3.yaml` overlay. Terraform uploads that overlay to
`pipeline-code/gold/v3/capacity_v3.yaml`. Release-specific overlays and
`--set` overrides are appended after the default and are passed identically to
all normal and recovery steps. The Terraform S3 resource intentionally ignores config content
changes after its first creation. Therefore the repository file and the live
object below must both be updated during deployment:

    s3://lotus-sandbox-gold-curated-data/pipeline-code/gold/auto/job_config.json

Do not deploy the state-machine change until the two entrypoints, sidecar ZIP,
capacity overlay, and live config are present in S3.

## Deliberate fail-closed limits

- A `writing` journal with a changed snapshot blocks automatic rollback. An
  operator must determine whether Iceberg committed before the journal reply
  was lost.
- A changed table with no pre-run snapshot (the first build of that table)
  cannot be restored automatically.
- `snapshot_retention_days: 7` documents the minimum operational expectation;
  it does not create Iceberg tags or run snapshot-expiration maintenance.
- The recovery lease serializes the recovery programs only. Normal Gold runs
  are serialized by `gold_run_control` and Step Functions, but any independent
  Athena/Spark writer or future table-maintenance job must be prohibited during
  recovery or integrated with the same writer-exclusion policy. This repository
  cannot fence writers it does not own merely by adding a sidecar recovery ZIP.

## Validation before production

Execute controlled failures in a non-production environment:

1. Fail a pre-publish stage and confirm no table moves, one failed control row,
   unchanged successful watermark, cluster termination, and a failure alert.
2. Fail publish after one or more table commits. Confirm exact reverse rollback,
   five-table pin verification, then one failed control row.
3. Make one head unexpected and confirm the finalizer is skipped, the control
   reservation remains unresolved, and a P1 alert is delivered.
4. Interrupt rollback after one table pointer move and verify a retry reconciles
   the journal without duplicate control closure.
5. Retain Step Functions history, EMR step IDs, rollback summary, before/after
   snapshot IDs, control-table query output, and SNS delivery evidence.
