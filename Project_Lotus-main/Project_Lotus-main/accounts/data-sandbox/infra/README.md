# Project Lotus, data-sandbox, infra layer

For the isolated manifest-driven Silver v2 demo and its onboarding-owned
Lake Formation reader grant, see [MANIFEST_DEMO.md](MANIFEST_DEMO.md).

See the [repository README](../../../README.md) for the layer split, the pipeline and the
conventions.

This root provisions the data pipeline infrastructure, including storage,
catalog tables, EMR, and Step Functions. See the repository README's
[EMR capacity and job lifecycle](../../../README.md#emr-capacity-and-job-lifecycle)
for node sizes, On-Demand policy, job retries, and the one-hour idle timeout.

## Belongs here

S3, S3 Tables, Glue, EMR, Athena, Step Functions, EventBridge, log groups, VPC resources.

## Resolving what `../security/` created

Resolve by name. Never hardcode an ARN, and never read the other root's state.

```hcl
data "aws_iam_role"  "glue"   { name = "${var.project}-glue-ingestion" }
data "aws_kms_alias" "bronze" { name = "alias/${var.project}-${var.environment}/bronze" }
```

## Preprocessing EMR logs and output bucket

The preprocessing cluster uses `s3://lotus-sandbox-ml-preprocessing-data/`
for EMR logs. The feature-engineering runtime role is also granted access to
this bucket for job outputs. Terraform enables versioning, default SSE-S3
encryption, and S3 public-access blocking, and prevents accidental bucket
deletion.

If this bucket already exists, import it into this root's Terraform state
before the first plan/apply:

```powershell
terraform import aws_s3_bucket.preprocessing_data lotus-sandbox-ml-preprocessing-data
```

Import the bucket only; Terraform will then manage its versioning, encryption,
and public-access-block settings alongside the EMR log prefix.

## EMR step logs

Silver/Gold clusters archive logs under
`s3://lotus-sandbox-bronze-landing-data/emr-logs/lotus-sandbox-bronze-silver/`;
feature-engineering clusters use
`s3://lotus-sandbox-ml-preprocessing-data/emr-logs/preprocessing-pipeline/`.
After a cluster shuts down, open its cluster-ID folder, then `steps/<step-ID>/`
to find that step's archived logs. S3 archival is not a live view: EMR publishes
those files as nodes scale down or the cluster shuts down.

Feature-engineering step `controller`, `stderr`, `stdout`, and `syslog` files
are also streamed to CloudWatch Logs while the cluster runs, under
`/aws/emr/<project>-<environment>-preprocessing-pipeline`. The EMR EC2
instance-profile role needs `s3:PutObject` on the log prefixes,
`s3:GetObject` on the bootstrap script, and scoped CloudWatch Logs permissions;
`emr_iam.tf` grants these accesses.

## Feature-engineering retries and alerts

The feature-engineering workflow creates one fresh transient preprocessing EMR
cluster per execution by default. Failed **training** and **scoring** jobs are
each retried once on that same cluster. Both attempts use distinct EMR step
names. The cluster is terminated immediately after success or after the retry
fails. To use a reusable cluster instead, set
`enable_ml_feature_engineering_transient_cluster = false` and
`create_persistent_emr_pp_cluster = true`; the workflow then uses the active
persistent cluster and does not terminate it. A final training failure, scoring
failure, or cluster creation/termination failure publishes the execution
details to the dedicated feature-engineering SNS topic. The configured email
subscription is pending until its recipient confirms the SNS subscription
email.

 #------------------------------------------------------------------
# Silver Iceberg Terraform bootstrap

This small root module registers the three Trust Score Silver Iceberg tables in
the AWS Glue Data Catalog and defines their initial Iceberg schemas and partition
specifications. It uses the existing bucket:

    s3://lotus-sandbox-silver-conformed-data/

Terraform creates the catalog/database contract. The Silver Spark job writes the
first data files and Iceberg snapshots. The job also enables additive schema
evolution (`mergeSchema=true`) while rejecting unsafe type changes.

## Apply

From this directory:

    $env:AWS_REGION = "ca-central-1"
    terraform init
    terraform plan -out silver-iceberg.tfplan
    terraform apply silver-iceberg.tfplan

For the checked-in smoke configuration, use an isolated table prefix and
warehouse that match `examples/trust_score_silver_emr_smoke.json`:

    terraform apply -var='table_root_prefix=trust-score/silver-smoke/tables' -var='iceberg_warehouse_uri=s3://lotus-sandbox-silver-conformed-data/trust-score/iceberg-warehouse-smoke/'

Use a separate state key/workspace for each environment. If the existing account
Terraform root already owns the Glue database, set:

    terraform apply -var='create_glue_database=false'

If the three Glue tables already exist, import them into this state before the
first plan (replace the account ID and database name):

    terraform import 'aws_glue_catalog_table.silver["account_changes_batch"]' 123456789012:lotus_sandbox_silver_conformed:account_changes_batch

Do not delete and recreate a live Iceberg table to make Terraform happy.
Investigate the schema/partition difference and perform an explicit Iceberg
migration instead.

The bucket must exist before this module is applied. The existing data-sandbox
Terraform root already provisions the `silver-conformed` bucket with versioning,
encryption, and public-access blocking.

When integrating this module into `accounts/data-sandbox/infra`, use the
already-managed database and bucket instead of creating a second database:

    module "silver_iceberg" {
      source = "../../../project_lotus/terraform/silver_iceberg"

      providers = { aws = aws }

      silver_bucket_name   = aws_s3_bucket.medallion["silver-conformed"].bucket
      glue_database_name   = aws_glue_catalog_database.medallion["silver-conformed"].name
      create_glue_database = false
    }

Also point the EMR cluster's named Iceberg catalog warehouse at the Silver
bucket (for example, `s3://lotus-sandbox-silver-conformed-data/iceberg-warehouse/`),
not the Gold bucket. The job passes its own `warehouse_uri`, but keeping the
cluster default aligned prevents accidental writes to the wrong layer.

## Partition strategy

Each table uses one hidden Iceberg `day` transform on its event timestamp:

- account: `day(event_timestamp)`
- device: `day(event_timestamp)`
- audit: `day(api_timestamp)`

This keeps event-time queries prunable without adding a duplicate partition column
to the published P1 schema. Do not partition by high-cardinality IDs or MNO; that
would create many small partitions and hurt scan performance. Validate pruning
with `EXPLAIN` and compare bytes scanned before and after a representative date
filter.

## Schema evolution policy

Phase 1 should enable additive evolution because the Silver job may add a reviewed
nullable column while retaining old snapshots. Iceberg field IDs keep old data
readable and populate the new field as null for older files. Keep renames, deletes,
type changes, and nullability tightening as explicit migration reviews.

`ignore_changes` and `prevent_destroy` are intentional: Terraform bootstraps the
table but must not replace a live Iceberg table or roll back a schema that Spark
has evolved. Update the Terraform schema block only for a new version or a
reviewed migration; do not use a crawler to mutate these tables.

## Verification after the first EMR write

Run the Silver job against an isolated smoke prefix/database, then verify:

1. `aws glue get-table --database-name <db> --name account_changes_batch` (and
   the other two names) returns an Iceberg table.
2. The table S3 location contains `metadata/` and data files.
3. Spark can read all three tables through `glue_catalog.<db>.<table>` and
   `EXPLAIN` shows a pushed event-day filter.
4. A second run is readable and does not duplicate the configured deduplication
   keys.
5. Add one nullable test column through the reviewed Spark migration path and
   verify old snapshots still read; then capture the new schema in the release
   notes.

AWS's [Glue Catalog table resource](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_catalog_table.html)
supports the Iceberg `open_table_format_input` contract, including schema and
partition specs. EMR includes a compatible Iceberg runtime; see the [EMR Iceberg
guide](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-iceberg.html).

## What else is required

Terraform table registration is only one part of an Iceberg deployment. The
runtime also needs:

- an existing encrypted/versioned Silver S3 bucket and a separate warehouse
  prefix;
- a remote, encrypted Terraform state backend with state locking and a tracked
  `.terraform.lock.hcl` provider lock file;
- an EMR release with Iceberg installed, the Glue-backed Spark catalog
  settings, and `IcebergSparkSessionExtensions`;
- an EMR runtime role that can read Bronze/reference/artifact prefixes, write
  Silver and warehouse prefixes, call the required Glue APIs, and use the KMS
  key when customer-managed encryption is enabled;
- a single-writer or optimistic-concurrency policy for each table, with stable
  run IDs and deduplication keys;
- monitoring for accepted/rejected rows, commit failures, snapshot growth,
  small files, and scan volume;
- a scheduled maintenance process for compaction, snapshot expiration, and
  orphan-file cleanup. Do not let a Glue crawler rewrite the Iceberg schema.

## Read/write and pruning checks on EMR

After the first job run, submit a small Spark SQL check using the same catalog
settings:

    spark-sql \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.glue_catalog.type=glue \
      --conf spark.sql.catalog.glue_catalog.warehouse=s3://lotus-sandbox-silver-conformed-data/iceberg-warehouse/ \
      -e "SELECT count(*) FROM glue_catalog.lotus_sandbox_silver_conformed.account_changes_batch"

Use `EXPLAIN` with an event-time predicate and confirm the plan contains an
event-day partition filter. Record the query runtime and bytes scanned for a
representative unfiltered query versus the same query filtered to one event day;
those two measurements are the Phase-1 partition benchmark.

Terraform creates the initial Glue/Iceberg contract, but the first successful
Spark commit is what proves that data files and Iceberg metadata snapshots are
being generated. Capture the Glue table response, S3 `metadata/` listing, Spark
read/write result, and benchmark in the release evidence.
