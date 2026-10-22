data "aws_iam_role" "silver_gold_control" {
  name = "${var.project}-${var.environment}-silver-gold-control-role"
}

locals {
  silver_gold_control_name = "${var.project}-${var.environment}-silver-gold-control"

  # This table sits on the Silver->Gold boundary, not the Bronze one, so it
  # lives in the Silver bucket under its own control/ folder rather than in the
  # Bronze landing bucket. The Glue *database* is still the shared "control"
  # database used by the Bronze validation table - a Glue database is a
  # catalog namespace, not a storage location, so pointing a second table's
  # `location` at a different bucket under the same database is ordinary Glue
  # usage and keeps every gate under one Lake Formation grant surface.
  silver_gold_control_bucket_name = var.silver_bucket_name

  silver_gold_control_database_name = local.bronze_schema_validator_control_database_name
  silver_gold_control_table_name    = "silver_gold_control"

  silver_gold_control_table_prefix   = "control/silver_gold_control"
  silver_gold_control_table_location = "s3://${local.silver_gold_control_bucket_name}/${local.silver_gold_control_table_prefix}/"

  # Where the Gate 2 release object is written and watched for. Kept in the
  # same bucket as the control table so one EventBridge rule and one bucket
  # notification setting cover both.
  silver_gold_release_prefix = "gold-release"

  silver_gold_control_athena_workgroup_name = "primary"
  # Athena results for this table's own queries live in the Silver bucket too,
  # under the same control/ folder, rather than reusing the Bronze bucket's
  # athena-results/ prefix - this table's queries have nothing to do with
  # Bronze validation.
  silver_gold_control_athena_output_location = "s3://${local.silver_gold_control_bucket_name}/control/athena-results/"

  # Gate 2 requires all of these to be 'succeeded' before Gold is released.
  # tu_portps and mno_activation join this list when their Silver jobs exist;
  # nothing else has to change when they do.
  silver_gold_enabled_datasets = [
    "account_changes_batch",
    "device_lookup_batch",
  ]
}
