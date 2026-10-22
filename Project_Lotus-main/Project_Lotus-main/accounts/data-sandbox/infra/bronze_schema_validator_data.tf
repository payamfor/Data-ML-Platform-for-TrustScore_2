data "aws_iam_role" "bronze_schema_validator" {
  name = "${var.project}-${var.environment}-bronze-schema-validator-role"
}

locals {
  bronze_schema_validator_name        = "${var.project}-${var.environment}-bronze-schema-validator"
  bronze_schema_validator_bucket_name = "${var.project}-${var.environment}-bronze-landing-data"

  bronze_schema_validator_control_database_name = "control"
  bronze_schema_validator_control_table_name    = "bronze_schema_validation"

  bronze_schema_validator_schema_prefix         = "schema_registry"
  bronze_schema_validator_control_table_prefix  = "control/bronze_schema_validation"
  bronze_schema_validator_athena_results_prefix = "athena-results"

  bronze_schema_validator_control_table_location = "s3://${local.bronze_schema_validator_bucket_name}/${local.bronze_schema_validator_control_table_prefix}/"
  bronze_schema_validator_athena_output_location = "s3://${local.bronze_schema_validator_bucket_name}/${local.bronze_schema_validator_athena_results_prefix}/"

  # The state-machine ARN is deterministic from its Terraform-managed name.
  # Constructing it here lets Lambda code/config updates deploy independently
  # instead of waiting for the state machine and its EMR dependencies.
  bronze_schema_validator_silver_state_machine_arn = "arn:${data.aws_partition.current.partition}:states:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:stateMachine:${local.sfn_bronze_silver_name}"
}
