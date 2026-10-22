data "archive_file" "bronze_schema_validator" {
  type        = "zip"
  source_file = "${path.module}/../../../project_lotus/lambdas/bronze_schema_validator/bronze_schema_validator.py"
  output_path = "${path.module}/bronze_schema_validator.zip"
}

resource "aws_cloudwatch_log_group" "bronze_schema_validator" {
  name              = "/aws/lambda/${local.bronze_schema_validator_name}"
  retention_in_days = 14
}

resource "aws_lambda_function" "bronze_schema_validator" {
  function_name = local.bronze_schema_validator_name
  description   = "Validates Bronze Parquet schemas before Silver processing."

  role    = data.aws_iam_role.bronze_schema_validator.arn
  runtime = "python3.12"
  handler = "bronze_schema_validator.lambda_handler"

  architectures = ["x86_64"]

  filename         = data.archive_file.bronze_schema_validator.output_path
  source_code_hash = data.archive_file.bronze_schema_validator.output_base64sha256

  memory_size = 1024
  timeout     = 100

  layers = [
    aws_lambda_layer_version.bronze_schema_validator_pyarrow.arn
  ]

  environment {
    variables = {
      CONTROL_DATABASE = local.bronze_schema_validator_control_database_name
      CONTROL_TABLE    = local.bronze_schema_validator_control_table_name

      ATHENA_WORKGROUP = "primary"

      SCHEMA_BUCKET = local.bronze_schema_validator_bucket_name
      SCHEMA_PREFIX = local.bronze_schema_validator_schema_prefix

      MAX_WORKERS      = "4"
      MERGE_CHUNK_SIZE = "25"

      # The trigger into Silver. On overall_status == PASS, this Lambda starts
      # one execution of the per-dataset Gate1->Silver->Gate2-check state
      # machine for exactly the dataset it just validated - independently of
      # whatever the other datasets are doing. See stepfunctions_bronze_silver.tf
      # for why this is not a Map/Parallel fan-out over all datasets at once.
      #
      # DATASET_SPECS_JSON only lists datasets that HAVE a Silver job. A
      # schema-validated dataset with no entry here (partner_configuration,
      # deliberately out of scope for now - see the orchestration doc, O-3) is
      # validated same as any other but starts nothing afterward.
      SILVER_STATE_MACHINE_ARN = local.bronze_schema_validator_silver_state_machine_arn
      DATASET_SPECS_JSON       = jsonencode(local.silver_dataset_specs)
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.bronze_schema_validator,
    aws_glue_catalog_table.bronze_schema_validation
  ]
}
