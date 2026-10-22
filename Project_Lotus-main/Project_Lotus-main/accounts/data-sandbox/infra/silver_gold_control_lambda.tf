# Trigger infra deployment for silver_gold_control Lambda code update.
data "archive_file" "silver_gold_control" {
  type        = "zip"
  source_file = "${path.module}/../../../project_lotus/lambdas/silver_gold_control/silver_gold_control.py"
  output_path = "${path.module}/silver_gold_control.zip"
}

resource "aws_cloudwatch_log_group" "silver_gold_control" {
  name              = "/aws/lambda/${local.silver_gold_control_name}"
  retention_in_days = 14
}

resource "aws_lambda_function" "silver_gold_control" {
  function_name = local.silver_gold_control_name
  description   = "Records Silver outcomes, evaluates Gate 2, and releases the Gold run."

  role    = data.aws_iam_role.silver_gold_control.arn
  runtime = "python3.12"
  handler = "silver_gold_control.lambda_handler"

  architectures = ["x86_64"]

  filename         = data.archive_file.silver_gold_control.output_path
  source_code_hash = data.archive_file.silver_gold_control.output_base64sha256

  # No PyArrow here, unlike the Bronze validator: this function only issues
  # Athena statements and reads small JSON objects.
  memory_size = 512

  # Athena MERGE statements are polled to completion inside the handler, and a
  # cold control table can take tens of seconds on its first statement.
  timeout = 300

  environment {
    variables = {
      CONTROL_DATABASE           = local.silver_gold_control_database_name
      CONTROL_TABLE              = local.silver_gold_control_table_name
      BRONZE_VALIDATION_TABLE    = local.bronze_schema_validator_control_table_name
      ORCHESTRATION_CLAIM_BUCKET = local.pipeline_claim_bucket
      ORCHESTRATION_CLAIM_PREFIX = local.pipeline_claim_prefix

      ATHENA_WORKGROUP = local.silver_gold_control_athena_workgroup_name

      RELEASE_BUCKET = local.silver_gold_control_bucket_name
      RELEASE_PREFIX = local.silver_gold_release_prefix

      GOLD_JOB_CONFIG_URI   = local.gold_job_config_uri
      SILVER_JOB_CONFIG_URI = local.silver_job_config_uri

      # No GOLD_STATE_MACHINE_ARN here: this Lambda no longer starts Gold.
      # EventBridge starts it directly - see security/stepfunctions_iam.tf's
      # eventbridge_gold_trigger role for why - and the `release_gate` action
      # below is now called FROM INSIDE that execution's first state instead.
      SNS_TOPIC_ARN = aws_sns_topic.pipeline_alerts["silver"].arn

      ENABLED_DATASETS = join(",", local.silver_gold_enabled_datasets)
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.silver_gold_control,
    aws_glue_catalog_table.silver_gold_control,
    aws_s3_object.gold_job_config,
    aws_s3_object.silver_job_config
  ]
}

# ---------------------------------------------------------------------------
# Gate 2 trigger: release object -> EventBridge -> this Lambda
#
# Both the control table's prefix and the release object live in the Silver
# bucket (var.silver_bucket_name), not the Bronze landing bucket - this is the
# Silver -> Gold boundary, so its control data belongs with Silver.
# ---------------------------------------------------------------------------

resource "aws_s3_object" "silver_gold_control_prefixes" {
  for_each = toset([
    "control/",
    "control/athena-results/",
    "${local.silver_gold_release_prefix}/",
  ])

  bucket       = local.silver_gold_control_bucket_name
  key          = each.value
  content      = ""
  content_type = "application/x-directory"
}

# S3 only emits to EventBridge when the bucket is explicitly told to. Without
# this the rule below matches nothing and Gate 2 never fires.
resource "aws_s3_bucket_notification" "silver_conformed_eventbridge" {
  bucket      = local.silver_gold_control_bucket_name
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "gold_release" {
  name        = "${local.silver_gold_control_name}-release"
  description = "Starts the Gold pipeline when a Gate 2 release object is written."

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = {
        name = [local.silver_gold_control_bucket_name]
      }
      object = {
        key = [
          {
            wildcard = "${local.silver_gold_release_prefix}/run_date=*/release.json"
          }
        ]
      }
    }
  })

  depends_on = [aws_s3_bucket_notification.silver_conformed_eventbridge]
}

# Targets the Gold state machine directly, NOT this Lambda.
#
# The original design targeted aws_lambda_function.silver_gold_control here,
# which needs an aws_lambda_permission granting events.amazonaws.com
# lambda:InvokeFunction - and that call itself failed to apply:
#
#   AccessDeniedException: ... is not authorized to perform: lambda:AddPermission
#   ... with an explicit deny in an identity-based policy
#
# That deny sits on the CI/CD role that applies this Terraform
# (gha-lotus-data-sandbox-infra), not on anything in this repo, so there is no
# Terraform-side fix for it - only whoever owns that role's policy can lift it.
# Targeting Step Functions instead needs no Lambda resource-based policy at
# all: authorization is the ordinary identity-based
# data.aws_iam_role.eventbridge_gold_trigger role, which grants nothing but
# states:StartExecution on this one state machine. See stepfunctions_gold.tf's
# ResolveRelease state for where the logic this Lambda used to run before
# starting Gold itself now runs instead - as the execution's own first step.
resource "aws_cloudwatch_event_target" "gold_release" {
  rule      = aws_cloudwatch_event_rule.gold_release.name
  target_id = "gold-state-machine"
  arn       = aws_sfn_state_machine.gold.arn
  role_arn  = data.aws_iam_role.eventbridge_gold_trigger.arn
}

output "silver_gold_control_function_name" {
  description = "Name of the Silver-to-Gold control Lambda."
  value       = aws_lambda_function.silver_gold_control.function_name
}
