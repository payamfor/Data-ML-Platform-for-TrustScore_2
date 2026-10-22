# ===============================================================================
# VERSION 2 - Trust Score ML pipeline: "new data arrived" -> start a pipeline
# =================================================================================
# Added in v2; no existing file in this root was modified. See ../CHANGES_V2.md.
#
# The gold bucket sends object-created events to EventBridge; the two rules keep
# only COMPLETION MARKERS (keys ending in ml_v2_trigger_marker_suffix) under the
# training and scoring prefixes. One uploaded batch = one marker = one run.
# Same mechanism as the Gold release trigger in silver_gold_control_lambda.tf,
# except the target is a state machine (started with the events role from
# ../security) rather than a Lambda.
# ===============================================================================

# Owns ALL notification settings of the gold bucket. No other file in this repo
# manages them (checked for v2); if any were set up by hand in the console,
# they would be replaced - see ml_v2_manage_gold_bucket_notification.
resource "aws_s3_bucket_notification" "ml_v2_sagemaker_eventbridge" {
  count = var.ml_v2_manage_sagemaker_bucket_notification ? 1 : 0

  bucket      = aws_s3_bucket.preprocessing_data.id
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "ml_v2_training_data_arrived" {
  name        = "${local.ml_v2_name}-training-data-arrived"
  description = "VERSION 2: a training batch is complete (${var.ml_v2_trigger_marker_suffix}) under s3://${local.ml_v2_input_bucket}/${var.ml_v2_training_data_prefix}"

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [local.ml_v2_input_bucket] }
      object = { key = [{ wildcard = "${var.ml_v2_training_data_prefix}*${var.ml_v2_trigger_marker_suffix}" }] }
    }
  })

  depends_on = [aws_s3_bucket_notification.ml_v2_sagemaker_eventbridge]
}

resource "aws_cloudwatch_event_rule" "ml_v2_scoring_data_arrived" {
  name        = "${local.ml_v2_name}-scoring-data-arrived"
  description = "VERSION 2: a scoring batch is complete (${var.ml_v2_trigger_marker_suffix}) under s3://${local.ml_v2_input_bucket}/${var.ml_v2_scoring_data_prefix}"

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [local.ml_v2_input_bucket] }
      object = { key = [{ wildcard = "${var.ml_v2_scoring_data_prefix}*${var.ml_v2_trigger_marker_suffix}" }] }
    }
  })

  depends_on = [aws_s3_bucket_notification.ml_v2_sagemaker_eventbridge]
}

resource "aws_cloudwatch_event_target" "ml_v2_start_training" {
  rule      = aws_cloudwatch_event_rule.ml_v2_training_data_arrived.name
  target_id = "trust-score-ml-training"
  arn       = aws_sfn_state_machine.ml_v2_training.arn
  role_arn  = data.aws_iam_role.ml_v2_events.arn

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 10
  }
}

resource "aws_cloudwatch_event_target" "ml_v2_start_scoring" {
  rule      = aws_cloudwatch_event_rule.ml_v2_scoring_data_arrived.name
  target_id = "trust-score-ml-scoring"
  arn       = aws_sfn_state_machine.ml_v2_scoring.arn
  role_arn  = data.aws_iam_role.ml_v2_events.arn

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 10
  }
}

# The independent batch-inference flow reads daily data from the SageMaker
# bucket; it does not trigger the feature-engineering scoring state machine.
resource "aws_s3_bucket_notification" "ml_v2_batch_scoring_eventbridge" {
  bucket      = aws_s3_bucket.sagemaker.id
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "ml_v2_batch_scoring_data_arrived" {
  name        = "${local.ml_v2_name}-batch-scoring-data-arrived"
  description = "Start batch inference when ${var.ml_v2_batch_scoring_data_prefix}dt=<date>/_SUCCESS is uploaded."

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [local.ml_v2_output_bucket] }
      object = {
        key = [{ wildcard = "${var.ml_v2_batch_scoring_data_prefix}dt=*/${var.ml_v2_trigger_marker_suffix}" }]
      }
    }
  })

  depends_on = [aws_s3_bucket_notification.ml_v2_batch_scoring_eventbridge]
}

resource "aws_cloudwatch_event_target" "ml_v2_start_batch_scoring" {
  rule      = aws_cloudwatch_event_rule.ml_v2_batch_scoring_data_arrived.name
  target_id = "trust-score-ml-batch-scoring"
  arn       = aws_sfn_state_machine.ml_v2_batch_scoring.arn
  role_arn  = data.aws_iam_role.ml_v2_events.arn

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 10
  }
}

# -----------------------------------------------------------------------------
# VERSION 2.2: champion Approved in the Model Registry -> deploy it
# -----------------------------------------------------------------------------
# SageMaker emits "SageMaker Model Package State Change" to the default bus on
# its own - no bucket-style opt-in needed. The training pipeline always
# creates a version as PendingManualApproval and then (auto-approve) updates
# it to Approved, so this rule fires once per champion either way; a person
# approving in Studio / the CLI fires it too.
# Re-approving an already Approved version (e.g. a metadata update) may fire it
# again: the deploy then re-rolls the same model blue/green - harmless.

resource "aws_cloudwatch_event_rule" "ml_v2_champion_approved" {
  name        = "${local.ml_v2_name}-champion-approved"
  description = "VERSION 2.2: a Trust Score champion version was Approved in ${local.ml_v2_model_package_group}; deploy it to ${local.ml_v2_endpoint_name}."

  event_pattern = jsonencode({
    source        = ["aws.sagemaker"]
    "detail-type" = ["SageMaker Model Package State Change"]
    detail = {
      ModelPackageGroupName = [local.ml_v2_model_package_group]
      ModelApprovalStatus   = ["Approved"]
    }
  })
}

resource "aws_cloudwatch_event_target" "ml_v2_deploy_champion" {
  rule      = aws_cloudwatch_event_rule.ml_v2_champion_approved.name
  target_id = "trust-score-ml-deploy"
  arn       = aws_sfn_state_machine.ml_v2_deploy.arn
  role_arn  = data.aws_iam_role.ml_v2_events.arn

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 10
  }
}


# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "ml_v2_training_state_machine_arn" {
  description = "VERSION 2: start manually with aws stepfunctions start-execution --state-machine-arn <this> --input '{}'"
  value       = aws_sfn_state_machine.ml_v2_training.arn
}

output "ml_v2_scoring_state_machine_arn" {
  description = "VERSION 2: nightly scoring state machine."
  value       = aws_sfn_state_machine.ml_v2_scoring.arn
}

output "ml_v2_batch_scoring_state_machine_arn" {
  description = "Separate state machine for daily batch inference from the SageMaker data bucket."
  value       = aws_sfn_state_machine.ml_v2_batch_scoring.arn
}

output "ml_v2_alerts_topic_arn" {
  description = "VERSION 2: ML pipeline alerts (training finished/failed, scoring failed, drift)."
  value       = aws_sns_topic.ml_v2_alerts.arn
}

output "ml_v2_training_trigger" {
  description = "VERSION 2: upload this marker LAST, in a training batch folder, to start training."
  value       = "s3://${local.ml_v2_input_bucket}/${var.ml_v2_training_data_prefix}<batch folder>/${var.ml_v2_trigger_marker_suffix}"
}

output "ml_v2_scoring_trigger" {
  description = "VERSION 2: upload this marker LAST, in a scoring batch folder, to score it."
  value       = "s3://${local.ml_v2_input_bucket}/${var.ml_v2_scoring_data_prefix}batch_date=YYYY-MM-DD/${var.ml_v2_trigger_marker_suffix}"
}

output "ml_v2_batch_scoring_trigger" {
  description = "Upload this marker last to start batch inference for the date."
  value       = "s3://${local.ml_v2_output_bucket}/${var.ml_v2_batch_scoring_data_prefix}dt=YYYY-MM-DD/${var.ml_v2_trigger_marker_suffix}"
}

output "ml_v2_images" {
  description = "VERSION 2: the exact images (by digest) the pipeline runs. Re-apply after pushing new images."
  value       = { training = local.ml_v2_training_image, inference = local.ml_v2_inference_image }
}

output "ml_v2_champion_approved_rule" {
  description = "VERSION 2.2: EventBridge rule that deploys each Approved champion."
  value       = aws_cloudwatch_event_rule.ml_v2_champion_approved.name
}
