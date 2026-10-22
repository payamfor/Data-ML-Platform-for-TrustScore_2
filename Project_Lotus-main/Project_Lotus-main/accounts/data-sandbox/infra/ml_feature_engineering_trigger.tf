locals {
  ml_feature_engineering_trigger_prefix = "enstream-ml-preprocessing/event_trigger/"
  ml_feature_engineering_scoring_prefix = "score_event_trigger/"
}

# S3 must be configured to publish object-created events to EventBridge.
resource "aws_s3_bucket_notification" "ml_feature_engineering_eventbridge" {
  bucket      = var.gold_bucket_name
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "ml_feature_engineering_success_uploaded" {
  name        = "${var.project}-${var.environment}-feature-engineering-success-uploaded"
  description = "Start feature engineering training when an _SUCCESS marker is uploaded."

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = {
        name = [var.gold_bucket_name]
      }
      object = {
        key = [{
          wildcard = "${local.ml_feature_engineering_trigger_prefix}*_SUCCESS"
        }]
      }
    }
  })

  depends_on = [aws_s3_bucket_notification.ml_feature_engineering_eventbridge]
}

resource "aws_cloudwatch_event_target" "ml_feature_engineering_training" {
  rule      = aws_cloudwatch_event_rule.ml_feature_engineering_success_uploaded.name
  target_id = "feature-engineering-training"
  arn       = aws_sfn_state_machine.ml_feature_engineering.arn
  role_arn  = data.aws_iam_role.eventbridge_ml_feature_engineering.arn

  input = jsonencode({
    mode = "training"
  })

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 10
  }
}

resource "aws_cloudwatch_event_rule" "ml_feature_engineering_scoring_success_uploaded" {
  name        = "${var.project}-${var.environment}-feature-engineering-scoring-success-uploaded"
  description = "Start feature engineering scoring when an _SUCCESS marker is uploaded under s3://${aws_s3_bucket.preprocessing_data.bucket}/${local.ml_feature_engineering_scoring_prefix}."

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = {
        name = [aws_s3_bucket.preprocessing_data.bucket]
      }
      object = {
        key = [{ wildcard = "${local.ml_feature_engineering_scoring_prefix}*_SUCCESS" }]
      }
    }
  })

  depends_on = [aws_s3_bucket_notification.ml_v2_sagemaker_eventbridge]
}

resource "aws_cloudwatch_event_target" "ml_feature_engineering_scoring" {
  rule      = aws_cloudwatch_event_rule.ml_feature_engineering_scoring_success_uploaded.name
  target_id = "feature-engineering-scoring"
  arn       = aws_sfn_state_machine.ml_feature_engineering.arn
  role_arn  = data.aws_iam_role.eventbridge_ml_feature_engineering.arn

  input_transformer {
    input_paths = {
      event_time = "$.time"
    }

    input_template = "{\"mode\":\"scoring\",\"event_time\":\"<event_time>\"}"
  }

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 10
  }
}

output "ml_feature_engineering_scoring_trigger" {
  description = "Upload an _SUCCESS marker under this prefix to start the feature-engineering scoring mode."
  value       = "s3://${aws_s3_bucket.preprocessing_data.bucket}/${local.ml_feature_engineering_scoring_prefix}<batch>/_SUCCESS"
}
