variable "enrichment_ac_secret_arn" {
  description = "Optional full ARN of the client's AC hashing secret; match the security root value."
  type        = string
  default     = null
}

variable "enrichment_at_secret_arn" {
  description = "Optional full ARN of the client's AT hashing secret; match the security root value."
  type        = string
  default     = null
}

locals {
  enrichment_hasher_name  = "${var.project}-${var.environment}-enrichment-response-hasher"
  enrichment_ac_secret_id = var.enrichment_ac_secret_arn != null ? var.enrichment_ac_secret_arn : "tulip/${var.environment}/salt-ac-hashing"
  enrichment_at_secret_id = var.enrichment_at_secret_arn != null ? var.enrichment_at_secret_arn : "tulip/${var.environment}/salt-audit-trail-hashing"
}

# Security creates the bounded Lambda role before this root applies.
data "aws_iam_role" "enrichment_response_hasher" {
  name = "${local.enrichment_hasher_name}-role"
}

resource "aws_sqs_queue" "enrichment_response_hasher_dlq" {
  name                      = "${local.enrichment_hasher_name}-dlq"
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "enrichment_response_hasher" {
  name                       = local.enrichment_hasher_name
  visibility_timeout_seconds = 720 # Six times the 120-second Lambda timeout.
  message_retention_seconds  = 1209600
  sqs_managed_sse_enabled    = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.enrichment_response_hasher_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_cloudwatch_log_group" "enrichment_response_hasher" {
  name              = "/aws/lambda/${local.enrichment_hasher_name}"
  retention_in_days = 14
}

data "archive_file" "enrichment_response_hasher" {
  type        = "zip"
  source_file = "${path.module}/../../../project_lotus/lambdas/enrichment_response_hasher/enrichment_response_hasher.py"
  output_path = "${path.module}/enrichment_response_hasher.zip"
}

resource "aws_lambda_function" "enrichment_response_hasher" {
  function_name = local.enrichment_hasher_name
  description   = "Hash MSISDNs in completed PortPS and IDV response files."
  role          = data.aws_iam_role.enrichment_response_hasher.arn
  runtime       = "python3.12"
  handler       = "enrichment_response_hasher.lambda_handler"
  architectures = ["x86_64"]

  filename         = data.archive_file.enrichment_response_hasher.output_path
  source_code_hash = data.archive_file.enrichment_response_hasher.output_base64sha256
  memory_size      = 1024
  timeout          = 120

  environment {
    variables = {
      INPUT_BUCKET           = aws_s3_bucket.medallion["bronze-landing"].bucket
      OUTPUT_BUCKET          = aws_s3_bucket.medallion["bronze-landing"].bucket
      CONTINUATION_QUEUE_URL = aws_sqs_queue.enrichment_response_hasher.url
      AC_SECRET_ID           = local.enrichment_ac_secret_id
      AT_SECRET_ID           = local.enrichment_at_secret_id
      MAX_ENVELOPES          = "100"
    }
  }

  depends_on = [aws_cloudwatch_log_group.enrichment_response_hasher]
}

resource "aws_lambda_event_source_mapping" "enrichment_response_hasher" {
  event_source_arn = aws_sqs_queue.enrichment_response_hasher.arn
  function_name    = aws_lambda_function.enrichment_response_hasher.arn
  batch_size       = 1
  scaling_config {
    maximum_concurrency = 5
  }
}

# The Bronze bucket already has EventBridge enabled in bronze_arrival.tf.
resource "aws_cloudwatch_event_rule" "enrichment_response_arrival" {
  name        = "${local.enrichment_hasher_name}-arrival"
  description = "Queue final provider response objects for bounded MSISDN hashing."
  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.medallion["bronze-landing"].bucket] }
      object = {
        key = [
          { wildcard = "enterprise-enrichment/responses/provider=portps/*" },
          { wildcard = "enterprise-enrichment/responses/provider=idv/*" }
        ]
      }
    }
  })
  depends_on = [aws_s3_bucket_notification.bronze_arrival_eventbridge]
}

data "aws_iam_policy_document" "enrichment_response_queue" {
  statement {
    sid       = "AllowArrivalEventBridgeRule"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.enrichment_response_hasher.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.enrichment_response_arrival.arn]
    }
  }
}

resource "aws_sqs_queue_policy" "enrichment_response_hasher" {
  queue_url = aws_sqs_queue.enrichment_response_hasher.id
  policy    = data.aws_iam_policy_document.enrichment_response_queue.json
}

resource "aws_cloudwatch_event_target" "enrichment_response_arrival" {
  rule       = aws_cloudwatch_event_rule.enrichment_response_arrival.name
  target_id  = "enrichment-response-queue"
  arn        = aws_sqs_queue.enrichment_response_hasher.arn
  depends_on = [aws_sqs_queue_policy.enrichment_response_hasher]
}

resource "aws_cloudwatch_metric_alarm" "enrichment_response_hasher_dlq" {
  alarm_name          = "${local.enrichment_hasher_name}-dlq-not-empty"
  alarm_description   = "A provider response could not be hashed after retries; inspect the DLQ before publishing Silver."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    QueueName = aws_sqs_queue.enrichment_response_hasher_dlq.name
  }
  alarm_actions = [aws_sns_topic.pipeline_alerts["silver"].arn]
}

output "enrichment_response_hasher_function_name" {
  value = aws_lambda_function.enrichment_response_hasher.function_name
}

output "enrichment_response_hasher_dlq_url" {
  value = aws_sqs_queue.enrichment_response_hasher_dlq.url
}
