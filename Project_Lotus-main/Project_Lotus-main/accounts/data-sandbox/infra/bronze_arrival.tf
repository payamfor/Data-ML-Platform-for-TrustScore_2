# S3 object arrivals are routed to one four-minute window per dataset and date.
# A small arrival-only Lambda claims the first object with S3
# If-None-Match: *. No Athena query or control table is involved before wait.
locals {
  # The first matching file claims this window; all later files in the same
  # dataset/date folder are included when validation runs after this delay.
  bronze_arrival_validator_wait_seconds = 4 * 60

  pipeline_claim_bucket = aws_s3_bucket.medallion["bronze-landing"].bucket
  pipeline_claim_prefix = "control/pipeline-claims"
  # Dedicated prefix avoids the incomplete legacy claims written by the old
  # Athena-dependent arrival path.
  pipeline_arrival_claim_prefix = "control/bronze-arrival-claims"
}

resource "aws_cloudwatch_log_group" "sfn_bronze_arrival" {
  name              = "/aws/vendedlogs/states/${var.project}-${var.environment}-bronze-arrival"
  retention_in_days = 30
}

resource "aws_sfn_state_machine" "bronze_arrival" {
  name     = "${var.project}-${var.environment}-bronze-arrival"
  role_arn = data.aws_iam_role.sfn_bronze_arrival.arn

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_bronze_arrival.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  definition = jsonencode({
    Comment = "Automatic object arrivals use a four-minute window; manual folder replays validate and process the complete prefix immediately."
    StartAt = "DetectTrigger"
    States = {
      DetectTrigger = {
        Type = "Choice"
        Choices = [
          {
            Variable  = "$.detail.object.key"
            IsPresent = true
            Next      = "ParseEventFolder"
          },
          {
            Variable  = "$.folder_path"
            IsPresent = true
            Next      = "ClaimManualReplay"
          }
        ]
        Default = "InvalidTrigger"
      }
      InvalidTrigger = {
        Type  = "Fail"
        Error = "InvalidBronzeArrivalInput"
        Cause = "Provide either an EventBridge S3 Object Created event or {\"folder_path\":\"s3://.../bronze/<dataset>/YYYY-MM-DD/\"}."
      }
      ParseEventFolder = {
        Type = "Pass"
        Parameters = {
          "dataset_name.$" = "States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 1)"
          "run_date.$"     = "States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 2)"
          "folder_path.$"  = "States.Format('s3://{}/bronze/{}/{}/', $.detail.bucket.name, States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 1), States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 2))"
        }
        ResultPath = "$.folder"
        Next       = "ClaimWindow"
      }
      ClaimWindow = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.bronze_arrival_claim.arn
          Payload = {
            "dataset_name.$" = "$.folder.dataset_name"
            "run_date.$"     = "$.folder.run_date"
            "folder_path.$"  = "$.folder.folder_path"
            trigger_type     = "automatic"
            reprocess_id     = ""
            "owner.$"        = "$$.Execution.Id"
          }
        }
        ResultPath = "$.claim"
        Next       = "WindowClaimed"
        Retry      = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 5, BackoffRate = 2, MaxAttempts = 2 }]
        Catch      = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "ClaimFailed" }]
      }
      ClaimManualReplay = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.bronze_arrival_claim.arn
          Payload = {
            "folder_path.$"  = "$.folder_path"
            trigger_type     = "manual"
            "reprocess_id.$" = "$$.Execution.Name"
            "owner.$"        = "$$.Execution.Id"
          }
        }
        ResultPath = "$.claim"
        Next       = "WindowClaimed"
        Retry      = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 5, BackoffRate = 2, MaxAttempts = 2 }]
        Catch      = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "ClaimFailed" }]
      }
      WindowClaimed = {
        Type    = "Choice"
        Choices = [{ Variable = "$.claim.Payload.claimed", BooleanEquals = true, Next = "SelectArrivalDelay" }]
        Default = "AlreadyScheduled"
      }
      SelectArrivalDelay = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.claim.Payload.trigger_type"
          StringEquals = "manual"
          Next         = "ValidateBronzeFolder"
        }]
        Default = "Wait4Minutes"
      }
      AlreadyScheduled = { Type = "Succeed" }
      ClaimFailed = {
        Type  = "Fail"
        Error = "ArrivalWindowClaimFailed"
      }
      Wait4Minutes = {
        Type    = "Wait"
        Seconds = local.bronze_arrival_validator_wait_seconds
        Next    = "ValidateBronzeFolder"
      }
      ValidateBronzeFolder = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.bronze_schema_validator.arn
          Payload = {
            "dataset_name.$" = "$.claim.Payload.dataset_name"
            "run_date.$"     = "$.claim.Payload.run_date"
            "folder_path.$"  = "$.claim.Payload.folder_path"
            "trigger_type.$" = "$.claim.Payload.trigger_type"
            "reprocess_id.$" = "$.claim.Payload.reprocess_id"
          }
        }
        Retry = [{
          ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"]
          IntervalSeconds = 5
          BackoffRate     = 2
          MaxAttempts     = 2
        }]
        End = true
      }
    }
  })
}

# This is the exact landing bucket used by the validator and the example
# s3://lotus-sandbox-bronze-landing-data/bronze/account_changes_batch/2026-08-16/.
resource "aws_s3_bucket_notification" "bronze_arrival_eventbridge" {
  bucket      = aws_s3_bucket.medallion["bronze-landing"].id
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "bronze_arrival" {
  name = "${var.project}-${var.environment}-bronze-arrival"
  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.medallion["bronze-landing"].bucket] }
      object = {
        key = [
          { wildcard = "bronze/account_changes_batch/*/*.parquet" },
          { wildcard = "bronze/device_lookup_batch/*/*.parquet" }
        ]
      }
    }
  })
  depends_on = [aws_s3_bucket_notification.bronze_arrival_eventbridge]
}

resource "aws_cloudwatch_event_target" "bronze_arrival" {
  rule      = aws_cloudwatch_event_rule.bronze_arrival.name
  target_id = "bronze-arrival-state-machine"
  arn       = aws_sfn_state_machine.bronze_arrival.arn
  role_arn  = data.aws_iam_role.eventbridge_bronze_arrival.arn
}
