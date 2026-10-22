data "archive_file" "bronze_arrival_claim" {
  type        = "zip"
  source_file = "${path.module}/../../../project_lotus/lambdas/bronze_arrival_claim/bronze_arrival_claim.py"
  output_path = "${path.module}/bronze_arrival_claim.zip"
}

resource "aws_cloudwatch_log_group" "bronze_arrival_claim" {
  name              = "/aws/lambda/${var.project}-${var.environment}-bronze-arrival-claim"
  retention_in_days = 14
}

resource "aws_lambda_function" "bronze_arrival_claim" {
  function_name = "${var.project}-${var.environment}-bronze-arrival-claim"
  description   = "Claims the first S3 event for one Bronze dataset/date arrival window."

  role          = data.aws_iam_role.bronze_arrival_claim.arn
  runtime       = "python3.12"
  handler       = "bronze_arrival_claim.lambda_handler"
  architectures = ["x86_64"]

  filename         = data.archive_file.bronze_arrival_claim.output_path
  source_code_hash = data.archive_file.bronze_arrival_claim.output_base64sha256
  memory_size      = 128
  timeout          = 30

  environment {
    variables = {
      CLAIM_BUCKET     = local.pipeline_claim_bucket
      CLAIM_PREFIX     = local.pipeline_arrival_claim_prefix
      ENABLED_DATASETS = join(",", local.silver_gold_enabled_datasets)
    }
  }

  depends_on = [aws_cloudwatch_log_group.bronze_arrival_claim]
}
