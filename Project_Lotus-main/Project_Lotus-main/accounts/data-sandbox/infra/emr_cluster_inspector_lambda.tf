data "archive_file" "emr_cluster_inspector" {
  type        = "zip"
  source_file = "${path.module}/../../../project_lotus/lambdas/emr_cluster_inspector/emr_cluster_inspector.py"
  output_path = "${path.module}/emr_cluster_inspector.zip"
}

resource "aws_cloudwatch_log_group" "emr_cluster_inspector" {
  name              = "/aws/lambda/${var.project}-${var.environment}-emr-cluster-inspector"
  retention_in_days = 14
}

resource "aws_lambda_function" "emr_cluster_inspector" {
  function_name = "${var.project}-${var.environment}-emr-cluster-inspector"
  description   = "Reads an EMR cluster state and counts pending or running steps."

  role          = data.aws_iam_role.emr_cluster_inspector.arn
  runtime       = "python3.12"
  handler       = "emr_cluster_inspector.lambda_handler"
  architectures = ["x86_64"]

  filename         = data.archive_file.emr_cluster_inspector.output_path
  source_code_hash = data.archive_file.emr_cluster_inspector.output_base64sha256
  memory_size      = 128
  timeout          = 30

  depends_on = [aws_cloudwatch_log_group.emr_cluster_inspector]
}
