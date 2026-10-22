locals {
  github_pipeline_operations_execution_arns = [
    "arn:${data.aws_partition.current.partition}:states:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:execution:${var.project}-${var.environment}-trust-score-ml-training:d343f118-a231-37df-198e-9d994930d41d_265accbe-bd1c-1dd1-d717-cabd2ba8ab06",
  ]
}

data "aws_iam_policy_document" "github_pipeline_operations_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [data.aws_iam_openid_connect_provider.ghe_actions.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.enstream.ghe.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.enstream.ghe.com:ref"
      values   = ["refs/heads/main"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.enstream.ghe.com:sub"
      values = [
        "repo:data-and-ai/Project_Lotus:environment:data-sandbox-operations"
      ]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.enstream.ghe.com:job_workflow_ref"
      values = [
        "data-and-ai/Project_Lotus/.github/workflows/stop-ml-training-stepfunction.yml@refs/heads/main"
      ]
    }
  }
}

resource "aws_iam_role" "github_pipeline_operations" {
  name                 = "${local.name_prefix}-github-pipeline-operations"
  assume_role_policy   = data.aws_iam_policy_document.github_pipeline_operations_assume.json
  permissions_boundary = local.permissions_boundary
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "github_pipeline_operations" {
  name = "stop-lotus-pipeline-executions"
  role = aws_iam_role.github_pipeline_operations.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "StopApprovedDataPipelineExecutions"
      Effect = "Allow"
      Action = [
        "states:DescribeExecution",
        "states:StopExecution"
      ]
      Resource = local.github_pipeline_operations_execution_arns
    }]
  })
}

output "github_pipeline_operations_role_arn" {
  value = aws_iam_role.github_pipeline_operations.arn
}