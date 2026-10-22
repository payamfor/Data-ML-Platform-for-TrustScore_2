# IAM role assumed by GitHub Actions (GHE OIDC, no static keys) to publish the
# feature-engineering EMR artifacts to
# s3://lotus-sandbox-ml-preprocessing-data/pp_prod_artifacts/.
#
# Trust mirrors ecr_iam.tf: main branch, the data-sandbox-infra environment, and
# only the workflow file(s) allowed to publish. For a reusable workflow the
# job_workflow_ref claim is the called workflow, so deploy-feature-engineering.yml
# is trusted whether it is run directly or called from terraform-data-sandbox.yml.

locals {
  fe_artifacts_bucket_name = "lotus-sandbox-ml-preprocessing-data"
  fe_artifacts_prefix      = "pp_prod_artifacts"
  fe_artifacts_bucket_arn  = "arn:${data.aws_partition.current.partition}:s3:::${local.fe_artifacts_bucket_name}"
}

data "aws_iam_policy_document" "fe_artifacts_deploy_assume_role" {
  statement {
    sid     = "GheOidcFeatureEngineeringDeploy"
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
      values   = ["repo:data-and-ai/Project_Lotus:environment:data-sandbox-infra"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.enstream.ghe.com:job_workflow_ref"
      values   = ["data-and-ai/Project_Lotus/.github/workflows/deploy-feature-engineering.yml@refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "fe_artifacts_deploy" {
  name                 = "${local.name_prefix}-fe-artifacts-deploy-role"
  assume_role_policy   = data.aws_iam_policy_document.fe_artifacts_deploy_assume_role.json
  permissions_boundary = local.permissions_boundary
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "fe_artifacts_deploy_access" {
  name = "fe-artifacts-s3-publish"
  role = aws_iam_role.fe_artifacts_deploy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListArtifactPrefix"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.fe_artifacts_bucket_arn
        Condition = {
          StringLike = {
            "s3:prefix" = [local.fe_artifacts_prefix, "${local.fe_artifacts_prefix}/*"]
          }
        }
      },
      {
        Sid    = "PublishArtifacts"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload"
        ]
        Resource = "${local.fe_artifacts_bucket_arn}/${local.fe_artifacts_prefix}/*"
      }
    ]
  })
}

output "fe_artifacts_deploy_role_arn" {
  description = "ARN of the role assumed by GitHub Actions to publish feature-engineering EMR artifacts to S3."
  value       = aws_iam_role.fe_artifacts_deploy.arn
}
