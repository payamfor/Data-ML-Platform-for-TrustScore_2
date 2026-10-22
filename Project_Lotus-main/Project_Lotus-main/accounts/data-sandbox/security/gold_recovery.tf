# Both failure-only recovery applications verify the active tagged workflow
# cluster and the terminal state of the exact failed Gold step. This is
# metadata read only: no terminate/start/submit permission is added to the job
# runtime role.
resource "aws_iam_role_policy" "gold_recovery_cluster_read" {
  name = "${local.name_prefix}-gold-recovery-cluster-read"
  role = aws_iam_role.gold_job_runtime.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "VerifyGoldRecoveryPriorCluster"
      Effect = "Allow"
      Action = [
        "elasticmapreduce:DescribeCluster",
        "elasticmapreduce:ListSteps"
      ]
      Resource = "arn:${data.aws_partition.current.partition}:elasticmapreduce:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:cluster/*"
    }]
  })
}
