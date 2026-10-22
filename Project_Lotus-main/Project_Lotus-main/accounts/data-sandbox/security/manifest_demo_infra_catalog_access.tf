# InfraSec owns the shared workflow's gha-<project>-data-<environment>-infra role.
# The Security CI role has an explicit identity-policy Deny on PutRolePolicy for
# that role. Generate a handover document only; do not attach it from this root
# or change the CI role's trust policy, boundary or administrative permissions.
# Retained as a diagnostic handover only. V2 onboarding now owns the demo
# reader grant and uses manifest_reader_grants.tf on the Silver runtime instead.
# These outputs are not prerequisites and attach no policy; see ../infra/MANIFEST_DEMO.md.
locals {
  manifest_demo_infra_catalog_read_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DescribeManifestDemoDatabase"
        Effect = "Allow"
        Action = ["glue:GetDatabase"]
        Resource = [
          local.silver_runtime_glue_catalog_arn,
          local.silver_runtime_glue_database_arn
        ]
      },
      {
        Sid    = "DescribeManifestDemoTable"
        Effect = "Allow"
        Action = ["glue:GetTable"]
        Resource = [
          local.silver_runtime_glue_catalog_arn,
          local.silver_runtime_glue_database_arn,
          "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${local.silver_runtime_glue_database_name}/account_changes_batch_1"
        ]
      }
    ]
  })
}

output "manifest_demo_infra_catalog_read_policy_json" {
  description = "Scoped Glue metadata-read policy for InfraSec to apply through the Infra deployment role's owning stack. This output grants no permissions."
  value       = local.manifest_demo_infra_catalog_read_policy
}

output "manifest_demo_infra_catalog_grantor_role_name" {
  description = "Externally managed Infra deployment role for the optional original-table access handover; this output changes no permissions."
  value       = "gha-${var.project}-data-${var.environment}-infra"
}
