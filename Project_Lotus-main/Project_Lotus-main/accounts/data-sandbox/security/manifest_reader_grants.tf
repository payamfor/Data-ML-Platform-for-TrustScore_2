# V2 manifest onboarding runs as the existing Silver runtime role. It grants
# SELECT/DESCRIBE to a platform-approved reader before activating the contract.
# Manage this IAM capability here, not through a console policy or EMR script.
#
# Lake Formation does not support resource ARNs for GrantPermissions. "*" is
# required for this one action; the region condition is NOT a table/reader
# restriction. AWS still checks the caller's LF grant authority on each table.
# The reviewed v2 job enforces the approved reader and exact target checks.
# Any code using this shared runtime role gains the same IAM capability, so
# only trusted operators may change job code or its approved-reader arguments.
locals {
  silver_manifest_reader_grants_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "GrantManifestTableReaderAccess"
      Effect   = "Allow"
      Action   = ["lakeformation:GrantPermissions"]
      Resource = "*"
      Condition = {
        StringEquals = {
          "aws:RequestedRegion" = data.aws_region.current.region
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "silver_manifest_reader_grants" {
  name   = "${local.name_prefix}-silver-manifest-reader-grants"
  role   = aws_iam_role.silver_job_runtime.id
  policy = local.silver_manifest_reader_grants_policy
}
