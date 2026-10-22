# Lake Formation grants for the persistent Silver EMR runtime role.
#
# These grants intentionally live in INFRA rather than SECURITY because
# the Infra deployment role is responsible for provisioning table access.

data "aws_iam_role" "silver_job_runtime" {
  name = "${var.project}-${var.environment}-silver-job-runtime"
}


locals {
  # Every normal Silver business/enrichment table managed by the shared
  # aws_glue_catalog_table.silver for_each map. Deriving this set from the
  # registry prevents a newly provisioned table from silently missing its
  # Lake Formation writer grant.
  silver_runtime_lakeformation_tables = toset(keys(local.table_definitions))
}


# Runtime role discovers the namespace and can create the original _1 demo.
# CREATE_TABLE is database-scoped in Lake Formation; the Security IAM policy
# limits glue:CreateTable to account_changes_batch_1. The runtime receives LF
# table permissions as the creator, so no grant depends on the demo table
# existing before the manifest job runs. V2 onboarding uses the same runtime
# identity and adds its developer reader grant before contract activation.
resource "aws_lakeformation_permissions" "silver_runtime_database" {
  principal = data.aws_iam_role.silver_job_runtime.arn

  permissions = [
    "DESCRIBE",
    "CREATE_TABLE"
  ]

  database {
    name = aws_glue_catalog_database.silver[0].name
  }
}


# Silver Spark performs writes/updates against the business/enrichment tables.
# Full Table Access (ALL) is required for Iceberg write/delete operations.
resource "aws_lakeformation_permissions" "silver_runtime_tables" {
  for_each = local.silver_runtime_lakeformation_tables

  principal = data.aws_iam_role.silver_job_runtime.arn

  permissions = [
    "ALL"
  ]

  table {
    database_name = aws_glue_catalog_database.silver[0].name
    name          = aws_glue_catalog_table.silver[each.value].name
  }
}


# Each quarantine table is a standalone resource (not part of the silver
# for_each map), so each gets its own LF grant.

resource "aws_lakeformation_permissions" "silver_runtime_quarantine_ac" {
  principal   = data.aws_iam_role.silver_job_runtime.arn
  permissions = ["ALL"]

  table {
    database_name = aws_glue_catalog_database.silver[0].name
    name          = aws_glue_catalog_table.silver_quarantine_account_changes_batch.name
  }
}

resource "aws_lakeformation_permissions" "silver_runtime_quarantine_dl" {
  principal   = data.aws_iam_role.silver_job_runtime.arn
  permissions = ["ALL"]

  table {
    database_name = aws_glue_catalog_database.silver[0].name
    name          = aws_glue_catalog_table.silver_quarantine_device_lookup_batch.name
  }
}

resource "aws_lakeformation_permissions" "silver_runtime_quarantine_ats" {
  principal   = data.aws_iam_role.silver_job_runtime.arn
  permissions = ["ALL"]

  table {
    database_name = aws_glue_catalog_database.silver[0].name
    name          = aws_glue_catalog_table.silver_quarantine_audit_trail_services_3.name
  }
}
