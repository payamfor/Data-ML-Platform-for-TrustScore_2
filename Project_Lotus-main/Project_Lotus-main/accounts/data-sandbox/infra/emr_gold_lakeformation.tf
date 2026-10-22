# Lake Formation grants for the persistent Gold EMR runtime role.
#
# These grants intentionally live in INFRA rather than SECURITY because
# gha-tulip-data-sandbox-infra is the Lake Formation Data Lake Administrator.
#
# The role itself is created in ../security/emr_gold_runtime.tf and resolved
# here by name: the security root applies before infra in CI, and a name
# lookup avoids a remote-state dependency between the two roots.

data "aws_iam_role" "gold_job_runtime" {
  name = "${var.project}-${var.environment}-gold-job-runtime"
}


# Runtime role needs access to discover/read the Gold Glue namespace.
resource "aws_lakeformation_permissions" "gold_runtime_database" {
  principal = data.aws_iam_role.gold_job_runtime.arn

  permissions = [
    "DESCRIBE"
  ]

  database {
    name = aws_glue_catalog_database.gold[0].name
  }
}

resource "aws_lakeformation_permissions" "feature_engineering_runtime_gold_database" {
  principal = data.aws_iam_role.silver_job_runtime.arn

  permissions = [
    "DESCRIBE"
  ]

  database {
    name = aws_glue_catalog_database.gold[0].name
  }
}

resource "aws_lakeformation_permissions" "feature_engineering_runtime_gold_source_tables" {
  for_each = toset([
    "account_changes_canonical_events",
    "msisdn_lifecycle_account_customer_mapping",
    "gold_run_control",
    "onboarding_nbc"
  ])

  principal = data.aws_iam_role.silver_job_runtime.arn

  permissions = [
    "DESCRIBE",
    "SELECT"
  ]

  table {
    database_name = aws_glue_catalog_database.gold[0].name
    name          = aws_glue_catalog_table.gold[each.key].name
  }
}

resource "aws_lakeformation_permissions" "feature_engineering_runtime_run_control_table" {
  principal = data.aws_iam_role.silver_job_runtime.arn

  permissions = [
    "ALL"
  ]

  table {
    database_name = aws_glue_catalog_database.gold[0].name
    name          = aws_glue_catalog_table.gold["feature_engineering_run_control"].name
  }
}


# Gold Spark merges into every table: MERGE performs row-level updates,
# inserts and deletes, so Full Table Access (ALL) is required on all Gold tables.
resource "aws_lakeformation_permissions" "gold_runtime_tables" {
  for_each = local.gold_table_definitions

  principal = data.aws_iam_role.gold_job_runtime.arn

  permissions = [
    "ALL"
  ]

  table {
    database_name = aws_glue_catalog_database.gold[0].name
    name          = aws_glue_catalog_table.gold[each.key].name
  }
}


# The Gold pipeline reads the Silver tables as its input, so the same runtime
# role needs read access to the Silver namespace. SELECT and DESCRIBE only -
# nothing in the Gold job writes back to Silver.
resource "aws_lakeformation_permissions" "gold_runtime_silver_database" {
  principal = data.aws_iam_role.gold_job_runtime.arn

  permissions = [
    "DESCRIBE"
  ]

  database {
    name = aws_glue_catalog_database.silver[0].name
  }
}

resource "aws_lakeformation_permissions" "gold_runtime_silver_tables" {
  for_each = local.table_definitions

  principal = data.aws_iam_role.gold_job_runtime.arn

  permissions = [
    "DESCRIBE",
    "SELECT"
  ]

  table {
    database_name = aws_glue_catalog_database.silver[0].name
    name          = aws_glue_catalog_table.silver[each.key].name
  }
}
