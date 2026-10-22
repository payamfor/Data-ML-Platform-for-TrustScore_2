# Lake Formation grants for the interactive developer SSO role.
#
# These grants intentionally live in INFRA rather than SECURITY, same as
# emr_silver_lakeformation.tf. The Infra deployment role must have the required
# Glue reads and Lake Formation grant authority; comments do not grant access.
#
# Pipeline runtime roles (bronze/silver/gold job runtime) already have their
# own Lake Formation grants elsewhere in this directory. This file is for the
# human role used to sign in interactively (Incedo-Lotus-Developer), which had
# no grant of its own and so failed on Glue/Athena reads with a Lake Formation
# permission error even though the underlying tables exist.

locals {
  developer_role_arn = "arn:aws:iam::162591926854:role/aws-reserved/sso.amazonaws.com/ca-central-1/AWSReservedSSO_Incedo-Lotus-Developer_8701a4602e438b67"

  developer_databases = {
    control = aws_glue_catalog_database.bronze_schema_validator_control.name
    silver  = aws_glue_catalog_database.silver[0].name
    gold    = aws_glue_catalog_database.gold[0].name
  }

  # Full table access for ad hoc Athena/Iceberg use, matching every pipeline
  # runtime role's table grant elsewhere in this account (see
  # emr_silver_lakeformation.tf, emr_gold_lakeformation.tf,
  # bronze_schema_validator_lakeformation.tf - all "ALL"). This previously
  # excluded ALTER and DROP; that data-only restriction blocked Athena's
  # Iceberg UPDATE/MERGE path for some callers and was widened back to ALL on
  # explicit request. ALL includes DROP TABLE and ALTER TABLE - a developer
  # can now drop or restructure any table in these three databases from
  # Athena, not just its data.
  developer_table_permissions = ["ALL"]
}


# Database-level DESCRIBE. This part worked on the first apply.
resource "aws_lakeformation_permissions" "developer_database" {
  for_each    = local.developer_databases
  principal   = local.developer_role_arn
  permissions = ["DESCRIBE"]

  database {
    name = each.value
  }
}


# Named-table grants, not a database-wide wildcard.
#
# A `table { database_name = ..., wildcard = true }` grant failed here with
# Lake Formation's "Resource does not exist or requester is not authorized"
# AccessDeniedException, on all three databases, while the identical
# principal's DESCRIBE grant on the database itself succeeded. Named-table
# grants are the form emr_silver_lakeformation.tf already uses successfully
# in this account, so every grant below mirrors that rather than the wildcard
# selector.
#
# Each named-table resource here has to be listed explicitly, matching how
# emr_silver_lakeformation.tf already grants the Silver runtime role: the
# quarantine tables are standalone resources outside the for_each maps, so
# each gets its own grant.

resource "aws_lakeformation_permissions" "developer_control_table" {
  principal   = local.developer_role_arn
  permissions = local.developer_table_permissions

  table {
    database_name = local.developer_databases.control
    name          = aws_glue_catalog_table.bronze_schema_validation.name
  }
}

# Silver -> Gold handoff control table. Without this grant the developer role
# can see the control database (via developer_database["control"]) but cannot
# read or query the silver_gold_control table rows in Athena.
resource "aws_lakeformation_permissions" "developer_silver_gold_control_table" {
  principal   = local.developer_role_arn
  permissions = local.developer_table_permissions

  table {
    database_name = local.developer_databases.control
    name          = aws_glue_catalog_table.silver_gold_control.name
  }
}

resource "aws_lakeformation_permissions" "developer_silver_tables" {
  for_each = local.table_definitions

  principal   = local.developer_role_arn
  permissions = local.developer_table_permissions

  table {
    database_name = local.developer_databases.silver
    name          = aws_glue_catalog_table.silver[each.key].name
  }
}

# V2 manifest onboarding owns the demo reader grant on account_changes_batch_1.
# Stop Infra from attempting the failed grant or later revoking a grant made by
# onboarding. If an earlier grant reached Terraform state, forget it without an
# AWS revoke. Normal business-table grants above/below remain Terraform-managed.
removed {
  from = aws_lakeformation_permissions.developer_manifest_demo_table
  lifecycle {
    destroy = false
  }
}

resource "aws_lakeformation_permissions" "developer_silver_quarantine_ac" {
  principal   = local.developer_role_arn
  permissions = local.developer_table_permissions

  table {
    database_name = local.developer_databases.silver
    name          = aws_glue_catalog_table.silver_quarantine_account_changes_batch.name
  }
}

resource "aws_lakeformation_permissions" "developer_silver_quarantine_dl" {
  principal   = local.developer_role_arn
  permissions = local.developer_table_permissions

  table {
    database_name = local.developer_databases.silver
    name          = aws_glue_catalog_table.silver_quarantine_device_lookup_batch.name
  }
}

resource "aws_lakeformation_permissions" "developer_silver_quarantine_ats" {
  principal   = local.developer_role_arn
  permissions = local.developer_table_permissions

  table {
    database_name = local.developer_databases.silver
    name          = aws_glue_catalog_table.silver_quarantine_audit_trail_services_3.name
  }
}

resource "aws_lakeformation_permissions" "developer_silver_quarantine_tu_portps" {
  principal   = local.developer_role_arn
  permissions = local.developer_table_permissions

  table {
    database_name = local.developer_databases.silver
    name          = aws_glue_catalog_table.enrichment_quarantine_tu_portps.name
  }
}

resource "aws_lakeformation_permissions" "developer_silver_quarantine_mno_activation" {
  principal   = local.developer_role_arn
  permissions = local.developer_table_permissions

  table {
    database_name = local.developer_databases.silver
    name          = aws_glue_catalog_table.enrichment_quarantine_mno_activation.name
  }
}

resource "aws_lakeformation_permissions" "developer_gold_tables" {
  for_each = local.gold_table_definitions

  principal   = local.developer_role_arn
  permissions = local.developer_table_permissions

  table {
    database_name = local.developer_databases.gold
    name          = aws_glue_catalog_table.gold[each.key].name
  }
}
