# Lake Formation grants for the Silver -> Gold control table.
#
# Three principals touch it, with deliberately different grants:
#
#   control Lambda  ALL      - it is the only writer (MERGE from Athena)
#   Gold runtime    SELECT   - Gold may read why it was released, never write it
#   Silver runtime  SELECT   - diagnostic read; Silver's own writes go through
#                              the Lambda, so it needs no write grant here

data "aws_iam_role" "silver_gold_control_lf" {
  name = "${var.project}-${var.environment}-silver-gold-control-role"
}

resource "aws_lakeformation_permissions" "silver_gold_control_database" {
  principal = data.aws_iam_role.silver_gold_control_lf.arn

  permissions = [
    "DESCRIBE"
  ]

  database {
    name = aws_glue_catalog_database.bronze_schema_validator_control.name
  }
}

resource "aws_lakeformation_permissions" "silver_gold_control_table" {
  principal = data.aws_iam_role.silver_gold_control_lf.arn

  permissions = [
    "ALL"
  ]

  table {
    database_name = aws_glue_catalog_database.bronze_schema_validator_control.name
    name          = aws_glue_catalog_table.silver_gold_control.name
  }
}

# The Bronze validation table is also read by the control Lambda's Gate 1 query
# path when an operator runs the gate check through it. DESCRIBE on the
# database above is not sufficient to read a table's rows.
resource "aws_lakeformation_permissions" "silver_gold_control_reads_bronze_validation" {
  principal = data.aws_iam_role.silver_gold_control_lf.arn

  permissions = [
    "SELECT"
  ]

  table {
    database_name = aws_glue_catalog_database.bronze_schema_validator_control.name
    name          = aws_glue_catalog_table.bronze_schema_validation.name
  }
}

# ---------------------------------------------------------------------------
# EMR runtime roles: read-only on the handoff control table.
#
# The role lookups themselves live in emr_gold_lakeformation.tf and
# emr_silver_lakeformation.tf and are reused here.
# ---------------------------------------------------------------------------

resource "aws_lakeformation_permissions" "silver_gold_control_gold_runtime_database" {
  principal = data.aws_iam_role.gold_job_runtime.arn

  permissions = ["DESCRIBE"]

  database {
    name = aws_glue_catalog_database.bronze_schema_validator_control.name
  }
}

resource "aws_lakeformation_permissions" "silver_gold_control_gold_runtime_table" {
  principal = data.aws_iam_role.gold_job_runtime.arn

  permissions = ["SELECT"]

  table {
    database_name = aws_glue_catalog_database.bronze_schema_validator_control.name
    name          = aws_glue_catalog_table.silver_gold_control.name
  }
}

resource "aws_lakeformation_permissions" "silver_gold_control_silver_runtime_database" {
  principal = data.aws_iam_role.silver_job_runtime.arn

  permissions = ["DESCRIBE"]

  database {
    name = aws_glue_catalog_database.bronze_schema_validator_control.name
  }
}

resource "aws_lakeformation_permissions" "silver_gold_control_silver_runtime_table" {
  principal = data.aws_iam_role.silver_job_runtime.arn

  permissions = ["SELECT"]

  table {
    database_name = aws_glue_catalog_database.bronze_schema_validator_control.name
    name          = aws_glue_catalog_table.silver_gold_control.name
  }
}
