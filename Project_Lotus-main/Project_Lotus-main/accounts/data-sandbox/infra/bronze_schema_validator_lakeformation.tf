data "aws_iam_role" "bronze_schema_validator_lf" {
  name = "${var.project}-${var.environment}-bronze-schema-validator-role"
}

resource "aws_lakeformation_permissions" "bronze_schema_validator_control_database" {
  principal = data.aws_iam_role.bronze_schema_validator_lf.arn

  permissions = [
    "DESCRIBE"
  ]

  database {
    name = aws_glue_catalog_database.bronze_schema_validator_control.name
  }
}

resource "aws_lakeformation_permissions" "bronze_schema_validator_control_table" {
  principal = data.aws_iam_role.bronze_schema_validator_lf.arn

  permissions = [
    "ALL"
  ]

  table {
    database_name = aws_glue_catalog_database.bronze_schema_validator_control.name
    name          = aws_glue_catalog_table.bronze_schema_validation.name
  }
}