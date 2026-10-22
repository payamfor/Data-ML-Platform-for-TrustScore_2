resource "aws_lambda_layer_version" "bronze_schema_validator_pyarrow" {
  layer_name          = "${var.project}-${var.environment}-bronze-schema-validator-pyarrow"
  description         = "PyArrow dependency layer for Bronze schema validator Lambda."
  filename            = "${path.module}/layers/pyarrow_layer.zip"
  source_code_hash    = filebase64sha256("${path.module}/layers/pyarrow_layer.zip")
  compatible_runtimes = ["python3.12"]

  compatible_architectures = [
    "x86_64"
  ]
}