resource "aws_s3_object" "bronze_schema_validator_prefixes" {
  for_each = toset([
    "bronze/",
    "schema_registry/",
    "control/",
    "athena-results/"
  ])

  bucket       = local.bronze_schema_validator_bucket_name
  key          = each.value
  content      = ""
  content_type = "application/x-directory"
}
