locals {
  bronze_schema_registry_documents = {
    "account_changes_batch/current.json" = {
      active_version = 1
    }

    "account_changes_batch/v1.json" = {
      dataset_name   = "account_changes_batch"
      schema_version = 1
      columns = [
        {
          name = "id"
          type = "bigint"
        },
        {
          name = "change_key"
          type = "string"
        },
        {
          name = "change_type"
          type = "string"
        },
        {
          name = "notes"
          type = "string"
        },
        {
          name = "timestamp"
          type = "datetime"
        }
      ]
    }
  }
}

resource "aws_s3_object" "bronze_schema_registry" {
  for_each = local.bronze_schema_registry_documents

  bucket       = local.bronze_schema_validator_bucket_name
  key          = "${local.bronze_schema_validator_schema_prefix}/${each.key}"
  content      = jsonencode(each.value)
  content_type = "application/json"
}
