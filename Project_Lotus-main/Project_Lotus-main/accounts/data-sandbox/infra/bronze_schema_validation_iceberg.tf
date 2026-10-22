resource "aws_glue_catalog_database" "bronze_schema_validator_control" {
  name = local.bronze_schema_validator_control_database_name
}

resource "aws_glue_catalog_table" "bronze_schema_validation" {
  name          = local.bronze_schema_validator_control_table_name
  database_name = aws_glue_catalog_database.bronze_schema_validator_control.name
  description   = "File-level Bronze Parquet schema validation control table."

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = 2

      iceberg_table_input {
        location = local.bronze_schema_validator_control_table_location

        schema {
          schema_id = 0
          type      = "struct"

          fields {
            id       = 1
            name     = "file_key"
            required = true
            type     = jsonencode("string")
          }

          fields {
            id       = 2
            name     = "attempt_id"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 3
            name     = "dataset_name"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 4
            name     = "folder_path"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 5
            name     = "file_name"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 6
            name     = "file_path"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 7
            name     = "file_etag"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 8
            name     = "file_size"
            required = false
            type     = jsonencode("long")
          }

          fields {
            id       = 9
            name     = "folder_file_count"
            required = false
            type     = jsonencode("int")
          }

          fields {
            id       = 10
            name     = "schema_version"
            required = false
            type     = jsonencode("int")
          }

          fields {
            id       = 11
            name     = "status"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 12
            name     = "added_columns_json"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 13
            name     = "removed_columns_json"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 14
            name     = "datatype_changes_json"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 15
            name     = "actual_schema_json"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 16
            name     = "error_message"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 17
            name     = "attempt_count"
            required = false
            type     = jsonencode("int")
          }

          fields {
            id       = 18
            name     = "validation_start_ts"
            required = false
            type     = jsonencode("timestamp")
          }

          fields {
            id       = 19
            name     = "validation_end_ts"
            required = false
            type     = jsonencode("timestamp")
          }
        }

        properties = {
          "format-version" = "2"
        }
      }
    }
  }
}
