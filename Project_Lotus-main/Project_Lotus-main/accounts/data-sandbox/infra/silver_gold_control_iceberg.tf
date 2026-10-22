# Silver -> Gold handoff control table (Gate 2).
#
# One row per (run_date, dataset_name), keyed by control_key so that a Silver
# retry updates its row instead of adding a second answer for the same
# question. Gold is released only when every enabled dataset has a row here
# with status = 'succeeded' for the run date.
#
# Field IDs are stable Iceberg identifiers. Do not renumber an existing field;
# additive evolution takes the next unused ID.

resource "aws_glue_catalog_table" "silver_gold_control" {
  name          = local.silver_gold_control_table_name
  database_name = aws_glue_catalog_database.bronze_schema_validator_control.name
  description   = "Silver-to-Gold handoff control table. One row per run date and Silver dataset."

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = 2

      iceberg_table_input {
        location = local.silver_gold_control_table_location

        schema {
          schema_id = 0
          type      = "struct"

          fields {
            id       = 1
            name     = "control_key"
            required = true
            type     = jsonencode("string")
          }

          fields {
            id       = 2
            name     = "run_date"
            required = false
            type     = jsonencode("date")
          }

          fields {
            id       = 3
            name     = "dataset_name"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 4
            name     = "silver_run_id"
            required = false
            type     = jsonencode("string")
          }

          # succeeded | failed | blocked.
          # 'blocked' is Silver exit code 2 and gates Gold exactly like 'failed'.
          fields {
            id       = 5
            name     = "status"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 6
            name     = "exit_code"
            required = false
            type     = jsonencode("int")
          }

          # 1 on the first attempt, 2 after the single permitted retry.
          fields {
            id       = 7
            name     = "attempt_number"
            required = false
            type     = jsonencode("int")
          }

          fields {
            id       = 8
            name     = "rows_published"
            required = false
            type     = jsonencode("long")
          }

          fields {
            id       = 9
            name     = "bronze_folder_path"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 10
            name     = "summary_uri"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 11
            name     = "error_message"
            required = false
            type     = jsonencode("string")
          }

          fields {
            id       = 12
            name     = "recorded_ts"
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
