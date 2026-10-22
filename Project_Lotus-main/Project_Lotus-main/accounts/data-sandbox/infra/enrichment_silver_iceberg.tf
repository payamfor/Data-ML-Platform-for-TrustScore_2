# Resources owned only by the standalone PortPS/IDV enrichment Silver job.
# The existing daily silver_pipeline state machine and quarantine resources are
# intentionally not modified by this file.

locals {
  enrichment_quarantine_base         = "s3://${var.silver_bucket_name}/quarantine/iceberg"
  enrichment_quarantine_tu_location  = "${local.enrichment_quarantine_base}/silver_quarantine_tu_portps/"
  enrichment_quarantine_mno_location = "${local.enrichment_quarantine_base}/silver_quarantine_mno_activation/"

  _enrichment_quarantine_reason_fields = [
    { id = 1, name = "quarantine_id", type = "string", doc = "Deterministic rejected-record identifier." },
    { id = 2, name = "run_id", type = "string", doc = "Enrichment Silver run identifier." },
    { id = 3, name = "run_date", type = "date", doc = "Requested Toronto business date." },
    { id = 4, name = "validation_stage", type = "string", doc = "RAW_DQ, CANDIDATE_DQ or TARGET_CONFLICT." },
    { id = 5, name = "error_rule_ids_json", type = "string", doc = "Blocking rule IDs." },
    { id = 6, name = "warning_rule_ids_json", type = "string", doc = "Warning rule IDs." },
    { id = 7, name = "error_details_json", type = "string", doc = "Structured blocking-rule details." },
    { id = 8, name = "raw_payload", type = "string", doc = "PII-redacted source evidence." },
    { id = 9, name = "source_file", type = "string", doc = "Hashed-response part URI." },
    { id = 10, name = "quarantined_at", type = "timestamp", doc = "UTC quarantine time." },
    { id = 11, name = "quarantined_date", type = "date", doc = "Iceberg partition date." },
    { id = 12, name = "recovery_status", type = "string", doc = "Operator recovery status." },
  ]

  _enrichment_quarantine_tu_fields = [
    { id = 101, name = "request_id", type = "string", doc = "Provider call identifier." },
    { id = 102, name = "record_id", type = "string", doc = "Deterministic PortPS event identifier." },
    { id = 103, name = "msisdn", type = "string", doc = "Nullable; disabled by default in YAML." },
    { id = 104, name = "phone_number_AC_hash", type = "string", doc = "Account-change phone hash." },
    { id = 105, name = "phone_number_AT_hash", type = "string", doc = "Account-token phone hash." },
    { id = 106, name = "ownership_status", type = "string", doc = "PortPS ownership status." },
    { id = 107, name = "ownership_last_alt_spid", type = "string", doc = "Previous alternate SPID." },
    { id = 108, name = "region", type = "string", doc = "Provider region." },
    { id = 109, name = "state", type = "string", doc = "Provider state/province." },
    { id = 110, name = "event_timestamp", type = "timestamp", doc = "Port event time." },
    { id = 111, name = "event_date", type = "date", doc = "Port event date." },
    { id = 112, name = "event_type", type = "string", doc = "Inter- or intra-SPID port." },
    { id = 113, name = "event_seq", type = "int", doc = "Sequence within number history." },
    { id = 114, name = "lnp_type", type = "int", doc = "Line-number portability type." },
    { id = 115, name = "is_inter_carrier_port", type = "boolean", doc = "Inter-carrier indicator." },
    { id = 116, name = "from_spid", type = "string", doc = "Previous SPID." },
    { id = 117, name = "from_mno_raw", type = "string", doc = "Previous carrier label." },
    { id = 118, name = "from_mno_std", type = "string", doc = "Previous standardized carrier." },
    { id = 119, name = "to_spid", type = "string", doc = "New SPID." },
    { id = 120, name = "to_mno_raw", type = "string", doc = "New carrier label." },
    { id = 121, name = "to_mno_std", type = "string", doc = "New standardized carrier." },
    { id = 122, name = "sv_type", type = "string", doc = "Provider service type." },
    { id = 123, name = "lrn", type = "string", doc = "Location routing number." },
    { id = 124, name = "correlation_id", type = "string", doc = "Enterprise correlation ID." },
    { id = 125, name = "source_name", type = "string", doc = "Source lineage name." },
    { id = 126, name = "schema_version", type = "int", doc = "Contract version." },
    { id = 127, name = "ingestion_ts", type = "timestamp", doc = "Hashed response ingestion time." },
    { id = 128, name = "last_updated_ts", type = "timestamp", doc = "Last source update time." },
  ]

  _enrichment_quarantine_mno_fields = [
    { id = 101, name = "request_id", type = "string", doc = "Provider call identifier." },
    { id = 102, name = "msisdn", type = "string", doc = "Nullable; disabled by default in YAML." },
    { id = 103, name = "phone_number_AC_hash", type = "string", doc = "Account-change phone hash." },
    { id = 104, name = "phone_number_AT_hash", type = "string", doc = "Account-token phone hash." },
    { id = 105, name = "api_ts", type = "timestamp", doc = "IDV source event time." },
    { id = 106, name = "mno", type = "string", doc = "Standardized carrier." },
    { id = 107, name = "activation_date", type = "date", doc = "Contract activation date." },
    { id = 108, name = "response_code", type = "string", doc = "Provider response code." },
    { id = 109, name = "created_ts", type = "timestamp", doc = "Hashed response ingestion time." },
  ]

  enrichment_quarantine_tu_fields  = concat(local._enrichment_quarantine_reason_fields, local._enrichment_quarantine_tu_fields)
  enrichment_quarantine_mno_fields = concat(local._enrichment_quarantine_reason_fields, local._enrichment_quarantine_mno_fields)
  enrichment_quarantine_properties = {
    "format-version"                  = tostring(var.iceberg_format_version)
    "write.format.default"            = "parquet"
    "write.parquet.compression-codec" = "zstd"
    "write.spark.accept-any-schema"   = "false"
  }
}

resource "aws_glue_catalog_table" "enrichment_quarantine_tu_portps" {
  depends_on    = [aws_glue_catalog_database.silver]
  catalog_id    = data.aws_caller_identity.current.account_id
  database_name = var.glue_database_name
  name          = "silver_quarantine_tu_portps"
  description   = "Rejected rows from the standalone PortPS enrichment Silver job."

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = var.iceberg_format_version
      iceberg_table_input {
        location   = local.enrichment_quarantine_tu_location
        properties = local.enrichment_quarantine_properties
        schema {
          schema_id            = 0
          type                 = "struct"
          identifier_field_ids = []
          dynamic "fields" {
            for_each = local.enrichment_quarantine_tu_fields
            content {
              id       = fields.value.id
              name     = fields.value.name
              required = false
              type     = jsonencode(fields.value.type)
              doc      = fields.value.doc
            }
          }
        }
        partition_spec {
          spec_id = 0
          fields {
            field_id  = 1000
            name      = "quarantined_date"
            source_id = 11
            transform = "identity"
          }
        }
      }
    }
  }
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [open_table_format_input]
  }
}

resource "aws_glue_catalog_table" "enrichment_quarantine_mno_activation" {
  depends_on    = [aws_glue_catalog_database.silver]
  catalog_id    = data.aws_caller_identity.current.account_id
  database_name = var.glue_database_name
  name          = "silver_quarantine_mno_activation"
  description   = "Rejected rows from the standalone IDV/MNO enrichment Silver job."

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = var.iceberg_format_version
      iceberg_table_input {
        location   = local.enrichment_quarantine_mno_location
        properties = local.enrichment_quarantine_properties
        schema {
          schema_id            = 0
          type                 = "struct"
          identifier_field_ids = []
          dynamic "fields" {
            for_each = local.enrichment_quarantine_mno_fields
            content {
              id       = fields.value.id
              name     = fields.value.name
              required = false
              type     = jsonencode(fields.value.type)
              doc      = fields.value.doc
            }
          }
        }
        partition_spec {
          spec_id = 0
          fields {
            field_id  = 1000
            name      = "quarantined_date"
            source_id = 11
            transform = "identity"
          }
        }
      }
    }
  }
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [open_table_format_input]
  }
}

resource "aws_lakeformation_permissions" "enrichment_runtime_quarantine_tu" {
  principal   = data.aws_iam_role.silver_job_runtime.arn
  permissions = ["ALL"]
  table {
    database_name = aws_glue_catalog_database.silver[0].name
    name          = aws_glue_catalog_table.enrichment_quarantine_tu_portps.name
  }
}

resource "aws_lakeformation_permissions" "enrichment_runtime_quarantine_mno" {
  principal   = data.aws_iam_role.silver_job_runtime.arn
  permissions = ["ALL"]
  table {
    database_name = aws_glue_catalog_database.silver[0].name
    name          = aws_glue_catalog_table.enrichment_quarantine_mno_activation.name
  }
}

output "enrichment_quarantine_tu_iceberg_table" {
  description = "TU PortPS quarantine table; pass as --quarantine-table for tu_portps runs."
  value       = "glue_catalog.${var.glue_database_name}.${aws_glue_catalog_table.enrichment_quarantine_tu_portps.name}"
}
output "enrichment_quarantine_tu_iceberg_location" {
  description = "TU PortPS quarantine S3 location; pass as --quarantine-table-location."
  value       = local.enrichment_quarantine_tu_location
}
output "enrichment_quarantine_mno_iceberg_table" {
  description = "MNO activation quarantine table; pass as --quarantine-table for mno_activation runs."
  value       = "glue_catalog.${var.glue_database_name}.${aws_glue_catalog_table.enrichment_quarantine_mno_activation.name}"
}
output "enrichment_quarantine_mno_iceberg_location" {
  description = "MNO activation quarantine S3 location; pass as --quarantine-table-location."
  value       = local.enrichment_quarantine_mno_location
}
