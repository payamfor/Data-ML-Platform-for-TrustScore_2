# Provisions the curated Gold Glue database and its ten Iceberg tables.
#
# The lineage schemas here are transcribed mechanically from the client POC
# pipeline's single source of truth:
#
#   project_lotus/project_lotus/gold/trust_score_05/lineage/schemas.py
#
# The feature engineering run-control schema is transcribed from
# feature_engineering_emr_v1/scripts/create_run_control_table.sql.
#
# Column order, names and types are FROZEN there ("The Gold tables already
# exist in production and this pipeline merges into them"), so this file must
# not reorder or retype a column. Field IDs are positional (1..N in schema
# order) and must never be renumbered after the first apply.
#
# Two lineages plus four operational tables:
#
#   account_changes_canonical_events            lineage 1, stage 1   ( 9 cols)
#   msisdn_lifecycle                            lineage 1, stage 2   (28 cols)
#   msisdn_lifecycle_account_mapping            lineage 1, stage 3   (16 cols)
#   msisdn_lifecycle_account_customer_mapping   lineage 1, stage 4   (17 cols)
#   normalized_canonical_events                 lineage 1, stage 5   (18 cols)
#   msisdn_imei_map                             lineage 2            (10 cols)
#   gold_run_control                            watermark bookkeeping ( 8 cols)
#   feature_engineering_run_control            feature engineering watermark (10 cols)
#   dq_metrics                                  tiered check results (19 cols)
#   onboarding_nbc                              NBC fraud labels (3 cols)
#
# Partitioning follows io/merge_sink.py::ensure_exists exactly: a `date`
# partition column is an identity partition, anything else (always a timestamp
# here) becomes `day(col)`. gold_run_control is deliberately unpartitioned,
# matching io/control.py.
#
# Sort order matters and is not decoration. merge_sink.py: "Write order is a
# requirement, not a preference: phone_number_AC_hash is not the partition
# column, so per-file min/max on the sort key is the only pruning a
# slice-scoped merge can use." The job applies it via ALTER TABLE only on a
# table it creates itself, and it skips creation when the table already
# exists - so provisioning here without sort_order would silently drop it.

variable "gold_bucket_name" {
  description = "S3 bucket backing the Gold Iceberg tables."
  type        = string
  default     = "lotus-sandbox-gold-curated-data"
}

variable "gold_glue_database_name" {
  description = "Glue database for the curated Gold Iceberg tables. Pinned by security/emr_iam.tf and security/sagemaker_iam.tf, which derive the same name; changing it here alone silently breaks those grants."
  type        = string
  default     = "lotus_sandbox_gold_curated"
}

variable "create_gold_glue_database" {
  description = "Create the Gold Glue database. Set false and import when it already exists: terraform import 'aws_glue_catalog_database.gold[0]' <account_id>:<database_name>"
  type        = bool
  default     = true
}


locals {
  gold_table_root = "s3://${var.gold_bucket_name}/${trim(var.table_root_prefix, "/")}"

  # Base properties for every Gold table.
  _gold_base_properties = {
    "format-version"                  = tostring(var.iceberg_format_version)
    "write.format.default"            = "parquet"
    "write.parquet.compression-codec" = "zstd"
    # Gold column order, names and types are frozen upstream in schemas.py.
    # Schema drift must be a reviewed migration, never an implicit merge.
    "write.spark.accept-any-schema" = "false"
  }

  # The six merged lineage tables additionally carry the two properties
  # merge_sink.py sets, both of which exist because of production incidents
  # recorded in its comments:
  #
  #   write.distribution-mode = hash
  #     A declared write order makes Iceberg default to `range`, which samples
  #     and globally sorts on every write. That ran the executors out of local
  #     disk on a full rebuild. `hash` distributes by partition instead.
  #
  #   read.split.open-file-cost = 134217728
  #     Charges every file the full 128MB split target so one file lands per
  #     task. The default 4MB let one task open up to 32 files, exhausting the
  #     S3 connection pool ("Timeout waiting for connection from pool").
  _gold_merge_properties = merge(local._gold_base_properties, {
    "write.distribution-mode"   = "hash"
    "read.split.open-file-cost" = "134217728"
  })
}


locals {
  # ---------------------------------------------------------------------
  # Field lists - generated from schemas.py, positional IDs in schema order
  # ---------------------------------------------------------------------

  gold_canonical_events_fields = [
    { id = 1, name = "ac_event_id", type = "string" },
    { id = 2, name = "phone_number_AC_hash", type = "string" },
    { id = 3, name = "mno", type = "string" },
    { id = 4, name = "ac_event_type", type = "string" },
    { id = 5, name = "ac_event_ts", type = "timestamp" },
    { id = 6, name = "correlation_id", type = "string" },
    { id = 7, name = "updated_ts", type = "timestamp" },
    { id = 8, name = "created_ts", type = "timestamp" },
    { id = 9, name = "schema_version", type = "int" },
  ]

  gold_lifecycle_fields = [
    { id = 1, name = "lifecycle_uid", type = "string" },
    { id = 2, name = "phone_number_AC_hash", type = "string" },
    { id = 3, name = "mno", type = "string" },
    { id = 4, name = "mno_segment_id", type = "int" },
    { id = 5, name = "lifecycle_id", type = "int" },
    { id = 6, name = "lifecycle_start_ts", type = "timestamp" },
    { id = 7, name = "lifecycle_end_ts", type = "timestamp" },
    { id = 8, name = "lifecycle_is_open", type = "int" },
    { id = 9, name = "lifecycle_infer_type", type = "string" },
    { id = 10, name = "is_start_inferred", type = "int" },
    { id = 11, name = "is_end_inferred", type = "int" },
    { id = 12, name = "recycle_ind", type = "int" },
    { id = 13, name = "mno_port_ind", type = "int" },
    { id = 14, name = "temporary_pn_ind", type = "int" },
    { id = 15, name = "plan_change_ind", type = "int" },
    { id = 16, name = "plan_change_cnt", type = "int" },
    { id = 17, name = "updated_ts", type = "timestamp" },
    { id = 18, name = "created_ts", type = "timestamp" },
    { id = 19, name = "schema_version", type = "int" },
    { id = 20, name = "start_confidence", type = "string" },
    { id = 21, name = "end_confidence", type = "string" },
    { id = 22, name = "confidence", type = "string" },
    { id = 23, name = "mno_is_supported", type = "int" },
    { id = 24, name = "tu_enriched", type = "int" },
    { id = 25, name = "start_event_id", type = "string" },
    { id = 26, name = "start_event_type", type = "string" },
    { id = 27, name = "end_event_id", type = "string" },
    { id = 28, name = "end_event_type", type = "string" },
  ]

  gold_account_mapping_fields = [
    { id = 1, name = "acct_id", type = "string" },
    { id = 2, name = "phone_number_AC_hash", type = "string" },
    { id = 3, name = "mno", type = "string" },
    { id = 4, name = "lifecycle_uid", type = "string" },
    { id = 5, name = "source_event_id", type = "string" },
    { id = 6, name = "from_ts", type = "timestamp" },
    { id = 7, name = "to_ts", type = "timestamp" },
    { id = 8, name = "seq_order", type = "int" },
    { id = 9, name = "segment_type", type = "string" },
    { id = 10, name = "account_type", type = "string" },
    { id = 11, name = "updated_ts", type = "timestamp" },
    { id = 12, name = "created_ts", type = "timestamp" },
    { id = 13, name = "schema_version", type = "int" },
    { id = 14, name = "link_route", type = "string" },
    { id = 15, name = "link_confidence", type = "string" },
    { id = 16, name = "acct_link_confidence_min", type = "string" },
  ]

  gold_customer_mapping_fields = [
    { id = 1, name = "customer_id", type = "string" },
    { id = 2, name = "acct_id", type = "string" },
    { id = 3, name = "mno", type = "string" },
    { id = 4, name = "phone_number_AC_hash", type = "string" },
    { id = 5, name = "lifecycle_uid", type = "string" },
    { id = 6, name = "seq_order", type = "int" },
    { id = 7, name = "source_event_id", type = "string" },
    { id = 8, name = "from_ts", type = "timestamp" },
    { id = 9, name = "to_ts", type = "timestamp" },
    { id = 10, name = "segment_type", type = "string" },
    { id = 11, name = "account_type", type = "string" },
    { id = 12, name = "updated_ts", type = "timestamp" },
    { id = 13, name = "updated_source", type = "string" },
    { id = 14, name = "created_ts", type = "timestamp" },
    { id = 15, name = "schema_version", type = "int" },
    { id = 16, name = "link_confidence", type = "string" },
    { id = 17, name = "customer_link_confidence_min", type = "string" },
  ]

  gold_normalized_events_fields = [
    { id = 1, name = "customer_id", type = "string" },
    { id = 2, name = "acct_id", type = "string" },
    { id = 3, name = "phone_number_AC_hash", type = "string" },
    { id = 4, name = "phone_number_AT_hash", type = "string" },
    { id = 5, name = "mno", type = "string" },
    { id = 6, name = "event_id", type = "string" },
    { id = 7, name = "event_ts", type = "timestamp" },
    { id = 8, name = "event_family", type = "string" },
    { id = 9, name = "event_type", type = "string" },
    { id = 10, name = "event_source", type = "string" },
    { id = 11, name = "event_weight", type = "double" },
    { id = 12, name = "industry", type = "string" },
    { id = 13, name = "service_provider_name", type = "string" },
    { id = 14, name = "use_case", type = "string" },
    { id = 15, name = "updated_ts", type = "timestamp" },
    { id = 16, name = "updated_source", type = "string" },
    { id = 17, name = "created_ts", type = "timestamp" },
    { id = 18, name = "schema_version", type = "int" },
  ]

  gold_imei_map_fields = [
    { id = 1, name = "phone_number_AC_hash", type = "string" },
    { id = 2, name = "phone_number_AT_hash", type = "string" },
    { id = 3, name = "imei", type = "string" },
    { id = 4, name = "mno", type = "string" },
    { id = 5, name = "tac", type = "string" },
    { id = 6, name = "oldIMEI", type = "string" },
    { id = 7, name = "from_ts", type = "timestamp" },
    { id = 8, name = "to_ts", type = "timestamp" },
    { id = 9, name = "last_updated_ts", type = "timestamp" },
    { id = 10, name = "event_date", type = "date" },
  ]

  gold_run_control_fields = [
    { id = 1, name = "job_name", type = "string" },
    { id = 2, name = "run_id", type = "string" },
    { id = 3, name = "watermark_from", type = "timestamp" },
    { id = 4, name = "watermark_to", type = "timestamp" },
    { id = 5, name = "slice_size", type = "long" },
    { id = 6, name = "status", type = "string" },
    { id = 7, name = "started_ts", type = "timestamp" },
    { id = 8, name = "finished_ts", type = "timestamp" },
  ]

  feature_engineering_run_control_fields = [
    { id = 1, name = "job_name", type = "string" },
    { id = 2, name = "run_id", type = "string" },
    { id = 3, name = "watermark_from", type = "timestamp" },
    { id = 4, name = "watermark_to", type = "timestamp" },
    { id = 5, name = "slice_size", type = "long" },
    { id = 6, name = "status", type = "string" },
    { id = 7, name = "started_ts", type = "timestamp" },
    { id = 8, name = "finished_ts", type = "timestamp" },
    { id = 9, name = "model_type", type = "string" },
    { id = 10, name = "limit_applied", type = "boolean" },
  ]

  onboarding_nbc_fields = [
    { id = 1, name = "phone_number_AC_hash", type = "string" },
    { id = 2, name = "fraud_type", type = "string" },
    { id = 3, name = "fraud_timestamp", type = "timestamp" },
  ]

  gold_dq_metrics_fields = [
    { id = 1, name = "run_id", type = "string" },
    { id = 2, name = "dataset", type = "string" },
    { id = 3, name = "layer", type = "string" },
    { id = 4, name = "metric_name", type = "string" },
    { id = 5, name = "dimension", type = "string" },
    { id = 6, name = "severity", type = "string" },
    { id = 7, name = "checked_field", type = "string" },
    { id = 8, name = "group_key", type = "string" },
    { id = 9, name = "group_value", type = "string" },
    { id = 10, name = "total_rows", type = "long" },
    { id = 11, name = "violation_rows", type = "long" },
    { id = 12, name = "metric_value", type = "string" },
    { id = 13, name = "status", type = "string" },
    { id = 14, name = "description", type = "string" },
    { id = 15, name = "batch_id", type = "string" },
    { id = 16, name = "event_date_min", type = "date" },
    { id = 17, name = "event_date_max", type = "date" },
    { id = 18, name = "check_ts", type = "timestamp" },
    { id = 19, name = "check_date", type = "date" },
  ]
}


locals {
  # ---------------------------------------------------------------------
  # Table registry
  #
  # partition = null means deliberately unpartitioned (gold_run_control).
  # sort_by is a list of field IDs, in write-order precedence; every merged
  # table leads with phone_number_AC_hash. The two append-only operational
  # tables carry no write order because nothing merges into them by slice.
  # ---------------------------------------------------------------------
  gold_table_definitions = {
    account_changes_canonical_events = {
      description = "Lineage 1 stage 1: carrier event vocabulary translated into the eleven canonical types."
      fields      = local.gold_canonical_events_fields
      partition   = { name = "ac_event_ts_day", source_id = 5, transform = "day", field_id = 1000 }
      sort_by     = [2, 5]
      properties  = local._gold_merge_properties
    }

    msisdn_lifecycle = {
      description = "Lineage 1 stage 2: one row per phone-number lifecycle, with inferred boundaries and confidence."
      fields      = local.gold_lifecycle_fields
      partition   = { name = "lifecycle_start_ts_day", source_id = 6, transform = "day", field_id = 1000 }
      sort_by     = [2, 6]
      properties  = local._gold_merge_properties
    }

    msisdn_lifecycle_account_mapping = {
      description = "Lineage 1 stage 3: one row per lifecycle within an account, with the link route and confidence."
      fields      = local.gold_account_mapping_fields
      partition   = { name = "from_ts_day", source_id = 6, transform = "day", field_id = 1000 }
      sort_by     = [2, 6]
      properties  = local._gold_merge_properties
    }

    msisdn_lifecycle_account_customer_mapping = {
      description = "Lineage 1 stage 4: the account mapping carried up to the customer level."
      fields      = local.gold_customer_mapping_fields
      partition   = { name = "from_ts_day", source_id = 8, transform = "day", field_id = 1000 }
      sort_by     = [4, 8]
      properties  = local._gold_merge_properties
    }

    normalized_canonical_events = {
      description = "Lineage 1 stage 5: serving shape - every canonical event flattened onto its account and customer."
      fields      = local.gold_normalized_events_fields
      partition   = { name = "event_ts_day", source_id = 7, transform = "day", field_id = 1000 }
      sort_by     = [3, 7]
      properties  = local._gold_merge_properties
    }

    msisdn_imei_map = {
      description = "Lineage 2: one row per contiguous phone-number/device association, valid [from_ts, to_ts)."
      fields      = local.gold_imei_map_fields
      partition   = { name = "event_date", source_id = 10, transform = "identity", field_id = 1000 }
      sort_by     = [1, 7]
      properties  = local._gold_merge_properties
    }

    gold_run_control = {
      description = "One row per run per job: the watermark a run consumed, recorded rather than recomputed."
      fields      = local.gold_run_control_fields
      partition   = null
      sort_by     = []
      properties  = local._gold_base_properties
    }

    feature_engineering_run_control = {
      description = "Feature engineering watermark run history, including mode and whether the run used a limit or explicit dates."
      fields      = local.feature_engineering_run_control_fields
      partition   = null
      sort_by     = []
      properties  = local._gold_base_properties
    }

    onboarding_nbc = {
      description = "NBC fraud labels keyed by hashed phone number, for feature-engineering label-to-customer mapping."
      fields      = local.onboarding_nbc_fields
      partition   = null
      sort_by     = []
      properties  = local._gold_base_properties
    }

    dq_metrics = {
      description = "Tiered data-quality check results; same shape as the Silver DQ table so both layers trend in one query."
      fields      = local.gold_dq_metrics_fields
      partition   = { name = "check_date", source_id = 19, transform = "identity", field_id = 1000 }
      sort_by     = []
      properties  = local._gold_base_properties
    }
  }

  gold_table_locations = {
    for table_name, _ in local.gold_table_definitions : table_name =>
    "${local.gold_table_root}/dataset=${table_name}/version=${var.table_version}/"
  }
}


resource "aws_glue_catalog_database" "gold" {
  count = var.create_gold_glue_database ? 1 : 0

  name         = var.gold_glue_database_name
  location_uri = "s3://${var.gold_bucket_name}/"
  description  = "Trust Score curated Gold Iceberg database."
}


resource "aws_glue_catalog_table" "gold" {
  for_each = local.gold_table_definitions

  # When this root also creates the database, make the catalog dependency
  # explicit. With create_gold_glue_database=false, Terraform simply waits on
  # the pre-existing database name supplied by the caller.
  depends_on = [aws_glue_catalog_database.gold]

  catalog_id    = data.aws_caller_identity.current.account_id
  database_name = var.gold_glue_database_name
  name          = each.key
  description   = each.value.description

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = var.iceberg_format_version

      iceberg_table_input {
        location   = local.gold_table_locations[each.key]
        properties = each.value.properties

        schema {
          schema_id = 0
          type      = "struct"

          # Every column is nullable, which matches the table the pipeline
          # would have created for itself: schemas.py::schema_to_ddl emits
          # "<name> <TYPE>" with no NOT NULL, so the non-nullable flags in the
          # StructType are DataFrame-level contract assertions rather than
          # table constraints. Marking them required here would make Terraform's
          # table stricter than the one the code expects and would reject a
          # legitimately absent value at write time.
          #
          # Iceberg requires identifier fields to be required=true, so there
          # are deliberately none. Row identity is enforced by the MERGE ... ON
          # clause in io/merge_sink.py, not by the catalog.
          identifier_field_ids = []

          dynamic "fields" {
            for_each = each.value.fields
            content {
              id       = fields.value.id
              name     = fields.value.name
              required = false
              type     = jsonencode(fields.value.type)
            }
          }
        }

        dynamic "partition_spec" {
          for_each = each.value.partition == null ? [] : [each.value.partition]
          content {
            spec_id = 0

            fields {
              field_id  = partition_spec.value.field_id
              name      = partition_spec.value.name
              source_id = partition_spec.value.source_id
              transform = partition_spec.value.transform
            }
          }
        }

        # ASC / NULLS FIRST is what `ALTER TABLE ... WRITE ORDERED BY <cols>`
        # produces, which is the statement this replaces.
        dynamic "sort_order" {
          for_each = length(each.value.sort_by) == 0 ? [] : [each.value.sort_by]
          content {
            order_id = 1

            dynamic "fields" {
              for_each = sort_order.value
              content {
                source_id  = fields.value
                transform  = "identity"
                direction  = "asc"
                null_order = "nulls-first"
              }
            }
          }
        }
      }
    }
  }

  # Spark owns Iceberg snapshots and any reviewed schema migration after
  # bootstrap. Terraform must not replace a live table or revert an evolution.
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [open_table_format_input]
  }
}


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "gold_glue_database_name" {
  description = "Glue database containing the Gold and feature engineering Iceberg tables."
  value       = var.gold_glue_database_name
}

output "gold_iceberg_table_names" {
  description = "Fully qualified Glue/Iceberg table names, keyed by short name."
  value = {
    for name in keys(local.gold_table_definitions) : name =>
    "${var.gold_glue_database_name}.${name}"
  }
}

output "gold_iceberg_table_locations" {
  description = "S3 location used by each Gold Iceberg table."
  value       = local.gold_table_locations
}
