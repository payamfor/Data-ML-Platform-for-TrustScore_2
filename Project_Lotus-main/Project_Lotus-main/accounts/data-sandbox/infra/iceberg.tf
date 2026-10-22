data "aws_s3_bucket" "silver" {
  bucket = var.silver_bucket_name
}

resource "aws_glue_catalog_database" "silver" {
  count = var.create_glue_database ? 1 : 0

  name         = var.glue_database_name
  location_uri = "s3://${var.silver_bucket_name}/"
  description  = "Trust Score conformed Silver Iceberg database."
}

resource "aws_glue_catalog_table" "silver" {
  for_each = local.table_definitions

  # When this root also creates the database, make the catalog dependency
  # explicit. With create_glue_database=false, Terraform simply waits on the
  # pre-existing database name supplied by the caller.
  depends_on = [aws_glue_catalog_database.silver]

  catalog_id    = data.aws_caller_identity.current.account_id
  database_name = var.glue_database_name
  name          = each.key
  description   = each.value.description

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = var.iceberg_format_version

      iceberg_table_input {
        location = local.table_locations[each.key]

        properties = {
          "format-version"                  = tostring(var.iceberg_format_version)
          "write.format.default"            = "parquet"
          "write.parquet.compression-codec" = "zstd"
          "write.spark.accept-any-schema"   = tostring(var.enable_schema_evolution)
        }

        schema {
          schema_id            = 0
          type                 = "struct"
          identifier_field_ids = each.value.identifier_field_ids

          dynamic "fields" {
            for_each = each.value.fields
            content {
              id   = fields.value.id
              name = fields.value.name
              # Iceberg requires identifier fields to be required=true. Tables
              # may additionally declare a non-identifier field as required to
              # mirror an upstream StructType contract; existing definitions
              # without that attribute retain their previous nullable behavior.
              required = contains(each.value.identifier_field_ids, fields.value.id) || try(fields.value.required, false)
              type     = jsonencode(fields.value.type)
              doc      = fields.value.doc
            }
          }
        }

        partition_spec {
          spec_id = 0

          fields {
            field_id  = each.value.partition.field_id
            name      = each.value.partition.name
            source_id = each.value.partition.source_id
            transform = each.value.partition.transform
          }
        }
      }
    }
  }

  # Spark owns additive schema evolution and Iceberg snapshots after bootstrap.
  # Terraform must not replace a live table or revert a schema evolved by Spark.
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [open_table_format_input]
  }
}

#------------outputs-----------------

output "silver_bucket_name" {
  description = "S3 bucket used by the Silver Iceberg tables."
  value       = data.aws_s3_bucket.silver.bucket
}

output "glue_database_name" {
  description = "Glue database containing the Silver Iceberg tables."
  value       = var.glue_database_name
}

output "iceberg_table_names" {
  description = "Fully qualified Glue/Iceberg table names."
  value = {
    for name in keys(local.table_definitions) : name =>
    "${var.glue_database_name}.${name}"
  }
}

output "iceberg_table_locations" {
  description = "S3 locations used by each Iceberg table."
  value       = local.table_locations
}

output "spark_iceberg_catalog_config" {
  description = "Spark settings to pass to EMR spark-submit or the cluster configuration."
  value = {
    "spark.sql.extensions"                     = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "spark.sql.catalog.glue_catalog"           = "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.glue_catalog.type"      = "glue"
    "spark.sql.catalog.glue_catalog.warehouse" = local.warehouse_uri
    "spark.sql.defaultCatalog"                 = "glue_catalog"
  }
}
