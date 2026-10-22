locals {
  # emr_vpc_id     = data.aws_ssm_parameter.vpc_id.value
  # emr_subnet_ids = split(",", data.aws_ssm_parameter.subnet_ids.value)

  table_root    = "s3://${var.silver_bucket_name}/${trim(var.table_root_prefix, "/")}"
  warehouse_uri = var.iceberg_warehouse_uri != "" ? var.iceberg_warehouse_uri : "s3://${var.silver_bucket_name}/iceberg-warehouse/"


  #-----------------------------------iceberg ----------------------------------------------------------------------
  # Field IDs are stable Iceberg identifiers. Do not renumber an existing field;
  # additive schema evolution must receive a new field ID from the Spark writer.
  table_definitions = {
    account_changes_batch = {
      description          = "Trust Score account-change Silver events."
      identifier_field_ids = [1]
      partition = {
        name      = "event_day"
        source_id = 4 # event_timestamp
        transform = "day"
        field_id  = 1000
      }
      fields = [
        { id = 1, name = "record_id", type = "long", doc = "Source account-change identifier." },
        { id = 2, name = "phone_number_AC_hash", type = "string", doc = "Source account-change hash." },
        { id = 3, name = "event_type", type = "string", doc = "Normalized account event type." },
        { id = 4, name = "event_timestamp", type = "timestamp", doc = "Event time in the configured business timezone." },
        { id = 5, name = "mno", type = "string", doc = "Normalized mobile-network operator." },
        { id = 6, name = "subId", type = "string", doc = "Subscriber identifier from notes." },
        { id = 7, name = "msisdnChangeSide", type = "string", doc = "MSISDN change side from notes." },
        { id = 8, name = "correlationId", type = "string", doc = "Correlation identifier from notes." },
        { id = 9, name = "otherMSISDN", type = "string", doc = "Other MSISDN involved in the event from notes." },
        { id = 10, name = "ingestion_ts", type = "timestamp", doc = "Silver ingestion timestamp." },
        { id = 11, name = "source_name", type = "string", doc = "Lineage source name." },
        { id = 12, name = "source_event_id", type = "long", doc = "Numeric source-record pointer." },
        { id = 13, name = "schema_version", type = "int", doc = "Published Silver schema version." },
        { id = 14, name = "event_date", type = "date", doc = "Date partition key derived from event_timestamp." },
      ]
    }

    device_lookup_batch = {
      description          = "Trust Score device and SIM lookup Silver events."
      identifier_field_ids = [1]
      partition = {
        name      = "event_day"
        source_id = 8 # event_timestamp
        transform = "day"
        field_id  = 1000
      }
      fields = [
        { id = 1, name = "record_id", type = "long", doc = "Source device-event identifier." },
        { id = 2, name = "phone_number_AC_hash", type = "string", doc = "Source account-change hash." },
        { id = 3, name = "phone_number_AT_hash", type = "string", doc = "Source account-token hash." },
        { id = 4, name = "mno", type = "string", doc = "Normalized mobile-network operator." },
        { id = 5, name = "imei", type = "string", doc = "Cleaned device IMEI." },
        { id = 6, name = "imsi", type = "string", doc = "Cleaned subscriber IMSI." },
        { id = 7, name = "date", type = "date", doc = "Source event date." },
        { id = 8, name = "event_timestamp", type = "timestamp", doc = "Event time in the configured business timezone." },
        { id = 9, name = "correlation_id", type = "string", doc = "Source or generated correlation identifier." },
        { id = 10, name = "related_id", type = "string", doc = "Related source record identifier." },
        { id = 11, name = "event_type", type = "string", doc = "Normalized device/SIM event type." },
        { id = 12, name = "tac", type = "string", doc = "Type Allocation Code from notes." },
        { id = 13, name = "oldIMSI", type = "string", doc = "Previous IMSI from notes." },
        { id = 14, name = "oldIMEI", type = "string", doc = "Previous IMEI from notes." },
        { id = 15, name = "fromADCSnapshot", type = "string", doc = "Source ADC snapshot marker." },
        { id = 16, name = "ingestion_ts", type = "timestamp", doc = "Silver ingestion timestamp." },
        { id = 17, name = "source_name", type = "string", doc = "Lineage source name." },
        { id = 18, name = "source_event_id", type = "long", doc = "Numeric source-record pointer." },
        { id = 19, name = "schema_version", type = "int", doc = "Published Silver schema version." },
        { id = 20, name = "event_date", type = "date", doc = "Date partition key derived from event_timestamp." },
      ]
    }

    tu_portps = {
      description          = "TransUnion Port/PS Silver port-history events."
      identifier_field_ids = [2] # record_id
      partition = {
        name      = "event_day"
        source_id = 10 # event_timestamp
        transform = "day"
        field_id  = 1000
      }
      fields = [
        { id = 1, name = "request_id", type = "string", required = true, doc = "Identifier of the TU Port/PS API request that returned this event." },
        { id = 2, name = "record_id", type = "string", doc = "SHA-256 primary key derived from the port-event natural key." },
        { id = 3, name = "msisdn", type = "string", required = true, doc = "MSISDN returned by the TU response; protected as required by the source contract." },
        { id = 4, name = "phone_number_AC_hash", type = "string", required = true, doc = "Account-change hash used to join the port event to Gold lineage." },
        { id = 5, name = "phone_number_AT_hash", type = "string", required = true, doc = "Audit-trail hash for cross-feed consistency checks." },
        { id = 6, name = "ownership_status", type = "string", doc = "Current ownership status returned by TU." },
        { id = 7, name = "ownership_last_alt_spid", type = "string", doc = "Most recent alternate service-provider identifier from ownership data." },
        { id = 8, name = "region", type = "string", doc = "Geographic region returned by TU." },
        { id = 9, name = "state", type = "string", doc = "Province or state returned by TU." },
        { id = 10, name = "event_timestamp", type = "timestamp", required = true, doc = "Normalized port-event timestamp." },
        { id = 11, name = "event_date", type = "date", required = true, doc = "Date derived from event_timestamp." },
        { id = 12, name = "event_type", type = "string", required = true, doc = "Normalized port type, such as INTER_SPID_PORT or INTRA_SPID_PORT." },
        { id = 13, name = "event_seq", type = "int", doc = "Chronological sequence of the event for the phone number." },
        { id = 14, name = "lnp_type", type = "int", doc = "Port classification code derived from the normalized event type." },
        { id = 15, name = "is_inter_carrier_port", type = "boolean", required = true, doc = "True when the event moves the number between carriers." },
        { id = 16, name = "from_spid", type = "string", doc = "Service-provider identifier from which the number ported." },
        { id = 17, name = "from_mno_raw", type = "string", doc = "Raw port-out carrier name resolved from from_spid." },
        { id = 18, name = "from_mno_std", type = "string", doc = "Standardized port-out carrier name." },
        { id = 19, name = "to_spid", type = "string", required = true, doc = "Service-provider identifier to which the number ported." },
        { id = 20, name = "to_mno_raw", type = "string", doc = "Raw port-in carrier name resolved from to_spid." },
        { id = 21, name = "to_mno_std", type = "string", doc = "Standardized port-in carrier name." },
        { id = 22, name = "sv_type", type = "string", doc = "Service type returned for the port event." },
        { id = 23, name = "lrn", type = "string", doc = "Local routing number returned for the port event." },
        { id = 24, name = "correlation_id", type = "string", doc = "Source correlation identifier, when present." },
        { id = 25, name = "source_name", type = "string", required = true, doc = "Lineage source name." },
        { id = 26, name = "schema_version", type = "int", required = true, doc = "Published Silver schema version." },
        { id = 27, name = "ingestion_ts", type = "timestamp", required = true, doc = "Silver ingestion timestamp." },
        { id = 28, name = "last_updated_ts", type = "timestamp", required = true, doc = "Timestamp of the latest update to this Silver event." },
      ]
    }

    mno_activation = {
      description          = "MNO-confirmed activation-date Silver enrichment responses."
      identifier_field_ids = [1, 5] # request_id + api_ts (IDV_PRIMARY_KEY)
      partition = {
        name      = "api_day"
        source_id = 5 # api_ts
        transform = "day"
        field_id  = 1000
      }
      fields = [
        { id = 1, name = "request_id", type = "string", doc = "Identifier of the activation-date API request." },
        { id = 2, name = "msisdn", type = "string", required = true, doc = "MSISDN returned by the activation response; protected as required by the source contract." },
        { id = 3, name = "phone_number_AC_hash", type = "string", required = true, doc = "Account-change hash used to join the response to Gold lineage." },
        { id = 4, name = "phone_number_AT_hash", type = "string", required = true, doc = "Audit-trail hash for cross-feed consistency checks." },
        { id = 5, name = "api_ts", type = "timestamp", doc = "Timestamp of the activation-date API call." },
        { id = 6, name = "mno", type = "string", doc = "Carrier returned by the activation-date API." },
        { id = 7, name = "activation_date", type = "date", doc = "Carrier-confirmed contract activation date." },
        { id = 8, name = "response_code", type = "string", required = true, doc = "Activation API response code; success is represented as string code 0." },
        { id = 9, name = "created_ts", type = "timestamp", required = true, doc = "Timestamp at which the Silver response row was created." },
      ]
    }

    audit_trail_services_3 = {
      description          = "Trust Score API audit Silver events."
      identifier_field_ids = [1]
      partition = {
        name      = "event_day"
        source_id = 13 # api_timestamp
        transform = "day"
        field_id  = 1000
      }
      fields = [
        { id = 1, name = "record_id", type = "string", doc = "Source audit-record identifier." },
        { id = 2, name = "operation", type = "string", doc = "API operation name." },
        { id = 3, name = "mno", type = "string", doc = "Normalized mobile-network operator." },
        { id = 4, name = "msisdn", type = "string", doc = "Raw MSISDN (present only when not hashed/encrypted)." },
        { id = 5, name = "phone_number_AT_hash", type = "string", doc = "Supplied SHA-256 account-token hash." },
        { id = 6, name = "encrypted_msisdn", type = "string", doc = "Encrypted MSISDN payload." },
        { id = 7, name = "partner_id", type = "string", doc = "Normalized partner identifier." },
        { id = 8, name = "partner_name", type = "string", doc = "Partner reference name." },
        { id = 9, name = "service_provider_id", type = "string", doc = "Normalized service-provider identifier." },
        { id = 10, name = "service_provider_name", type = "string", doc = "Service-provider reference name." },
        { id = 11, name = "response_code", type = "string", doc = "API response code." },
        { id = 12, name = "source_ip", type = "string", doc = "Source IP address of the API caller." },
        { id = 13, name = "api_timestamp", type = "timestamp", doc = "API event time in the configured business timezone." },
        { id = 14, name = "date", type = "date", doc = "Source event date." },
        { id = 15, name = "timestamp_ms", type = "timestamp", doc = "Millisecond-precision event timestamp." },
        { id = 16, name = "brand", type = "string", doc = "Normalized source brand." },
        { id = 17, name = "industry", type = "string", doc = "Service-provider industry reference." },
        { id = 18, name = "api_type", type = "string", doc = "Versioned API operation classification." },
        { id = 19, name = "processing_time_ms", type = "long", doc = "Request processing duration in milliseconds." },
        { id = 20, name = "correlation_id", type = "string", doc = "Request/correlation identifier." },
        { id = 21, name = "source_name", type = "string", doc = "Lineage source name." },
        { id = 22, name = "source_event_id", type = "string", doc = "String request/source pointer." },
        { id = 23, name = "schema_version", type = "int", doc = "Published Silver schema version." },
        { id = 24, name = "event_date", type = "date", doc = "Date partition key derived from api_timestamp." },
      ]
    }
  }

  table_locations = {
    for table_name, table in local.table_definitions : table_name =>
    "${local.table_root}/dataset=${table_name}/version=${var.table_version}/"
  }
}

