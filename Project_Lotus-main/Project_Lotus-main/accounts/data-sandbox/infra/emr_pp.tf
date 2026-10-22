# Persistent Spark cluster used for Bronze -> Silver and Gold Spark jobs.
#
# Primary and core nodes are On-Demand: in YARN cluster deploy mode the
# ApplicationMaster/Spark driver lands on a core node, so a Spot interruption
# there would kill the driver mid-job and leave the step record stuck in
# RUNNING. Core nodes are memory-optimized (r5.2xlarge, 64 GiB) because the
# Gold driver alone can request up to 8 GiB heap + 2 GiB overhead for
# full-history runs (see submit_emr.sh SPARK_CONF), which would consume nearly
# all YARN-available memory on a 16 GiB node and leave no room for executors.
#
# Task nodes are Spot: they hold no HDFS data and never run the driver, so an
# interruption only loses in-flight executor work, which YARN retries
# elsewhere. This is where Spot cost savings live in this cluster.
#
# Apply ../security first so the EMR roles, instance profile, and Silver
# runtime role exist.

variable "emr_preprocessing_log_uri" {
  description = "S3 URI prefix for EMR logs, for example s3://my-log-bucket/emr/."
  type        = string
  default     = "s3://lotus-sandbox-ml-preprocessing-data/emr-logs/preprocessing-pipeline/"
}

variable "emr_preprocessing_release_label" {
  description = "Amazon EMR release label. EMR 7.14.0 supports Spark Iceberg with the Glue Data Catalog and Lake Formation Full Table Access."
  type        = string
  default     = "emr-7.14.0"
}

variable "emr_preprocessing_primary_instance_type" {
  description = "On-Demand instance type for the single EMR primary node."
  type        = string
  default     = "m5.xlarge"
}

variable "emr_pp_core_instance_type" {
  description = "On-Demand memory-optimized instance type for EMR core nodes. Core nodes run the YARN ApplicationMaster/Spark driver in cluster deploy mode, so they stay On-Demand rather than Spot."
  type        = string
  default     = "r5.2xlarge"
}

variable "emr_pp_core_target_on_demand_capacity" {
  description = "Target On-Demand capacity for EMR core nodes (weight of 1 per instance below == node count equivalent)."
  type        = number
  default     = 2
}

variable "emr_pp_task_instance_type" {
  description = "Spot memory-optimized instance type for EMR task nodes. Task nodes hold no HDFS data and never run the driver, so they are the safe place for Spot capacity and cost savings."
  type        = string
  default     = "r5.2xlarge"
}

variable "emr_pp_task_target_spot_capacity" {
  description = "Target weighted Spot capacity for EMR task nodes. This is extra, elastic shuffle/executor capacity on top of the fixed core fleet; 0 disables the task fleet."
  type        = number
  default     = 2
}

variable "create_persistent_emr_pp_cluster" {
  description = "Whether Terraform should create the reusable preprocessing EMR cluster and its task fleet. Enable this together with enable_ml_feature_engineering_transient_cluster=false to run feature-engineering jobs on the reusable cluster."
  type        = bool
  default     = false
}


# Dedicated names avoid collisions if other Terraform files already define
# aws_region/current or aws_caller_identity/current.
data "aws_region" "emr_pp_current" {}

data "aws_caller_identity" "emr_pp_current" {}


locals {
  emr_pp_cluster_name              = "${var.project}-${var.environment}-preprocessing-pipeline"
  emr_pp_cloudwatch_log_group_name = "/aws/emr/${local.emr_pp_cluster_name}"

  emr_pp_cloudwatch_agent_configuration = jsonencode({
    logs = {
      logs_collected = {
        files = {
          collect_list = [
            for step_log in ["controller", "stderr", "stdout", "syslog"] : {
              file_path       = "/mnt/var/log/hadoop/steps/*/${step_log}"
              log_group_name  = local.emr_pp_cloudwatch_log_group_name
              log_stream_name = "{instance_id}-step-${step_log}"
              timezone        = "UTC"
            }
          ]
        }
      }
    }
  })

  emr_pp_cloudwatch_bootstrap_script = <<-SCRIPT
    #!/bin/bash
    set -euo pipefail

    sudo yum install -y amazon-cloudwatch-agent

    sudo tee /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json >/dev/null <<'CW_CONFIG'
    ${local.emr_pp_cloudwatch_agent_configuration}
    CW_CONFIG

    sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
      -a fetch-config \
      -m ec2 \
      -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json \
      -s
  SCRIPT

  # Default Iceberg warehouse for Bronze -> Silver processing.
  #
  # Actual Silver table locations are:
  #
  # s3://lotus-sandbox-silver-conformed-data/tables/
  #   dataset=<table_name>/version=<version>/
  #
  # Existing Glue/Iceberg tables retain their explicitly configured locations.
  emr_pp_iceberg_warehouse_uri = "s3://${aws_s3_bucket.medallion["silver-conformed"].bucket}/tables/"

  emr_pp_managed_policy_tags = {
    "for-use-with-amazon-emr-managed-policies" = "true"
  }

  # Shared by the optional Terraform-managed cluster and the transient cluster
  # created by Step Functions. Keeping this independent of aws_emr_cluster
  # prevents createCluster.sync from pulling a permanent cluster into the plan.
  emr_pp_configurations = [
    {
      Classification = "iceberg-defaults"
      Properties = {
        "iceberg.enabled" = "true"
      }
    },
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.sql.catalogImplementation"                           = "hive"
        "spark.hive.metastore.client.factory.class"                 = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        "spark.sql.catalog.glue_catalog"                            = "org.apache.iceberg.spark.SparkCatalog"
        "spark.sql.catalog.glue_catalog.catalog-impl"               = "org.apache.iceberg.aws.glue.GlueCatalog"
        "spark.sql.catalog.glue_catalog.io-impl"                    = "org.apache.iceberg.hadoop.HadoopFileIO"
        "spark.sql.catalog.glue_catalog.warehouse"                  = local.emr_pp_iceberg_warehouse_uri
        "spark.sql.catalog.glue_catalog.client.region"              = data.aws_region.emr_pp_current.region
        "spark.sql.catalog.glue_catalog.glue.account-id"            = data.aws_caller_identity.emr_pp_current.account_id
        "spark.sql.catalog.glue_catalog.glue.lakeformation-enabled" = "true"
        "spark.sql.defaultCatalog"                                  = "glue_catalog"
        "spark.sql.extensions"                                      = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        "spark.sql.catalog.dropDirectoryBeforeTable.enabled"        = "true"
        "spark.yarn.maxAppAttempts"                                 = "1"
      }
    }
  ]
}


# Security root creates these persistent IAM resources.
data "aws_iam_role" "emr_pp_service" {
  name = "${var.project}-${var.environment}-emr-service"
}

data "aws_iam_instance_profile" "emr_pp_ec2" {
  name = "${var.project}-${var.environment}-emr-ec2"
}

resource "aws_s3_bucket" "preprocessing_data" {
  bucket = "lotus-sandbox-ml-preprocessing-data"

  tags = {
    Name = "lotus-sandbox-ml-preprocessing-data"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "preprocessing_data" {
  bucket = aws_s3_bucket.preprocessing_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "preprocessing_data" {
  bucket = aws_s3_bucket.preprocessing_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "preprocessing_data" {
  bucket = aws_s3_bucket.preprocessing_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


# Ensure the preprocessing bucket's EMR log prefix exists.
resource "aws_s3_object" "emr_pp_log_prefix" {
  bucket  = aws_s3_bucket.preprocessing_data.id
  key     = "emr-logs/preprocessing-pipeline/"
  content = ""

  depends_on = [
    aws_s3_bucket_server_side_encryption_configuration.preprocessing_data
  ]
}

resource "aws_s3_object" "emr_pp_cloudwatch_bootstrap" {
  bucket       = aws_s3_bucket.preprocessing_data.id
  key          = "emr-bootstrap/preprocessing-cloudwatch-logs.sh"
  content      = local.emr_pp_cloudwatch_bootstrap_script
  content_type = "text/x-shellscript"

  depends_on = [
    aws_s3_bucket_server_side_encryption_configuration.preprocessing_data
  ]
}


# ---------------------------------------------------------------------------
# EMR managed security groups
# ---------------------------------------------------------------------------

resource "aws_security_group" "emr_pp_primary" {
  name        = "${local.emr_pp_cluster_name}-primary"
  description = "EMR primary-node security group for ${local.emr_pp_cluster_name}."
  vpc_id      = var.emr_vpc_id

  tags = merge(local.emr_pp_managed_policy_tags, {
    Name = "${local.emr_pp_cluster_name}-primary"
  })
}

resource "aws_security_group" "emr_pp_core" {
  name        = "${local.emr_pp_cluster_name}-core"
  description = "EMR core-node security group for ${local.emr_pp_cluster_name}."
  vpc_id      = var.emr_vpc_id

  tags = merge(local.emr_pp_managed_policy_tags, {
    Name = "${local.emr_pp_cluster_name}-core"
  })
}

resource "aws_security_group" "emr_pp_service_access" {
  name        = "${local.emr_pp_cluster_name}-svc-access"
  description = "EMR service-access security group, required for private-subnet clusters that use custom managed security groups."
  vpc_id      = var.emr_vpc_id

  tags = merge(local.emr_pp_managed_policy_tags, {
    Name = "${local.emr_pp_cluster_name}-svc-access"
  })
}


# ---------------------------------------------------------------------------
# Security-group rules
# ---------------------------------------------------------------------------

resource "aws_vpc_security_group_ingress_rule" "emr_pp_primary_from_primary" {
  security_group_id            = aws_security_group.emr_pp_primary.id
  referenced_security_group_id = aws_security_group.emr_pp_primary.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "emr_pp_primary_from_core" {
  security_group_id            = aws_security_group.emr_pp_primary.id
  referenced_security_group_id = aws_security_group.emr_pp_core.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "emr_pp_core_from_primary" {
  security_group_id            = aws_security_group.emr_pp_core.id
  referenced_security_group_id = aws_security_group.emr_pp_primary.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "emr_pp_core_from_core" {
  security_group_id            = aws_security_group.emr_pp_core.id
  referenced_security_group_id = aws_security_group.emr_pp_core.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_egress_rule" "emr_pp_primary_all" {
  security_group_id = aws_security_group.emr_pp_primary.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "emr_pp_core_all" {
  security_group_id = aws_security_group.emr_pp_core.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_ingress_rule" "emr_pp_service_access_from_primary" {
  security_group_id            = aws_security_group.emr_pp_service_access.id
  referenced_security_group_id = aws_security_group.emr_pp_primary.id
  from_port                    = 9443
  to_port                      = 9443
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "emr_pp_service_access_to_primary" {
  security_group_id            = aws_security_group.emr_pp_service_access.id
  referenced_security_group_id = aws_security_group.emr_pp_primary.id
  from_port                    = 8443
  to_port                      = 8443
  ip_protocol                  = "tcp"
}


# ---------------------------------------------------------------------------
# EMR cluster
# ---------------------------------------------------------------------------

resource "aws_emr_cluster" "daily_data_pp_pipeline" {
  count = var.create_persistent_emr_pp_cluster ? 1 : 0

  name          = local.emr_pp_cluster_name
  release_label = var.emr_preprocessing_release_label

  applications = [
    "Hadoop",
    "Spark"
  ]

  service_role = data.aws_iam_role.emr_pp_service.arn

  # Defined in:
  # accounts/data-sandbox/infra/emr_runtime_security_configuration.tf
  #
  # Required by the feature-engineering Step Functions job, which submits
  # each preprocessing step with the Silver runtime role as ExecutionRoleArn.
  security_configuration = aws_emr_security_configuration.runtime_roles.name

  log_uri = var.emr_preprocessing_log_uri

  keep_job_flow_alive_when_no_steps = true
  termination_protection            = false
  step_concurrency_level            = 1

  bootstrap_action {
    name = "stream-emr-step-logs-to-cloudwatch"
    path = "s3://${aws_s3_bucket.preprocessing_data.bucket}/${aws_s3_object.emr_pp_cloudwatch_bootstrap.key}"
  }


  # NOTE:
  # Instance fleets benefit from multiple subnets/AZs because EMR can use
  # additional capacity pools when allocating Spot instances.
  ec2_attributes {
    subnet_ids = var.emr_subnet_ids

    instance_profile = data.aws_iam_instance_profile.emr_pp_ec2.arn

    emr_managed_master_security_group = aws_security_group.emr_pp_primary.id
    emr_managed_slave_security_group  = aws_security_group.emr_pp_core.id
    service_access_security_group     = aws_security_group.emr_pp_service_access.id
  }


  # -------------------------------------------------------------------------
  # Primary node
  # -------------------------------------------------------------------------

  master_instance_fleet {
    instance_type_configs {
      instance_type = var.emr_preprocessing_primary_instance_type

      ebs_config {
        # Primary node stores YARN/EMR metadata and logs rather than the
        # Bronze/Silver datasets themselves.
        size                 = 50
        type                 = "gp3"
        volumes_per_instance = 1
      }
    }

    target_on_demand_capacity = 1

    launch_specifications {
      # No spot_specification:
      # primary node remains On-Demand.
    }
  }


  # -------------------------------------------------------------------------
  # Core nodes
  # -------------------------------------------------------------------------

  core_instance_fleet {
    instance_type_configs {
      instance_type     = var.emr_pp_core_instance_type
      weighted_capacity = 1

      ebs_config {
        size                 = 200
        type                 = "gp3"
        volumes_per_instance = 1
      }
    }

    target_on_demand_capacity = var.emr_pp_core_target_on_demand_capacity

    launch_specifications {
      # No spot_specification: core nodes run the driver, so they stay
      # On-Demand and are never subject to Spot interruption/fallback.
    }
  }


  # -------------------------------------------------------------------------
  # Spark + Glue + Iceberg + Lake Formation Full Table Access
  # -------------------------------------------------------------------------

  configurations_json = jsonencode(local.emr_pp_configurations)


  tags = merge(local.emr_pp_managed_policy_tags, {
    Name = local.emr_pp_cluster_name
  })


  lifecycle {
    # Do not modify/replace an already-running cluster because of unrelated
    # Terraform changes.
    #
    # Configuration changes are picked up the next time Terraform creates
    # the cluster after the previous cluster has been terminated.
    ignore_changes = all
  }

  depends_on = [
    aws_s3_bucket_versioning.preprocessing_data,
    aws_s3_bucket_server_side_encryption_configuration.preprocessing_data,
    aws_s3_bucket_public_access_block.preprocessing_data,
    aws_s3_object.emr_pp_log_prefix
  ]
}


# ---------------------------------------------------------------------------
# Task nodes (Spot)
# ---------------------------------------------------------------------------
#
# Extra, elastic executor capacity. Task nodes hold no HDFS data and never run
# the driver, so losing one to a Spot interruption only costs the in-flight
# tasks it was running; YARN reschedules them on surviving nodes. This is a
# separate resource (rather than an inline fleet block, as master/core use)
# because aws_emr_cluster only supports master_instance_fleet and
# core_instance_fleet inline; a task fleet attaches via aws_emr_instance_fleet
# referencing the cluster id.

resource "aws_emr_instance_fleet" "preprocessing_task" {
  count = var.create_persistent_emr_pp_cluster ? 1 : 0

  cluster_id = aws_emr_cluster.daily_data_pp_pipeline[0].id
  name       = "preprocessing_task"

  instance_type_configs {
    instance_type     = var.emr_pp_task_instance_type
    weighted_capacity = 1

    ebs_config {
      size                 = 200
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  target_spot_capacity = var.emr_pp_task_target_spot_capacity

  launch_specifications {
    spot_specification {
      allocation_strategy = "capacity-optimized"

      # AWS only allows SWITCH_TO_ON_DEMAND or TERMINATE_CLUSTER here; since
      # terminating the whole cluster over optional extra capacity would be
      # far more destructive than a temporary On-Demand task node, fall back
      # to On-Demand rather than lose the cluster.
      timeout_action           = "SWITCH_TO_ON_DEMAND"
      timeout_duration_minutes = 10
    }
  }

  lifecycle {
    # Same reasoning as the cluster resource above: do not resize/replace a
    # running task fleet because of unrelated Terraform changes.
    ignore_changes = all
  }
}


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "emr_pp_cluster_id" {
  description = "ID of the Terraform-managed EMR cluster, when enabled."
  value       = try(aws_emr_cluster.daily_data_pp_pipeline[0].id, null)
}

output "emr_pp_cluster_master_public_dns" {
  description = "Public DNS name of the Terraform-managed EMR cluster, when enabled."
  value       = try(aws_emr_cluster.daily_data_pp_pipeline[0].master_public_dns, null)
}
