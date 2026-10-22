# Optional Terraform-managed EMR cluster for small maintenance/cleanup jobs.
# The daily Silver/Gold Step Functions cluster uses the separate full-size
# emr_* variables below; these persistent-cluster settings do not resize it.
#
# Apply ../security first so the EMR roles, instance profile, and Silver
# runtime role exist.

variable "emr_log_uri" {
  description = "S3 URI prefix for EMR logs, for example s3://my-log-bucket/emr/."
  type        = string
  default     = "s3://lotus-sandbox-bronze-landing-data/emr-logs/lotus-sandbox-bronze-silver/"
}

variable "emr_release_label" {
  description = "Amazon EMR release label. EMR 7.14.0 supports Spark Iceberg with the Glue Data Catalog and Lake Formation Full Table Access."
  type        = string
  default     = "emr-7.14.0"
}

variable "emr_primary_instance_type" {
  description = "On-Demand instance type for the single EMR primary node."
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_type" {
  description = "On-Demand memory-optimized instance type for EMR core nodes. Core nodes run the YARN ApplicationMaster/Spark driver in cluster deploy mode, so they stay On-Demand rather than Spot."
  type        = string
  default     = "r5.2xlarge"
}

variable "emr_core_target_on_demand_capacity" {
  description = "Target On-Demand capacity for EMR core nodes (weight of 1 per instance below == node count equivalent)."
  type        = number
  default     = 2

  validation {
    condition     = var.emr_core_target_on_demand_capacity >= 2 && floor(var.emr_core_target_on_demand_capacity) == var.emr_core_target_on_demand_capacity
    error_message = "Keep at least two whole On-Demand core nodes for the Silver/Gold workflow."
  }
}

variable "emr_task_instance_type" {
  description = "Memory-optimized instance type for EMR task nodes. The test uses On-Demand; Spot can be enabled later."
  type        = string
  default     = "r5.2xlarge"
}

variable "emr_task_target_spot_capacity" {
  description = "Target weighted Spot capacity for EMR task nodes. Keep 0 for the capacity test."
  type        = number
  default     = 0

  validation {
    condition     = var.emr_task_target_spot_capacity >= 0 && floor(var.emr_task_target_spot_capacity) == var.emr_task_target_spot_capacity
    error_message = "Task Spot capacity must be a non-negative whole node count."
  }
}

variable "emr_task_target_on_demand_capacity" {
  description = "Target On-Demand task node count (weight 1). Use two for the Gold capacity test."
  type        = number
  default     = 2

  validation {
    condition     = var.emr_task_target_on_demand_capacity >= 0 && floor(var.emr_task_target_on_demand_capacity) == var.emr_task_target_on_demand_capacity
    error_message = "Task On-Demand capacity must be a non-negative whole node count."
  }
}

variable "create_persistent_emr_cluster" {
  description = "Create the small Terraform-managed maintenance/test EMR cluster. Step Functions transient clusters are independent."
  type        = bool
  default     = true
}


# Dedicated names avoid  collisions if other Terraform files already define
# aws_region/current or aws_caller_identity/current. 
data "aws_region" "emr_current" {}

data "aws_caller_identity" "emr_current" {}


locals {
  emr_cluster_name = "${var.project}-${var.environment}-data-pipeline"

  emr_step_concurrency_level = 1
  emr_primary_ebs_size_gib   = 50
  emr_worker_ebs_size_gib    = 200

  # Only the optional Terraform-managed cluster uses this minimal capacity.
  # Keep it separate from the r5.2xlarge Silver/Gold transient cluster.
  emr_persistent_primary_instance_type   = "m5.xlarge"
  emr_persistent_core_instance_type      = "m5.xlarge"
  emr_persistent_core_on_demand_capacity = 1
  emr_persistent_task_instance_type      = "m5.xlarge"
  emr_persistent_task_on_demand_capacity = 0
  emr_persistent_task_spot_capacity      = 0

  # Default Iceberg warehouse for Bronze -> Silver processing.
  #
  # Actual Silver table locations are:
  #
  # s3://lotus-sandbox-silver-conformed-data/tables/
  #   dataset=<table_name>/version=<version>/
  #
  # Existing Glue/Iceberg tables retain their explicitly configured locations.
  emr_iceberg_warehouse_uri = "s3://${aws_s3_bucket.medallion["silver-conformed"].bucket}/tables/"

  emr_managed_policy_tags = {
    "for-use-with-amazon-emr-managed-policies" = "true"
  }

  # Shared by the optional Terraform-managed cluster and the transient cluster
  # created by Step Functions. Keeping this independent of aws_emr_cluster
  # prevents createCluster.sync from pulling a permanent cluster into the plan.
  emr_configurations = [
    {
      Classification = "yarn-site"
      Properties = {
        "yarn.node-labels.enabled"                          = "true"
        "yarn.node-labels.am.default-node-label-expression" = "CORE"
      }
    },
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
        "spark.sql.catalog.glue_catalog.type"                       = "glue"
        "spark.sql.catalog.glue_catalog.warehouse"                  = local.emr_iceberg_warehouse_uri
        "spark.sql.catalog.glue_catalog.client.region"              = data.aws_region.emr_current.region
        "spark.sql.catalog.glue_catalog.glue.account-id"            = data.aws_caller_identity.emr_current.account_id
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
data "aws_iam_role" "emr_service" {
  name = "${var.project}-${var.environment}-emr-service"
}

data "aws_iam_instance_profile" "emr_ec2" {
  name = "${var.project}-${var.environment}-emr-ec2"
}


# Ensure the configured EMR log prefix exists.
resource "aws_s3_object" "emr_log_prefix" {
  bucket  = "lotus-sandbox-bronze-landing-data"
  key     = "emr-logs/lotus-sandbox-bronze-silver/"
  content = ""
}


# ---------------------------------------------------------------------------
# EMR managed security groups
# ---------------------------------------------------------------------------

resource "aws_security_group" "emr_primary" {
  name        = "${local.emr_cluster_name}-primary"
  description = "EMR primary-node security group for ${local.emr_cluster_name}."
  vpc_id      = var.emr_vpc_id

  tags = merge(local.emr_managed_policy_tags, {
    Name = "${local.emr_cluster_name}-primary"
  })
}

resource "aws_security_group" "emr_core" {
  name        = "${local.emr_cluster_name}-core"
  description = "EMR core-node security group for ${local.emr_cluster_name}."
  vpc_id      = var.emr_vpc_id

  tags = merge(local.emr_managed_policy_tags, {
    Name = "${local.emr_cluster_name}-core"
  })
}

resource "aws_security_group" "emr_service_access" {
  name        = "${local.emr_cluster_name}-svc-access"
  description = "EMR service-access security group, required for private-subnet clusters that use custom managed security groups."
  vpc_id      = var.emr_vpc_id

  tags = merge(local.emr_managed_policy_tags, {
    Name = "${local.emr_cluster_name}-svc-access"
  })
}


# ---------------------------------------------------------------------------
# Security-group rules
# ---------------------------------------------------------------------------

resource "aws_vpc_security_group_ingress_rule" "emr_primary_from_primary" {
  security_group_id            = aws_security_group.emr_primary.id
  referenced_security_group_id = aws_security_group.emr_primary.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "emr_primary_from_core" {
  security_group_id            = aws_security_group.emr_primary.id
  referenced_security_group_id = aws_security_group.emr_core.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "emr_core_from_primary" {
  security_group_id            = aws_security_group.emr_core.id
  referenced_security_group_id = aws_security_group.emr_primary.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "emr_core_from_core" {
  security_group_id            = aws_security_group.emr_core.id
  referenced_security_group_id = aws_security_group.emr_core.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_egress_rule" "emr_primary_all" {
  security_group_id = aws_security_group.emr_primary.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "emr_core_all" {
  security_group_id = aws_security_group.emr_core.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_ingress_rule" "emr_service_access_from_primary" {
  security_group_id            = aws_security_group.emr_service_access.id
  referenced_security_group_id = aws_security_group.emr_primary.id
  from_port                    = 9443
  to_port                      = 9443
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "emr_service_access_to_primary" {
  security_group_id            = aws_security_group.emr_service_access.id
  referenced_security_group_id = aws_security_group.emr_primary.id
  from_port                    = 8443
  to_port                      = 8443
  ip_protocol                  = "tcp"
}


# ---------------------------------------------------------------------------
# EMR cluster
# ---------------------------------------------------------------------------

resource "aws_emr_cluster" "daily_data_pipeline" {
  count = var.create_persistent_emr_cluster ? 1 : 0

  name          = local.emr_cluster_name
  release_label = var.emr_release_label

  applications = [
    "Hadoop",
    "Spark"
  ]

  service_role = data.aws_iam_role.emr_service.arn

  # Defined in:
  # accounts/data-sandbox/infra/emr_runtime_security_configuration.tf
  #
  # Enables application-scoped/runtime IAM roles so EMR steps can be
  # submitted with --execution-role-arn.
  security_configuration = aws_emr_security_configuration.runtime_roles.name

  log_uri = var.emr_log_uri

  keep_job_flow_alive_when_no_steps = true
  termination_protection            = false
  step_concurrency_level            = local.emr_step_concurrency_level


  # NOTE:
  # Instance fleets benefit from multiple subnets/AZs because EMR can use
  # additional capacity pools when allocating Spot instances.
  ec2_attributes {
    subnet_ids = var.emr_subnet_ids

    instance_profile = data.aws_iam_instance_profile.emr_ec2.arn

    emr_managed_master_security_group = aws_security_group.emr_primary.id
    emr_managed_slave_security_group  = aws_security_group.emr_core.id
    service_access_security_group     = aws_security_group.emr_service_access.id
  }


  # -------------------------------------------------------------------------
  # Primary node
  # -------------------------------------------------------------------------

  master_instance_fleet {
    instance_type_configs {
      instance_type = local.emr_persistent_primary_instance_type

      ebs_config {
        # Primary node stores YARN/EMR metadata and logs rather than the
        # Bronze/Silver datasets themselves.
        size                 = local.emr_primary_ebs_size_gib
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
      instance_type     = local.emr_persistent_core_instance_type
      weighted_capacity = 1

      ebs_config {
        size                 = local.emr_worker_ebs_size_gib
        type                 = "gp3"
        volumes_per_instance = 1
      }
    }

    target_on_demand_capacity = local.emr_persistent_core_on_demand_capacity

    launch_specifications {
      # Cleanup driver runs on the single On-Demand core node.
    }
  }


  # -------------------------------------------------------------------------
  # Spark + Glue + Iceberg + Lake Formation Full Table Access
  # -------------------------------------------------------------------------

  configurations_json = jsonencode(local.emr_configurations)

  # No managed scaling policy: cleanup capacity stays fixed while jobs run.


  # -------------------------------------------------------------------------
  # Auto termination
  # -------------------------------------------------------------------------

  auto_termination_policy {
    # Terminate after one hour of idle time.
    #
    # The IAM runtime role and its Lake Formation grants are independent of
    # the EMR cluster, so they survive cluster termination/recreation.
    idle_timeout = var.transient_emr_idle_timeout_seconds
  }


  tags = merge(local.emr_managed_policy_tags, {
    Name = local.emr_cluster_name
  })


  lifecycle {
    # Do not modify/replace an already-running cluster because of unrelated
    # Terraform changes.
    #
    # Configuration changes are picked up the next time Terraform creates
    # the cluster after the previous cluster has auto-terminated.
    ignore_changes = all
  }
}


# ---------------------------------------------------------------------------
# Optional task nodes (disabled for the maintenance cluster by default)
# ---------------------------------------------------------------------------
#
# Extra executor capacity if a future maintenance workload requires it. This
# is a separate resource (rather than an inline fleet block, as master/core use)
# because aws_emr_cluster only supports master_instance_fleet and
# core_instance_fleet inline; a task fleet attaches via aws_emr_instance_fleet
# referencing the cluster id.

resource "aws_emr_instance_fleet" "task" {
  count = var.create_persistent_emr_cluster && (local.emr_persistent_task_on_demand_capacity + local.emr_persistent_task_spot_capacity > 0) ? 1 : 0

  cluster_id = aws_emr_cluster.daily_data_pipeline[0].id
  name       = "task"

  instance_type_configs {
    instance_type     = local.emr_persistent_task_instance_type
    weighted_capacity = 1

    ebs_config {
      size                 = local.emr_worker_ebs_size_gib
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  target_on_demand_capacity = local.emr_persistent_task_on_demand_capacity
  target_spot_capacity      = local.emr_persistent_task_spot_capacity

  dynamic "launch_specifications" {
    for_each = local.emr_persistent_task_spot_capacity > 0 ? [1] : []
    content {
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

output "emr_cluster_id" {
  description = "ID of the Terraform-managed EMR cluster, when enabled."
  value       = try(aws_emr_cluster.daily_data_pipeline[0].id, null)
}

output "emr_cluster_master_public_dns" {
  description = "Public DNS name of the Terraform-managed EMR cluster, when enabled."
  value       = try(aws_emr_cluster.daily_data_pipeline[0].master_public_dns, null)
}
