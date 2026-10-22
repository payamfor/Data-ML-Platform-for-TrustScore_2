variable "ml_feature_engineering_artifact_root" {
  description = "S3 prefix containing the uploaded feature engineering job artifacts."
  type        = string
  default     = "s3://lotus-sandbox-ml-preprocessing-data/pp_prod_artifacts/"

  validation {
    condition     = startswith(var.ml_feature_engineering_artifact_root, "s3://")
    error_message = "ml_feature_engineering_artifact_root must be an S3 URI."
  }
}

variable "ml_feature_engineering_py_files" {
  description = "ZIP dependencies under the artifact root, passed to spark-submit --py-files."
  type        = list(string)
  default     = ["ts05_features.zip", "pydeps.zip"]

  validation {
    condition     = length(var.ml_feature_engineering_py_files) > 0
    error_message = "At least one Python dependency ZIP must be configured."
  }
}

variable "ml_feature_engineering_config_files" {
  description = "Configuration files under the artifact root, localized with spark-submit --files."
  type        = list(string)
  default     = ["conf/base.yaml", "conf/sandbox.yaml"]

  validation {
    condition     = length(var.ml_feature_engineering_config_files) > 0
    error_message = "At least one feature engineering config file must be configured."
  }
}

variable "ml_feature_engineering_entry_point" {
  description = "Spark application entry point under the artifact root."
  type        = string
  default     = "feature_engineering_job.py"
}

variable "enable_ml_feature_engineering_transient_cluster" {
  description = "Create a fresh transient EMR cluster for each feature-engineering execution, reuse it for any job retry, and terminate it after the final outcome. Set false to use an active persistent preprocessing cluster instead."
  type        = bool
  default     = true
}

variable "ml_feature_engineering_alert_email" {
  description = "Email address subscribed to feature-engineering failure alerts."
  type        = string
  default     = "dinesh.p@enstream.com"
}

locals {
  ml_feature_engineering_artifact_root = trimsuffix(var.ml_feature_engineering_artifact_root, "/")

  ml_feature_engineering_py_file_uris = [
    for path in var.ml_feature_engineering_py_files : "${local.ml_feature_engineering_artifact_root}/${path}"
  ]

  ml_feature_engineering_config_file_uris = [
    for path in var.ml_feature_engineering_config_files : "${local.ml_feature_engineering_artifact_root}/${path}"
  ]

  ml_feature_engineering_config_args = flatten([
    for path in var.ml_feature_engineering_config_files : ["--config", basename(path)]
  ])

  ml_feature_engineering_spark_args_prefix = concat([
    "spark-submit",
    "--deploy-mode",
    "cluster",
    "--py-files",
    join(",", local.ml_feature_engineering_py_file_uris),
    "--files",
    join(",", local.ml_feature_engineering_config_file_uris),
    "${local.ml_feature_engineering_artifact_root}/${var.ml_feature_engineering_entry_point}"
  ], local.ml_feature_engineering_config_args)

  ml_feature_engineering_spark_args_asl = join(", ", [
    for argument in local.ml_feature_engineering_spark_args_prefix :
    "'${replace(argument, "'", "\\'")}'"
  ])

  # The job writes an execution-scoped no-retry marker after terminal failures.
  ml_feature_engineering_no_retry_prefix = "feature_engineering_control/no_retry"
  ml_feature_engineering_control_args_asl = join(", ", [
    "'--alert-topic-arn', '${aws_sns_topic.ml_feature_engineering_alerts.arn}'",
    "'--stop-marker', States.Format('s3://${aws_s3_bucket.preprocessing_data.bucket}/${local.ml_feature_engineering_no_retry_prefix}/{}.json', $$.Execution.Name)",
  ])

  ml_feature_engineering_training_args_asl = "${local.ml_feature_engineering_spark_args_asl}, '--mode', 'training', '--attempt', States.Format('{}', $.attempt), ${local.ml_feature_engineering_control_args_asl}"
  ml_feature_engineering_scoring_args_asl  = "${local.ml_feature_engineering_spark_args_asl}, '--mode', 'scoring', '--batch-date', $.scoring_input.batch_date, '--scoring-output-root', 's3://${aws_s3_bucket.sagemaker.bucket}/scoring_data', '--attempt', States.Format('{}', $.attempt), ${local.ml_feature_engineering_control_args_asl}"

  ml_feature_engineering_state_machine_name = "${var.project}-${var.environment}-feature-engineering"
  ml_feature_engineering_alert_topic_name   = "${var.project}-${var.environment}-feature-engineering-alerts"

  ml_feature_engineering_instance_fleets = concat(
    [
      {
        Name                   = "master"
        InstanceFleetType      = "MASTER"
        TargetOnDemandCapacity = 1
        InstanceTypeConfigs = [{
          InstanceType     = var.emr_preprocessing_primary_instance_type
          WeightedCapacity = 1
          EbsConfiguration = {
            EbsBlockDeviceConfigs = [{
              VolumeSpecification = { VolumeType = "gp3", SizeInGB = 50 }
              VolumesPerInstance  = 1
            }]
          }
        }]
      },
      {
        Name                   = "core"
        InstanceFleetType      = "CORE"
        TargetOnDemandCapacity = var.emr_pp_core_target_on_demand_capacity
        InstanceTypeConfigs = [{
          InstanceType     = var.emr_pp_core_instance_type
          WeightedCapacity = 1
          EbsConfiguration = {
            EbsBlockDeviceConfigs = [{
              VolumeSpecification = { VolumeType = "gp3", SizeInGB = 200 }
              VolumesPerInstance  = 1
            }]
          }
        }]
      }
    ],
    var.emr_pp_task_target_spot_capacity > 0 ? [
      {
        Name                   = "task"
        InstanceFleetType      = "TASK"
        TargetOnDemandCapacity = 0
        TargetSpotCapacity     = var.emr_pp_task_target_spot_capacity
        InstanceTypeConfigs = [{
          InstanceType     = var.emr_pp_task_instance_type
          WeightedCapacity = 1
          EbsConfiguration = {
            EbsBlockDeviceConfigs = [{
              VolumeSpecification = { VolumeType = "gp3", SizeInGB = 200 }
              VolumesPerInstance  = 1
            }]
          }
        }]
        LaunchSpecifications = {
          SpotSpecification = {
            AllocationStrategy     = "capacity-optimized"
            TimeoutAction          = "SWITCH_TO_ON_DEMAND"
            TimeoutDurationMinutes = 10
          }
        }
      }
    ] : []
  )

  ml_feature_engineering_cluster_lookup_states = {
    for name, state in {
      CheckPersistentCluster = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.emr_cluster_inspector.arn
          Payload = {
            cluster_id = try(aws_emr_cluster.daily_data_pp_pipeline[0].id, "unavailable")
          }
        }
        ResultSelector = {
          "cluster_state.$" = "$.Payload.cluster_state"
        }
        ResultPath = "$.cluster_check"
        Next       = "ChoosePersistentCluster"
      }

      ChoosePersistentCluster = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.cluster_check.cluster_state"
            StringEquals = "RUNNING"
            Next         = "UsePersistentCluster"
          },
          {
            Variable     = "$.cluster_check.cluster_state"
            StringEquals = "WAITING"
            Next         = "UsePersistentCluster"
          }
        ]
        Default = "NoReusableCluster"
      }

      UsePersistentCluster = {
        Type = "Pass"
        Parameters = {
          "ClusterId"  = try(aws_emr_cluster.daily_data_pp_pipeline[0].id, "unavailable")
          IsPersistent = true
        }
        ResultPath = "$.created_cluster"
        Next       = "ChooseJobMode"
      }
    } : name => state if var.create_persistent_emr_pp_cluster && !var.enable_ml_feature_engineering_transient_cluster
  }

  ml_feature_engineering_no_cluster_states = var.enable_ml_feature_engineering_transient_cluster ? {} : {
    NoReusableCluster = {
      Type  = "Fail"
      Error = "NoReusableCluster"
      Cause = "Transient cluster creation is disabled and no active persistent preprocessing EMR cluster is available."
    }
  }

  ml_feature_engineering_transient_cluster_states = {
    for name, state in {
      CreateCluster = {
        Type     = "Task"
        Resource = "arn:aws:states:::elasticmapreduce:createCluster.sync"
        Parameters = {
          "Name.$"              = "States.Format('${local.emr_pp_cluster_name}-{}-{}', $$.Execution.Name, $.attempt)"
          ReleaseLabel          = var.emr_preprocessing_release_label
          Applications          = [{ Name = "Hadoop" }, { Name = "Spark" }]
          ServiceRole           = data.aws_iam_role.emr_pp_service.arn
          JobFlowRole           = data.aws_iam_instance_profile.emr_pp_ec2.name
          SecurityConfiguration = aws_emr_security_configuration.runtime_roles.name
          LogUri                = var.emr_preprocessing_log_uri
          BootstrapActions = [{
            Name = "stream-emr-step-logs-to-cloudwatch"
            ScriptBootstrapAction = {
              Path = "s3://${aws_s3_bucket.preprocessing_data.bucket}/${aws_s3_object.emr_pp_cloudwatch_bootstrap.key}"
            }
          }]
          VisibleToAllUsers    = true
          StepConcurrencyLevel = 1
          Configurations       = local.emr_pp_configurations
          AutoTerminationPolicy = {
            IdleTimeout = var.transient_emr_idle_timeout_seconds
          }
          Tags = [
            {
              Key   = "lotus-workflow"
              Value = "feature-engineering"
            },
            {
              Key   = "for-use-with-amazon-emr-managed-policies"
              Value = "true"
            }
          ]
          Instances = {
            KeepJobFlowAliveWhenNoSteps   = true
            TerminationProtected          = false
            Ec2SubnetIds                  = var.emr_subnet_ids
            EmrManagedMasterSecurityGroup = aws_security_group.emr_pp_primary.id
            EmrManagedSlaveSecurityGroup  = aws_security_group.emr_pp_core.id
            ServiceAccessSecurityGroup    = aws_security_group.emr_pp_service_access.id
            InstanceFleets                = local.ml_feature_engineering_instance_fleets
          }
        }
        ResultSelector = {
          "ClusterId.$" = "$.ClusterId"
          IsPersistent  = false
        }
        ResultPath = "$.created_cluster"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.create_error"
          Next        = "NotifyFailure"
        }]
        Next = "ChooseJobMode"
      }
    } : name => state if var.enable_ml_feature_engineering_transient_cluster
  }
}

resource "aws_sns_topic" "ml_feature_engineering_alerts" {
  name = local.ml_feature_engineering_alert_topic_name
}

resource "aws_sns_topic_subscription" "ml_feature_engineering_alert_email" {
  topic_arn = aws_sns_topic.ml_feature_engineering_alerts.arn
  protocol  = "email"
  endpoint  = var.ml_feature_engineering_alert_email
}

resource "aws_sfn_state_machine" "ml_feature_engineering" {
  name     = local.ml_feature_engineering_state_machine_name
  role_arn = data.aws_iam_role.ml_feature_engineering_stepfunction.arn

  definition = jsonencode({
    Comment = "Create and clean up a transient preprocessing EMR cluster for each job execution by default; retries reuse the same cluster."
    StartAt = "ValidateJobMode"
    States = merge({
      ValidateJobMode = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.mode"
            StringEquals = "training"
            Next         = "InitializeAttempt"
          },
          {
            Variable     = "$.mode"
            StringEquals = "scoring"
            Next         = "ResolveScoringInput"
          }
        ]
        Default = "InvalidJobMode"
      }

      InitializeAttempt = {
        Type       = "Pass"
        Result     = 1
        ResultPath = "$.attempt"
        Next = var.enable_ml_feature_engineering_transient_cluster ? "CreateCluster" : (
          var.create_persistent_emr_pp_cluster ? "CheckPersistentCluster" : "NoReusableCluster"
        )
      }

      ResolveScoringInput = {
        Type = "Choice"
        Choices = [{
          Variable  = "$.batch_date"
          IsPresent = true
          Next      = "UseProvidedScoringDate"
        }]
        Default = "ExtractScoringDateFromObjectKey"
      }

      UseProvidedScoringDate = {
        Type       = "Pass"
        Parameters = { "batch_date.$" = "$.batch_date" }
        ResultPath = "$.scoring_input"
        Next       = "InitializeAttempt"
      }

      ExtractScoringDateFromObjectKey = {
        Type = "Pass"
        Parameters = {
          "batch_date.$" = "States.ArrayGetItem(States.StringSplit($.event_time, 'T'), 0)"
        }
        ResultPath = "$.scoring_input"
        Next       = "InitializeAttempt"
      }

      ChooseJobMode = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.mode"
            StringEquals = "training"
            Next         = "SubmitTrainingStep"
          },
          {
            Variable     = "$.mode"
            StringEquals = "scoring"
            Next         = "SubmitScoringStep"
          }
        ]
        Default = "InvalidJobMode"
      }

      SubmitTrainingStep = {
        Type     = "Task"
        Resource = "arn:aws:states:::elasticmapreduce:addStep.sync"
        Parameters = {
          "ClusterId.$" = "$.created_cluster.ClusterId"
          # Runtime role with feature-engineering S3 permissions, defined in
          # ../security/emr_feature_engineering_output_iam.tf.
          ExecutionRoleArn = data.aws_iam_role.silver_job_runtime.arn
          Step = {
            "Name.$"        = "States.Format('${local.ml_feature_engineering_state_machine_name}-training-attempt-{}', $.attempt)"
            ActionOnFailure = "CONTINUE"
            HadoopJarStep = {
              Jar      = "command-runner.jar"
              "Args.$" = "States.Array(${local.ml_feature_engineering_training_args_asl})"
            }
          }
        }
        ResultPath = "$.emr_step"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "RecordJobFailure"
        }]
        Next = "RecordJobSuccess"
      }

      SubmitScoringStep = {
        Type     = "Task"
        Resource = "arn:aws:states:::elasticmapreduce:addStep.sync"
        Parameters = {
          "ClusterId.$" = "$.created_cluster.ClusterId"
          # Keep scoring on the same feature-engineering runtime role.
          ExecutionRoleArn = data.aws_iam_role.silver_job_runtime.arn
          Step = {
            "Name.$"        = "States.Format('${local.ml_feature_engineering_state_machine_name}-scoring-attempt-{}', $.attempt)"
            ActionOnFailure = "CONTINUE"
            HadoopJarStep = {
              Jar      = "command-runner.jar"
              "Args.$" = "States.Array(${local.ml_feature_engineering_scoring_args_asl})"
            }
          }
        }
        ResultPath = "$.emr_step"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "RecordJobFailure"
        }]
        Next = "RecordJobSuccess"
      }

      RecordJobSuccess = {
        Type       = "Pass"
        Result     = "success"
        ResultPath = "$.job_status"
        Next       = "ChooseClusterCleanup"
      }

      RecordJobFailure = {
        Type       = "Pass"
        Result     = "failed"
        ResultPath = "$.job_status"
        Next       = "DecideNextAction"
      }

      PrepareJobRetry = {
        Type       = "Pass"
        Result     = 2
        ResultPath = "$.attempt"
        Next       = "ChooseJobMode"
      }

      ChooseClusterCleanup = {
        Type = "Choice"
        Choices = [{
          Variable      = "$.created_cluster.IsPersistent"
          BooleanEquals = true
          Next          = "FinalizeJobOutcome"
        }]
        Default = "TerminateCluster"
      }

      TerminateCluster = {
        Type       = "Task"
        Resource   = "arn:aws:states:::elasticmapreduce:terminateCluster.sync"
        Parameters = { "ClusterId.$" = "$.created_cluster.ClusterId" }
        ResultPath = "$.termination"
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 30
          BackoffRate     = 2
          MaxAttempts     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.termination_error"
          Next        = "NotifyFailure"
        }]
        Next = "FinalizeJobOutcome"
      }

      DecideNextAction = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.job_status"
            StringEquals = "success"
            Next         = "ChooseClusterCleanup"
          },
          {
            Variable      = "$.attempt"
            NumericEquals = 1
            Next          = "CheckNoRetryMarker"
          }
        ]
        Default = "ChooseClusterCleanup"
      }

      # The job writes this marker when a retry cannot help (no Gold run control
      # entry). No marker (the call fails) means a normal retry.
      CheckNoRetryMarker = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:s3:headObject"
        Parameters = {
          Bucket  = aws_s3_bucket.preprocessing_data.bucket
          "Key.$" = "States.Format('${local.ml_feature_engineering_no_retry_prefix}/{}.json', $$.Execution.Name)"
        }
        ResultPath = "$.no_retry_marker"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.no_retry_marker_error"
          Next        = "PrepareJobRetry"
        }]
        Next = "ChooseClusterCleanup"
      }

      FinalizeJobOutcome = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.job_status"
          StringEquals = "success"
          Next         = "FeatureEngineeringSucceeded"
        }]
        Default = "NotifyFailure"
      }

      FeatureEngineeringSucceeded = {
        Type = "Succeed"
      }

      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.ml_feature_engineering_alerts.arn
          Subject     = "Feature engineering job failed"
          "Message.$" = "States.JsonToString($)"
        }
        ResultPath = "$.notification"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.notification_error"
          Next        = "FeatureEngineeringFailed"
        }]
        Next = "FeatureEngineeringFailed"
      }

      FeatureEngineeringFailed = {
        Type  = "Fail"
        Error = "FeatureEngineeringFailed"
        Cause = "Feature engineering failed after the configured attempt(s). Review the execution details and EMR logs."
      }

      InvalidJobMode = {
        Type  = "Fail"
        Error = "InvalidJobMode"
        Cause = "Execution input mode must be training or scoring."
      }
    }, local.ml_feature_engineering_cluster_lookup_states, local.ml_feature_engineering_transient_cluster_states, local.ml_feature_engineering_no_cluster_states)
  })

  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

output "ml_feature_engineering_state_machine_arn" {
  description = "ARN of the feature engineering Step Functions state machine."
  value       = aws_sfn_state_machine.ml_feature_engineering.arn
}
