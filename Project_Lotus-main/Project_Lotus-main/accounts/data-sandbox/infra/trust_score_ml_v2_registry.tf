# =============================================================================
# VERSION 2.2 - Trust Score ML: Model Registry + the deploy state machine
# =============================================================================
# See ../CHANGES_V2_2.md.
#
# REGISTER (in the training state machine, trust_score_ml_v2_stepfunctions.tf)
#   Finalize (training job) saves the champion as model.tar.gz
#   -> ReadChampion -> RegisterChampion: a new version in the EXISTING group
#      aws_sagemaker_model_package_group.trust_score (sagemaker.tf), always
#      created PendingManualApproval
#   -> ApproveChampion (UpdateModelPackage -> Approved) when
#      ml_v2_auto_approve_champion; otherwise a person approves it.
#
# DEPLOY (this file; started by the EventBridge rule in
#         trust_score_ml_v2_eventbridge.tf on "package Approved")
#   BuildContext -> CreateModel (from the package) -> CreateEndpointConfig
#   -> FindEndpoint -> CreateEndpoint | UpdateEndpoint (blue/green)
#   -> wait until InService on the NEW config -> SNS
#   A rolled-back or failed update is reported; the old model keeps serving.
# ===============================================================================

variable "ml_v2_auto_approve_champion" {
  description = "Approve each newly registered champion automatically, which deploys it to the live endpoint. false = it stays PendingManualApproval until someone approves it (Studio, or aws sagemaker update-model-package --model-package-arn <arn> --model-approval-status Approved)."
  type        = bool
  default     = true
}

locals {
  ml_v2_model_package_group = aws_sagemaker_model_package_group.trust_score.model_package_group_name

  ml_v2_deploy_sm_name = "${local.ml_v2_name}-deploy"

  # Where train.py --mode finalize writes champion documents:
  # <models_root>/version_id=<models_version>/champion/ (sm_common.champion_root).
  # The SageMaker training jobs set models_root to ml_v2_output_root directly.
  ml_v2_champion_root_key = "${var.ml_v2_output_prefix}version_id=${trimprefix(trim(var.ml_v2_models_version, "/"), "version_id=")}/champion/"

  # aws-sdk integrations name errors <Service>.<Exception>.
  ml_v2_aws_sdk_retry = [{
    ErrorEquals     = ["SageMaker.SageMakerException", "SageMaker.AmazonSageMakerException", "SageMaker.ThrottlingException", "ThrottlingException"]
    IntervalSeconds = 30
    MaxAttempts     = 3
    BackoffRate     = 2
  }]

  ml_v2_deploy_status_path = "$.endpoint.matches[0].EndpointStatus"

  ml_v2_deploy_definition = {
    Comment        = "Trust Score ML deploy (v2.2): an Approved champion model package -> the live real-time endpoint (create, or blue/green update with auto-rollback)."
    StartAt        = "BuildContext"
    TimeoutSeconds = 4 * local.ml_v2_hours
    States = {
      BuildContext = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.ml_v2_helper.arn
          Payload = {
            action             = "deploy_context"
            "execution_name.$" = "$$.Execution.Name"
            "detail.$"         = "$.detail"
          }
        }
        ResultSelector = {
          "model_package_arn.$"     = "$.Payload.model_package_arn"
          "model_package_version.$" = "$.Payload.model_package_version"
          "champion_run_id.$"       = "$.Payload.champion_run_id"
          "model_name.$"            = "$.Payload.model_name"
          "endpoint_config_name.$"  = "$.Payload.endpoint_config_name"
        }
        ResultPath = "$.context"
        Retry      = local.ml_v2_lambda_retry
        Catch      = local.ml_v2_catch_all
        Next       = "CreateModel"
      }
      # The container, image digest, model.tar.gz and environment all come from
      # the registered package - the model is exactly what was registered.
      CreateModel = {
        Type     = "Task"
        Resource = "arn:aws:states:::sagemaker:createModel"
        Parameters = merge(
          {
            "ModelName.$"    = "$.context.model_name"
            PrimaryContainer = { "ModelPackageName.$" = "$.context.model_package_arn" }
            ExecutionRoleArn = data.aws_iam_role.ml_v2_sagemaker_execution.arn
            Tags             = local.ml_v2_job_tags
          },
          local.ml_v2_vpc_config != null ? { VpcConfig = local.ml_v2_vpc_config } : null,
        )
        ResultPath = null
        Retry      = local.ml_v2_sagemaker_retry
        Catch      = local.ml_v2_catch_all
        Next       = "CreateEndpointConfig"
      }
      CreateEndpointConfig = {
        Type     = "Task"
        Resource = "arn:aws:states:::sagemaker:createEndpointConfig"
        Parameters = {
          "EndpointConfigName.$" = "$.context.endpoint_config_name"
          ProductionVariants = [{
            VariantName                                 = "champion"
            "ModelName.$"                               = "$.context.model_name"
            InstanceType                                = var.ml_v2_endpoint_instance_type
            InitialInstanceCount                        = var.ml_v2_endpoint_instance_count
            InitialVariantWeight                        = 1
            ContainerStartupHealthCheckTimeoutInSeconds = 600
            ModelDataDownloadTimeoutInSeconds           = 1200
          }]
          Tags = local.ml_v2_job_tags
        }
        ResultPath = null
        Retry      = local.ml_v2_sagemaker_retry
        Catch      = local.ml_v2_catch_all
        Next       = "FindEndpoint"
      }
      # ListEndpoints (not DescribeEndpoint) so "does not exist yet" is data,
      # not an error to catch. NameContains is a substring match; the filter
      # keeps the exact name.
      FindEndpoint = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sagemaker:listEndpoints"
        Parameters = {
          NameContains = local.ml_v2_endpoint_name
          MaxResults   = 100
        }
        ResultSelector = { "matches.$" = "$.Endpoints[?(@.EndpointName == '${local.ml_v2_endpoint_name}')]" }
        ResultPath     = "$.endpoint"
        Retry          = local.ml_v2_aws_sdk_retry
        Catch          = local.ml_v2_catch_all
        Next           = "CountEndpoint"
      }
      CountEndpoint = {
        Type       = "Pass"
        Parameters = { "count.$" = "States.ArrayLength($.endpoint.matches)", "matches.$" = "$.endpoint.matches" }
        ResultPath = "$.endpoint"
        Next       = "EndpointExists"
      }
      EndpointExists = {
        Type = "Choice"
        Choices = [{
          Variable      = "$.endpoint.count"
          NumericEquals = 0
          Next          = "CreateEndpoint"
        }]
        Default = "EndpointState"
      }
      EndpointState = {
        Type = "Choice"
        Choices = [
          { Variable = local.ml_v2_deploy_status_path, StringEquals = "InService", Next = "UpdateEndpoint" },
          # A Failed endpoint serves nothing and cannot be updated: replace it.
          { Variable = local.ml_v2_deploy_status_path, StringEquals = "Failed", Next = "DeleteFailedEndpoint" },
          {
            Or   = [for status in concat(local.ml_v2_endpoint_transitional_statuses, ["Deleting"]) : { Variable = local.ml_v2_deploy_status_path, StringEquals = status }]
            Next = "WaitWhileBusy"
          },
        ]
        Default = "EndpointUnusable"
      }
      WaitWhileBusy = {
        Type    = "Wait"
        Seconds = 60
        Next    = "FindEndpoint"
      }
      DeleteFailedEndpoint = {
        Type       = "Task"
        Resource   = "arn:aws:states:::aws-sdk:sagemaker:deleteEndpoint"
        Parameters = { EndpointName = local.ml_v2_endpoint_name }
        ResultPath = null
        Retry      = local.ml_v2_aws_sdk_retry
        Catch      = local.ml_v2_catch_all
        Next       = "WaitWhileBusy"
      }
      EndpointUnusable = {
        Type = "Pass"
        Parameters = {
          Error     = "LiveEndpointUnusable"
          "Cause.$" = "States.Format('Endpoint ${local.ml_v2_endpoint_name} is {} and cannot be updated. Delete it (aws sagemaker delete-endpoint) and re-run this execution to recreate it.', ${local.ml_v2_deploy_status_path})"
        }
        ResultPath = "$.error"
        Next       = "NotifyFailure"
      }
      CreateEndpoint = {
        Type     = "Task"
        Resource = "arn:aws:states:::sagemaker:createEndpoint"
        Parameters = {
          EndpointName           = local.ml_v2_endpoint_name
          "EndpointConfigName.$" = "$.context.endpoint_config_name"
          Tags                   = local.ml_v2_job_tags
        }
        ResultPath = null
        Retry      = local.ml_v2_sagemaker_retry
        Catch      = local.ml_v2_catch_all
        Next       = "WaitForEndpoint"
      }
      # Blue/green: the new fleet is started and health-checked beside the old
      # one; traffic moves only when it is healthy, and the old fleet is kept
      # for ml_v2_endpoint_bluegreen_wait_seconds. A new container that fails
      # to start never receives traffic - SageMaker rolls the update back.
      UpdateEndpoint = {
        Type     = "Task"
        Resource = "arn:aws:states:::sagemaker:updateEndpoint"
        Parameters = {
          EndpointName               = local.ml_v2_endpoint_name
          "EndpointConfigName.$"     = "$.context.endpoint_config_name"
          RetainAllVariantProperties = false
          DeploymentConfig = {
            BlueGreenUpdatePolicy = {
              TrafficRoutingConfiguration      = { Type = "ALL_AT_ONCE", WaitIntervalInSeconds = 0 }
              TerminationWaitInSeconds         = var.ml_v2_endpoint_bluegreen_wait_seconds
              MaximumExecutionTimeoutInSeconds = 3600
            }
          }
        }
        ResultPath = null
        Retry      = local.ml_v2_sagemaker_retry
        Catch      = local.ml_v2_catch_all
        Next       = "WaitForEndpoint"
      }
      WaitForEndpoint = {
        Type    = "Wait"
        Seconds = 60
        Next    = "DescribeEndpoint"
      }
      DescribeEndpoint = {
        Type           = "Task"
        Resource       = "arn:aws:states:::aws-sdk:sagemaker:describeEndpoint"
        Parameters     = { EndpointName = local.ml_v2_endpoint_name }
        ResultSelector = { "status.$" = "$.EndpointStatus", "config.$" = "$.EndpointConfigName" }
        ResultPath     = "$.live"
        Retry          = local.ml_v2_aws_sdk_retry
        Catch          = local.ml_v2_catch_all
        Next           = "EndpointReady"
      }
      EndpointReady = {
        Type = "Choice"
        Choices = [
          {
            And = [
              { Variable = "$.live.status", StringEquals = "InService" },
              { Variable = "$.live.config", StringEqualsPath = "$.context.endpoint_config_name" },
            ]
            Next = "NotifyDeployed"
          },
          # InService, but on the OLD config: the update was rolled back.
          { Variable = "$.live.status", StringEquals = "InService", Next = "RolledBack" },
          {
            Or   = [for status in local.ml_v2_endpoint_transitional_statuses : { Variable = "$.live.status", StringEquals = status }]
            Next = "WaitForEndpoint"
          },
        ]
        Default = "DeployFailed"
      }
      RolledBack = {
        Type = "Pass"
        Parameters = {
          Error     = "DeploymentRolledBack"
          "Cause.$" = "States.Format('The update to {} was rolled back; the endpoint still serves {}. Usually the new container failed its health check: see CloudWatch /aws/sagemaker/Endpoints/${local.ml_v2_endpoint_name}.', $.context.endpoint_config_name, $.live.config)"
        }
        ResultPath = "$.error"
        Next       = "NotifyFailure"
      }
      DeployFailed = {
        Type = "Pass"
        Parameters = {
          Error     = "DeploymentFailed"
          "Cause.$" = "States.Format('Endpoint ${local.ml_v2_endpoint_name} is {} after deploying {}.', $.live.status, $.context.endpoint_config_name)"
        }
        ResultPath = "$.error"
        Next       = "NotifyFailure"
      }
      NotifyDeployed = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.ml_v2_alerts.arn
          Subject     = "Trust Score champion deployed"
          "Message.$" = "States.Format('Model package {} (version {}, champion run {}) is live on endpoint ${local.ml_v2_endpoint_name} (config {}).', $.context.model_package_arn, $.context.model_package_version, $.context.champion_run_id, $.live.config)"
        }
        ResultPath = null
        End        = true
      }
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.ml_v2_alerts.arn
          Subject     = "Trust Score champion deployment FAILED"
          "Message.$" = "States.Format('Deploy execution {} failed: {}', $$.Execution.Name, States.JsonToString($.error))"
        }
        ResultPath = null
        Next       = "Failed"
      }
      Failed = { Type = "Fail", Error = "DeployPipelineFailed", Cause = "See the SNS alert, the state machine log and /aws/sagemaker/Endpoints/<endpoint>." }
    }
  }
}

resource "aws_cloudwatch_log_group" "ml_v2_deploy_sm" {
  name              = "/aws/vendedlogs/states/${local.ml_v2_deploy_sm_name}"
  retention_in_days = 30
}

resource "aws_sfn_state_machine" "ml_v2_deploy" {
  name       = local.ml_v2_deploy_sm_name
  role_arn   = data.aws_iam_role.ml_v2_sfn.arn
  type       = "STANDARD"
  definition = jsonencode(local.ml_v2_deploy_definition)

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.ml_v2_deploy_sm.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
}

output "ml_v2_deploy_state_machine_arn" {
  description = "VERSION 2.2: deploys an Approved champion. Manual (re)deploy or rollback: start it with {\"detail\": {\"ModelPackageArn\": \"<package version arn>\"}}."
  value       = aws_sfn_state_machine.ml_v2_deploy.arn
}

output "ml_v2_model_package_group_name" {
  description = "VERSION 2.2: Model Registry group every champion is registered in."
  value       = local.ml_v2_model_package_group
}
