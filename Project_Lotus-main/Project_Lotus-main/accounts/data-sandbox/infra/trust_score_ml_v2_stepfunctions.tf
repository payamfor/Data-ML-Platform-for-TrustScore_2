# ===================================================================================
# VERSION 2 - Trust Score ML pipeline: the two Step Functions state machines
# =========================================================================================
# Added in v2; no existing file in this root was modified. See ../CHANGES_V2.md.
#
# TRAINING  (new training batch marker, or a drift retrain)
#   CheckAlreadyRunning -> BuildContext -> Prepare
#   -> Sweep (Map, N slices in parallel) -> CountFailedSlices -> Finalize
#   -> ReadChampion -> RegisterChampion -> [ApproveChampion] (v2.2) -> NotifySuccess
#     Finalize = train.py --mode finalize  merge the slices, pick the champion and
#                SAVE it as the job's model artifact (model.tar.gz)
#     Register = CreateModelPackage in the Model Registry; Approve fires the
#                EventBridge rule that deploys it (trust_score_ml_v2_registry.tf)
#
# SCORING   (new scoring batch marker)
#   BuildContext -> [CheckLiveEndpoint (v2.2)] -> Score
#   -> Score (Processing job; v2.2: scores through the LIVE endpoint)
#   -> ReadSummary -> DriftDetected? -> NotifyDrift -> [RetrainGate -> StartRetraining]
# ==================================================================================

# Optional pieces below are written `cond ? { ... } : null` inside merge().
# merge() skips null arguments, and null is the only "empty" value whose type
# unifies with an object that has attributes - `cond ? { a = 1 } : {}` is a type
# error ("true and false result expressions must have consistent types").

locals {
  ml_v2_hours = 3600

  ml_v2_models_root = "s3://${local.ml_v2_output_bucket}/${var.ml_v2_output_prefix}"
  ml_v2_job_environment = {
    "TS05_RUN_ID.$"    = "$.context.run_id"
    AWS_REGION         = data.aws_region.current.region
    AWS_DEFAULT_REGION = data.aws_region.current.region
    # Prevent thread lock contention on multi-core instances:
    OMP_NUM_THREADS      = "2"
    OPENBLAS_NUM_THREADS = "2"
    MKL_NUM_THREADS      = "2"
    NUMEXPR_NUM_THREADS  = "2"
  }

  ml_v2_sagemaker_retry = [{
    ErrorEquals     = ["SageMaker.AmazonSageMakerException", "SageMaker.ResourceLimitExceededException", "ThrottlingException"]
    IntervalSeconds = 30
    MaxAttempts     = 8
    BackoffRate     = 2
    MaxDelaySeconds = 600
    JitterStrategy  = "FULL"
  }]

  ml_v2_lambda_retry = [{
    ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
    IntervalSeconds = 2
    MaxAttempts     = 3
    BackoffRate     = 2
  }]

  ml_v2_catch_all = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NotifyFailure" }]

  ml_v2_job_result = {
    "job.$"    = "$.TrainingJobName"
    "status.$" = "$.TrainingJobStatus"
  }

  # One CreateTrainingJob request per job kind; only the name/hyperparameter
  # paths, the instance and the time limit differ.
  ml_v2_training_job = {
    for kind, spec in {
      prepare = {
        name = "$.context.prepare.job_name", params = "$.context.prepare.hyperparameters"
        type = var.ml_v2_prepare_instance_type, hours = var.ml_v2_max_runtime_hours.prepare
        env  = local.ml_v2_job_environment
      }
      sweep = {
        name = "$.slice.job_name", params = "$.slice.hyperparameters"
        type = var.ml_v2_sweep_instance_type, hours = var.ml_v2_max_runtime_hours.sweep
        env  = merge(local.ml_v2_job_environment, { "TS05_RUN_ID.$" = "$.run_id" })
      }
      finalize = {
        name = "$.context.finalize.job_name", params = "$.context.finalize.hyperparameters"
        type = var.ml_v2_finalize_instance_type, hours = var.ml_v2_max_runtime_hours.finalize
        env  = local.ml_v2_job_environment
      }
      } : kind => merge(
      {
        "TrainingJobName.$" = spec.name
        "HyperParameters.$" = spec.params
        AlgorithmSpecification = {
          TrainingImage     = local.ml_v2_training_image
          TrainingInputMode = "File"
        }
        RoleArn     = data.aws_iam_role.ml_v2_sagemaker_execution.arn
        Environment = spec.env
        OutputDataConfig = {
          S3OutputPath = "${local.ml_v2_output_root}sagemaker-output/training/"
        }
        ResourceConfig = {
          InstanceType   = spec.type
          InstanceCount  = 1
          VolumeSizeInGB = var.ml_v2_training_volume_size_gb
        }
        StoppingCondition = merge(
          { MaxRuntimeInSeconds = spec.hours * local.ml_v2_hours },
          var.ml_v2_use_spot_training ? { MaxWaitTimeInSeconds = max(spec.hours, var.ml_v2_spot_max_wait_hours) * local.ml_v2_hours } : null,
        )
        EnableManagedSpotTraining = var.ml_v2_use_spot_training
        Tags                      = local.ml_v2_job_tags
      },
      local.ml_v2_vpc_config != null ? { VpcConfig = local.ml_v2_vpc_config } : null,
    )
  }

  # ---------------------------------------------------------------------------
  # TRAINING
  # ---------------------------------------------------------------------------
  ml_v2_training_definition = {
    Comment = "Trust Score ML training (v2): data checks + feature selection -> sweep (N parallel Spot jobs) -> champion."
    StartAt = "CheckAlreadyRunning"
    States = merge({
      CheckAlreadyRunning = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sfn:listExecutions"
        Parameters = {
          StateMachineArn = local.ml_v2_training_sm_arn
          StatusFilter    = "RUNNING"
          MaxResults      = 10
        }
        ResultSelector = { "running.$" = "States.ArrayLength($.Executions)" }
        ResultPath     = "$.guard"
        Next           = "IsAnotherRunRunning"
        Catch          = local.ml_v2_catch_all
      }
      # This execution is itself RUNNING, so "another run" means more than one.
      IsAnotherRunRunning = {
        Type = "Choice"
        Choices = [{
          Variable           = "$.guard.running"
          NumericGreaterThan = 1
          Next               = "SkippedAlreadyRunning"
        }]
        Default = "BuildContext"
      }
      # A run already in progress may have read its inputs before this batch's
      # data landed, so it cannot be assumed to cover it. Say so instead of
      # skipping silently; retraining is manual in Phase 1 (PRD 7.1.7), so a
      # person starts the next run once the current one has finished.
      SkippedAlreadyRunning = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.ml_v2_alerts.arn
          Subject     = "Trust Score ML training skipped: a run is in progress"
          "Message.$" = "States.Format('Training execution {} was skipped because another training run is in progress. That run may have read its data before this batch arrived. When it finishes, start the training state machine again so the new batch is trained on.', $$.Execution.Name)"
        }
        ResultPath = null
        End        = true
      }
      BuildContext = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.ml_v2_helper.arn
          Payload = {
            action             = "training_context"
            "execution_name.$" = "$$.Execution.Name"
            "start_time.$"     = "$$.Execution.StartTime"
          }
        }
        ResultSelector = {
          "run_id.$"       = "$.Payload.run_id"
          "total_arms.$"   = "$.Payload.total_arms"
          "prepare.$"      = "$.Payload.prepare"
          "slices.$"       = "$.Payload.slices"
          "finalize.$"     = "$.Payload.finalize"
          "champion_key.$" = "$.Payload.champion_key" # v2.2
          "champion_uri.$" = "$.Payload.champion_uri" # v2.2
        }
        ResultPath = "$.context"
        Retry      = local.ml_v2_lambda_retry
        Catch      = local.ml_v2_catch_all
        Next       = "Prepare"
      }
      Prepare = {
        Type           = "Task"
        Resource       = "arn:aws:states:::sagemaker:createTrainingJob.sync"
        Parameters     = local.ml_v2_training_job.prepare
        ResultSelector = local.ml_v2_job_result
        ResultPath     = "$.results.prepare"
        Retry          = local.ml_v2_sagemaker_retry
        Catch          = local.ml_v2_catch_all
        Next           = "Sweep"
      }
      Sweep = {
        Type           = "Map"
        ItemsPath      = "$.context.slices"
        MaxConcurrency = var.ml_v2_sweep_slice_count
        ItemSelector = {
          "slice.$"  = "$$.Map.Item.Value"
          "run_id.$" = "$.context.run_id"
        }
        ItemProcessor = {
          ProcessorConfig = { Mode = "INLINE" }
          StartAt         = "SweepSlice"
          States = {
            SweepSlice = {
              Type           = "Task"
              Resource       = "arn:aws:states:::sagemaker:createTrainingJob.sync"
              Parameters     = local.ml_v2_training_job.sweep
              ResultSelector = local.ml_v2_job_result
              Retry          = local.ml_v2_sagemaker_retry
              # One failed slice must not abort the others: their work is
              # resumable and already paid for. Failures are counted below.
              Catch = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "SliceFailed" }]
              End   = true
            }
            SliceFailed = {
              Type       = "Pass"
              Parameters = { "job.$" = "$.slice.job_name", status = "Failed", "error.$" = "$.error" }
              End        = true
            }
          }
        }
        ResultPath = "$.results.sweep"
        Catch      = local.ml_v2_catch_all
        Next       = "CountFailedSlices"
      }
      CountFailedSlices = {
        Type       = "Pass"
        Parameters = { "failed.$" = "$.results.sweep[?(@.status == 'Failed')]" }
        ResultPath = "$.slice_check"
        Next       = "CountFailedSlicesLength"
      }
      CountFailedSlicesLength = {
        Type       = "Pass"
        Parameters = { "count.$" = "States.ArrayLength($.slice_check.failed)", "failed.$" = "$.slice_check.failed" }
        ResultPath = "$.slice_check"
        Next       = "AllSlicesSucceeded"
      }
      AllSlicesSucceeded = {
        Type = "Choice"
        Choices = [{
          Variable           = "$.slice_check.count"
          NumericGreaterThan = 0
          Next               = "SlicesFailed"
        }]
        Default = "Finalize"
      }
      SlicesFailed = {
        Type = "Pass"
        Parameters = {
          Error     = "SweepSlicesFailed"
          "Cause.$" = "States.Format('{} sweep slice(s) failed; re-run the execution to resume them: {}', $.slice_check.count, States.JsonToString($.slice_check.failed))"
        }
        ResultPath = "$.error"
        Next       = "NotifyFailure"
      }
      Finalize = {
        Type       = "Task"
        Resource   = "arn:aws:states:::sagemaker:createTrainingJob.sync"
        Parameters = local.ml_v2_training_job.finalize
        # v2.2: also the model artifact SageMaker packaged from /opt/ml/model -
        # the saved champion (champion.json + model.joblib + reference scores).
        ResultSelector = merge(local.ml_v2_job_result, { "model_data.$" = "$.ModelArtifacts.S3ModelArtifacts" })
        ResultPath     = "$.results.finalize"
        Retry          = local.ml_v2_sagemaker_retry
        Catch          = local.ml_v2_catch_all
        Next           = "ReadChampion"
      }

      # v2.2: the champion document finalize wrote (history copy, keyed by run id).
      ReadChampion = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:s3:getObject"
        Parameters = {
          Bucket  = local.ml_v2_output_bucket
          "Key.$" = "$.context.champion_key"
        }
        ResultSelector = { "doc.$" = "States.StringToJson($.Body)" }
        ResultPath     = "$.champion"
        Retry          = [{ ErrorEquals = ["S3.NoSuchKey", "S3.NoSuchKeyException", "S3.S3Exception"], IntervalSeconds = 10, MaxAttempts = 3, BackoffRate = 2 }]
        Catch          = local.ml_v2_catch_all
        Next           = "RegisterChampion"
      }
      # v2.2: a new version in the Model Registry. Always PendingManualApproval
      # first; approval (automatic below, or by a person) is what deploys it.
      RegisterChampion = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sagemaker:createModelPackage"
        Parameters = {
          ModelPackageGroupName       = local.ml_v2_model_package_group
          "ModelPackageDescription.$" = "States.Format('Trust Score champion {} ({} features, preprocessing {}, {}) from training run {}', $.champion.doc.model, $.champion.doc.top_n_features, $.champion.doc.preprocessing, $.champion.doc.hyperparam_id, $.context.run_id)"
          ModelApprovalStatus         = "PendingManualApproval"
          # Idempotent per run: a retried call returns the same version.
          "ClientToken.$" = "$.context.run_id"
          InferenceSpecification = {
            Containers = [{
              Image            = local.ml_v2_inference_image
              "ModelDataUrl.$" = "$.results.finalize.model_data"
              Environment      = local.ml_v2_live_model_environment
            }]
            SupportedContentTypes                   = ["application/x-parquet", "text/csv", "application/json"]
            SupportedResponseMIMETypes              = ["application/x-parquet", "application/json", "text/csv"]
            SupportedRealtimeInferenceInstanceTypes = [var.ml_v2_endpoint_instance_type]
          }
          ModelMetrics = {
            ModelQuality = {
              Statistics = { ContentType = "application/json", "S3Uri.$" = "$.context.champion_uri" }
            }
          }
          MetadataProperties = { "GeneratedBy.$" = "$.results.finalize.job" }
          CustomerMetadataProperties = {
            "run_id.$"        = "$.context.run_id"
            "model.$"         = "$.champion.doc.model"
            "hyperparam_id.$" = "$.champion.doc.hyperparam_id"
            "metric.$"        = "$.champion.doc.criteria.metric"
            "value.$"         = "States.Format('{}', $.champion.doc.value)"
            "training_job.$"  = "$.results.finalize.job"
          }
        }
        ResultSelector = { "arn.$" = "$.ModelPackageArn" }
        ResultPath     = "$.registration"
        Retry          = local.ml_v2_aws_sdk_retry
        Catch          = local.ml_v2_catch_all
        Next           = var.ml_v2_auto_approve_champion ? "ApproveChampion" : "NotifySuccess"
      }

      NotifySuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.ml_v2_alerts.arn
          Subject     = "Trust Score ML training finished"
          "Message.$" = "States.Format('Training run {} finished: {} arms swept. Champion {} ({} = {}) saved by {} and registered as {}. ${var.ml_v2_auto_approve_champion ? "It was auto-approved and is being deployed to ${local.ml_v2_endpoint_name}." : "It is PendingManualApproval: approve it to deploy it to ${local.ml_v2_endpoint_name}."}', $.context.run_id, $.context.total_arms, $.champion.doc.model, $.champion.doc.criteria.metric, $.champion.doc.value, $.results.finalize.job, $.registration.arn)"
        }
        ResultPath = null
        End        = true
      }

      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.ml_v2_alerts.arn
          Subject     = "Trust Score ML training FAILED"
          "Message.$" = "States.Format('Training execution {} failed: {}', $$.Execution.Name, States.JsonToString($.error))"
        }
        ResultPath = null
        Next       = "Failed"
      }
      Failed = { Type = "Fail", Error = "TrainingPipelineFailed", Cause = "See the SNS alert and the SageMaker job logs." }
      },
      var.ml_v2_auto_approve_champion ? {
        # v2.2: Pending -> Approved is the state change the deploy rule listens for.
        ApproveChampion = {
          Type     = "Task"
          Resource = "arn:aws:states:::aws-sdk:sagemaker:updateModelPackage"
          Parameters = {
            "ModelPackageArn.$"     = "$.registration.arn"
            ModelApprovalStatus     = "Approved"
            "ApprovalDescription.$" = "States.Format('Auto-approved by training run {} (ml_v2_auto_approve_champion = true).', $.context.run_id)"
          }
          ResultPath = null
          Retry      = local.ml_v2_aws_sdk_retry
          Catch      = local.ml_v2_catch_all
          Next       = "NotifySuccess"
        }
      } : {},
    )
  }

  # ---------------------------------------------------------------------------
  # SCORING
  # ---------------------------------------------------------------------------
  ml_v2_scoring_job = merge(
    {
      "ProcessingJobName.$" = "$.context.job_name"
      ProcessingResources = {
        ClusterConfig = {
          InstanceCount  = 1
          InstanceType   = var.ml_v2_scoring_instance_type
          VolumeSizeInGB = var.ml_v2_scoring_volume_size_gb
        }
      }
      AppSpecification = {
        ImageUri               = local.ml_v2_inference_image
        ContainerEntrypoint    = ["python3", "/opt/program/inference.py"]
        "ContainerArguments.$" = "$.context.container_arguments"
      }
      Environment = merge(
        {
          AWS_REGION         = data.aws_region.current.region
          AWS_DEFAULT_REGION = data.aws_region.current.region
        },
        # v2.2: inference.py scores through the live endpoint when this is set.
        var.ml_v2_batch_scoring_via_endpoint ? {
          TS05_SCORING_ENDPOINT          = local.ml_v2_endpoint_name
          TS05_ENDPOINT_ROWS_PER_REQUEST = tostring(var.ml_v2_endpoint_batch_rows_per_request)
          TS05_ENDPOINT_CONCURRENCY      = tostring(var.ml_v2_endpoint_batch_concurrency)
        } : null,
      )

      ProcessingOutputConfig = {
        Outputs = [{
          OutputName = "summary"
          S3Output = {
            "S3Uri.$"    = "$.context.summary_s3_prefix"
            LocalPath    = "/opt/ml/processing/output"
            S3UploadMode = "EndOfJob"
          }
        }]
      }
      RoleArn           = data.aws_iam_role.ml_v2_sagemaker_execution.arn
      StoppingCondition = { MaxRuntimeInSeconds = var.ml_v2_max_runtime_hours.scoring * local.ml_v2_hours }
      Tags              = local.ml_v2_job_tags
    },
    local.ml_v2_vpc_config != null ? { NetworkConfig = { VpcConfig = local.ml_v2_vpc_config } } : null,
  )

  ml_v2_scoring_after_ok = var.ml_v2_notify_on_scoring_success ? "NotifyScored" : "Done"

  ml_v2_scoring_states = merge(
    {
      BuildContext = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.ml_v2_helper.arn
          Payload = {
            action             = "scoring_context"
            "execution_name.$" = "$$.Execution.Name"
            "start_time.$"     = "$$.Execution.StartTime"
            "detail.$"         = "$.detail"
          }
        }
        ResultSelector = {
          "batch_date.$"          = "$.Payload.batch_date"
          "input_uri.$"           = "$.Payload.input_uri"
          "job_name.$"            = "$.Payload.job_name"
          "container_arguments.$" = "$.Payload.container_arguments"
          "summary_s3_prefix.$"   = "$.Payload.summary_s3_prefix"
          "summary_bucket.$"      = "$.Payload.summary_bucket"
          "summary_key.$"         = "$.Payload.summary_key"
        }
        ResultPath = "$.context"
        Retry      = local.ml_v2_lambda_retry
        Catch      = local.ml_v2_catch_all
        Next       = "Score"
      }
      Score = {
        Type           = "Task"
        Resource       = "arn:aws:states:::sagemaker:createProcessingJob.sync"
        Parameters     = local.ml_v2_scoring_job
        ResultSelector = { "job.$" = "$.ProcessingJobName", "status.$" = "$.ProcessingJobStatus" }
        ResultPath     = "$.results.score"
        Retry          = local.ml_v2_sagemaker_retry
        Catch          = local.ml_v2_catch_all
        Next           = "ReadSummary"
      }
      ReadSummary = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:s3:getObject"
        Parameters = {
          "Bucket.$" = "$.context.summary_bucket"
          "Key.$"    = "$.context.summary_key"
        }
        ResultSelector = { "summary.$" = "States.StringToJson($.Body)" }
        ResultPath     = "$.scoring"
        # An EndOfJob upload can lag the job's Completed status by seconds.
        Retry = [{ ErrorEquals = ["S3.NoSuchKey", "S3.NoSuchKeyException", "S3.S3Exception"], IntervalSeconds = 10, MaxAttempts = 3, BackoffRate = 2 }]
        Catch = local.ml_v2_catch_all
        Next  = "DriftDetected"
      }
      DriftDetected = {
        Type = "Choice"
        Choices = [{
          Variable      = "$.scoring.summary.retrain_recommended"
          BooleanEquals = true
          Next          = "NotifyDrift"
        }]
        Default = local.ml_v2_scoring_after_ok
      }
      NotifyDrift = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.ml_v2_alerts.arn
          Subject     = "Trust Score drift: retraining recommended"
          "Message.$" = "States.Format('Batch {} was scored, but the data has drifted from what the model was trained on: {}. Scores: {}. Automatic retraining is ${var.ml_v2_auto_retrain_on_drift ? "ON (subject to the ${var.ml_v2_retrain_cooldown_days}-day cooldown)" : "OFF - start the training state machine manually if appropriate"}.', $.context.batch_date, States.JsonToString($.scoring.summary.reasons), $.scoring.summary.output_prefix)"
        }
        ResultPath = null
        Next       = var.ml_v2_auto_retrain_on_drift ? "RetrainGate" : "Done"
      }
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.ml_v2_alerts.arn
          Subject     = "Trust Score batch scoring FAILED"
          "Message.$" = "States.Format('Scoring execution {} failed: {}', $$.Execution.Name, States.JsonToString($.error))"
        }
        ResultPath = null
        Next       = "Failed"
      }
      Done   = { Type = "Succeed" }
      Failed = { Type = "Fail", Error = "ScoringPipelineFailed", Cause = "See the SNS alert and the SageMaker processing job logs." }
    },
    var.ml_v2_notify_on_scoring_success ? {
      NotifyScored = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.ml_v2_alerts.arn
          Subject     = "Trust Score batch scored"
          "Message.$" = "States.Format('Batch {} scored: {} rows, review queue of {} at {}.', $.context.batch_date, $.scoring.summary.rows_scored, $.scoring.summary.queue_length, $.scoring.summary.output_prefix)"
        }
        ResultPath = null
        Next       = "Done"
      }
    } : null,

    # v2.2: do not spend the feature cluster on a night that cannot be scored.
    var.ml_v2_batch_scoring_via_endpoint ? {
      CheckLiveEndpoint = {
        Type           = "Task"
        Resource       = "arn:aws:states:::aws-sdk:sagemaker:describeEndpoint"
        Parameters     = { EndpointName = local.ml_v2_endpoint_name }
        ResultSelector = { "status.$" = "$.EndpointStatus" }
        ResultPath     = "$.endpoint"
        Retry          = [{ ErrorEquals = ["SageMaker.ThrottlingException", "ThrottlingException"], IntervalSeconds = 10, MaxAttempts = 3, BackoffRate = 2 }]
        # DescribeEndpoint fails when the endpoint does not exist (no champion
        # has been approved and deployed yet).
        Catch = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NoLiveEndpoint" }]
        Next  = "LiveEndpointReady"
      }
      LiveEndpointReady = {
        Type = "Choice"
        Choices = [
          { Variable = "$.endpoint.status", StringEquals = "InService", Next = "Score" },
          # A deployment in progress: wait for it, so the whole night is scored
          # by one champion.
          {
            Or   = [for status in local.ml_v2_endpoint_transitional_statuses : { Variable = "$.endpoint.status", StringEquals = status }]
            Next = "WaitForLiveEndpoint"
          },
        ]
        Default = "LiveEndpointUnusable"
      }
      WaitForLiveEndpoint = {
        Type    = "Wait"
        Seconds = 300
        Next    = "CheckLiveEndpoint"
      }
      NoLiveEndpoint = {
        Type = "Pass"
        Parameters = {
          Error     = "NoLiveEndpoint"
          "Cause.$" = "States.Format('Endpoint ${local.ml_v2_endpoint_name} is not available ({}). A champion must be trained, registered, Approved and deployed first; or set ml_v2_batch_scoring_via_endpoint = false to score in-process.', States.JsonToString($.error))"
        }
        ResultPath = "$.error"
        Next       = "NotifyFailure"
      }
      LiveEndpointUnusable = {
        Type = "Pass"
        Parameters = {
          Error     = "LiveEndpointUnusable"
          "Cause.$" = "States.Format('Endpoint ${local.ml_v2_endpoint_name} is {}; the night was not scored.', $.endpoint.status)"
        }
        ResultPath = "$.error"
        Next       = "NotifyFailure"
      }
    } : null,

    var.ml_v2_auto_retrain_on_drift ? {
      RetrainGate = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.ml_v2_helper.arn
          Payload = {
            action         = "retrain_gate"
            "start_time.$" = "$$.Execution.StartTime"
          }
        }
        ResultSelector = { "allowed.$" = "$.Payload.allowed", "next_allowed_after.$" = "$.Payload.next_allowed_after" }
        ResultPath     = "$.gate"
        Retry          = local.ml_v2_lambda_retry
        Catch          = local.ml_v2_catch_all
        Next           = "RetrainAllowed"
      }
      RetrainAllowed = {
        Type = "Choice"
        Choices = [{
          Variable      = "$.gate.allowed"
          BooleanEquals = true
          Next          = "StartRetraining"
        }]
        Default = "Done"
      }
      StartRetraining = {
        Type     = "Task"
        Resource = "arn:aws:states:::states:startExecution"
        Parameters = {
          StateMachineArn = local.ml_v2_training_sm_arn
          Input = {
            source                                         = "drift"
            "batch_date.$"                                 = "$.context.batch_date"
            "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$" = "$$.Execution.Id"
          }
        }
        ResultPath = null
        Catch      = local.ml_v2_catch_all
        Next       = "Done"
      }
    } : null,
  )

  ml_v2_scoring_definition = {
    Comment = "Trust Score ML nightly batch scoring (v2): score -> review queue -> drift check -> optional retrain."
    StartAt = "BuildContext"
    States  = local.ml_v2_scoring_states
  }

  ml_v2_batch_scoring_states = merge(local.ml_v2_scoring_states, {
    BuildContext = merge(local.ml_v2_scoring_states.BuildContext, {
      Parameters = merge(
        local.ml_v2_scoring_states.BuildContext.Parameters,
        {
          Payload = merge(
            local.ml_v2_scoring_states.BuildContext.Parameters.Payload,
            { action = "batch_scoring_context" },
          )
        },
      )
    })
  })

  ml_v2_batch_scoring_definition = {
    Comment = "Trust Score ML batch scoring: same inference flow as validation scoring, with batch-specific input and output partitions."
    StartAt = "BuildContext"
    States  = local.ml_v2_batch_scoring_states
  }
}

# Log groups and logging follow stepfunctions_gold.tf.
resource "aws_cloudwatch_log_group" "ml_v2_training_sm" {
  name              = "/aws/vendedlogs/states/${local.ml_v2_training_sm_name}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "ml_v2_scoring_sm" {
  name              = "/aws/vendedlogs/states/${local.ml_v2_scoring_sm_name}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "ml_v2_batch_scoring_sm" {
  name              = "/aws/vendedlogs/states/${local.ml_v2_batch_scoring_sm_name}"
  retention_in_days = 30
}

resource "aws_sfn_state_machine" "ml_v2_training" {
  name       = local.ml_v2_training_sm_name
  role_arn   = data.aws_iam_role.ml_v2_sfn.arn
  type       = "STANDARD"
  definition = jsonencode(local.ml_v2_training_definition)

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.ml_v2_training_sm.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
}

resource "aws_sfn_state_machine" "ml_v2_scoring" {
  name       = local.ml_v2_scoring_sm_name
  role_arn   = data.aws_iam_role.ml_v2_sfn.arn
  type       = "STANDARD"
  definition = jsonencode(local.ml_v2_scoring_definition)

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.ml_v2_scoring_sm.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
}

resource "aws_sfn_state_machine" "ml_v2_batch_scoring" {
  name       = local.ml_v2_batch_scoring_sm_name
  role_arn   = data.aws_iam_role.ml_v2_sfn.arn
  type       = "STANDARD"
  definition = jsonencode(local.ml_v2_batch_scoring_definition)

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.ml_v2_batch_scoring_sm.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
}
