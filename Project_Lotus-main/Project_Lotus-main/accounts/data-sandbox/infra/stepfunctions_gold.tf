# Gold: resolve the release, then five stages, strictly sequential, one run id.
#
# EventBridge starts this execution directly (a states:StartExecution target -
# see security/stepfunctions_iam.tf's eventbridge_gold_trigger role), so the
# raw execution input is the S3 "Object Created" event for the release.json
# object, not a resolved run id. The first state, ResolveRelease, calls the
# control Lambda's `release_gate` action to turn that into one, then merges
# the result onto the execution's own root - see that state's comment.
#
# Each of the five stages is TWO states, not one:
#
#   BuildArgs_<stage>  Task, lambda:invoke of the control Lambda's
#                      `build_gold_args` action. Reads the console-editable
#                      job config object in S3 (gold_job_config.tf) and
#                      returns the full spark-submit argument list for this
#                      one stage. See that action's docstring for why this
#                      has to happen in code and cannot be inline ASL: there
#                      is no Amazon States Language intrinsic that flattens an
#                      arbitrary-length list of --conf entries into repeated
#                      CLI flags.
#   Stage_<stage>      Task, elasticmapreduce:addStep.sync. Its Args is a bare
#                      JSONPath reference to what BuildArgs_<stage> returned -
#                      not a literal HadoopJarStep.Args string built in
#                      Terraform, the way it was before this file started
#                      reading arguments from S3.
#
# The five Stage_* states are the five STAGES of one logical Gold run, not
# five independent jobs. Three properties of the staged job drive this shape
# and must not be "simplified" away:
#
#   1. Every stage receives the SAME --run-id. The job raises a ConfigError if
#      it is missing, and gold_run_control refuses a reused id, so the id is
#      minted once, by ResolveRelease, and threaded through as $.run_id -
#      never regenerated per state.
#   2. EMR step concurrency must be 1. The cluster sets
#      step_concurrency_level = 1; the job detects a violation as duplicate
#      running records and fails.
#   3. Stage N refuses to start until stage N-1's manifest exists. The
#      sequential chain here matches a dependency the job already enforces, so
#      a mis-ordered definition fails loudly rather than corrupting a run.
#
# Failure handling: after the normal stage retry is exhausted, recovery is
# submitted to the active recovery cluster before termination (normally the
# SAME transient cluster). The recovery command separately records the failed
# cluster and recovery cluster so a reviewed replay can use replacement compute.
# A pre-publish
# failure verifies unchanged Gold heads and appends the failed control row. A
# publish failure first restores and verifies all five pinned heads in a
# rollback-only step, then runs a separate control-only finalizer. If rollback
# or verification fails, finalization is skipped and a P1 alert is sent.
#
# --first-run / --watermark-from: appended by build_gold_args only for the
# prepare stage, and only when $.first_run is true - the staged job reads
# them once, at prepare, and freezes them into its own run state, so passing
# them again on a later stage would do nothing but would misstate what
# actually happened if anyone reads that stage's own argument list back later.

locals {
  sfn_gold_name = "${var.project}-${var.environment}-trust-score-gold"

  gold_stages     = ["prepare", "accounts", "customers", "validate", "publish"]
  gold_last_stage = local.gold_stages[length(local.gold_stages) - 1]

  # The part of a stage's pair that does not depend on where it sits in the
  # chain. Factored out so the Next-carrying stages and the End-carrying final
  # stage can be built as two separately-typed maps without duplicating the
  # state bodies. See the comment on `States` for why the two have to stay
  # separately typed.
  gold_build_args_body = {
    for stage in local.gold_stages :
    stage => {
      Type     = "Task"
      Resource = "arn:aws:states:::lambda:invoke"
      Parameters = {
        FunctionName = aws_lambda_function.silver_gold_control.arn
        Payload = {
          action              = "build_gold_args"
          stage               = stage
          "run_id.$"          = "$.run_id"
          "first_run.$"       = "$.first_run"
          "config_overlays.$" = "$.config_overlays"
          "overrides.$"       = "$.overrides"
        }
      }
      ResultPath = "$.gold_args"

      Retry = [{
        ErrorEquals     = ["States.ALL"]
        MaxAttempts     = 1
        IntervalSeconds = 15
        BackoffRate     = 1.0
      }]

      Catch = [{
        ErrorEquals = ["States.ALL"]
        ResultPath  = "$.error"
        # Prepare argument construction precedes the reservation. Later
        # argument failures happen after prepare opened the reservation and
        # therefore use the no-step-submitted finalizer path.
        Next = stage == local.gold_stages[0] ? "TerminateOnError" : "BuildArgsFailureRecoveryArgs_${stage}"
      }]
    }
  }

  gold_stage_body = {
    for stage in local.gold_stages :
    stage => {
      Type     = "Task"
      Resource = "arn:aws:states:::elasticmapreduce:addStep.sync"

      Parameters = {
        "ClusterId.$"    = "$.cluster_record.Payload.cluster_id"
        ExecutionRoleArn = data.aws_iam_role.gold_job_runtime.arn

        Step = {
          "Name.$" = "States.Format('gold-${stage}-{}', $.run_id)"

          # CONTINUE, not CANCEL_AND_WAIT: the state machine decides what
          # happens next, and a cluster-level cancel would race it.
          ActionOnFailure = "CONTINUE"

          HadoopJarStep = {
            Jar      = "command-runner.jar"
            "Args.$" = "$.gold_args.Payload.args"
          }
        }
      }

      ResultPath = "$.stages.${stage}"

      Retry = [{
        ErrorEquals     = ["States.ALL"]
        MaxAttempts     = 1
        IntervalSeconds = 30
        BackoffRate     = 1.0
      }]

      Catch = [{
        ErrorEquals = ["States.ALL"]
        ResultPath  = "$.error"
        Next        = stage == local.gold_last_stage ? "BuildPublishRollbackArgs" : "BuildControlRecoveryArgs_${stage}"
      }]
    }
  }

  # Control finalization is generated per failed stage so the stage name is a
  # trusted definition literal. It is a separate Spark application from table
  # rollback and never runs after a failed publish rollback.
  gold_control_recovery_build_body = {
    for stage in local.gold_stages :
    stage => {
      Type     = "Task"
      Resource = "arn:aws:states:::lambda:invoke"
      Parameters = {
        FunctionName = aws_lambda_function.silver_gold_control.arn
        Payload = {
          action                  = "build_gold_failure_args"
          recovery_type           = "finalize"
          failure_kind            = "spark_step"
          failed_stage            = stage
          "run_id.$"              = "$.run_id"
          "failed_cluster_id.$"   = "$.cluster_record.Payload.cluster_id"
          "recovery_cluster_id.$" = "$.cluster_record.Payload.cluster_id"
          "config_overlays.$"     = "$.config_overlays"
          "overrides.$"           = "$.overrides"
        }
      }
      ResultPath = "$.finalizer_args"
      Next       = "FinalizeFailedGoldRun_${stage}"
      Retry = [{
        ErrorEquals     = ["States.ALL"]
        MaxAttempts     = 1
        IntervalSeconds = 15
        BackoffRate     = 1.0
      }]
      Catch = [{
        ErrorEquals = ["States.ALL"]
        ResultPath  = "$.recovery_error"
        Next        = "TerminateRecoveryRequired"
      }]
    }
  }

  # A BuildArgs failure after prepare submitted no Spark application for the
  # target stage. The finalizer proves that exact absence, verifies the prior
  # stage marker and all five pinned heads, then closes the open reservation.
  gold_argument_failure_recovery_build_body = {
    for stage in slice(local.gold_stages, 1, length(local.gold_stages)) :
    stage => {
      Type     = "Task"
      Resource = "arn:aws:states:::lambda:invoke"
      Parameters = {
        FunctionName = aws_lambda_function.silver_gold_control.arn
        Payload = {
          action                  = "build_gold_failure_args"
          recovery_type           = "finalize"
          failure_kind            = "argument_build"
          failed_stage            = stage
          "run_id.$"              = "$.run_id"
          "failed_cluster_id.$"   = "$.cluster_record.Payload.cluster_id"
          "recovery_cluster_id.$" = "$.cluster_record.Payload.cluster_id"
          "config_overlays.$"     = "$.config_overlays"
          "overrides.$"           = "$.overrides"
        }
      }
      ResultPath = "$.finalizer_args"
      Next       = "FinalizeFailedGoldRun_${stage}"
      Retry = [{
        ErrorEquals     = ["States.ALL"]
        MaxAttempts     = 1
        IntervalSeconds = 15
        BackoffRate     = 1.0
      }]
      Catch = [{
        ErrorEquals = ["States.ALL"]
        ResultPath  = "$.recovery_error"
        Next        = "TerminateRecoveryRequired"
      }]
    }
  }

  gold_control_recovery_step_body = {
    for stage in local.gold_stages :
    stage => {
      Type     = "Task"
      Resource = "arn:aws:states:::elasticmapreduce:addStep.sync"
      Parameters = {
        "ClusterId.$"    = "$.cluster_record.Payload.cluster_id"
        ExecutionRoleArn = data.aws_iam_role.gold_job_runtime.arn
        Step = {
          "Name.$"        = "States.Format('gold-finalize-failed-${stage}-{}', $.run_id)"
          ActionOnFailure = "CONTINUE"
          HadoopJarStep = {
            Jar      = "command-runner.jar"
            "Args.$" = "$.finalizer_args.Payload.args"
          }
        }
      }
      ResultPath = "$.recovery.finalize"
      Next       = "TerminateOnError"
      Catch = [{
        ErrorEquals = ["States.ALL"]
        ResultPath  = "$.recovery_error"
        Next        = "TerminateRecoveryRequired"
      }]
    }
  }
}

resource "aws_cloudwatch_log_group" "sfn_gold" {
  name              = "/aws/vendedlogs/states/${local.sfn_gold_name}"
  retention_in_days = 30
}

resource "aws_sfn_state_machine" "gold" {
  name     = local.sfn_gold_name
  role_arn = data.aws_iam_role.sfn_gold.arn

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_gold.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  definition = jsonencode({
    Comment = "Gold lineage with failure-only exact-snapshot publish rollback and separate run-control finalization."
    StartAt = "ResolveRelease"

    # Several separately-typed maps, merged at the top level.
    #
    # The Next-carrying stages and the End-carrying final stage cannot be
    # built by one for-expression with a conditional attribute. Two ways of
    # doing that both produce invalid Amazon States Language:
    #
    #   Next = <cond> ? null : "..."   jsonencode keeps the null, and a state
    #                                  with "Next": null is rejected.
    #   merge(body, cond ? {End=true} : {Next="..."})
    #                                  type unification across the one
    #                                  for-expression coerces the boolean to
    #                                  the string "true", and "End": "true" is
    #                                  rejected.
    #
    # Merging distinct maps at the top level keeps each key's own type, so
    # "End" stays a JSON boolean and "Next" is simply absent from the final
    # stage.
    States = merge(
      {
        # EventBridge starts this execution directly as a StartExecution
        # target (see security/stepfunctions_iam.tf's eventbridge_gold_trigger
        # role for why - a Lambda target would need lambda:AddPermission,
        # which this account's CI role is explicitly denied), so the raw
        # execution input here is the S3 EventBridge event itself: $.detail
        # is {bucket:{name}, object:{key}}, nothing has resolved run_id yet.
        #
        # This state calls the same release_gate logic that used to run
        # before Gold was started, and is now what starts it: it reads
        # release.json, re-checks the control table, and mints the run id.
        # Passing the complete start input as `request` also supports controlled
        # manual starts with either run_date or release_uri. The Lambda validates
        # that exactly one automatic/manual input form was supplied.
        # ResultSelector projects the Lambda's Payload fields onto the
        # execution's OWN root ($), replacing the raw EventBridge event, so
        # every stage below reads $.run_id exactly as it always has.
        ResolveRelease = {
          Type     = "Task"
          Resource = "arn:aws:states:::lambda:invoke"
          Parameters = {
            FunctionName = aws_lambda_function.silver_gold_control.arn
            Payload = {
              action      = "release_gate"
              "request.$" = "$"
            }
          }
          ResultSelector = {
            "run_id.$"          = "$.Payload.run_id"
            "run_date.$"        = "$.Payload.run_date"
            "first_run.$"       = "$.Payload.first_run"
            "dry_run.$"         = "$.Payload.dry_run"
            "config_overlays.$" = "$.Payload.config_overlays"
            "overrides.$"       = "$.Payload.overrides"
            "released_by.$"     = "$.Payload.released_by"
            "release_uri.$"     = "$.Payload.release_uri"
            stages              = {}
            recovery            = {}
          }
          ResultPath = "$"
          Next       = "ReadCluster"

          Retry = [{
            ErrorEquals     = ["States.ALL"]
            MaxAttempts     = 1
            IntervalSeconds = 15
            BackoffRate     = 1.0
          }]

          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "ReleaseResolutionFailed"
          }]
        }

        ReadCluster = {
          Type     = "Task"
          Resource = "arn:aws:states:::lambda:invoke"
          Parameters = {
            FunctionName = aws_lambda_function.silver_gold_control.arn
            Payload      = { action = "get_cluster", "run_date.$" = "$.run_date" }
          }
          ResultPath = "$.cluster_record"
          Next       = "ClusterReady"
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "ClusterLookupFailed"
          }]
        }

        ClusterReady = {
          Type = "Choice"
          Choices = [{
            Variable     = "$.cluster_record.Payload.status"
            StringEquals = "ready"
            Next         = "BuildArgs_${local.gold_stages[0]}"
          }]
          Default = "ClusterLookupFailed"
        }

        ClusterLookupFailed = {
          Type     = "Task"
          Resource = "arn:aws:states:::sns:publish"
          Parameters = {
            TopicArn    = aws_sns_topic.pipeline_alerts["gold"].arn
            Subject     = "Gold cluster unavailable"
            "Message.$" = "States.Format('No ready daily EMR cluster exists for Gold run {} on {}.', $.run_id, $.run_date)"
          }
          Next = "Failed"
        }
      },
      # BuildArgs_<stage> always flows into its own Stage_<stage> - that Next
      # never varies by position - so this map does not need the
      # last-stage-is-different split the Stage_* maps below need.
      {
        for stage in local.gold_stages :
        "BuildArgs_${stage}" => merge(
          local.gold_build_args_body[stage],
          { Next = "Stage_${stage}" }
        )
      },
      {
        for index, stage in local.gold_stages :
        "Stage_${stage}" => merge(
          local.gold_stage_body[stage],
          { Next = "BuildArgs_${local.gold_stages[index + 1]}" }
        )
        if index < length(local.gold_stages) - 1
      },
      {
        "Stage_${local.gold_last_stage}" = merge(
          local.gold_stage_body[local.gold_last_stage],
          { Next = "TerminateCluster" }
        )
      },
      {
        for stage in local.gold_stages :
        "BuildControlRecoveryArgs_${stage}" => local.gold_control_recovery_build_body[stage]
      },
      {
        for stage in local.gold_stages :
        "FinalizeFailedGoldRun_${stage}" => local.gold_control_recovery_step_body[stage]
      },
      {
        for stage in slice(local.gold_stages, 1, length(local.gold_stages)) :
        "BuildArgsFailureRecoveryArgs_${stage}" => local.gold_argument_failure_recovery_build_body[stage]
      },
      {
        # Publish is the only business-table-writing stage. Its failure branch
        # rolls back and verifies data first. Only a successful rollback is
        # allowed to enter the separately generated publish finalizer above.
        BuildPublishRollbackArgs = {
          Type     = "Task"
          Resource = "arn:aws:states:::lambda:invoke"
          Parameters = {
            FunctionName = aws_lambda_function.silver_gold_control.arn
            Payload = {
              action                  = "build_gold_failure_args"
              recovery_type           = "rollback_publish"
              failure_kind            = "spark_step"
              failed_stage            = "publish"
              "run_id.$"              = "$.run_id"
              "failed_cluster_id.$"   = "$.cluster_record.Payload.cluster_id"
              "recovery_cluster_id.$" = "$.cluster_record.Payload.cluster_id"
              "config_overlays.$"     = "$.config_overlays"
              "overrides.$"           = "$.overrides"
            }
          }
          ResultPath = "$.rollback_args"
          Next       = "RollbackFailedPublish"
          Retry = [{
            ErrorEquals     = ["States.ALL"]
            MaxAttempts     = 1
            IntervalSeconds = 15
            BackoffRate     = 1.0
          }]
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.recovery_error"
            Next        = "TerminateRecoveryRequired"
          }]
        }

        RollbackFailedPublish = {
          Type     = "Task"
          Resource = "arn:aws:states:::elasticmapreduce:addStep.sync"
          Parameters = {
            "ClusterId.$"    = "$.cluster_record.Payload.cluster_id"
            ExecutionRoleArn = data.aws_iam_role.gold_job_runtime.arn
            Step = {
              "Name.$"        = "States.Format('gold-rollback-failed-publish-{}', $.run_id)"
              ActionOnFailure = "CONTINUE"
              HadoopJarStep = {
                Jar      = "command-runner.jar"
                "Args.$" = "$.rollback_args.Payload.args"
              }
            }
          }
          ResultPath = "$.recovery.rollback"
          Next       = "BuildControlRecoveryArgs_publish"
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.recovery_error"
            # Do not run recover_gold_run.py. The unresolved reservation blocks
            # another Gold run until an operator completes exact recovery.
            Next = "TerminateRecoveryRequired"
          }]
        }

        # Distinct from StageFailed below: $.run_id and $.run_date do not
        # exist yet if ResolveRelease itself is what failed, so this message
        # cannot reference them the way StageFailed does.
        ReleaseResolutionFailed = {
          Type     = "Task"
          Resource = "arn:aws:states:::sns:publish"
          Parameters = {
            TopicArn    = aws_sns_topic.pipeline_alerts["gold"].arn
            Subject     = "Gold release resolution failed"
            "Message.$" = "States.Format('Could not resolve the Gate 2 release object into a Gold run before any stage started. Cause: {}', $.error.Cause)"
          }
          Next = "Failed"
        }

        StageFailed = {
          Type           = "Task"
          Resource       = "arn:aws:states:::sns:publish"
          TimeoutSeconds = 30
          Parameters = {
            TopicArn    = aws_sns_topic.pipeline_alerts["gold"].arn
            "Subject.$" = "States.Format('Gold stage failed: {}', $.run_id)"
            "Message.$" = "States.Format('Gold run {} for run_date {} failed after its normal retry. Daily EMR cluster {} was terminated. When a run reservation existed, the failure branch verified safe table state before appending the failed control record. The last successful watermark remains authoritative and the next execution must use a new run_id. Error: {}', $.run_id, $.run_date, $.cluster_record.Payload.cluster_id, States.JsonToString($.error))"
          }
          ResultPath = "$.failure_notification"
          Next       = "Failed"
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.alert_error"
            Next        = "Failed"
          }]
        }

        TerminateCluster = {
          Type       = "Task"
          Resource   = "arn:aws:states:::elasticmapreduce:terminateCluster.sync"
          Parameters = { "ClusterId.$" = "$.cluster_record.Payload.cluster_id" }
          ResultPath = "$.termination"
          Next       = "MarkClusterTerminated"
          Retry = [{
            ErrorEquals     = ["States.ALL"]
            IntervalSeconds = 30
            BackoffRate     = 2
            MaxAttempts     = 2
          }]
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.termination_error"
            Next        = "TerminationFailed"
          }]
        }

        TerminateOnError = {
          Type       = "Task"
          Resource   = "arn:aws:states:::elasticmapreduce:terminateCluster.sync"
          Parameters = { "ClusterId.$" = "$.cluster_record.Payload.cluster_id" }
          ResultPath = "$.termination"
          Next       = "MarkClusterTerminatedOnError"
          Retry = [{
            ErrorEquals     = ["States.ALL"]
            IntervalSeconds = 30
            BackoffRate     = 2
            MaxAttempts     = 2
          }]
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.termination_error"
            Next        = "TerminationFailed"
          }]
        }

        # A rollback/finalizer failure is not a normal failed run: the control
        # reservation deliberately remains unresolved. Terminate first, close
        # the cluster registry record, then send the explicit P1 alert.
        TerminateRecoveryRequired = {
          Type       = "Task"
          Resource   = "arn:aws:states:::elasticmapreduce:terminateCluster.sync"
          Parameters = { "ClusterId.$" = "$.cluster_record.Payload.cluster_id" }
          ResultPath = "$.termination"
          Next       = "MarkClusterTerminatedRecoveryRequired"
          Retry = [{
            ErrorEquals     = ["States.ALL"]
            IntervalSeconds = 30
            BackoffRate     = 2
            MaxAttempts     = 2
          }]
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.termination_error"
            Next        = "RecoveryTerminationFailedAlert"
          }]
        }

        MarkClusterTerminatedRecoveryRequired = {
          Type     = "Task"
          Resource = "arn:aws:states:::lambda:invoke"
          Parameters = {
            FunctionName = aws_lambda_function.silver_gold_control.arn
            Payload      = { action = "close_cluster", "run_date.$" = "$.run_date" }
          }
          ResultPath = "$.cluster_closed"
          Retry = [{
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
            IntervalSeconds = 5
            BackoffRate     = 2
            MaxAttempts     = 2
          }]
          Next = "RecoveryRequiredAlert"
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.mark_error"
            Next        = "RecoveryRequiredAlert"
          }]
        }

        RecoveryRequiredAlert = {
          Type           = "Task"
          Resource       = "arn:aws:states:::sns:publish"
          TimeoutSeconds = 30
          Parameters = {
            TopicArn    = aws_sns_topic.pipeline_alerts["gold"].arn
            "Subject.$" = "States.Format('P1 Gold recovery required: {}', $.run_id)"
            "Message.$" = "States.Format('P1: Gold run {} failed and automatic rollback or control finalization did not complete. Cluster {} has been terminated. recover_gold_run.py was not allowed to close an unsafe publish reservation; gold_run_control may remain running and will block the next Gold run. Do not delete the reservation or advance the watermark. Review the failed publish, pinned snapshots, rollback journal, and recovery_error: {}', $.run_id, $.cluster_record.Payload.cluster_id, States.JsonToString($.recovery_error))"
          }
          ResultPath = "$.recovery_notification"
          Next       = "RecoveryRequired"
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.alert_error"
            Next        = "RecoveryRequired"
          }]
        }

        RecoveryTerminationFailedAlert = {
          Type           = "Task"
          Resource       = "arn:aws:states:::sns:publish"
          TimeoutSeconds = 30
          Parameters = {
            TopicArn    = aws_sns_topic.pipeline_alerts["gold"].arn
            "Subject.$" = "States.Format('P1 Gold recovery and termination required: {}', $.run_id)"
            "Message.$" = "States.Format('P1: Gold run {} has an unresolved recovery reservation and cluster {} could not be terminated. Recovery error: {}. Termination error: {}. Stop the cluster and complete reviewed recovery before another Gold run.', $.run_id, $.cluster_record.Payload.cluster_id, States.JsonToString($.recovery_error), States.JsonToString($.termination_error))"
          }
          ResultPath = "$.recovery_notification"
          Next       = "CleanupFailed"
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.alert_error"
            Next        = "CleanupFailed"
          }]
        }

        MarkClusterTerminated = {
          Type     = "Task"
          Resource = "arn:aws:states:::lambda:invoke"
          Parameters = {
            FunctionName = aws_lambda_function.silver_gold_control.arn
            Payload      = { action = "close_cluster", "run_date.$" = "$.run_date" }
          }
          ResultPath = "$.cluster_closed"
          Retry = [{
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
            IntervalSeconds = 5
            BackoffRate     = 2
            MaxAttempts     = 2
          }]
          Next = "StartFeatureEngineering"
        }

        StartFeatureEngineering = {
          Type     = "Task"
          Resource = "arn:aws:states:::states:startExecution"
          Parameters = {
            StateMachineArn = aws_sfn_state_machine.ml_feature_engineering.arn
            Input = {
              mode                                           = "training"
              "gold_run_id.$"                                = "$.run_id"
              "gold_run_date.$"                              = "$.run_date"
              "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$" = "$$.Execution.Id"
            }
          }
          ResultPath = "$.feature_engineering_execution"
          End        = true
        }

        MarkClusterTerminatedOnError = {
          Type     = "Task"
          Resource = "arn:aws:states:::lambda:invoke"
          Parameters = {
            FunctionName = aws_lambda_function.silver_gold_control.arn
            Payload      = { action = "close_cluster", "run_date.$" = "$.run_date" }
          }
          ResultPath = "$.cluster_closed"
          Retry = [{
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
            IntervalSeconds = 5
            BackoffRate     = 2
            MaxAttempts     = 2
          }]
          Next = "StageFailed"
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.mark_error"
            Next        = "StageFailed"
          }]
        }

        TerminationFailed = {
          Type           = "Task"
          Resource       = "arn:aws:states:::sns:publish"
          TimeoutSeconds = 30
          Parameters = {
            TopicArn    = aws_sns_topic.pipeline_alerts["gold"].arn
            Subject     = "Gold EMR termination failed"
            "Message.$" = "States.Format('Gold run {} failed or finished, but terminating daily cluster {} failed. Error: {}. Check the Gold Step Functions role, workflow tag, and one-hour EMR idle termination fallback.', $.run_id, $.cluster_record.Payload.cluster_id, States.JsonToString($.termination_error))"
          }
          ResultPath = "$.termination_notification"
          Next       = "CleanupFailed"
          Catch = [{
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.termination_alert_error"
            Next        = "CleanupFailed"
          }]
        }

        CleanupFailed = {
          Type  = "Fail"
          Error = "GoldClusterTerminationFailed"
          Cause = "Daily EMR cluster termination failed after cleanup retries; inspect execution history and terminate the cluster manually if necessary."
        }

        Failed = {
          Type  = "Fail"
          Error = "GoldStageFailed"
          Cause = "A Gold stage failed after its normal retry; see the SNS alert, recovery output and EMR step logs."
        }

        RecoveryRequired = {
          Type  = "Fail"
          Error = "GoldRecoveryRequired"
          Cause = "Gold rollback or control finalization failed. The reservation remains unresolved and requires reviewed recovery."
        }
      }
    )
  })

  depends_on = [
    aws_sns_topic.pipeline_alerts,
    aws_lambda_function.silver_gold_control,
    aws_s3_object.gold_recovery_tools,
    aws_s3_object.gold_recovery_entrypoint,
    aws_s3_object.gold_recovery_rollback_entrypoint,
    aws_s3_object.gold_capacity_v3_overlay
  ]
}

output "gold_state_machine_arn" {
  description = "ARN of the sequential Gold state machine."
  value       = aws_sfn_state_machine.gold.arn
}
