# =============================================================================
# VERSION 2.2 - Trust Score ML: Model Registry + live endpoint permissions
# =============================================================================
# See ../CHANGES_V2_2.md. Adds policies to the v2 roles; creates no role, so
# the permissions boundary question does not arise.
#
#   <name>-sfn-role            + read champion documents, register the champion
#                                in the Model Registry, create/update the live
#                                endpoint (deploy state machine), check it
#                                (scoring state machine)
#   <name>-events-role         + start the deploy state machine
#   <prefix>-sagemaker-execution + InvokeEndpoint on the live endpoint (the
#                                nightly batch Processing job runs as this role)
#
# Names are built from the same conventions ../infra uses, never read from its
# state: the group is infra/sagemaker.tf's local.model_package_group, the
# endpoint and the state machine are infra's local.ml_v2_endpoint_name and
# local.ml_v2_deploy_sm_name.
# =============================================================================

locals {
  ml_v2_model_package_group      = "${local.name_prefix}-trust-score"
  ml_v2_live_endpoint_name       = "${local.ml_v2_name}-live"
  ml_v2_live_endpoint_arn        = "${local.ml_v2_arn_prefix.sagemaker}:endpoint/${local.ml_v2_live_endpoint_name}"
  ml_v2_deploy_state_machine_arn = "${local.ml_v2_arn_prefix.states}:stateMachine:${local.ml_v2_name}-deploy"
}

# -----------------------------------------------------------------------------
# Step Functions role (training: register; deploy: endpoint; scoring: check)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "ml_v2_sfn_registry" {
  # ReadChampion: the champion.json finalize wrote, under models/.../champion/history/.
  statement {
    sid       = "ReadChampionDocuments"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${local.ml_v2_output_bucket_arn}/${var.ml_v2_output_prefix}version_id=*/champion/*"]
  }

  # CreateModelPackage validates the training job's ModelDataUrl as the caller.
  statement {
    sid       = "ReadTrainingModelArtifactsForRegistration"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${local.ml_v2_output_bucket_arn}/${var.ml_v2_output_prefix}sagemaker-output/training/*/output/model.tar.gz"]
  }

  # Model package creation reads the custom inference image from ECR before it
  # can register the package, so the Step Functions role must be able to pull it.
  statement {
    sid    = "PullCustomInferenceImageForModelPackage"
    effect = "Allow"
    actions = [
      "ecr:GetAuthorizationToken"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "ReadCustomInferenceImageForModelPackage"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
      "ecr:DescribeImages",
      "ecr:DescribeRepositories",
      "ecr:ListImages"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:ecr:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:repository/${var.project}-${var.environment}-model_autoencoder_inference"
    ]
  }

  statement {
    sid    = "RegisterChampionInModelRegistry"
    effect = "Allow"

    actions = [
      "sagemaker:CreateModelPackage",
      "sagemaker:DescribeModelPackage",
      "sagemaker:UpdateModelPackage",
      "sagemaker:DescribeModelPackageGroup",
      "sagemaker:AddTags",
      "sagemaker:ListTags"
    ]

    resources = [
      "${local.ml_v2_arn_prefix.sagemaker}:model-package-group/${local.ml_v2_model_package_group}",
      "${local.ml_v2_arn_prefix.sagemaker}:model-package/${local.ml_v2_model_package_group}/*"
    ]
  }

  # Models and endpoint configs are named by the helper Lambda from the job-name
  # prefix (ts05-live-v<N>-<id>), so they are scoped exactly like the jobs.
  statement {
    sid    = "DeployLiveEndpoint"
    effect = "Allow"

    actions = [
      "sagemaker:CreateModel",
      "sagemaker:DescribeModel",
      "sagemaker:CreateEndpointConfig",
      "sagemaker:DescribeEndpointConfig",
      "sagemaker:CreateEndpoint",
      "sagemaker:UpdateEndpoint",
      "sagemaker:DeleteEndpoint",
      "sagemaker:DescribeEndpoint",
      "sagemaker:AddTags",
      "sagemaker:ListTags"
    ]

    resources = [
      "${local.ml_v2_arn_prefix.sagemaker}:model/${var.ml_v2_job_name_prefix}-*",
      "${local.ml_v2_arn_prefix.sagemaker}:endpoint-config/${var.ml_v2_job_name_prefix}-*",
      local.ml_v2_live_endpoint_arn
    ]
  }

  # ListEndpoints does not support resource-level permissions.
  statement {
    sid       = "FindLiveEndpoint"
    effect    = "Allow"
    actions   = ["sagemaker:ListEndpoints"]
    resources = ["*"]
  }
  # CreateModel passes the execution role: already allowed by
  # PassExecutionRoleToSageMakerOnly in trust_score_ml_v2_iam.tf.
}

resource "aws_iam_policy" "ml_v2_sfn_registry" {
  name        = "${local.ml_v2_name}-sfn-registry-policy"
  description = "VERSION 2.2: register the Trust Score champion and deploy it to the live endpoint."
  policy      = data.aws_iam_policy_document.ml_v2_sfn_registry.json
}

resource "aws_iam_role_policy_attachment" "ml_v2_sfn_registry" {
  role       = aws_iam_role.ml_v2_sfn.name
  policy_arn = aws_iam_policy.ml_v2_sfn_registry.arn
}

# -----------------------------------------------------------------------------
# EventBridge role: may also start the deploy state machine
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "ml_v2_events_deploy" {
  statement {
    sid       = "StartDeployStateMachine"
    effect    = "Allow"
    actions   = ["states:StartExecution"]
    resources = [local.ml_v2_deploy_state_machine_arn]
  }
}

resource "aws_iam_policy" "ml_v2_events_deploy" {
  name   = "${local.ml_v2_name}-events-deploy-policy"
  policy = data.aws_iam_policy_document.ml_v2_events_deploy.json
}

resource "aws_iam_role_policy_attachment" "ml_v2_events_deploy" {
  role       = aws_iam_role.ml_v2_events.name
  policy_arn = aws_iam_policy.ml_v2_events_deploy.arn
}

# -----------------------------------------------------------------------------
# EXISTING SageMaker execution role: the batch job calls the live endpoint
# -----------------------------------------------------------------------------
# The endpoint itself also runs as this role: it pulls the inference image and
# reads model.tar.gz under <ml_v2_output_prefix>sagemaker-output/ - both
# already granted (sagemaker_iam.tf, trust_score_ml_v2_iam.tf).

data "aws_iam_policy_document" "ml_v2_execution_live_endpoint" {
  statement {
    sid       = "BatchScoringInvokesLiveEndpoint"
    effect    = "Allow"
    actions   = ["sagemaker:InvokeEndpoint"]
    resources = [local.ml_v2_live_endpoint_arn]
  }
}

resource "aws_iam_role_policy" "ml_v2_execution_live_endpoint" {
  name   = "trust-score-ml-v2-live-endpoint"
  role   = aws_iam_role.sagemaker_execution.id
  policy = data.aws_iam_policy_document.ml_v2_execution_live_endpoint.json
}
