# ---------------------------------------------------------------------------
# Feature-engineering job output permissions
#
# The job's EMR execution role checks and manages output under
# s3://lotus-${var.environment}-sagemaker-data/mock-data/version1.
# ---------------------------------------------------------------------------

locals {
  silver_runtime_feature_gold_table_names = toset([
    "account_changes_canonical_events",
    "msisdn_lifecycle_account_customer_mapping",
    "gold_run_control",
    "onboarding_nbc"
  ])

  silver_runtime_feature_gold_database_name = replace(
    "${var.project}-${var.environment}-gold-curated",
    "-",
    "_"
  )

  silver_runtime_feature_gold_catalog_arn  = "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:catalog"
  silver_runtime_feature_gold_database_arn = "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:database/${local.silver_runtime_feature_gold_database_name}"
  silver_runtime_feature_gold_table_arns = [
    for table_name in local.silver_runtime_feature_gold_table_names :
    "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${local.silver_runtime_feature_gold_database_name}/${table_name}"
  ]
  silver_runtime_feature_gold_bucket_arn          = "arn:${data.aws_partition.current.partition}:s3:::lotus-${var.environment}-gold-curated-data"
  silver_runtime_feature_preprocessing_bucket_arn = "arn:${data.aws_partition.current.partition}:s3:::lotus-sandbox-ml-preprocessing-data"
  silver_runtime_feature_run_control_table_arn    = "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${local.silver_runtime_feature_gold_database_name}/feature_engineering_run_control"
  silver_runtime_feature_run_control_s3_prefixes = [
    "tables/dataset=feature_engineering_run_control/version=*",
    "tables/dataset=feature_engineering_run_control/version=*/*"
  ]
  silver_runtime_feature_gold_s3_objects = [
    for table_name in local.silver_runtime_feature_gold_table_names :
    "${local.silver_runtime_feature_gold_bucket_arn}/tables/dataset=${table_name}/version=*/*"
  ]
  silver_runtime_feature_gold_s3_prefixes = flatten([
    for table_name in local.silver_runtime_feature_gold_table_names : [
      "tables/dataset=${table_name}/version=*",
      "tables/dataset=${table_name}/version=*/*"
    ]
  ])
  silver_runtime_feature_run_control_s3_objects = "${local.silver_runtime_feature_gold_bucket_arn}/tables/dataset=feature_engineering_run_control/version=*/*"
}

resource "aws_iam_role_policy" "silver_feature_engineering_output_access" {
  name = "silver-feature-engineering-output-access"
  role = aws_iam_role.silver_job_runtime.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid      = "PublishFeatureEngineeringStopAlerts"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = "arn:${data.aws_partition.current.partition}:sns:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:${var.project}-${var.environment}-feature-engineering-alerts"
      },
      {
        Sid    = "ListFeatureEngineeringOutput"
        Effect = "Allow"

        Action = ["s3:ListBucket"]

        Resource = "arn:${data.aws_partition.current.partition}:s3:::lotus-${var.environment}-sagemaker-data"

        Condition = {
          StringLike = {
            "s3:prefix" = [
              "mock-data/version1",
              "mock-data/version1/*",
              "scoring",
              "scoring/*",
              "scoring_data",
              "scoring_data/*",
              "outputs",
              "outputs/*"
            ]
          }
        }
      },
      {
        Sid    = "ManageFeatureEngineeringOutputObjects"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]

        Resource = "arn:${data.aws_partition.current.partition}:s3:::lotus-${var.environment}-sagemaker-data/mock-data/version1/*"
      },
      {
        Sid      = "ListBatchScoringInputAndOutput"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:${data.aws_partition.current.partition}:s3:::lotus-${var.environment}-sagemaker-data"

        Condition = {
          StringLike = {
            "s3:prefix" = [
              "scoring/dt=*/data",
              "scoring/dt=*/data/*",
              "outputs/predictions",
              "outputs/predictions/*",
              "outputs/monitoring",
              "outputs/monitoring/*"
            ]
          }
        }
      },
      {
        Sid      = "ReadBatchScoringInput"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "arn:${data.aws_partition.current.partition}:s3:::lotus-${var.environment}-sagemaker-data/scoring/dt=*/data/*"
      },
      {
        Sid    = "WriteFeatureEngineeringBatchScoringOutput"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = "arn:${data.aws_partition.current.partition}:s3:::lotus-${var.environment}-sagemaker-data/scoring_data/*"
      },
      {
        Sid    = "ManageBatchScoringOutputs"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [
          "arn:${data.aws_partition.current.partition}:s3:::lotus-${var.environment}-sagemaker-data/outputs/predictions/*",
          "arn:${data.aws_partition.current.partition}:s3:::lotus-${var.environment}-sagemaker-data/outputs/monitoring/*"
        ]
      },
      {
        Sid      = "ListPreprocessingOutputBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = local.silver_runtime_feature_preprocessing_bucket_arn
      },
      {
        Sid    = "ManagePreprocessingOutputObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = "${local.silver_runtime_feature_preprocessing_bucket_arn}/*"
      },
      {
        Sid    = "ListFeatureEngineeringGoldSourceTable"
        Effect = "Allow"
        Action = ["s3:ListBucket"]

        Resource = local.silver_runtime_feature_gold_bucket_arn

        Condition = {
          StringLike = {
            "s3:prefix" = local.silver_runtime_feature_gold_s3_prefixes
          }
        }
      },
      {
        Sid    = "ReadFeatureEngineeringGoldSourceTable"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]

        Resource = local.silver_runtime_feature_gold_s3_objects
      },
      {
        Sid    = "ListFeatureEngineeringRunControlTable"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads"
        ]
        Resource = local.silver_runtime_feature_gold_bucket_arn

        Condition = {
          StringLike = {
            "s3:prefix" = local.silver_runtime_feature_run_control_s3_prefixes
          }
        }
      },
      {
        Sid    = "ManageFeatureEngineeringRunControlTable"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = local.silver_runtime_feature_run_control_s3_objects
      },
      {
        Sid    = "ReadFeatureEngineeringGoldGlueTable"
        Effect = "Allow"

        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetTableVersion",
          "glue:GetTableVersions"
        ]

        Resource = concat([
          local.silver_runtime_feature_gold_catalog_arn,
          local.silver_runtime_feature_gold_database_arn
        ], local.silver_runtime_feature_gold_table_arns)
      },
      {
        Sid    = "FeatureEngineeringRunControlGlueAccess"
        Effect = "Allow"

        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetTableVersion",
          "glue:GetTableVersions",
          "glue:UpdateTable"
        ]

        Resource = [
          local.silver_runtime_feature_gold_catalog_arn,
          local.silver_runtime_feature_gold_database_arn,
          local.silver_runtime_feature_run_control_table_arn
        ]
      }
    ]
  })
}
