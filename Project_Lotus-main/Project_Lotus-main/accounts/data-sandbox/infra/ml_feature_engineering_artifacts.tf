# S3 objects for feature engineering EMR artifacts.
#
# Tracks the feature-engineering ZIP packages, driver entry point, and YAML
# configurations in Terraform state.
#
# The artifacts are produced by:
#   project_lotus/feature_engineering_emr/scripts/package.sh
# which packages:
#   - dist/ts05_features.zip          -> spark-submit --py-files
#   - dist/pydeps.zip                 -> spark-submit --py-files (vendored PyYAML)
#   - dist/feature_engineering_job.py -> entry point script
#   - dist/conf/base.yaml             -> spark-submit --files
#   - dist/conf/sandbox.yaml          -> spark-submit --files
#
# Target prefix in S3 matches var.ml_feature_engineering_artifact_root:
#   s3://lotus-sandbox-ml-preprocessing-data/pp_prod_artifacts/

locals {
  fe_artifact_source_dir = "${path.module}/../../../project_lotus/feature_engineering_emr"
  fe_artifact_dist_dir   = "${local.fe_artifact_source_dir}/dist"

  fe_artifact_s3_prefix = "pp_prod_artifacts"

  fe_artifacts = {
    "ts05_features.zip" = {
      source       = "${local.fe_artifact_dist_dir}/ts05_features.zip"
      content_type = "application/zip"
    }
    "pydeps.zip" = {
      source       = "${local.fe_artifact_dist_dir}/pydeps.zip"
      content_type = "application/zip"
    }
    "feature_engineering_job.py" = {
      source       = "${local.fe_artifact_dist_dir}/feature_engineering_job.py"
      content_type = "text/x-python"
    }
    "conf/base.yaml" = {
      source       = "${local.fe_artifact_dist_dir}/conf/base.yaml"
      content_type = "text/yaml"
    }
    "conf/sandbox.yaml" = {
      source       = "${local.fe_artifact_dist_dir}/conf/sandbox.yaml"
      content_type = "text/yaml"
    }
  }
}

resource "aws_s3_object" "feature_engineering_artifacts" {
  for_each = {
    for k, v in local.fe_artifacts :
    k => v if fileexists(v.source)
  }

  bucket       = aws_s3_bucket.preprocessing_data.id
  key          = "${local.fe_artifact_s3_prefix}/${each.key}"
  source       = each.value.source
  etag         = filemd5(each.value.source)
  content_type = each.value.content_type

  depends_on = [
    aws_s3_bucket.preprocessing_data,
    aws_s3_bucket_server_side_encryption_configuration.preprocessing_data
  ]
}

output "feature_engineering_artifact_uris" {
  description = "S3 URIs of the deployed feature engineering EMR artifacts."
  value = {
    for k, v in local.fe_artifacts :
    k => "s3://${aws_s3_bucket.preprocessing_data.bucket}/${local.fe_artifact_s3_prefix}/${k}"
  }
}
