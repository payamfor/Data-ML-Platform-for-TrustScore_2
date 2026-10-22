# Package and publish the additive Gold recovery sidecar.  Keeping this in the
# Infra deployment prevents the state machine from being updated while its EMR
# recovery entrypoint is missing or at a different version in S3.
data "archive_file" "gold_recovery_tools" {
  type        = "zip"
  output_path = "${path.module}/gold_recovery_tools.zip"

  source {
    content  = file("${path.module}/../../../project_lotus/lineage/gold_recovery/__init__.py")
    filename = "gold_recovery/__init__.py"
  }
  source {
    content  = file("${path.module}/../../../project_lotus/lineage/gold_recovery/finalize_cli.py")
    filename = "gold_recovery/finalize_cli.py"
  }
  source {
    content  = file("${path.module}/../../../project_lotus/lineage/gold_recovery/plan.py")
    filename = "gold_recovery/plan.py"
  }
  source {
    content  = file("${path.module}/../../../project_lotus/lineage/gold_recovery/rollback_cli.py")
    filename = "gold_recovery/rollback_cli.py"
  }
  source {
    content  = file("${path.module}/../../../project_lotus/lineage/gold_recovery/runtime.py")
    filename = "gold_recovery/runtime.py"
  }
}

resource "aws_s3_object" "gold_recovery_tools" {
  bucket       = local.gold_job_config_bucket
  key          = "pipeline-code/gold/v3/recovery/v1/gold_recovery_tools.zip"
  source       = data.archive_file.gold_recovery_tools.output_path
  etag         = filemd5(data.archive_file.gold_recovery_tools.output_path)
  content_type = "application/zip"
}

resource "aws_s3_object" "gold_recovery_entrypoint" {
  bucket       = local.gold_job_config_bucket
  key          = "pipeline-code/gold/v3/recovery/v1/recover_gold_run.py"
  source       = "${path.module}/../../../project_lotus/lineage/recover_gold_run.py"
  etag         = filemd5("${path.module}/../../../project_lotus/lineage/recover_gold_run.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "gold_recovery_rollback_entrypoint" {
  bucket       = local.gold_job_config_bucket
  key          = "pipeline-code/gold/v3/recovery/v1/rollback_gold_publish.py"
  source       = "${path.module}/../../../project_lotus/lineage/rollback_gold_publish.py"
  etag         = filemd5("${path.module}/../../../project_lotus/lineage/rollback_gold_publish.py")
  content_type = "text/x-python"
}

# The Gold job config references this execution-only overlay. Publishing it
# with the recovery artifacts prevents a fresh environment from receiving a
# job config whose capacity/convergence overlay does not exist in S3.
resource "aws_s3_object" "gold_capacity_v3_overlay" {
  bucket       = local.gold_job_config_bucket
  key          = "pipeline-code/gold/v3/capacity_v3.yaml"
  source       = "${path.module}/../../../project_lotus/lineage/capacity_v3.yaml"
  etag         = filemd5("${path.module}/../../../project_lotus/lineage/capacity_v3.yaml")
  content_type = "application/yaml"
}
