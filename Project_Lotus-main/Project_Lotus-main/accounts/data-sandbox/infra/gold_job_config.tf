# The Gold job's spark-submit arguments, as a console-editable S3 object.
#
# Amazon States Language has no intrinsic that flattens an arbitrary-length
# list of `--conf` entries into repeated CLI flags, so the state machine
# cannot assemble the final argument list on its own from a raw config map -
# see stepfunctions_gold.tf's BuildArgs_<stage> states, which call the control
# Lambda's `build_gold_args` action to do that instead. This file only seeds
# the S3 object that action reads.
#
# Terraform creates this object ONCE, from the repo copy below, and then
# leaves it alone (see the lifecycle block) - an operator editing it directly
# in the S3 console is the intended way to change an argument without a
# Terraform apply or a Lambda redeploy, and a later `terraform apply` for some
# unrelated resource must not silently revert that edit back to the repo's
# copy. The tradeoff this accepts: the deployed object and this repo file can
# drift apart, permanently, until someone deliberately re-seeds it (taint this
# resource, or copy the console version back into the repo file by hand).

locals {
  gold_job_config_bucket = var.gold_bucket_name
  gold_job_config_key    = "pipeline-code/gold/auto/job_config.json"
  gold_job_config_uri    = "s3://${local.gold_job_config_bucket}/${local.gold_job_config_key}"
}

resource "aws_s3_object" "gold_job_config" {
  bucket       = local.gold_job_config_bucket
  key          = local.gold_job_config_key
  source       = "${path.module}/gold_job_config.json"
  etag         = filemd5("${path.module}/gold_job_config.json")
  content_type = "application/json"

  # Seed once; never re-push. Both `source`'s content and its own `etag` are
  # ignored on every apply after creation, so neither an edit to the repo file
  # nor a console edit to the live object gets clobbered by an unrelated
  # apply. Re-seeding from the repo is a deliberate action (see the paragraph
  # above), not something that happens implicitly.
  lifecycle {
    ignore_changes = [source, etag]
  }
}

output "gold_job_config_uri" {
  description = "s3:// URI of the console-editable Gold job argument config. Edit this object directly to change spark-submit arguments without a Terraform apply."
  value       = local.gold_job_config_uri
}
