# =============================================================================
# VERSION 2.2 - Trust Score ML: the LIVE (real-time) inference endpoint
# =============================================================================
# Replaces the v2 Serverless endpoint. See ../CHANGES_V2_2.md.
#
# Terraform does NOT create this endpoint. The deploy state machine
# (trust_score_ml_v2_registry.tf) creates it the first time a champion model
# package is Approved in the Model Registry, and blue/green-updates it on every
# later approval. Terraform owns its name, its settings and (in ../security)
# who may call it. The endpoint therefore survives `terraform destroy`; delete
# it by hand if the stack is torn down:
#   aws sagemaker delete-endpoint --endpoint-name <output ml_v2_endpoint_name>
#
# The container is the inference image started with `serve`
# (container_entry.py -> gunicorn -> serve.py). It loads the champion from its
# OWN model artifact (/opt/ml/model, unpacked from the registered model.tar.gz),
# so it serves exactly the registry version it was deployed from.
#
# Billing: per instance-hour, around the clock (unlike Serverless).
# =============================================================================

variable "ml_v2_endpoint_instance_type" {
  description = "Instance type of the live endpoint. The model libraries need ~2-3 GB per server worker."
  type        = string
  default     = "ml.m5.xlarge"
}

variable "ml_v2_endpoint_instance_count" {
  description = "Instances behind the live endpoint. 2+ spreads them across Availability Zones."
  type        = number
  default     = 1

  validation {
    condition     = var.ml_v2_endpoint_instance_count >= 1
    error_message = "ml_v2_endpoint_instance_count must be at least 1."
  }
}

variable "ml_v2_endpoint_server_workers" {
  description = "gunicorn worker processes per instance. Each holds its own copy of the model."
  type        = number
  default     = 2
}

variable "ml_v2_endpoint_server_threads" {
  description = "Threads per gunicorn worker."
  type        = number
  default     = 4
}

variable "ml_v2_endpoint_max_rows_per_request" {
  description = "Rows allowed in one /invocations request (server-side limit)."
  type        = number
  default     = 10000
}

variable "ml_v2_endpoint_bluegreen_wait_seconds" {
  description = "Blue/green update: how long the OLD fleet is kept after traffic has moved to the new one (the window in which SageMaker can still roll back)."
  type        = number
  default     = 300

  validation {
    condition     = var.ml_v2_endpoint_bluegreen_wait_seconds >= 0 && var.ml_v2_endpoint_bluegreen_wait_seconds <= 3600
    error_message = "ml_v2_endpoint_bluegreen_wait_seconds must be between 0 and 3600."
  }
}

variable "ml_v2_batch_scoring_via_endpoint" {
  description = "Nightly batch scoring calls the live endpoint (InvokeEndpoint). false = the Processing job loads the champion in-process, as in v2 (scores identical to v2; see CHANGES_V2_2.md)."
  type        = bool
  default     = true
}

variable "ml_v2_endpoint_batch_rows_per_request" {
  description = "Batch scoring: rows per InvokeEndpoint request (also capped at ~5 MB per request by the client)."
  type        = number
  default     = 2000

  validation {
    condition     = var.ml_v2_endpoint_batch_rows_per_request >= 1 && var.ml_v2_endpoint_batch_rows_per_request <= var.ml_v2_endpoint_max_rows_per_request
    error_message = "ml_v2_endpoint_batch_rows_per_request must be between 1 and ml_v2_endpoint_max_rows_per_request."
  }
}

variable "ml_v2_endpoint_batch_concurrency" {
  description = "Batch scoring: InvokeEndpoint requests in flight at once. Keep <= instances x workers x threads."
  type        = number
  default     = 4
}

locals {
  ml_v2_endpoint_name = "${local.ml_v2_name}-live"

  # Baked into each registered model package version (InferenceSpecification).
  # A change here applies to versions registered AFTER the change; the instance
  # type/count above apply at the next deployment.
  ml_v2_live_model_environment = {
    TS05_CHAMPION_SOURCE      = "model_dir"
    TS05_MAX_ROWS_PER_REQUEST = tostring(var.ml_v2_endpoint_max_rows_per_request)
    TS05_SERVER_WORKERS       = tostring(var.ml_v2_endpoint_server_workers)
    TS05_SERVER_THREADS       = tostring(var.ml_v2_endpoint_server_threads)
    AWS_REGION                = data.aws_region.current.region
    AWS_DEFAULT_REGION        = data.aws_region.current.region
  }

  # Endpoint statuses that pass by themselves.
  ml_v2_endpoint_transitional_statuses = ["Creating", "Updating", "SystemUpdating", "RollingBack"]
}

output "ml_v2_endpoint_name" {
  description = "VERSION 2.2: the live endpoint (created by the deploy state machine). Invoke: aws sagemaker-runtime invoke-endpoint --endpoint-name <this> --content-type application/json --body fileb://rows.json out.json"
  value       = local.ml_v2_endpoint_name
}
