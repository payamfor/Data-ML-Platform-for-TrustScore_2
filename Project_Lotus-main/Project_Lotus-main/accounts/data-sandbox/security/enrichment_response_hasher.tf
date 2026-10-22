variable "enrichment_ac_secret_arn" {
  description = "Optional full ARN of the client's AC hashing secret. Leave null for tulip/<environment>/salt-ac-hashing in this account."
  type        = string
  default     = null
}

variable "enrichment_at_secret_arn" {
  description = "Optional full ARN of the client's AT hashing secret. Leave null for tulip/<environment>/salt-audit-trail-hashing in this account."
  type        = string
  default     = null
}

variable "enrichment_secret_kms_key_arns" {
  description = "Customer-managed KMS key ARNs needed to decrypt the two Secrets Manager secrets, if applicable."
  type        = list(string)
  default     = []
}

locals {
  enrichment_hasher_name       = "${var.project}-${var.environment}-enrichment-response-hasher"
  enrichment_hasher_bucket_arn = "arn:${data.aws_partition.current.partition}:s3:::${var.project}-${var.environment}-bronze-landing-data"
  enrichment_hasher_queue_arn  = "arn:${data.aws_partition.current.partition}:sqs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:${local.enrichment_hasher_name}"

  enrichment_ac_secret_resource = var.enrichment_ac_secret_arn != null ? var.enrichment_ac_secret_arn : "arn:${data.aws_partition.current.partition}:secretsmanager:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:secret:tulip/${var.environment}/salt-ac-hashing-*"
  enrichment_at_secret_resource = var.enrichment_at_secret_arn != null ? var.enrichment_at_secret_arn : "arn:${data.aws_partition.current.partition}:secretsmanager:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:secret:tulip/${var.environment}/salt-audit-trail-hashing-*"
}

data "aws_iam_policy_document" "enrichment_hasher_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "enrichment_response_hasher" {
  name                 = "${local.enrichment_hasher_name}-role"
  assume_role_policy   = data.aws_iam_policy_document.enrichment_hasher_assume_role.json
  permissions_boundary = local.permissions_boundary
}

data "aws_iam_policy_document" "enrichment_response_hasher" {
  statement {
    sid       = "ReadVersionedProviderResponses"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${local.enrichment_hasher_bucket_arn}/enterprise-enrichment/responses/*"]
  }

  statement {
    sid       = "WriteHashedResponsePartsAndCompletion"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${local.enrichment_hasher_bucket_arn}/enterprise-enrichment/hashed-responses/*"]
  }

  statement {
    sid       = "CheckHashedResponseCompletion"
    actions   = ["s3:ListBucket"]
    resources = [local.enrichment_hasher_bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["enterprise-enrichment/hashed-responses/*"]
    }
  }

  statement {
    sid       = "UseResponseWorkQueue"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:SendMessage"]
    resources = [local.enrichment_hasher_queue_arn]
  }

  statement {
    sid       = "ReadHashingSecrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [local.enrichment_ac_secret_resource, local.enrichment_at_secret_resource]
  }

  dynamic "statement" {
    for_each = length(var.enrichment_secret_kms_key_arns) == 0 ? [] : [1]
    content {
      sid       = "DecryptHashingSecrets"
      actions   = ["kms:Decrypt"]
      resources = var.enrichment_secret_kms_key_arns
      condition {
        test     = "StringEquals"
        variable = "kms:ViaService"
        values   = ["secretsmanager.${data.aws_region.current.region}.amazonaws.com"]
      }
    }
  }

  statement {
    sid       = "WriteLambdaLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.enrichment_hasher_name}:*"]
  }
}

resource "aws_iam_policy" "enrichment_response_hasher" {
  name   = "${local.enrichment_hasher_name}-runtime"
  policy = data.aws_iam_policy_document.enrichment_response_hasher.json
}

resource "aws_iam_role_policy_attachment" "enrichment_response_hasher" {
  role       = aws_iam_role.enrichment_response_hasher.name
  policy_arn = aws_iam_policy.enrichment_response_hasher.arn
}
