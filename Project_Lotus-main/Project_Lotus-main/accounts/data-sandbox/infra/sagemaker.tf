# SageMaker Studio domain and Model Registry for Trust Score 0.5, Phase 1/2
# (batch: Isolation Forest / KMeans, trained and tracked via SageMaker
# Pipelines, registered here, scored via Batch Transform). IAM lives in
# ../security/sagemaker_iam.tf and must exist before this is applied.

locals {
  sagemaker_domain_name  = "${var.project}-${var.environment}-studio"
  sagemaker_user_profile = "${var.project}-${var.environment}-ds"
  model_package_group    = "${var.project}-${var.environment}-trust-score"
}

data "aws_iam_role" "sagemaker_execution" {
  name = "${var.project}-${var.environment}-sagemaker-execution"
}

resource "aws_security_group" "sagemaker_domain" {
  name        = "${local.sagemaker_domain_name}-sg"
  description = "SageMaker Studio domain security group for ${local.sagemaker_domain_name}."
  vpc_id      = var.emr_vpc_id

  tags = {
    Name = "${local.sagemaker_domain_name}-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "sagemaker_domain_self" {
  security_group_id            = aws_security_group.sagemaker_domain.id
  referenced_security_group_id = aws_security_group.sagemaker_domain.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_egress_rule" "sagemaker_domain_all" {
  security_group_id = aws_security_group.sagemaker_domain.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_sagemaker_domain" "lotus" {
  domain_name             = local.sagemaker_domain_name
  auth_mode               = "IAM"
  vpc_id                  = var.emr_vpc_id
  subnet_ids              = var.emr_subnet_ids
  app_network_access_type = "VpcOnly"

  default_user_settings {
    execution_role  = data.aws_iam_role.sagemaker_execution.arn
    security_groups = [aws_security_group.sagemaker_domain.id]
  }

  tags = {
    Name = local.sagemaker_domain_name
  }
}

resource "aws_sagemaker_user_profile" "data_scientist" {
  domain_id         = aws_sagemaker_domain.lotus.id
  user_profile_name = local.sagemaker_user_profile

  user_settings {
    execution_role = data.aws_iam_role.sagemaker_execution.arn
  }
}

resource "aws_sagemaker_model_package_group" "trust_score" {
  model_package_group_name        = local.model_package_group
  model_package_group_description = "Trust Score 0.5 batch models (Isolation Forest, KMeans) — Phase 1/2 registry."

  tags = {
    Project   = var.project
    Stage     = var.environment
    ManagedBy = "terraform"
    Pipeline  = "trust-score-ml"
  }
}

output "sagemaker_domain_id" {
  description = "ID of the SageMaker Studio domain."
  value       = aws_sagemaker_domain.lotus.id
}

output "sagemaker_model_package_group_name" {
  description = "Name of the Model Registry group for Trust Score models."
  value       = aws_sagemaker_model_package_group.trust_score.model_package_group_name
}
