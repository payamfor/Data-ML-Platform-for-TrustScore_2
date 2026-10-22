# data "aws_ssm_parameter" "vpc_id" {
#   name = var.emr_vpc_id_ssm_path
# }

# data "aws_ssm_parameter" "subnet_ids" {
#   name = var.emr_subnet_ids_ssm_path
# }

data "aws_partition" "current" {}
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}
