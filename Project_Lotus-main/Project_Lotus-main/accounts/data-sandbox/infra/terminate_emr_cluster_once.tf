variable "enable_one_time_emr_cluster_termination" {
  description = "Temporarily create a state machine to terminate the two explicitly listed EMR clusters, including any running work. Set true only for the one-time operation, then set false and apply again to remove it."
  type        = bool
  default     = false
}

locals {
  one_time_emr_cluster_termination_ids = [
    "j-04830112EW0GTVA8T6PG",
    "j-04822213DR6OEVRN7T69",
  ]
}

resource "aws_sfn_state_machine" "terminate_emr_cluster_once" {
  count = var.enable_one_time_emr_cluster_termination ? 1 : 0

  name     = "${var.project}-${var.environment}-terminate-emr-cluster-once"
  role_arn = data.aws_iam_role.ml_feature_engineering_stepfunction.arn

  definition = jsonencode({
    Comment = "One-time termination of two explicitly identified feature-engineering EMR clusters."
    StartAt = "TerminateFirstCluster"
    States = {
      TerminateFirstCluster = {
        Type     = "Task"
        Resource = "arn:aws:states:::elasticmapreduce:terminateCluster.sync"
        Parameters = {
          ClusterId = local.one_time_emr_cluster_termination_ids[0]
        }
        ResultPath = "$.first_termination"
        Next       = "TerminateSecondCluster"
      }

      TerminateSecondCluster = {
        Type     = "Task"
        Resource = "arn:aws:states:::elasticmapreduce:terminateCluster.sync"
        Parameters = {
          ClusterId = local.one_time_emr_cluster_termination_ids[1]
        }
        End = true
      }
    }
  })

  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
    Purpose     = "one-time-emr-cluster-termination"
  }
}

output "one_time_emr_cluster_termination_state_machine_arn" {
  description = "ARN of the opt-in one-time EMR cluster termination state machine, or null when disabled."
  value       = try(aws_sfn_state_machine.terminate_emr_cluster_once[0].arn, null)
}
