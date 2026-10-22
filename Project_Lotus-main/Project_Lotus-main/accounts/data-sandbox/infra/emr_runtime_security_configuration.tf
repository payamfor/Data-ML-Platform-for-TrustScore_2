# EMR security configuration required for application-scoped/runtime IAM roles.

resource "aws_emr_security_configuration" "runtime_roles" {
  name = "${local.emr_cluster_name}-runtime-roles"

  configuration = jsonencode({
    AuthorizationConfiguration = {
      IAMConfiguration = {
        EnableApplicationScopedIAMRole = true
      }
    }
  })

  lifecycle {
    ignore_changes = [name, configuration]
  }
}