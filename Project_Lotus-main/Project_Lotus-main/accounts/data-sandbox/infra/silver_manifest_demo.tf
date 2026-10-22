# The user selected runtime-owned v2 onboarding for the original _1 table.
# Retire the earlier _1_tf replacement without deleting tables/data or revoking
# grants if someone applied that proposal. These blocks are no-ops when the
# resources never entered state. Retain them for existing-state compatibility.
# Any already-created _1_tf table becomes unmanaged and can be reviewed
# separately; it is not reused, renamed or cleaned up by the v2 job.
removed {
  from = aws_glue_catalog_table.silver_manifest_demo
  lifecycle {
    destroy = false
  }
}

removed {
  from = aws_lakeformation_permissions.silver_runtime_manifest_demo_table
  lifecycle {
    destroy = false
  }
}
