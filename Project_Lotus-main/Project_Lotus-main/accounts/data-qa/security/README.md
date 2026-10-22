# Project Lotus, data-qa, security layer

Root module for the InfraSec-owned security layer in `data-qa` (`364238921492`).

| | |
| -- | -- |
| State | `s3://enstream-tfstate-data-qa/security/lotus/terraform.tfstate` |
| Role | `gha-lotus-data-qa-security` |
| Environment | `data-qa-security`, no required reviewers |
| Code owner | `@data-and-ai/infrasec` |

This root is intentionally empty, for the same reason as the infra root.

It owns KMS keys, aliases and key policies, Secrets Manager containers and their
resource policies, all IAM roles and policies including workload execution roles,
and resource policies generally.

Every role this root creates must set `permissions_boundary = local.permissions_boundary`,
resolved from the account contract parameter. The account boundary,
`data-qa-boundary`, denies `iam:CreateRole` where the request sets a different
boundary, and denies it outright where the request sets none, so a role declared
without it fails at apply.

Secret values never pass through Terraform. This root creates the container; the
value arrives by rotation or by a human through the CLI.
