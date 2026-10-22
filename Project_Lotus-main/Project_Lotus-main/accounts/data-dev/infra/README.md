# Project Lotus, data-dev, infra layer

Root module for the Data-owned infra layer in `data-dev` (`586117079604`).

| | |
| -- | -- |
| State | `s3://enstream-tfstate-data-dev/infra/lotus/terraform.tfstate` |
| Role | `gha-lotus-data-dev-infra` |
| Environment | `data-dev-infra`, no required reviewers |
| Code owner | `@data-and-ai/dataai` |

This root is intentionally empty. It exists so that `init` and `plan` prove the
path resolves to the right account, state key, role and environment before any
resource can be applied.

A pull request touching this directory cannot merge without an approval from
`@data-and-ai/dataai`. A merge to `main` applies with no further approval.

Resources this root must never declare: KMS keys, aliases and key policies,
Secrets Manager containers and policies, IAM roles and policies, and S3 bucket
policies. Those are the security layer's, and the role is denied them.
