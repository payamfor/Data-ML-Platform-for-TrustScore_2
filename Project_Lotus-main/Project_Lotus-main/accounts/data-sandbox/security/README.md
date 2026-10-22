# Project Lotus, data-sandbox, security layer

See the [repository README](../../../README.md) for the layer split, the pipeline and the
conventions.

This root owns the account's IAM roles and policies, plus the permissions-boundary lookup.

## Belongs here

KMS keys, aliases and key policies. Secrets Manager containers and their policies. All IAM roles and
policies, including the execution roles the infra layer's services run as. S3 bucket policies.

## The permissions boundary

Every role this root creates must set `permissions_boundary = local.permissions_boundary`, which
`data.tf` and `locals.tf` resolve from the account's contract parameter.

```hcl
resource "aws_iam_role" "example" {
  name                 = "${var.project}-${var.environment}-example"
  permissions_boundary = local.permissions_boundary
}
```

### Naming

| | This account |
| -- | -- |
| Policy | `sandbox-boundary` |
| Published at | `/contract/_account/iam/sandbox-boundary-arn` |

Every other account names its boundary for the account slug, `<slug>-boundary`, published at
`/contract/_account/iam/<slug>-boundary-arn`. Read the target account's real parameter on promotion.

## IAM

Least privilege applies. Scope every policy to the actions and resources the role requires. Do not
attach broad AWS managed policies such as `PowerUserAccess`, and do not grant `Action: "*"` or
`Resource: "*"`.

## Manifest demo v2 reader access

`manifest_reader_grants.tf` attaches one additive inline policy,
`${local.name_prefix}-silver-manifest-reader-grants`, to the existing Silver
runtime role. It allows `lakeformation:GrantPermissions` only in this root's
provider region. It does not modify protected CI roles or replace runtime data
access. Use Terraform ownership rather than also creating a manual console policy.

AWS does not support resource ARN scoping for this action; `Resource: "*"` is
an action-specific exception to the general least-privilege rule above.
The region condition does not limit tables or recipients. Lake Formation
checks grant authority separately, and the reviewed v2 onboarding code checks
the exact Iceberg target and a separately approved reader ARN. Protect job code
and submission settings because other code using the same runtime role gains
this IAM capability too.

V2 manifest mode owns the isolated `account_changes_batch_1` reader grant.
Infra no longer attempts the conflicting grant and uses non-destructive
`removed` blocks to forget any old demo state without revocation. Existing
business-table grants remain in Infra. The earlier `_1_tf` replacement is
retired without deleting tables/data.

Apply Security through the approved workflow, then the Infra ownership cleanup.
Upload the supplied v2 artifacts and run manifest mode under the same Silver
runtime identity that created the original table. The full deployment, EMR and
Athena verification procedure is in [MANIFEST_DEMO.md](../infra/MANIFEST_DEMO.md).

The older `manifest_demo_infra_catalog_access.tf` still emits an optional
diagnostic policy document only; it attaches nothing and is not the v2 fix.
Its prior attempted attachment to the protected Infra CI role was denied.
The new policy instead targets the application runtime role managed here.
Permission boundaries, SCPs and LF grant authority still apply; this change
neither overrides a deny nor makes that role a Lake Formation administrator.
