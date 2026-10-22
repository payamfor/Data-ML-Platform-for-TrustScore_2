# Enterprise enrichment response hashing

This Lambda processes completed provider *response* objects, not the customer request JSON or the `202 Accepted` result from `portps-accept` / `idv-accept`. It accepts plain UTF-8 NDJSON, a single JSON response envelope, a JSON array of response envelopes, and gzip-compressed forms of all three. Gzip is detected from the file bytes rather than the filename. The client response uploader must write immutable response objects to one of:

```
s3://lotus-sandbox-bronze-landing-data/enterprise-enrichment/responses/provider=portps/<response-file>
s3://lotus-sandbox-bronze-landing-data/enterprise-enrichment/responses/provider=idv/<response-file>
```

The existing Bronze bucket's S3-to-EventBridge delivery sends each Object Created event under these provider prefixes to a dedicated SQS queue. One Lambda invocation reads at most 100 envelopes or roughly 32 MiB of uncompressed output (with a 20-second time reserve), writes one gzip NDJSON part, and queues the next cursor when needed. The queue invokes the same Lambda again; no single invocation is expected to process an arbitrarily large batch. After the last part, it writes `_SUCCESS.json`. Retries use deterministic output keys based on the immutable S3 object version. Failed messages reach a DLQ after five receives; the DLQ alarms to the Silver pipeline SNS topic. Non-JSON or unsupported provider response schemas fail rather than being silently published. The uploader should not put temporary or unrelated objects in these prefixes; filenames beginning with `_` are not processed.

For an S3 EventBridge event, `detail.object.key` is the key *within* the bucket, for example `enterprise-enrichment/responses/provider=idv/response.json`, not an `s3://...` URI. The Lambda checks for a prior `_SUCCESS.json` with `ListObjectsV2` restricted to the hashed-output prefix; this avoids a `HeadObject` 403 on a marker that does not yet exist when the role has no broad bucket-list permission.

Output layout:

```
s3://lotus-sandbox-bronze-landing-data/enterprise-enrichment/hashed-responses/provider=<portps|idv>/source_id=<sha256>/part-000000.jsonl.gz
s3://lotus-sandbox-bronze-landing-data/enterprise-enrichment/hashed-responses/provider=<portps|idv>/source_id=<sha256>/_SUCCESS.json
```

Each original envelope is preserved, including `payload` and `payload_hash`. Both `phone_number_AC_hash` and `phone_number_AT_hash` are added to every element in its `items` array. The source `msisdn` is normalized to Canadian `+1` E.164 for hashing. Hash bytes are UTF-8 SHA-256 of `<normalized_msisdn>.<secret_string>`. Salts come only from Secrets Manager; there is no local/fake-salt fallback. Raw MSISDN and provider payload remain in the restricted Bronze output, so maintain its PII access controls and retention policy. Logs contain no MSISDN, secrets, or response body.

The default secret names are `tulip/sandbox/salt-ac-hashing` and `tulip/sandbox/salt-audit-trail-hashing`. Set `enrichment_ac_secret_arn` and `enrichment_at_secret_arn` **in both the security and infra Terraform roots** if the secrets live in another account. Set `enrichment_secret_kms_key_arns` in the security root if those secrets use customer-managed KMS keys. For cross-account secrets, the owning account must also grant this Lambda role `secretsmanager:GetSecretValue` in the secret resource policies and `kms:Decrypt` in the KMS key policies. The role in `accounts/data-sandbox/security/enrichment_response_hasher.tf` is deployed before the queue/Lambda/EventBridge rule in `accounts/data-sandbox/infra/enrichment_response_hasher.tf`.

This component only hashes and stages completed responses. It does **not** write directly to Iceberg or start Gold. A separate Silver normalizer must consume a source prefix only after `_SUCCESS.json` exists, parse PortPS XML or IDV JSON, validate item-to-payload MSISDN correspondence, and idempotently merge rows into `lotus_sandbox_silver_conformed.tu_portps` or `lotus_sandbox_silver_conformed.mno_activation`. Gold should run only after that Silver commit. A successful accept-Lambda invocation or a completed hash marker alone is not evidence of committed Silver data.

**Hash compatibility gate:** the older client PortPS Bronze-to-Silver script hashes the 10-digit `PhoneNumber` from XML, whereas the IDV/submission path uses `+1` E.164. Before loading any hashed response into Lotus Silver/Gold, compare a known MSISDN and both secret-derived hashes against a known account-change row. The downstream PortPS normalizer must use the verified convention rather than silently recomputing from the raw 10-digit XML value.

Run local unit tests with `python -m unittest discover -s project_lotus/lambdas/enrichment_response_hasher -p 'test_*.py'`. The test salts are dummy values and are never used by the deployed Lambda.
