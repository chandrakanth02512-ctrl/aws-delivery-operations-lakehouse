# Release status and later AWS acceptance

As of 27 October 2026, the repository is **public** and all three phase PRs are merged into `main`
at `82afc56c27edd19ad87a80aa0e9f212d0747dbcc`. Their branches and merge history are preserved.
The subsequent [audit fixes](technical-audit.md) require a separate PR review; that PR is not merged
automatically. No AWS resource or workload has been deployed or executed by this audit.

## Preserved phase review order

1. [PR #1](https://github.com/chandrakanth02512-ctrl/aws-delivery-operations-lakehouse/pull/1):
   source contracts, snapshot layers, KPIs, infrastructure and OIDC/IAM definitions — merged.
2. [PR #2](https://github.com/chandrakanth02512-ctrl/aws-delivery-operations-lakehouse/pull/2):
   CDC, schema policy, tombstones and saved-plan recovery — merged after Phase 1.
3. [PR #3](https://github.com/chandrakanth02512-ctrl/aws-delivery-operations-lakehouse/pull/3):
   orchestration, monitoring, verification, analytics and measured optimization — merged after Phase 2.

Normal merge commits preserve the stack's ancestry. Future fixes target `main`; no branch deletion,
history rewrite or forced push is required. All future merges remain owner decisions.

## Before the next release or AWS deployment

- Review all code and execution/deployment permissions; retain the explicit no-live-AWS-verification
  statement until actual acceptance evidence exists.
- Inspect tracked files, commit diffs and CI artifacts for credentials/sensitive data. Synthetic input
  and generated artifacts are excluded from Git; review whether to publish bucket/repository identifiers.
- Select a repository license deliberately. No license grant or visibility change is made automatically.
- Ensure main contains the final README, source, tests, locked dependencies, SQL, diagrams, runbooks
  and verification evidence; confirm a fresh clone can reproduce the small local example.
- Confirm passing CI and main-only OIDC subject restrictions. The audit observed `main` as unprotected;
  the owner should configure required PR review and the `verify` check before enabling deployment.
  CI success alone does not enforce a merge policy. Repository settings are not changed by this audit.
- Use patched build tooling and repeat dependency/secret scanning for the actual release commit.
  A dependency compatibility check alone is not a vulnerability audit.
- Update the repository description/topics and choose any release tag only after review. Do not describe
  this as production-certified or Amazon-internal data/metrics.

## Future separately authorized AWS verification

1. Resolve the owner-reported SCP denial of `iam:ListOpenIDConnectProviders` through AWS Support/the
   organization administrator before proceeding. Do not bypass it or create IAM resources as a workaround.
   Then confirm the Free Plan, remaining credits, Sydney service availability and $10 budget alert. Do not
   upgrade the account to gain access. GitHub access does not establish AWS authentication.
2. Review the existing [bootstrap permissions](../infrastructure/bootstrap.yaml) and
   [one-time setup](deployment.md). Use temporary credentials/OIDC, not permanent access keys.
3. Confirm existing bucket region/encryption and applicable Lake Formation/KMS requirements without
   weakening bucket security. Review/execute the main-stack release change set only after approval.
4. Review the separate [operations stack](../infrastructure/operations.yaml) and its single-job role.
   Deploy only when authorized, with monitoring still off. Keep the Glue job at 6.0 / G.1X / 2 workers /
   15 minutes / zero Glue retries / concurrency one; no schedule.
5. Approve and upload one small source batch, prepare its deterministic execution request and authorize
   one manual job/orchestration run. Record execution ARN, Glue JobRunId, logs, execution/batch states,
   verification report, counts, elapsed DPU time and actual billed usage.
6. Inspect Glue Catalog schemas/locations and Iceberg format-v2 metadata. Verify source/commit checksums,
   quarantine and checkpoint behavior on real S3. A second run/recovery needs its own cost approval.
7. After separate query approval, execute Athena engine-3 verification/business SQL in the bounded
   workgroup. Record QueryExecutionIds, scan bytes, results and costs. Confirm the analytical denominators.
8. Validate real Step Functions `.sync` behavior, explicit redrive, concurrency rejection and best-effort
   cancellation before relying on them. Do not inject destructive table corruption into the account.
9. Confirm actual CloudWatch log paths/JSON shape, then separately review optional metric/alarm costs
   and action destinations. No alert activation or subscription is implied by infrastructure approval.

Record failures and actual results in a new cloud acceptance report. Leave unexecuted checks clearly
marked pending. Local Spark/JDBC/stub evidence cannot replace managed S3/GlueCatalog/Glue/Athena/OIDC
or CloudWatch acceptance, nor can a successful workflow make multi-table publication atomic.
