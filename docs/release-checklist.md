# Private review, merge order and later AWS acceptance

The coding project is prepared for review. The repository has **not** been made public, no PR was
merged automatically, and no AWS resource/workload was deployed or executed by this phase.

## Review and merge order

1. Review **PR #1** (`feat/phase-1-lakehouse` → `main`): source contracts, snapshot layers, KPI
   definitions, existing-bucket infrastructure and owner-reviewed OIDC/IAM bootstrap.
2. After Phase 1 is merged, retarget **PR #2** (`feat/phase-2-incremental-cdc`) to `main`. Confirm its
   diff contains Phase 2 only, rerun required checks if the base changed, then review/merge CDC,
   schema policy, tombstones and saved-plan recovery.
3. After Phase 2 is merged, retarget the **Phase 3 PR** (`feat/phase-3-operations`) from
   `feat/phase-2-incremental-cdc` to `main`. Confirm a Phase-3-only diff and passing checks, then
   review orchestration, monitoring, verification, analytics and the measured optimization.

Normal merge commits preserve this stack's ancestry. If the owner chooses squash merges, deliberately
reconcile the next branch with reviewed main before retargeting; do not force-push or blindly accept
repeated earlier-phase diffs. Do not merge later-phase PRs into an unreviewed predecessor just to
collapse the stack. All merges remain owner decisions.

## Before making the repository public

- Review all code and execution/deployment permissions; retain the explicit no-live-AWS-verification
  statement until actual acceptance evidence exists.
- Inspect tracked files, commit diffs and CI artifacts for credentials/sensitive data. Synthetic input
  and generated artifacts are excluded from Git; review whether to publish bucket/repository identifiers.
- Select a repository license deliberately. No license grant or visibility change is made automatically.
- Ensure main contains the final README, source, tests, locked dependencies, SQL, diagrams, runbooks
  and verification evidence; confirm a fresh clone can reproduce the small local example.
- Confirm passing required CI, repository branch protections and main-only OIDC subject restrictions.
  Review workflow changes as code. A dependency compatibility check is not a vulnerability audit.
- Update the repository description/topics and choose any release tag only after review. Do not describe
  this as production-certified or Amazon-internal data/metrics.

## Tomorrow's separately authorized AWS verification

1. Confirm the Free Plan, remaining credits, Sydney service availability and $10 budget alert. Do not
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
