# Troubleshooting

For CDC-specific errors, active-batch recovery, schema drift and snapshot adoption, use the
[Phase 2 recovery runbook](cdc-operations.md#recovery-procedure). The snapshot repair guidance below
must not be used to delete or bypass CDC state, tombstones or a pending publication plan.

| Symptom | Diagnosis / action |
|---|---|
| Python worker version mismatch | Use `uv sync --extra spark` and `uv run`. Local session configuration pins workers to the driver's interpreter. Glue uses its managed interpreter. |
| Java gateway cannot start | Confirm Java 17+ is installed and `JAVA_HOME` points to that runtime. Sandbox execution must permit local Spark sockets. |
| Dependency download fails | Check the environment's approved PyPI/Maven network policy and proxy; preserve TLS validation. Use a writable uv cache in restricted environments. |
| Iceberg tests skipped | Run `scripts/fetch_iceberg.py` and export `ICEBERG_SPARK_JAR`; H2 must be beside the Iceberg JAR. Do not upload these local test JARs to Glue. |
| `Cannot read unsupported version 3` | Stop publication. Inspect table metadata. The writer refuses non-v2 tables. Never try to downgrade v3 in place; recover to a separately reviewed v2 location/table. |
| `AccessDenied` after GitHub checkout works | GitHub and AWS authentication are independent. Bootstrap/OIDC has not been configured, or permissions differ. Do not create access keys as a workaround. |
| OIDC subject rejected | Workflow must run in the exact repository on main, with audience sts.amazonaws.com. Forks, PR subjects and GitHub Environment subjects are intentionally rejected. |
| Deployment job skipped | Verify manual dispatch on main, exact approval phrase and `AWS_DEPLOYMENT_APPROVED=true`, only after the owner approves IAM. |
| OIDC provider already exists | Use its ARN as `ExistingOidcProviderArn` in bootstrap. Do not create another provider or delete a shared one. |
| S3/KMS access denied | Inspect existing bucket policy/default encryption and role resource paths. A customer-managed KMS key requires a separate reviewed key permission change. |
| Catalog access denied | Check both IAM and Lake Formation if enabled. Do not disable governance or attach administrator permissions. |
| Landing checksum/row-count failure | Input changed under an immutable ID or upload is incomplete. Regenerate a fresh valid manifest and upload files first, manifest last. |
| Bronze output checksum failure | A committed object was externally changed. Stop and investigate; do not silently rewrite an audited batch. Recover from verified Landing under an explicitly managed repair. |
| Quality gate fails | Read `quarantine/<batch>/quality_report.json` and reason arrays. Fix the source or review the threshold as a business decision; do not discard rejects. |
| Partially published snapshot | `_publication.json` is FAILED/RUNNING. Do not query mixed versions. Rerun the same batch serially after correcting the failure. Table replacements converge; no cross-table atomicity is claimed. |
| Tables manually removed but replay says no-op | No-op trusts the publication record. After review, invalidate that record or bump the package version and rebuild. Never mutate curated tables outside the pipeline without a recovery procedure. |
| 15-minute timeout on larger profiles | Do not raise workers/timeout automatically. Inspect stage logs and first prove the small profile within the approved budget. See the measured scope and retained scan limits in docs/performance.md. |
| Athena scan cutoff | Narrow the query or projection. Raising the cap requires a deliberate cost decision. The small-profile queries should fit; live verification is pending. |
| Deleted stack fails to recreate databases | Catalog databases use Retain. Import retained resources or do an owner-led cleanup; do not blindly delete data. |

Logging is JSON at application boundaries, with batch IDs and failure types. Spark and AWS retain
their native logs as well. The Glue custom error/output log groups have seven-day retention. Cost
protection depends on review and manual execution; the $10 budget is not an automatic stop switch.

## Phase 3 execution diagnostics

For `EXECUTION_REQUIRES_EXPLICIT_RESUME`, inspect the failed/RUNNING execution report, then use the
same request and execution ID with `--resume` only after resolving its cause. A live competing writer
must finish first. `EXECUTION_ID_REUSED_WITH_DIFFERENT_PARAMETERS` requires the original parameters
or a deliberately new execution ID; never erase state to bypass it.

`PUBLICATION_VERIFICATION_FAILED` means stored schema/rows differ from the saved expected result.
Do not release readers, advance checkpoints manually or delete the frozen plan. A genuine mismatch
needs investigation; a recognized transient read failure can use bounded retry. If execution-state
storage fails too, inspect structured `pipeline_attempt_failed` logs for the original exception.

If optional metrics are absent later in AWS, first confirm monitoring was approved/enabled, the real
Glue error log group and JSON format, then filter behavior. Missing metrics are not proof of success.
See [operations recovery](operations.md#recovery-runbook), [monitoring](monitoring.md), and
[live acceptance prerequisites](release-checklist.md). No cloud action has been performed by these instructions.
