# AWS review, one-time bootstrap and controlled deployment

**Status: prepared only. No AWS credentials were configured or used during implementation.**
GitHub repository access does not grant AWS access. Nothing in CI deploys or starts jobs. The steps
below are for the owner after IAM review and explicit approval, not commands already executed.

**Phase 2 update:** no IAM or CloudFormation changes are required. The same release package/job now
supports `--pipeline_mode cdc`; see [CDC operations](cdc-operations.md). The Phase 2 PR is stacked on
`feat/phase-1-lakehouse` while PR #1 remains unmerged. Review/merge in dependency order, then prepare
the release from the final reviewed main commit. Never dispatch deployment from a feature branch.

**Phase 3 update:** the release is now 0.3.0 and the existing Glue adapter uses execution control and
stored-row verification in both modes. Review the PRs in [stack order](release-checklist.md) before
preparing the final main release. `infrastructure/operations.yaml` is a separate owner-reviewed
manual Step Functions/optional monitoring stack. The existing GitHub and CloudFormation deployment
roles cannot create its new IAM role or state machine; this phase does not broaden those policies.
Follow [operations setup](operations.md#prepared-cloud-request-and-later-deployment) only after
separate authorization. Monitoring defaults off, alarm actions remain disabled and no schedule exists.

## IAM review

Read [bootstrap.yaml](../infrastructure/bootstrap.yaml) in full before creating a change set.
The bootstrap is intentionally not deployable by the GitHub deployment role.

| Identity | Trust | Allowed scope | Deliberately absent |
|---|---|---|---|
| `delivery-ops-dev-github-deploy` | GitHub OIDC, exact repository + `refs/heads/main`, audience `sts.amazonaws.com` | Get/Put release objects under `landing/_artifacts/`; change sets for `delivery-ops-dev`; pass only the CFN service role to CloudFormation | No IAM mutation, source upload, bucket administration or job/query execution |
| `delivery-ops-dev-cfn-execution` | CloudFormation service | One Glue job definition, two databases, two log groups, one Athena workgroup; pass only the runtime role to Glue | No IAM policy editing, S3 writes, Glue job runs or Athena query execution |
| `delivery-ops-dev-glue-runtime` | Glue service | Read Landing/Bronze/Silver/Gold/quarantine; write Bronze/Silver/Gold/quarantine; delete only curated/quarantine objects; table metadata in two databases; two log groups | No Landing writes/deletes, bucket policy changes, Athena queries or IAM administration |

The sole broad **Allow** resource is `logs:DescribeLogGroups`, whose API does not support resource
scoping. Wildcards on table/object/log-stream ARNs are bounded to this project. Explicit Deny rules
prevent deployment identities from starting compute even if another policy is later attached.
`iam:PassRole` has both a single role ARN and an exact `iam:PassedToService` condition.

Runtime privileges can read all six project prefixes and delete current curated/quarantine objects;
that is required for replacement writes. Do not use these prefixes for unrelated data. There is no
permission to write Landing from the Glue runtime. The GitHub identity can change the job's code and
parameters, so protect `main`, review workflow/script changes, and keep runtime privileges bounded.

## One-time owner steps (after approval)

1. Sign in with an MFA-protected administrative **temporary session**, preferably an existing
   federated/SSO identity. Do not create permanent access keys. Confirm the account is still on the
   Free Plan, its available credits, the $10 monthly alert and service availability in Sydney.
   If a service is unavailable under the Free Plan, stop; do not upgrade the plan.
2. In S3, confirm `ck-delivery-ops-lakehouse-2026-1025` is in `ap-southeast-2`. Inspect its encryption
   and ownership settings read-only. These templates do not create the bucket or change its security.
   SSE-S3 is the assumed data path. If the existing bucket requires a customer-managed KMS key, stop
   for an additional least-privilege key-policy review; do not weaken the bucket's policy.
3. In IAM → Identity providers, look for `token.actions.githubusercontent.com`. If it exists, copy its
   ARN and use `ExistingOidcProviderArn`; leave that parameter empty only if it does not exist.
   The existing provider must include audience `sts.amazonaws.com`. Do not create a duplicate provider.
4. In CloudFormation, select Sydney → create stack **with a change set** → upload
   `infrastructure/bootstrap.yaml` → stack name `delivery-ops-bootstrap` → set the two parameters →
   acknowledge named IAM resources. **Review the change set and all three IAM policies before execute.**
   The provider is retained on deletion, because it may later be shared with other projects.
5. Once you explicitly authorize and execute the reviewed bootstrap, record its three outputs:
   `GitHubRoleArn`, `CloudFormationRoleArn`, `GlueRuntimeRoleArn`. No workloads run during bootstrap.
6. In this exact GitHub repository → Settings → Secrets and variables → Actions → **Variables**, set:

   | Variable | Value |
   |---|---|
   | `AWS_DEPLOY_ROLE_ARN` | Bootstrap `GitHubRoleArn` output |
   | `AWS_CFN_ROLE_ARN` | Bootstrap `CloudFormationRoleArn` output |
   | `AWS_DEPLOYMENT_APPROVED` | `true` **only after your IAM/deployment approval** |

   These role ARNs are identifiers, not secrets. Do not add AWS access-key secrets. Require reviewed
   PRs and successful CI on `main`, including changes to `.github/workflows/` and `infrastructure/`.
   Do not attach a GitHub Environment to this job without revisiting OIDC trust: environments change
   the default subject from branch-based to environment-based. The supplied trust requires main.
7. Review and merge the phase PRs in order. In Actions → **Reviewed AWS infrastructure**, select **main**, set
   `operation=plan`, and type `DEPLOY_REVIEWED_INFRASTRUCTURE`. This explicitly approved workflow
   uploads two immutable release objects and creates a CloudFormation change set; it does not execute
   it. Download `infrastructure-review-<run-id>` and review `change-set.json` and AWS Console changes.
8. On the **same main commit**, dispatch again with `operation=apply`, the same approval phrase and
   the exact planned change-set name. This deploys only the reviewed definitions. The workflow checks
   the commit, parameters, template and existing stack role, then waits for CloudFormation completion.
   If main has changed, prepare and review a new plan. Repeated no-change plans are handled gracefully.
9. Confirm the resulting job is Glue **6.0 / G.1X / 2 workers / 15-minute timeout / 0 retries /
   max concurrency 1**, with no trigger or schedule. Confirm the two Catalog databases and the
   `delivery-ops-dev` Athena engine 3 workgroup. **Deployment approval is not job-run approval.**

CLI equivalent for step 4, in a separately authorized temporary AWS session with this checkout:

```bash
aws sts get-caller-identity
aws s3api get-bucket-location --bucket ck-delivery-ops-lakehouse-2026-1025
aws iam list-open-id-connect-providers
# If a provider exists, supply its ARN instead of the empty value below.
aws cloudformation create-change-set \
  --region ap-southeast-2 --stack-name delivery-ops-bootstrap \
  --change-set-name bootstrap-review --change-set-type CREATE \
  --template-body file://infrastructure/bootstrap.yaml \
  --parameters ParameterKey=ExistingOidcProviderArn,ParameterValue= \
  --capabilities CAPABILITY_NAMED_IAM
aws cloudformation describe-change-set \
  --region ap-southeast-2 --stack-name delivery-ops-bootstrap --change-set-name bootstrap-review
# ONLY after owner approval of this exact change set:
aws cloudformation execute-change-set \
  --region ap-southeast-2 --stack-name delivery-ops-bootstrap --change-set-name bootstrap-review
aws cloudformation wait stack-create-complete \
  --region ap-southeast-2 --stack-name delivery-ops-bootstrap
aws cloudformation describe-stacks \
  --region ap-southeast-2 --stack-name delivery-ops-bootstrap --query 'Stacks[0].Outputs'
```

For subsequent bootstrap updates use an UPDATE change set after another policy review. Keep the
same main-stack name for repeated deployments. Retained databases/provider make deletion conservative:
if a stack is deleted, recreating it with the same retained names requires import or an owner-led
cleanup, not blind redeployment.

## Separately approved first cloud smoke test

Do not run this section until the owner authorizes an S3 upload **and one chargeable Glue execution**.
Use a temporary owner/operator session, not the GitHub deployment role. Its permissions deliberately
cannot start the job. No extra AWS authentication is needed in Codex for local verification.

```bash
BATCH_ID=$(uv run delivery-lakehouse generate --profile small)
uv run delivery-lakehouse upload-landing --batch-id "$BATCH_ID" --approve-s3-write
aws glue start-job-run --region ap-southeast-2 \
  --job-name delivery-ops-dev-phase1 --arguments "{\"--batch_id\":\"$BATCH_ID\"}"
# Poll the returned JobRunId; inspect CloudWatch and the publication object.
aws glue get-job-run --region ap-southeast-2 \
  --job-name delivery-ops-dev-phase1 --run-id REPLACE_WITH_RETURNED_RUN_ID
aws s3 cp s3://ck-delivery-ops-lakehouse-2026-1025/gold/_publication.json -
```

After SUCCEEDED, verify all tables use format v2 by inspecting Iceberg metadata JSON, and verify
Catalog locations remain under the configured Silver/Gold prefixes. If Lake Formation governs the
account, explicitly grant the runtime and analyst the necessary database/table rights. This project
assumes ordinary IAM-governed Glue Catalog access and never changes Lake Formation defaults.

Athena queries also incur usage and need separate approval. Select workgroup `delivery-ops-dev`,
then run [06_verification.sql](../sql/athena/06_verification.sql) and the business queries one statement
at a time. The expected small snapshot has 120 shipments; exact evidence is in the verification report.
A second approved Glue run with the identical batch must leave business rows unchanged and log a no-op.

For an analyst, grant only the project's Glue `GetDatabase/GetTable/GetPartitions`, read access to
Silver/Gold, and Athena execution/result access for this workgroup plus result-prefix permissions.
Do not attach broad administrator policies to the Glue runtime or GitHub role to fix access errors.

## Cleanup

Stop starting jobs and disable the deployment variable first. There are no scheduled workloads.
Review stack deletion before executing it. Log groups have seven-day retention; retained Catalog
metadata and S3 objects remain and can continue to incur small storage charges. Iceberg snapshots and
old release artifacts require explicit manual retention/cleanup; automated vacuum/compaction is
outside Phase 1. Never delete unrelated bucket contents or a shared OIDC provider.
