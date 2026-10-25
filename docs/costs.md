# Cost assumptions and guardrails

No AWS resources were provisioned and no AWS workload ran during implementation. Local Spark tests
consume no AWS credits. The following is a **planning estimate, not a verified Sydney price quote**.
Check the live Sydney Glue 6.0 rate in the AWS Pricing Calculator before the first approved run.

| Item | Conservative assumption | Example gross usage cost before credits |
|---|---|---|
| One Glue run, two G.1X workers | 2 DPUs × minutes / 60 × **$0.50/DPU-hour planning rate** | 5 min: ~$0.083; 15 min: ~$0.25 |
| Ten such runs/month | 5–15 minutes per run | ~$0.83–$2.50 |
| S3 data, metadata, artifacts and requests | Small profile, well under 1 GB, limited requests | Budget ~$0.10–$0.50/month |
| CloudWatch logs | Short jobs, seven-day retention | Budget ~$0.10–$0.50/month |
| Athena | A few manual queries, 10 MiB per-query cap | Usually cents at this scale; budget ~$0.10 |
| Glue Catalog, IAM, OIDC, CloudFormation definitions | Very small metadata footprint; no paid extensions | Normally no material incremental charge within applicable allowances |

**Allow roughly $1–$4/month for ten small manual runs**, with actual bills dependent on regional
pricing, logs, request count and storage retention. One small smoke run should fit within roughly
$0.10–$0.30 plus small ancillary usage under these assumptions. Idle definitions have no running Glue
compute cost; stored objects, logs and metadata can still be billable. Credits offset eligible charges
but do not change usage or guarantees. Taxes and non-project/account-wide usage are excluded.

[AWS Glue pricing](https://aws.amazon.com/glue/pricing/) describes DPU-hour billing and Catalog
allowances; [the Glue 6.0 announcement](https://aws.amazon.com/about-aws/whats-new/2026/08/aws-glue-6-0-price-reduction-iceberg-v3/)
announces reduced compute pricing. The $0.50 above deliberately remains a conservative planning
assumption rather than applying an unverified Sydney discount.
[S3 pricing](https://aws.amazon.com/s3/pricing/) and [Athena pricing](https://aws.amazon.com/athena/pricing/)
cover the ancillary charges.

All job definitions pin Glue 6.0, G.1X, two workers, 15-minute timeout, zero retries and concurrency one.
There are no schedules, crawlers, development endpoints, streaming jobs, VPC/NAT gateways, automated
compaction or autoscaling. GitHub deployment identities cannot start compute. Workflow execution
requires main, an owner-set approval variable and an explicit manual dispatch.

The existing **$10 budget alert is a notification, not a hard spending cap**. A scan limit is per query,
not per month; repeated runs and queries still accumulate costs. Two workers and 15 minutes are job
configuration controls, not an account-wide cost ceiling. Review the account's credits and billing
console after the first run. Never change the account to a Paid Plan to make this project work.
