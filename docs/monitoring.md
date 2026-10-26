# Monitoring prepared for later AWS activation

No alerts, metric filters, state machines or subscriptions were deployed. The three metric filters
and four alarms in `infrastructure/operations.yaml` require `EnableMonitoring=true`; the default is
false. Every alarm has `ActionsEnabled=false`, no action ARN and `TreatMissingData=notBreaching`.
No notification is sent until the owner separately reviews and configures actions.

## Execution signals

Application JSON logs carry an execution ID, batch ID, event timestamp and stage/status. Start and
completion records add start/end timestamps, attempt number or duration and record counts. Spark
retains its native diagnostics alongside these application records.

| Field | Meaning |
|---|---|
| `execution_id` / `batch_id` | Operational invocation identity / immutable source batch identity |
| `status`, `stage` | Execution outcome and current dependency stage |
| `started_at`, `completed_at`, `duration_seconds` | Execution timing; attempt details are retained in state |
| `source_rows` | Original source manifest row population |
| `input_rows` | Rows processed by this logical execution; zero for an already-published batch no-op |
| `rows_inserted`, `rows_updated`, `rows_deleted` | Accepted CDC changes classified against prior Silver |
| `accepted_changes`, `rows_rejected`, `rows_ignored` | Reconciled CDC acceptance, quality rejection and duplicate/stale audit counts |
| `output_rows` | Published Silver upserts / accepted snapshot rows on success; zero before publication, unknown/null after partial failure |
| `planned_output_rows` | Accepted output plan before publication; do not treat a failed plan as committed output |
| `quality_reject_fraction` | Rejected/input for execution summary; the existing quality gate remains per dataset |
| `error_category` | DATA_QUALITY, SOURCE_INTEGRITY, CONTRACT, AUTHORIZATION, RECOVERY_REQUIRED, TRANSIENT_IO or an unclassified/AWS error |

Snapshot inserted/updated/deleted counters are zero because full replacement is not CDC; its output
count reports accepted source rows. Gold derived rows and the CDC ordering ledger are not added to
Silver output volume, which would double-count the same business records. Verification separately
reports the number of target rows checked. Failed attempts preserve diagnostics even after recovery.

## CloudWatch definitions

| Signal | Source | Purpose |
|---|---|---|
| FailedExecutions | Completion JSON with status FAILED | Application failures, including exhausted retry limits |
| RejectedRows | Completion JSON `rows_rejected` | Quarantine visibility even when the quality gate permits publication |
| ExecutionDuration | Completion JSON `duration_seconds` | Completed-invocation timing; not an in-flight SLA timer |
| Orchestration failure alarm | AWS/States ExecutionsFailed | Includes request/job startup failures before application logging |
| Orchestration timeout alarm | AWS/States ExecutionsTimedOut | Detects workflows ending without an application completion record |

Filters target the existing `/aws-glue/jobs/delivery-ops-dev/error` log group because the JSON Python
handler writes to stderr. Confirm the actual managed Glue log group and JSON message shape before
enabling filters; that integration has not been tested. Logs retain the existing seven-day policy.
Custom metrics have no execution/batch dimensions, avoiding unbounded metric cardinality/cost.
Use execution IDs in logs and S3 reports for drill-down instead.

An absent completion metric is not proof of health. Pair application signals with state-machine
failure/timeout metrics and inspect Glue status after process termination or cancellation. Metric/log
delivery is not an exactly-once audit transaction; durable execution records and source/CDC state are
the authoritative evidence. A reporting-write interruption can require explicit recovery.

## Review before activation

Confirm the Free Plan's service eligibility without upgrading it; review the optional monitoring
cost; validate the real log-group path/JSON filters; choose any notification destination explicitly;
and only then review a change set enabling monitoring/actions. No SNS topic, subscriber address or
automatic schedule is supplied. The $10 budget remains an alert, not a spending stop.

Local tests validate JSON fields, definition defaults, IAM scope and failure behavior. They cannot
prove CloudWatch ingestion, alarm transitions, Step Functions service integration or message delivery.
