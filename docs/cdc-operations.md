# Phase 2 local operation and recovery

All commands in the local sections operate on disk. Do not deploy, upload to S3, start Glue, execute
Athena queries or change IAM as part of local verification. Phase 2 reuses the Phase 1 infrastructure.

## Reproduce the four-batch example

Use Python 3.13, Java 17+ and `uv` from the repository root:

```bash
uv sync --frozen --extra spark
uv run python scripts/fetch_iceberg.py
uv run delivery-lakehouse-cdc generate --root data/cdc > /tmp/delivery-cdc-batches.json
uv run python -c 'import json; print("\n".join(x["batch_id"] for x in json.load(open("/tmp/delivery-cdc-batches.json"))))' > /tmp/delivery-cdc-ids.txt
while IFS= read -r batch_id; do
  uv run delivery-lakehouse-cdc run-local --root data/cdc --batch-id "$batch_id" || break
done < /tmp/delivery-cdc-ids.txt
```

The local CDC command always uses real Iceberg v2 tables and persists its JDBC catalog in
`data/cdc/_catalog`. This allows separate invocations to recover the same tables. Set a different
`--root` for an independent demo, not for continuing an existing one. Configuration defaults to
`config/dev.json`; small is the default profile. Generate medium/large via a reviewed JSON config
with the desired `profile`. Their runtime/memory performance has not been benchmarked.

The local H2 connection uses `DATABASE_TO_LOWER=TRUE`, which lets Iceberg recognize its metadata
schema on reopening. Older experimental H2 catalogs created with uppercase defaults are not
automatically converted; keep those files and use a fresh local test root, or plan a catalog migration
if their data must be retained. This local JDBC setting does not affect AWS Glue Catalog adoption.

The optional single-session equivalent, including before/after observations and replay verification:

```bash
uv run python scripts/demo_cdc.py
```

It writes `artifacts/cdc-demo-summary.json` and uses a separate, fresh `data/cdc-demo` root. It refuses
an existing publication so that before/after observations describe real transitions. Choose another
`--root` to repeat the full demo. The CLI example can be rerun to verify source integrity and return
committed batch states. Use the exact batch ID from the generated manifest to replay one batch:

```bash
uv run delivery-lakehouse-cdc run-local --root data/cdc --batch-id REPLACE_WITH_64_CHARACTER_BATCH_ID
```

Inspect `data/cdc/gold/_cdc/batches/`, `reports/`, `checkpoint.json`, and `quarantine/cdc/`.
Application logs are JSON; Spark also emits native logs. Counts distinguish inserted, updated,
deleted, rejected and ignored rows. A failed report's counts describe its proposed plan, not a claim
that every row was published. Structured errors identify BRONZE, INITIALIZATION, VALIDATION or
PUBLICATION; per-attempt timestamps and duration aid diagnosis.

## Recovery procedure

1. Stop readers and other writers. Inspect the active batch ID, its state, error and completed steps.
2. For missing/corrupt Landing, restore the exact verified source bytes. Never change bytes beneath
   an existing batch ID. For invalid source data/schema, create a corrected **new batch** if the
   failed state has no publication plan. Read the quarantine reasons before changing any threshold.
3. If a `plan_key` exists, keep the source, plan, staged Parquet and target tables. Fix the transient
   storage/runtime issue and rerun **the same batch**, package version and configuration. Another
   batch is blocked with `RECOVER_ACTIVE_BATCH_FIRST` while recovery is required.
4. Check that the original plan was reused, the state is SUCCEEDED, every required step completed,
   publication is SUCCEEDED and the active checkpoint is cleared. Validate counts and representative
   records before releasing readers. Never delete a failed batch state to bypass the gate.

Recovery under a different package or rejection policy is deliberately refused. If code must change
while a plan is pending, operator-led recovery/migration is required; the pipeline does not guess.
On abrupt process death the state can remain RUNNING. The same-batch restart path supports that state.
`failure_hook` is a Python test seam only; the production CLI/Glue adapter exposes no failure flag.

## Adopt a completed Phase 1 snapshot

This is an explicit, one-time migration within the same table/catalog/storage locations. A successful
Phase 1 Iceberg publication must contain all six Silver and nine Gold tables. Plain Parquet outputs
from the Phase 1 convenience CLI cannot be adopted as though they were registered Iceberg tables.

The first CDC run refuses existing business data unless `--adopt-phase1-snapshot` is supplied (or
`adopt_snapshot=True` for the Python runner). It seeds every current source key into the ledger at
version 0, then processes the CDC batch normally. Use source versions starting at 1 and continuing
monotonically thereafter. Initialization can be retried after a partial ledger seed. Once initialized,
the Phase 1 snapshot runner refuses to overwrite that location. Unrelated fresh locations retain
full Phase 1 functionality. This is a one-way adoption; there is no automatic downgrade/reset command.

## Prepared AWS execution path — not executed

The existing `jobs/phase1.py` entry point now accepts optional arguments:

| Argument | Value |
|---|---|
| `--pipeline_mode` | `cdc`; default remains `snapshot` for Phase 1 |
| `--batch_id` | CDC manifest batch ID |
| `--adopt_phase1_snapshot` | `true` only for reviewed initial snapshot adoption; default `false` |

The release ZIP includes the CDC package automatically. The existing Glue job, runtime role,
Catalog databases, bucket prefixes and workgroup are reused. No CloudFormation or IAM modifications
are required. Keep Glue 6.0, two G.1X workers, timeout 15 minutes, retries 0, concurrency 1. Do not
upload the local Iceberg/H2 JARs to Glue or enable format v3.

After separate owner authorization, the existing deployment guide describes the reviewed OIDC
setup and release plan/apply process. The CDC CLI has an explicitly gated `upload-landing` command;
it uploads data files before the manifest. The GitHub deployment role cannot upload source batches
or start jobs/queries. An authorized operator would upload and submit batches **sequentially**,
waiting for success before the next. Deployment approval and workload execution approval remain
separate. No command in CI obtains AWS credentials or exercises this path.

Cloud acceptance must record actual S3 integrity checks, Glue JobRunIds/runtime, Glue Catalog table
schemas/locations and v2 metadata, controlled failure/restart results, Athena query IDs/results and
billed usage. Local JDBC-catalog evidence cannot establish managed S3/GlueCatalog/Athena correctness.

## Targeted diagnosis

| Error or symptom | Action |
|---|---|
| `RECOVER_ACTIVE_BATCH_FIRST` | Recover the named pending publication; do not process a new batch |
| `EXPLICIT_PHASE1_BASELINE_REQUIRED` | Review snapshot adoption; do not discard current data |
| `DEPENDENT_RECORD_INTEGRITY_FAILED` | Inspect integrity rows; supply explicit dependent deletes/reassignments |
| `BREAKING_SCHEMA_DRIFT` | Correct the source or review a migration; no automatic DDL is performed |
| `PUBLICATION_PLAN_SOURCE_OR_VERSION_MISMATCH` | Restore the exact source/runtime/configuration used by the saved plan |
| `COMMITTED_BRONZE_*CORRUPTED` | Stop and recover verified immutable objects; do not overwrite the audit trail |
| New nullable field absent from typed Silver | Expected: raw retention only until an explicit contract migration |
| Late event does not change shipment status | Supply the authoritative shipment after-image; events are evidence |
| Cancelled shipment remains in total shipments | Expected: it is excluded only from terminal KPI denominators |
| Local lock/catalog is in use | Stop the competing local process; never bypass single-writer serialization |
| A previously successful batch returns no-op | Expected; correction belongs in a new higher-version change/batch |
