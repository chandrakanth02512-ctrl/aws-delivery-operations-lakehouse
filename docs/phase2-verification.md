# Phase 2 verification report

Verification date: **26 October 2026**. This report covers local code and prepared integration,
not a deployed AWS workload. Phase 1 evidence remains in [verification.md](verification.md).

## Repository baseline

Phase 1 PR #1 was confirmed open, draft and unmerged. Phase 2 is based on
`158986db17fff50d1280aca5d9d4021f28cfdc22` (`feat/phase-1-lakehouse`), not the initial main branch.
The feature branch is `feat/phase-2-incremental-cdc`; its draft PR targets `feat/phase-1-lakehouse`.
No Phase 1 tests were removed. Infrastructure/IAM templates and the deployment workflow are unchanged.

## Executed local checks

Runtime: Python 3.13.5, Spark 4.1.1, local Java 21, Apache Iceberg 1.11.0 (Spark 4.1 / Scala 2.13),
H2 2.4.240 JDBC catalog. Hosted CI selects Java 17. Local Iceberg tests use real metadata/Parquet
files and real SQL MERGE; S3 adapter unit tests use botocore stubs.

```bash
uv sync --frozen --extra spark
uv run python scripts/fetch_iceberg.py
ICEBERG_SPARK_JAR=artifacts/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
  uv run pytest --cov=delivery_lakehouse --cov-report=term-missing \
    --junitxml=artifacts/phase2-pytest.xml -q
uv run ruff check .
uv run ruff format --check .
uv run cfn-lint infrastructure/*.yaml
uv run python scripts/data_dictionary.py --check
uv pip check
uv lock --check
uv build
uv run python scripts/package_glue.py
git diff --check
```

In the managed workspace the test/check executables are invoked from `.venv/bin/`, equivalent to
the locked `uv run` environment above. `UV_CACHE_DIR=/tmp/delivery-uv-cache` is used for a writable
dependency cache. Spark execution is allowed local sockets. No AWS credentials are needed.

Final combined test result: **109 passed, 1 skipped in 800.88 seconds (13:20), 91% package line
coverage**. All 56 added Phase 2 tests passed alongside the 53 preserved Phase 1 local tests.
The single skipped test is `tests/aws/test_readiness.py`, which requires live AWS authorization.
JUnit results are written to `artifacts/phase2-pytest.xml`. No known failing tests remain.

Static/package checks: Ruff check and format check, both CloudFormation templates, generated data
dictionary consistency, dependency compatibility, lock consistency, wheel/sdist build, deterministic
Glue ZIP and Git whitespace checks passed. Package version is 0.2.0. Build outputs are gitignored.

## What the tests establish

- All preserved Phase 1 generator, validation, quarantine, KPI, replay, storage, IAM-scope and
  packaging tests still execute alongside Phase 2.
- Deterministic chronological generation; explicit INSERT/UPDATE/DELETE, status changes,
  driver reassignment, cancellation, attempt corrections and duplicated changes.
- Immutable partial Bronze ingestion recovery; missing files, damaged files, source manifests,
  committed Bronze manifests and row counts fail integrity validation.
- Strict envelope types, canonical keys, duplicate JSON-field rejection and explicit compatible/
  breaking schema policy. Invalid newer records do not override valid lower-version changes.
- Version/time/hash ordering, tombstone preservation, repeated event/batch idempotency and a
  non-filtering global watermark. Replaying an old batch cannot rewind the latest publication.
- Actual Iceberg v2 INSERT/UPDATE/DELETE MERGE; source-key uniqueness; v3, location and schema guards.
- Driver reassignment retains the actual historical attempt driver. Attempt corrections and date
  changes update the correct KPI denominators and remove obsolete date/driver groups.
- Parent deletes with dependents fail before business-table publication; explicit dependent deletes
  succeed and remove empty current facts/KPI groups while retaining all version tombstones.
- Six-batch incremental Gold output equals full recomputation for every one of the nine Gold tables.
  Independently specified late-delivery expectations check the formulas beyond that shared-code oracle.
- Four failures occur after a Silver/Gold MERGE or at the final success-record write. Recovery reuses
  the frozen plan, blocks unrelated batches, preserves the committed watermark, converges without
  duplicate rows, and matches recomputation. State histories include FAILED then SUCCEEDED.
- A corrected new batch may supersede a validation-only failure. Retrying that old invalid batch
  again cannot rewind a newer committed checkpoint.
- Existing Phase 1 Iceberg data requires explicit adoption. Baseline keys seed at version 0; after
  adoption, accidental snapshot replacement is blocked. The baseline union is materialized before
  MERGE to avoid the Spark 4.1 multi-Iceberg-source planning failure found by this integration test.
- Execution JSON logs and Markdown quality reports, source/plan checksums, terminal state rules,
  checkpoint cleanup and explicit S3-upload gating are tested.

## Practical example evidence

The deterministic source starts with 120 valid shipments. Two new shipments and one explicit
deletion leave **121 current shipments**. `SHCDC0001` changes from IN_TRANSIT / DR0000 to
IN_TRANSIT / DR0003, then DELIVERED / DR0003. Its Monday 9 February promise is 07:00 UTC and actual
delivery is 08:00 UTC; Wednesday arrival corrects Monday's metrics despite an already newer global
watermark. It has two attempts, the first failed. Monday's two-shipment cohort contains one delivered
and one cancelled shipment: on-time 0%, average delay 60 minutes, first-attempt success 0%, terminal
failure 0%, total attempts 2. Zero delivered denominators for the cancelled driver's group yield NULL.

The standalone `uv run python scripts/demo_cdc.py` run **passed all four batches** and wrote
`artifacts/cdc-demo-summary.json`. Observations confirmed the status/driver/delay/attempt transitions
above, with watermark `2026-02-10T09:00:00+00:00` unchanged by Monday's late confirmation.
Replaying the older new-shipments batch left publication/checkpoint bytes unchanged. A fresh-process
CLI replay exposed H2's uppercase metadata mismatch with Iceberg's schema probe. The local JDBC
configuration now creates lowercase identifiers. A subsequent CLI bootstrap and replay in **two
separate processes both passed**, using `data/cdc-restart-verified`; a regression test independently
reopens the catalog. These are local filesystem/JDBC results.

## Not tested on AWS and remaining limits

The live AWS test is skipped by default because authentication and cloud execution are not authorized.
No infrastructure deployment, IAM mutation, OIDC exchange, live S3 upload/read, managed Glue job,
Glue Catalog transaction, CloudWatch ingestion or Athena query was performed. No AWS chargeable
resources were created, no account upgrade occurred, and no Phase 3 implementation was started.

Local correctness does not establish Glue/S3 commit behavior, Lake Formation access, managed runtime
performance or Athena compatibility in this account. Those require separately authorized acceptance
with actual JobRunIds, QueryExecutionIds, metadata and billing evidence. Iceberg format v2 is pinned
and locally inspected; that is not a claim that Athena has read these tables.

Current limits: one serialized writer; per-table rather than cross-table atomicity; readers must
avoid overlapping writes; bounded driver-side source decoding; full prospective reference checks;
unpartitioned tiny tables; no large CDC benchmark; no SCD2, schema auto-DDL, automatic retention,
tombstone expiry, scheduling, distributed lock or coordinated Athena publication. The staged-plan
manifest is checksummed, while Parquet payload verification is structural/row-count based.
These are documented design boundaries, not silently skipped implementation claims.

## Completion checklist

- [x] Deterministic CDC protocol and chronological source batches
- [x] Immutable incremental Bronze and corrupt/incomplete batch detection
- [x] Silver v2 MERGE, deterministic ordering, quarantine and reverse integrity checks
- [x] Explicit schema compatibility/migration policy and late-event handling
- [x] Incremental facts, dimensions and historical KPI corrections
- [x] Batch states, source checkpoint, tombstones and frozen-plan recovery
- [x] Failure simulations, full-recomputation comparisons and preserved Phase 1 tests
- [x] Local runbook, architecture, example, cost assumptions and file inventory
- [x] Final combined regression passed; verified implementation prepared on the stacked branch
- [ ] Live AWS acceptance — intentionally deferred; separate authorization required

GitHub review target: `feat/phase-2-incremental-cdc` → `feat/phase-1-lakehouse`. The draft PR must
be reviewed after Phase 1 PR #1; neither PR is merged or deployed by this implementation task.
The repository's **Lakehouse quality** workflow repeats the full suite with Java 17 and retains
JUnit/Glue ZIP evidence. Its run status is separate from the local results recorded above.
