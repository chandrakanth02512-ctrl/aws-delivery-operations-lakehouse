# Phase 3 verification report

Scope: execution control, prepared AWS orchestration/monitoring, post-write verification, bounded
recovery, SQL analytics and a measured incremental Gold optimization. Phase 1/2 tests and historical
reports are retained. Base: `f0e4467a3cee75e3cdc9a5044d5f2c7a330022e4` on
`feat/phase-2-incremental-cdc`; implementation branch: `feat/phase-3-operations`; package: **0.3.0**.

**Final local regression: 157 passed, 1 skipped, zero failures in 957.76 seconds (15m 57s).**
Package line coverage: **92%** (1,652 statements; 135 uncovered, 1,517 covered; 91.83% unrounded).
No AWS infrastructure, job, query, IAM change or monitoring resource was deployed/executed.
The compact [local evidence record](evidence/phase3-local-verification.json) retains the JUnit totals
and coverage counters without committing generated datasets or full runtime logs.

## Results recorded on 26 October 2026

| Check | Actual local result |
|---|---|
| Combined Phase 1/2/3 Pytest run with real local Spark/Iceberg | 157 passed, 1 skipped in 957.76 seconds; no failures/errors |
| Explicitly live AWS readiness test | 1 skipped: AWS access not authorized/configured |
| Ruff lint / formatting | Passed |
| cfn-lint: bootstrap, lakehouse and operations templates | Passed |
| Generated dictionary consistency | Passed |
| Locked dependencies and installed-package compatibility | Passed; 29 installed packages compatible |
| Wheel, source distribution, deterministic Glue import ZIP | Built successfully; operations modules and CLI entry point checked |
| Git whitespace and local documentation file links | Passed |
| Tracked/unignored file credential-pattern and sensitive-name review | No matches; generated data/JARs/builds remain ignored |

The run includes all **109 prior passing tests**, **45 new operations tests**, and **three additional
SQL-discovery cases**. The existing live AWS readiness test remains skipped. Selected development
runs are not added to the final count. Source, state and tables were exercised locally with real
Spark/Iceberg; AWS SDK/Glue adapter tests use stubs or mocks.

Runtime: Python **3.13.5**, PySpark **4.1.1**, Iceberg **1.11.0** for Spark 4.1/Scala 2.13,
H2 **2.4.240**, Java **21.0.12.1**, Pytest **9.1.1**, Ruff **0.17.2**, cfn-lint **1.58.0**,
uv **0.12.23**. Hosted CI uses Python 3.13 and Temurin Java 17; it repeats the complete suite,
static checks and package builds. The [Actions checks](https://github.com/chandrakanth02512-ctrl/aws-delivery-operations-lakehouse/actions/workflows/ci.yml)
are the authoritative hosted status for the PR/main commit being reviewed. Local success is not a
claim that a different commit or managed AWS run passed. CI retains JUnit, JSON coverage and Glue ZIP
artifacts for seven days.

## Reproduction

```bash
uv sync --frozen --extra spark
uv run python scripts/fetch_iceberg.py
ICEBERG_SPARK_JAR=artifacts/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
  uv run pytest --cov=delivery_lakehouse --cov-report=term-missing \
  --cov-report=json:artifacts/phase3-coverage.json --junitxml=artifacts/phase3-pytest.xml
uv run ruff check .
uv run ruff format --check .
uv run cfn-lint infrastructure/*.yaml
uv run python scripts/data_dictionary.py --check
uv lock --check
uv pip check
uv build
uv run python scripts/package_glue.py
git diff --check
```

In this restricted workspace, `UV_CACHE_DIR=/tmp/delivery-uv-cache` supplies a writable dependency
cache and Spark execution is permitted local network sockets. Those are environment constraints,
not application requirements. No runtime credentials are needed for this command sequence.

## Coverage of behavior

- Preserved Phase 1: seeded contracts, quality/reconciliation, raw ingestion, schema/FK validation,
  dimensional model/KPIs, snapshot idempotency, actual local Iceberg v2 and deployment safeguards.
- Preserved Phase 2: INSERT/UPDATE/DELETE, ordering/tombstones, duplicate/stale audit, schema policy,
  late corrections, restricted parent deletes, active-batch guard, per-table interrupted commit
  recovery, final-record recovery and all-nine-Gold-table full-recomputation comparison.
- Phase 3 control: ordered stages, deterministic names, conflicting request rejection, successful
  replay, explicit failed/interrupted resume, bounded transient retry, permanent failure behavior,
  shared local writer lock, metrics, final execution-record recovery and original-error logging
  when state persistence also fails.
- Phase 3 real local Iceberg: bootstrap with all sources, snapshot and CDC post-verification,
  failure after writes, frozen-plan recovery, integrity quality rejection and corrected-batch
  recovery, full recomputation, and event-only execution with no Gold writes/snapshots.
- Stored-row validation detects duplicate, missing, changed and schema-incompatible results.
- SQL tests cover boundary denominators, cancellation, station/driver aggregates, first attempts,
  late shipment details and weighted trends with missing calendar dates.
- Static infrastructure and mocked Glue adapter tests verify ordered handoff, success-only commit,
  safe defaults, role scope, absence of schedules, monitoring opt-in and disabled alarm actions.

## Focused performance evidence

The [saved benchmark](evidence/phase3-gold-benchmark.json) compares the Phase 2 planner with the
current planner using the same cache-warmed 120-shipment model and one event-only change.
All three baseline repetitions returned nine empty deltas and launched nine Spark jobs; all three
current repetitions returned no deltas and launched zero Spark jobs. Both emitted zero Gold rows.
Median measured planning time was 5,149.756 ms versus 0.016 ms. A separate regression suite was
running concurrently; these timings are descriptive and not an isolated throughput benchmark.
See [method and limits](performance.md). No AWS runtime/cost improvement is claimed.

## Not tested / acceptance limits

The live AWS bucket readiness test is intentionally skipped without explicit authorization. Managed
Glue 6.0, real S3 conditional writes, Glue Catalog/Lake Formation/KMS permissions, GitHub OIDC, IAM
simulation, CloudFormation deployment, Step Functions `.sync`/redrive/cancellation, CloudWatch
ingestion/alarm behavior and Athena engine-3 SQL/metadata execution remain unverified.

`cfn-lint` is local schema/rule validation, not an AWS ValidateTemplate or deployment result. Stubbed
SDK and Glue adapter tests cannot prove service behavior. Dependency compatibility checks are not a
vulnerability audit. No broad historical secret audit or large-volume production performance claim
is made. The tracked-file/diff credential-pattern review is an additional release check only.

Publication is atomic per Iceberg table only. Readers must coordinate with the publication marker
and avoid active writes. Direct callers/other job definitions must enforce the single-writer contract.
Old successful replay does not continuously audit externally modified tables. Retention, compaction,
distributed locking and destructive repair of external corruption remain deliberately out of scope.

See [private release and AWS acceptance checklist](release-checklist.md) for review/merge order,
visibility/license decisions and the separately approved live verification steps.
