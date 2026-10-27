# Local and cloud verification

Required: Python 3.13, Java 17 (Glue's major version; local Java 21 is also supported by Spark 4.1),
and `uv` 0.13.4 or later. The workflows pin 0.13.4 to address GHSA-hq85-5cpj-mr62.
The lockfile pins the full Python dependency graph. Runtime code uses only the Python
standard library plus boto3/PySpark supplied in Glue; Glue receives a deterministic import ZIP.

```bash
uv sync --frozen --extra spark
uv pip check
uv run ruff check .
uv run ruff format --check .
uv run cfn-lint infrastructure/*.yaml
uv run python scripts/data_dictionary.py --check
uv run python scripts/fetch_iceberg.py
ICEBERG_SPARK_JAR=artifacts/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
  uv run pytest --cov=delivery_lakehouse --cov-report=term-missing --junitxml=artifacts/pytest.xml
uv run python scripts/package_glue.py
uv build
```

`fetch_iceberg.py` downloads pinned Iceberg 1.11.0 (Spark 4.1 / Scala 2.13) and H2 2.4.240 JARs from
Maven Central and checks committed SHA-256 digests. Without `ICEBERG_SPARK_JAR`, the local Iceberg
snapshot and CDC integration tests are skipped; this is not full validation. Default tests skip live
AWS. The CI workflow downloads these dependencies and runs all local Iceberg tests.

- Unit tests verify deterministic generators (including medium/large counts), schemas/FKs, immutable
  source identities, checksum changes, interrupted ingestion recovery, conditional S3 request shape,
  storage safety, quality gate boundaries, packaging and infrastructure/trust restrictions.
- Spark integration tests execute actual local Landing → Bronze → Silver → Gold writes, verify
  quarantine and reconciliation, enforce relationships, check invalid types/NaN/infinity/collisions,
  validate KPI formulas with an independent oracle, and exercise quality-gate failure/recovery.
- Iceberg tests create/read real tables and metadata files, overwrite without duplicating rows,
  overwrite with an empty snapshot, reject v3, and run the complete 15-table pipeline locally.
- S3 tests use botocore Stubber with dummy credentials. They validate SDK request/error behavior;
  they do **not** prove live S3, IAM or Glue integration.
- CloudFormation is parsed/linted and safety assertions are tested. IAM policy simulation, actual
  resource creation, OIDC token exchange, AWS runtime behavior and Athena execution remain untested.

To run the cheap local demo:

```bash
BATCH_ID=$(uv run delivery-lakehouse generate)
uv run delivery-lakehouse run-local --batch-id "$BATCH_ID"
# Replay: same publication, no duplicate business rows.
uv run delivery-lakehouse run-local --batch-id "$BATCH_ID"
```

Settings live in `config/dev.json` and `config/test.json`. `--root` changes the data directory;
`--profile medium` or `large` explicitly increases synthetic volume. Small is always the default.
These commands never initialize an AWS client. Generated data/JARs/builds are gitignored.

Only after AWS setup and explicit read-only test approval:

```bash
uv run pytest tests/aws --run-aws -v
```

This currently checks only the existing bucket's region and does not start queries or jobs.
A paid cloud smoke test requires the separate procedure in [deployment.md](deployment.md), followed
by evidence capture: JobRunId and status, publication JSON, report totals, table format metadata,
Athena QueryExecutionIds/results, elapsed DPU time and actual billed usage. Do not label those passed
until they have actually completed. Full managed Glue runtime validation is not replaced by local tests.

## Phase 2 coverage and focused commands

The full command above includes all preserved Phase 1 tests and `tests/cdc/`. Focused tests:

```bash
uv run pytest tests/cdc/test_protocol.py tests/cdc/test_bronze.py tests/cdc/test_state.py -q
ICEBERG_SPARK_JAR=artifacts/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
  uv run pytest tests/cdc -q
uv lock --check
uv build
```

`test_sequence.py` processes six batches and compares all nine incremental Gold tables with a full
recomputation from final Silver. It verifies Monday/Wednesday lateness, duplicate batches/events,
tombstones, old checkpoint replay, compatible additions, event-only arrivals and unchanged fact
snapshot counts. `test_ordering.py` checks versions, timestamp/hash ties and deleted-key ordering.

`test_recovery.py` injects failures after Silver shipment MERGE, Gold fact MERGE, Gold KPI MERGE,
and during the final success-record write. These deliberately fail **after** side effects, before
the next checkpoint. Tests verify the active-batch gate, frozen-plan reuse, state transitions,
watermark preservation, retry idempotency and post-recovery full-recomputation equality.

`test_merge.py` also reopens the local JDBC catalog through an independent connection, preventing
the H2 identifier-case regression that single-session tests cannot detect. The CLI has additionally
been exercised through a real load/replay in separate processes.

`test_integrity.py` covers reassignment with historical attempts, corrected attempt numbering,
old/new date and driver groups, restricted parent deletion, explicit child deletion, empty groups,
breaking schema quarantine, invalid-newer/valid-older ordering, retry after a newer batch and explicit
Phase 1 adoption. Protocol/Bronze/state tests cover deterministic input, missing files, checksums,
interrupted immutable ingestion, schema policies, reports, logging and upload gating.

Tests use unique local namespaces and temporary data directories. They do not use AWS credentials.
The [Phase 2 verification report](phase2-verification.md) preserves its historical complete run; partial
focused runs are debugging evidence, not a substitute for the final combined suite.

## Phase 3 operations, SQL and recovery

The same full-suite command now includes `tests/operations/` alongside every Phase 1/2 test:

```bash
uv run pytest tests/operations/test_control.py tests/operations/test_glue_adapter.py \
  tests/operations/test_operations_infrastructure.py -q
ICEBERG_SPARK_JAR=artifacts/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
  uv run pytest tests/operations -q
```

Control tests check execution ordering, request identity, replay/resume, bounded retry classification,
final-record commit loss, competing local writers, counts and failure diagnostics even when state
storage fails. Adapter tests mock Glue libraries; they assert parameter forwarding and that failed
execution cannot call `Job.commit()`. They are not managed Glue integration tests.

Real local Iceberg operations tests load all six sources, verify the 16-table CDC publication, inject
a transient verification failure after writes, retry its frozen plan, reject invalid relationships,
recover through a corrected source batch and compare all nine Gold tables with full recomputation.
They also execute and verify the snapshot path, detect missing/duplicate/changed/schema-incompatible
stored rows, and prove an event-only CDC batch creates no Gold plan or new Gold snapshot.

SQL tests execute the hand-calculated boundary cohorts described in [analytics](analytics.md).
Infrastructure tests check manual-only orchestration, reuse of the bounded existing job, role scope,
opt-in monitoring and disabled alarm actions. `cfn-lint` validates all three templates locally;
no AWS `ValidateTemplate`, IAM simulation, deployment or Step Functions execution is performed.

The [technical audit](technical-audit.md) records current combined results, coverage, targeted
regressions and security tools. The [Phase 3 verification report](phase3-verification.md) retains its
historical results. [Performance evidence](performance.md) describes the narrow Gold-planning benchmark and
its limits. Focused results are supplementary; the combined regression and hosted CI remain the
release checks. Run Spark tests sequentially in one process; concurrent JVMs can distort timing and
exhaust a small machine. Only the explicitly opted-in bucket readiness test is live AWS.
