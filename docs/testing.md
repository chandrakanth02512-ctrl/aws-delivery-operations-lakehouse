# Local and cloud verification

Required: Python 3.13, Java 17 (Glue's major version; local Java 21 is also supported by Spark 4.1),
and `uv`. The lockfile pins the full Python dependency graph. Runtime code uses only the Python
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
```

`fetch_iceberg.py` downloads pinned Iceberg 1.11.0 (Spark 4.1 / Scala 2.13) and H2 2.4.240 JARs from
Maven Central and checks committed SHA-256 digests. Without `ICEBERG_SPARK_JAR`, three local Iceberg
tests are skipped; this must not be represented as full Iceberg validation. Default tests skip live
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
