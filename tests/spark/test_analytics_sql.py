from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
QUERIES = sorted((ROOT / "sql/athena").glob("*.sql"))


@pytest.mark.spark
@pytest.mark.parametrize("path", QUERIES, ids=lambda path: path.stem)
def test_business_queries_against_local_gold(spark, local_run, path):
    """Execute the portable SQL on Spark; this does not claim Athena execution."""
    _, _, _, result = local_run
    for name, location in result["tables"].items():
        if name.startswith("gold."):
            spark.read.parquet(location).createOrReplaceTempView(name.split(".")[1])
    sql = "\n".join(line for line in path.read_text().splitlines() if not line.startswith("--"))
    for statement in sql.split(";"):
        if statement.strip() and "$snapshots" not in statement:
            rows = spark.sql(statement.replace("delivery_ops_dev_gold.", "")).collect()
            assert rows, f"No business results from {path.name}"
            if path.name == "06_verification.sql":
                values = rows[0].asDict()
                for column, expected in {
                    "shipments": 120,
                    "distinct_shipments": 120,
                    "snapshot_batches": 1,
                    "orphan_attempts": 0,
                    "evidence_gaps": 0,
                }.items():
                    if column in values:
                        assert values[column] == expected
