import json
import os

import pytest

from delivery_lakehouse.config import Settings
from delivery_lakehouse.generator import write_landing
from delivery_lakehouse.ingestion.storage import LocalStore
from delivery_lakehouse.pipeline import run_pipeline
from delivery_lakehouse.tables import write_iceberg

pytestmark = [
    pytest.mark.spark,
    pytest.mark.iceberg,
    pytest.mark.skipif(
        not os.environ.get("ICEBERG_SPARK_JAR"),
        reason="Set ICEBERG_SPARK_JAR to enable real local Iceberg tests",
    ),
]


def test_iceberg_atomic_replacement_empty_write_and_v2_metadata(spark, tmp_path):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.writer_test")
    location = str(tmp_path / "items")
    frame = spark.createDataFrame([(1, "a"), (2, "b")], "id int, value string")
    table = write_iceberg(spark, frame, "lakehouse", "writer_test", "items", location)
    write_iceberg(spark, frame, "lakehouse", "writer_test", "items", location)
    assert spark.table(table).count() == 2
    assert spark.sql(f"SELECT count(*) FROM {table}.snapshots").first()[0] == 2
    for metadata in (tmp_path / "items" / "metadata").glob("*.metadata.json"):
        assert json.loads(metadata.read_text())["format-version"] == 2
    write_iceberg(spark, frame.limit(0), "lakehouse", "writer_test", "items", location)
    assert spark.table(table).count() == 0


def test_refuse_existing_v3_table(spark, tmp_path):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.writer_test")
    spark.sql("""CREATE TABLE lakehouse.writer_test.incompatible (id INT)
                  USING iceberg TBLPROPERTIES ('format-version'='3')""")
    with pytest.raises(ValueError, match="non-v2"):
        write_iceberg(
            spark,
            spark.createDataFrame([(1,)], "id int"),
            "lakehouse",
            "writer_test",
            "incompatible",
            str(tmp_path),
        )


def test_full_snapshot_publishes_15_v2_tables_and_replays(spark, tmp_path):
    settings = Settings(environment="test", database_prefix="delivery_ops_test")
    for layer in ("silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.delivery_ops_test_{layer}")
    store = LocalStore(tmp_path)
    batch = write_landing(tmp_path)["batch_id"]
    first = run_pipeline(spark, store, settings, batch, catalog="lakehouse")
    assert first["status"] == "SUCCEEDED"
    assert len(first["tables"]) == 15
    for table in first["tables"].values():
        properties = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
        assert properties["format-version"] == "2"
        assert spark.table(table).count() > 0
    second = run_pipeline(spark, store, settings, batch, catalog="lakehouse")
    assert first == second
    assert spark.table(first["tables"]["gold.fact_shipments"]).count() == 120
