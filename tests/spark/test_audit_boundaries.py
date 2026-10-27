import json
import os
import time

import pytest

from delivery_lakehouse.generator import generate
from delivery_lakehouse.tables import write_iceberg
from delivery_lakehouse.transforms.silver import BRONZE_SCHEMA, normalize

pytestmark = pytest.mark.spark


@pytest.mark.iceberg
@pytest.mark.skipif(not os.environ.get("ICEBERG_SPARK_JAR"), reason="Local Iceberg JAR required")
@pytest.mark.parametrize("mismatch", ["location", "schema"])
def test_snapshot_refuses_mismatched_existing_target_without_overwriting(spark, tmp_path, mismatch):
    import uuid

    database = f"audit_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE NAMESPACE lakehouse.{database}")
    location = str(tmp_path / "owned")
    original = spark.createDataFrame([(1, "preserve")], "id int, value string")
    table = write_iceberg(spark, original, "lakehouse", database, "items", location)
    replacement = spark.createDataFrame([(2, "replacement")], "id int, value string")
    if mismatch == "location":
        requested = str(tmp_path / "different-root")
        reason = "TABLE_LOCATION_MISMATCH"
    else:
        requested = location
        # Spark/Iceberg otherwise allows this implicit widening during a snapshot overwrite.
        replacement = replacement.selectExpr("cast(id as smallint) as id", "value")
        reason = "EXPLICIT_SCHEMA_MIGRATION_REQUIRED"
    snapshots = spark.sql(f"SELECT count(*) FROM {table}.snapshots").first()[0]
    with pytest.raises(ValueError, match=reason):
        write_iceberg(spark, replacement, "lakehouse", database, "items", requested)
    assert spark.table(table).collect() == original.collect()
    assert spark.sql(f"SELECT count(*) FROM {table}.snapshots").first()[0] == snapshots


def test_nonstandard_json_constant_is_quarantined_even_in_a_string_field(spark):
    row = generate(defects=False)["drivers"][0]
    raw = json.dumps({**row, "driver_name": float("nan")})
    frame = spark.createDataFrame(
        [(raw, "fixture", 1, "a" * 64, "2026-02-06T00:00:00Z", "hash", "record")],
        BRONZE_SCHEMA,
    )
    checked = normalize(frame, "drivers").first()
    assert "MALFORMED_JSON_OBJECT" in checked._errors
    assert checked._raw_json == raw


@pytest.mark.parametrize("host_timezone", ["Asia/Kolkata", "America/Los_Angeles"])
def test_cdc_watermark_is_utc_independent_of_python_host_timezone(
    spark, tmp_path, monkeypatch, host_timezone
):
    from datetime import datetime

    from delivery_lakehouse.cdc import pipeline
    from delivery_lakehouse.cdc.frames import RECORD_STATE_SCHEMA
    from delivery_lakehouse.cdc.state import read_plan
    from delivery_lakehouse.config import Settings
    from delivery_lakehouse.ingestion.storage import LocalStore

    instant = "2026-02-06T09:00:00.123456+00:00"
    delta = spark.createDataFrame(
        [("drivers", "DR1", 1, datetime.fromisoformat(instant), "change", False, "a" * 64)],
        RECORD_STATE_SCHEMA,
    )
    store = LocalStore(tmp_path)
    state = {
        "batch_id": "a" * 64,
        "attempt": 1,
        "manifest_sha256": "b" * 64,
        "counts": {},
        "schema_drift": {},
    }
    plan = {"winners": {}, "state_delta": delta}
    monkeypatch.setattr(pipeline, "plan_gold", lambda *args: {})
    monkeypatch.setattr(spark.catalog, "tableExists", lambda name: True)
    previous_timezone = os.environ.get("TZ")
    try:
        os.environ["TZ"] = host_timezone
        time.tzset()
        key = pipeline.stage_plan(spark, store, Settings(), "lakehouse", state, plan, {}, {}, {})
        assert read_plan(store, key)["watermark_after"] == instant
    finally:
        if previous_timezone is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous_timezone
        time.tzset()
