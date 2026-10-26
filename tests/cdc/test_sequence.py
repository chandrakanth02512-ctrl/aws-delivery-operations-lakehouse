import os
import uuid

import pytest
from pyspark.sql import functions as F

from delivery_lakehouse.cdc.contracts import source_schema
from delivery_lakehouse.cdc.generator import (
    chronological_changes,
    make_change,
    write_batch,
    write_sequence,
)
from delivery_lakehouse.cdc.pipeline import run_cdc
from delivery_lakehouse.cdc.state import CHECKPOINT_KEY, PUBLICATION_KEY, load_json
from delivery_lakehouse.config import Settings
from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.generator import generate
from delivery_lakehouse.ingestion.storage import LocalStore
from delivery_lakehouse.transforms.gold import build_gold

pytestmark = [
    pytest.mark.spark,
    pytest.mark.iceberg,
    pytest.mark.skipif(
        not os.environ.get("ICEBERG_SPARK_JAR"), reason="Local Iceberg JAR required"
    ),
]


@pytest.fixture(scope="module")
def sequence(spark, tmp_path_factory):
    settings = Settings(database_prefix=f"cdc_{uuid.uuid4().hex[:8]}")
    for layer in ("silver", "gold"):
        spark.sql(f"CREATE NAMESPACE lakehouse.{settings.database_prefix}_{layer}")
    store = LocalStore(tmp_path_factory.mktemp("cdc-sequence"))
    batches = write_sequence(store)
    results, observations = [], []
    fact_table = f"lakehouse.{settings.database_prefix}_gold.fact_shipments"
    for batch in batches:
        result = run_cdc(spark, store, settings, batch["batch_id"], catalog="lakehouse")
        assert result["status"] == "SUCCEEDED"
        results.append(result)
        row = spark.table(fact_table).filter("shipment_id = 'SHCDC0001'").first()
        observations.append(row.asDict() if row else None)
    source = generate(defects=False)
    late_changes = chronological_changes()[-1][2]
    duplicate_changes = [
        late_changes[0],
        late_changes[1],
        make_change("shipments", source["shipments"][19], "INSERT", 1, "2026-02-06T09:00:00Z"),
    ]
    schema = source_schema("shipments")
    schema["mobile_sync_note"] = {"type": "string", "nullable": True}
    duplicate_batch = write_batch(
        store,
        "redelivered-and-stale",
        "2026-02-12T12:00:00Z",
        duplicate_changes,
        schemas={"shipments": schema, "delivery_attempts": source_schema("delivery_attempts")},
    )
    snapshot_before = spark.sql(f"SELECT count(*) FROM {fact_table}.snapshots").first()[0]
    duplicates = run_cdc(spark, store, settings, duplicate_batch["batch_id"], catalog="lakehouse")
    snapshot_after = spark.sql(f"SELECT count(*) FROM {fact_table}.snapshots").first()[0]
    driver = {
        **source["drivers"][0],
        "driver_name": "Synthetic Updated Driver",
        "updated_at": "2026-02-12T10:00:00Z",
    }
    extra_event = {
        "event_id": "EV-LATE-EXTRA",
        "shipment_id": "SHCDC0001",
        "attempt_id": None,
        "station_id": "ST000",
        "event_type": "CREATED",
        "event_at": "2026-02-07T01:00:00Z",
        "updated_at": "2026-02-07T01:00:00Z",
    }
    dimension_batch = write_batch(
        store,
        "dimension-and-late-event",
        "2026-02-12T12:00:00Z",
        [
            make_change("drivers", driver, "UPDATE", 2, "2026-02-12T10:00:00Z"),
            make_change("delivery_events", extra_event, "INSERT", 1, "2026-02-07T01:00:00Z"),
        ],
    )
    dimension = run_cdc(spark, store, settings, dimension_batch["batch_id"], catalog="lakehouse")
    snapshot_final = spark.sql(f"SELECT count(*) FROM {fact_table}.snapshots").first()[0]
    return {
        "store": store,
        "settings": settings,
        "batches": batches,
        "results": results,
        "observations": observations,
        "duplicates": duplicates,
        "dimension": dimension,
        "snapshots": [snapshot_before, snapshot_after, snapshot_final],
    }


def table(sequence, layer, name):
    return f"lakehouse.{sequence['settings'].database_prefix}_{layer}.{name}"


def test_late_confirmation_updates_historical_fact_and_retains_watermark(spark, sequence):
    observations = sequence["observations"]
    assert observations[1]["status"] == "IN_TRANSIT"
    assert observations[1]["driver_id"] == "DR0000"
    assert observations[2]["driver_id"] == "DR0003"
    late = observations[3]
    assert late["status"] == "DELIVERED"
    assert str(late["promised_date"]) == "2026-02-09"
    assert float(late["delivery_delay_minutes"]) == pytest.approx(60)
    assert late["attempt_count"] == 2
    assert late["first_attempt_success"] == 0
    assert sequence["results"][3]["watermark_after"] == sequence["results"][2]["watermark_after"]
    assert spark.table(table(sequence, "gold", "fact_shipments")).count() == 121


def test_historical_daily_and_driver_kpis(spark, sequence):
    daily = (
        spark.table(table(sequence, "gold", "kpi_daily"))
        .filter("promised_date = DATE '2026-02-09'")
        .first()
    )
    assert daily.total_shipments == 2
    assert daily.delivered_shipments == 1
    assert daily.on_time_delivery_pct == 0
    assert float(daily.avg_delivery_delay_minutes) == 60
    assert daily.first_attempt_success_pct == 0
    assert daily.failed_delivery_pct == 0
    assert daily.total_attempts == 2
    drivers = spark.table(table(sequence, "gold", "kpi_driver_daily")).filter(
        "promised_date = DATE '2026-02-09'"
    )
    by_driver = {row.driver_id: row for row in drivers.collect()}
    assert by_driver["DR0000"].delivered_shipments == 0
    assert by_driver["DR0000"].on_time_delivery_pct is None
    assert by_driver["DR0003"].delivered_shipments == 1


def test_incremental_gold_matches_full_recomputation(spark, sequence):
    silver = {name: spark.table(table(sequence, "silver", name)) for name in CONTRACTS}
    for name, expected in build_gold(spark, silver).items():
        actual = spark.table(table(sequence, "gold", name))
        expected = expected.select(*actual.columns)
        assert expected.exceptAll(actual).count() == 0, name
        assert actual.exceptAll(expected).count() == 0, name


def test_duplicates_stale_changes_and_tombstones_do_not_rewrite_facts(spark, sequence):
    counts = sequence["duplicates"]["counts"]
    assert sum(value["accepted_changes"] for value in counts.values()) == 0
    assert sum(value["rows_ignored"] for value in counts.values()) == 3
    assert len(set(sequence["snapshots"])) == 1
    state = spark.table(table(sequence, "silver", "cdc_record_state"))
    tombstone = state.filter("dataset = 'shipments' AND record_key = 'SH00000019'").first()
    assert tombstone.is_deleted
    assert tombstone.source_version == 2
    assert (
        spark.table(table(sequence, "silver", "shipments"))
        .filter("shipment_id = 'SH00000019'")
        .count()
        == 0
    )


def test_dimension_update_and_late_event_are_incremental(spark, sequence):
    driver = (
        spark.table(table(sequence, "gold", "dim_driver")).filter("driver_id = 'DR0000'").first()
    )
    assert driver.driver_name == "Synthetic Updated Driver"
    assert sequence["dimension"]["counts"]["drivers"]["rows_updated"] == 1
    assert sequence["dimension"]["counts"]["delivery_events"]["rows_inserted"] == 1
    assert not any(step.startswith("gold.kpi") for step in sequence["dimension"]["completed_steps"])
    unaffected = (
        spark.table(table(sequence, "gold", "fact_shipments"))
        .filter("shipment_id = 'SH00000001'")
        .first()
    )
    assert unaffected._batch_id == sequence["batches"][0]["batch_id"]


def test_compatible_addition_is_retained_raw_without_schema_migration(spark, sequence):
    shipments = spark.table(table(sequence, "silver", "shipments"))
    assert "mobile_sync_note" not in shipments.columns
    raw = shipments.filter("shipment_id = 'SHCDC0001'").first()._raw_json
    assert "Confirmation arrived Wednesday" in raw
    changes = sequence["results"][3]["schema_drift"]["shipments"]
    assert changes[0]["classification"] == "compatible"
    assert changes[0]["policy"] == "retain_raw_only"


def test_batch_replay_never_rewinds_latest_publication(spark, sequence):
    store, settings = sequence["store"], sequence["settings"]
    before = store.read(PUBLICATION_KEY), store.read(CHECKPOINT_KEY)
    replay = run_cdc(
        spark, store, settings, sequence["batches"][0]["batch_id"], catalog="lakehouse"
    )
    assert replay == sequence["results"][0]
    assert before == (store.read(PUBLICATION_KEY), store.read(CHECKPOINT_KEY))
    assert load_json(store, CHECKPOINT_KEY)["active_batch_id"] is None


def test_all_changes_reconcile_and_tables_remain_v2(spark, sequence):
    for result in [*sequence["results"], sequence["duplicates"], sequence["dimension"]]:
        assert [item["status"] for item in result["history"]] == ["PENDING", "RUNNING", "SUCCEEDED"]
        assert all(value["reconciled"] for value in result["counts"].values())
    for name, contract in CONTRACTS.items():
        frame = spark.table(table(sequence, "silver", name))
        assert frame.count() == frame.select(contract.primary_key).distinct().count()
        properties = {
            row.key: row.value
            for row in spark.sql(f"SHOW TBLPROPERTIES {table(sequence, 'silver', name)}").collect()
        }
        assert properties["format-version"] == "2"
    assert (
        spark.table(table(sequence, "gold", "fact_shipments"))
        .filter(F.col("shipment_id").isNull())
        .count()
        == 0
    )
