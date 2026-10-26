import json
import os
from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from delivery_lakehouse.cdc.contracts import source_schema
from delivery_lakehouse.cdc.generator import make_change, write_batch
from delivery_lakehouse.cdc.pipeline import run_cdc
from delivery_lakehouse.cdc.state import CHECKPOINT_KEY, load_json, state_key
from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.generator import write_landing
from delivery_lakehouse.pipeline import run_pipeline
from delivery_lakehouse.transforms.gold import build_gold

pytestmark = [
    pytest.mark.spark,
    pytest.mark.iceberg,
    pytest.mark.skipif(
        not os.environ.get("ICEBERG_SPARK_JAR"), reason="Local Iceberg JAR required"
    ),
]


def table(settings, layer, name):
    return f"lakehouse.{settings.database_prefix}_{layer}.{name}"


def test_reassignment_attempt_correction_date_move_and_restricted_delete(spark, seeded_cdc):
    store, settings, data, _ = seeded_cdc
    old_driver = data["drivers"][0]["driver_id"]
    driver = {**data["drivers"][0], "driver_id": "DR-REASSIGNED"}
    shipment = {**data["shipments"][0], "assigned_driver_id": driver["driver_id"]}
    shipment["promised_at"] = (
        datetime.fromisoformat(shipment["promised_at"]) + timedelta(days=1)
    ).isoformat()
    attempt = {**data["delivery_attempts"][0], "attempt_number": 2}
    changes = [
        make_change("drivers", driver, "INSERT", 1, "2026-02-07T09:00:00Z"),
        make_change("shipments", shipment, "UPDATE", 2, "2026-02-07T09:00:00Z"),
        make_change("delivery_attempts", attempt, "UPDATE", 2, "2026-02-07T09:00:00Z"),
    ]
    batch = write_batch(store, "correct-assignment-date-attempt", "2026-02-07T12:00:00Z", changes)
    run_cdc(spark, store, settings, batch["batch_id"], catalog="lakehouse")
    fact = spark.table(table(settings, "gold", "fact_shipments")).first()
    assert fact.driver_id == driver["driver_id"]
    assert fact.has_first_attempt == 0
    assert fact.attempt_count == 1
    assert (
        spark.table(table(settings, "gold", "fact_delivery_attempts")).first().driver_id
        == old_driver
    )
    driver_kpi = spark.table(table(settings, "gold", "kpi_driver_daily"))
    assert driver_kpi.count() == 1
    assert driver_kpi.first().driver_id == driver["driver_id"]
    assert driver_kpi.first().first_attempt_success_pct is None
    dates = spark.table(table(settings, "gold", "dim_date")).collect()
    assert len(dates) == 1
    assert str(dates[0].date_key) == shipment["promised_at"][:10]
    silver = {name: spark.table(table(settings, "silver", name)) for name in CONTRACTS}
    for name, expected in build_gold(spark, silver).items():
        actual = spark.table(table(settings, "gold", name))
        assert expected.select(*actual.columns).exceptAll(actual).count() == 0, name
        assert actual.exceptAll(expected.select(*actual.columns)).count() == 0, name

    restricted = write_batch(
        store,
        "restricted-delete",
        "2026-02-08T12:00:00Z",
        [
            make_change(
                "shipments",
                None,
                "DELETE",
                3,
                "2026-02-08T09:00:00Z",
                record_id=shipment["shipment_id"],
            )
        ],
    )
    with pytest.raises(ValueError, match="DEPENDENT_RECORD_INTEGRITY_FAILED"):
        run_cdc(spark, store, settings, restricted["batch_id"], catalog="lakehouse")
    state = load_json(store, state_key(restricted["batch_id"]))
    assert state["status"] == "FAILED"
    assert state["integrity_errors"]["delivery_attempts"] == 1
    assert not state.get("plan_key")
    assert spark.table(table(settings, "gold", "fact_shipments")).count() == 1
    ledger = spark.table(table(settings, "silver", "cdc_record_state"))
    assert ledger.filter("dataset = 'shipments'").first().source_version == 2

    # Explicitly remove every dependent row in the same batch; no implicit cascade.
    data["drivers"].append(driver)
    deletes = [
        make_change(
            name,
            None,
            "DELETE",
            3,
            "2026-02-08T09:00:00Z",
            record_id=row[CONTRACTS[name].primary_key],
        )
        for name, rows in data.items()
        for row in rows
    ]
    cleanup = write_batch(store, "explicit-dependent-deletes", "2026-02-08T12:00:00Z", deletes)
    result = run_cdc(spark, store, settings, cleanup["batch_id"], catalog="lakehouse")
    assert result["status"] == "SUCCEEDED"
    for name in CONTRACTS:
        assert spark.table(table(settings, "silver", name)).count() == 0
    for name in build_gold(spark, silver):
        assert spark.table(table(settings, "gold", name)).count() == 0, name
    ledger = spark.table(table(settings, "silver", "cdc_record_state"))
    assert ledger.count() == sum(len(rows) for rows in data.values())
    assert ledger.filter("NOT is_deleted").count() == 0


def test_breaking_schema_quarantines_and_invalid_newer_change_does_not_mask_valid(
    spark, seeded_cdc
):
    store, settings, data, _ = seeded_cdc
    shipment = data["shipments"][0]
    renamed = {**shipment, "delivery_status": shipment["status"]}
    del renamed["status"]
    schema = source_schema("shipments")
    schema["delivery_status"] = schema.pop("status")
    broken = write_batch(
        store,
        "breaking-rename",
        "2026-02-07T12:00:00Z",
        [make_change("shipments", renamed, "UPDATE", 99, "2026-02-07T09:00:00Z")],
        schemas={"shipments": schema},
    )
    with pytest.raises(ValueError, match="REJECTION_THRESHOLD"):
        run_cdc(spark, store, settings, broken["batch_id"], catalog="lakehouse")
    failure = load_json(store, state_key(broken["batch_id"]))
    assert failure["status"] == "FAILED"
    assert failure["counts"]["shipments"]["rows_rejected"] == 1
    rejected = spark.read.parquet(store.uri(f"{failure['quality_prefix']}/rejected/shipments"))
    assert "BREAKING_SCHEMA_DRIFT" in rejected.first()._reasons
    assert "POSSIBLE_RENAME" in {d["change"] for d in failure["schema_drift"]["shipments"]}
    ledger = spark.table(table(settings, "silver", "cdc_record_state"))
    assert ledger.filter("dataset = 'shipments'").first().source_version == 1

    good = {**shipment, "weight_kg": 12.5}
    invalid = {**shipment, "status": "IMPOSSIBLE"}
    correction = write_batch(
        store,
        "valid-lower-version-and-invalid-newer",
        "2026-02-08T12:00:00Z",
        [
            make_change("shipments", good, "UPDATE", 2, "2026-02-08T09:00:00Z"),
            make_change("shipments", invalid, "UPDATE", 99, "2026-02-08T10:00:00Z"),
        ],
    )
    result = run_cdc(
        spark,
        store,
        replace(settings, max_reject_fraction=0.5),
        correction["batch_id"],
        catalog="lakehouse",
    )
    counts = result["counts"]["shipments"]
    assert counts["rows_updated"] == counts["rows_rejected"] == 1
    assert counts["reconciled"]
    assert spark.table(table(settings, "gold", "fact_shipments")).first().weight_kg == 12.5
    ledger = spark.table(table(settings, "silver", "cdc_record_state"))
    assert ledger.filter("dataset = 'shipments'").first().source_version == 2

    # A formerly invalid batch must not rewind a newer committed checkpoint on another failure.
    checkpoint = load_json(store, CHECKPOINT_KEY)
    with pytest.raises(ValueError, match="REJECTION_THRESHOLD"):
        run_cdc(spark, store, settings, broken["batch_id"], catalog="lakehouse")
    after_retry = load_json(store, CHECKPOINT_KEY)
    assert after_retry["last_succeeded_batch"] == checkpoint["last_succeeded_batch"]
    assert after_retry["source_event_watermark"] == checkpoint["source_event_watermark"]


def test_existing_phase1_snapshot_requires_explicit_adoption(spark, cdc_context):
    store, settings = cdc_context
    snapshot = write_landing(store.root)
    run_pipeline(spark, store, settings, snapshot["batch_id"], catalog="lakehouse")
    driver = spark.table(table(settings, "silver", "drivers")).first().asDict()
    # Read its source after-image to retain the JSON wire types.
    row = json.loads(driver["_raw_json"])
    row["driver_name"] = "Synthetic Adopted Driver"
    batch = write_batch(
        store,
        "adopt-phase1",
        "2026-02-07T12:00:00Z",
        [make_change("drivers", row, "UPDATE", 1, "2026-02-07T09:00:00Z")],
    )
    with pytest.raises(ValueError, match="EXPLICIT_PHASE1_BASELINE_REQUIRED"):
        run_cdc(spark, store, settings, batch["batch_id"], catalog="lakehouse")
    result = run_cdc(
        spark, store, settings, batch["batch_id"], catalog="lakehouse", adopt_snapshot=True
    )
    assert result["status"] == "SUCCEEDED"
    assert result["attempt"] == 2
    assert spark.table(table(settings, "gold", "fact_shipments")).count() == 120
    ledger = spark.table(table(settings, "silver", "cdc_record_state"))
    assert ledger.filter("source_version = 1").count() == 1
    assert ledger.filter("source_version = 0").count() == ledger.count() - 1
    with pytest.raises(ValueError, match="CDC"):
        run_pipeline(spark, store, settings, snapshot["batch_id"], catalog="lakehouse")
