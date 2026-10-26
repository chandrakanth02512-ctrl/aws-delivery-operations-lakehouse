import os

import pytest

from delivery_lakehouse.cdc.contracts import parse_json
from delivery_lakehouse.cdc.generator import make_change, write_batch
from delivery_lakehouse.cdc.pipeline import run_cdc
from delivery_lakehouse.cdc.state import (
    CHECKPOINT_KEY,
    PUBLICATION_KEY,
    load_json,
    read_plan,
    state_key,
)
from delivery_lakehouse.transforms.gold import build_gold

pytestmark = [
    pytest.mark.spark,
    pytest.mark.iceberg,
    pytest.mark.skipif(
        not os.environ.get("ICEBERG_SPARK_JAR"), reason="Local Iceberg JAR required"
    ),
]


@pytest.mark.parametrize(
    "failure_point", ["silver.shipments", "gold.fact_shipments", "gold.kpi_daily", "COMMIT_RECORD"]
)
def test_partial_publication_recovers_from_durable_plan(
    spark, seeded_cdc, monkeypatch, failure_point
):
    store, settings, data, initial = seeded_cdc
    shipment = {**data["shipments"][0], "weight_kg": 11.25, "updated_at": "2026-02-07T09:00:00Z"}
    batch = write_batch(
        store,
        "update-weight",
        "2026-02-07T12:00:00Z",
        [make_change("shipments", shipment, "UPDATE", 2, "2026-02-07T09:00:00Z")],
    )
    checkpoint_before = load_json(store, CHECKPOINT_KEY)
    failed = False
    original_write = store.write

    def fail_once(step):
        nonlocal failed
        if step == failure_point and not failed:
            failed = True
            raise OSError(f"controlled failure after committed MERGE: {step}")

    def fail_commit_record(key, content, **options):
        nonlocal failed
        if (
            failure_point == "COMMIT_RECORD"
            and key == state_key(batch["batch_id"])
            and parse_json(content)["status"] == "SUCCEEDED"
            and not failed
        ):
            failed = True
            raise OSError("controlled commit record failure")
        return original_write(key, content, **options)

    monkeypatch.setattr(store, "write", fail_commit_record)
    with pytest.raises(OSError, match="controlled"):
        run_cdc(
            spark, store, settings, batch["batch_id"], catalog="lakehouse", failure_hook=fail_once
        )
    state = load_json(store, state_key(batch["batch_id"]))
    assert state["status"] == "FAILED"
    assert state["error_category"] == "PUBLICATION"
    assert load_json(store, PUBLICATION_KEY)["status"] == "FAILED"
    assert (
        load_json(store, CHECKPOINT_KEY)["source_event_watermark"]
        == checkpoint_before["source_event_watermark"]
    )
    plan_before = store.read(state["plan_key"])
    plan = read_plan(store, state["plan_key"])
    assert plan["counts"]["shipments"]["rows_updated"] == 1
    # A different batch cannot consume the partially published state.
    with pytest.raises(RuntimeError, match="RECOVER_ACTIVE_BATCH_FIRST"):
        run_cdc(spark, store, settings, "b" * 64, catalog="lakehouse")
    recovered = run_cdc(spark, store, settings, batch["batch_id"], catalog="lakehouse")
    assert recovered["status"] == "SUCCEEDED"
    assert recovered["attempt"] == 2
    assert recovered["plan_key"] == state["plan_key"]
    assert store.read(state["plan_key"]) == plan_before
    assert recovered["counts"] == plan["counts"]
    assert [entry["status"] for entry in recovered["history"]] == [
        "PENDING",
        "RUNNING",
        "FAILED",
        "RUNNING",
        "SUCCEEDED",
    ]
    table = f"lakehouse.{settings.database_prefix}_gold.fact_shipments"
    assert spark.table(table).count() == 1
    assert spark.table(table).first().weight_kg == 11.25
    assert load_json(store, CHECKPOINT_KEY)["active_batch_id"] is None
    assert run_cdc(spark, store, settings, batch["batch_id"], catalog="lakehouse") == recovered
    assert initial["batch_id"] != recovered["batch_id"]
    # Every Gold table equals a clean recomputation from the recovered Silver state.
    silver = {
        name: spark.table(f"lakehouse.{settings.database_prefix}_silver.{name}") for name in data
    }
    expected = build_gold(spark, silver)
    for name in ("fact_shipments", "kpi_daily", "kpi_station_daily", "kpi_driver_daily"):
        actual = spark.table(f"lakehouse.{settings.database_prefix}_gold.{name}")
        assert expected[name].select(*actual.columns).exceptAll(actual).count() == 0
        assert actual.exceptAll(expected[name].select(*actual.columns)).count() == 0
