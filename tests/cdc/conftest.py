import uuid

import pytest

from delivery_lakehouse.cdc.generator import make_change, write_batch
from delivery_lakehouse.cdc.pipeline import run_cdc
from delivery_lakehouse.config import Settings
from delivery_lakehouse.generator import generate
from delivery_lakehouse.ingestion.storage import LocalStore


@pytest.fixture
def cdc_context(spark, tmp_path):
    settings = Settings(database_prefix=f"cdc_{uuid.uuid4().hex[:8]}")
    for layer in ("silver", "gold"):
        spark.sql(f"CREATE NAMESPACE lakehouse.{settings.database_prefix}_{layer}")
    return LocalStore(tmp_path), settings


@pytest.fixture
def seeded_cdc(spark, cdc_context):
    store, settings = cdc_context
    data = generate(defects=False)
    shipment = data["shipments"][0]
    selected = {
        "delivery_stations": [
            row for row in data["delivery_stations"] if row["station_id"] == shipment["station_id"]
        ],
        "drivers": [
            row for row in data["drivers"] if row["driver_id"] == shipment["assigned_driver_id"]
        ],
        "customers": [
            row for row in data["customers"] if row["customer_id"] == shipment["customer_id"]
        ],
        "shipments": [shipment],
        "delivery_attempts": [
            row
            for row in data["delivery_attempts"]
            if row["shipment_id"] == shipment["shipment_id"]
        ],
        "delivery_events": [
            row for row in data["delivery_events"] if row["shipment_id"] == shipment["shipment_id"]
        ],
    }
    changes = [
        make_change(dataset, row, "INSERT", 1, "2026-02-06T09:00:00Z")
        for dataset, rows in selected.items()
        for row in rows
    ]
    batch = write_batch(store, "test-baseline", "2026-02-06T12:00:00Z", changes)
    initial = run_cdc(spark, store, settings, batch["batch_id"], catalog="lakehouse")
    return store, settings, selected, initial
