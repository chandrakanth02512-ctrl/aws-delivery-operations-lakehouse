import os
import uuid

import pytest

from delivery_lakehouse.cdc.generator import make_change, write_batch
from delivery_lakehouse.cdc.state import CHECKPOINT_KEY, PUBLICATION_KEY, load_json, state_key
from delivery_lakehouse.config import Settings
from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.generator import generate, write_landing
from delivery_lakehouse.ingestion.storage import LocalStore
from delivery_lakehouse.operations import runner
from delivery_lakehouse.operations.verification import compare_rows
from delivery_lakehouse.quality.report import QualityGateError
from delivery_lakehouse.transforms.gold import build_gold

pytestmark = [
    pytest.mark.spark,
    pytest.mark.iceberg,
    pytest.mark.skipif(
        not os.environ.get("ICEBERG_SPARK_JAR"), reason="Local Iceberg JAR required"
    ),
]


def context(spark, root):
    settings = Settings(database_prefix=f"ops_{uuid.uuid4().hex[:8]}")
    for layer in ("silver", "gold"):
        spark.sql(f"CREATE NAMESPACE lakehouse.{settings.database_prefix}_{layer}")
    return LocalStore(root), settings


@pytest.fixture(scope="module")
def operations(spark, tmp_path_factory):
    store, settings = context(spark, tmp_path_factory.mktemp("operations"))
    data = generate(defects=False)
    changes = [
        make_change(name, row, "INSERT", 1, "2026-02-06T09:00:00Z")
        for name, rows in data.items()
        for row in rows
    ]
    batch = write_batch(store, "operations-bootstrap", "2026-02-06T12:00:00Z", changes)
    result = runner.run_execution(
        spark, store, settings, batch["batch_id"], catalog="lakehouse", execution_id="bootstrap"
    )
    return store, settings, data, batch, result


def test_cdc_execution_verifies_all_tables_before_success_and_replays(spark, operations):
    store, settings, data, batch, result = operations
    assert result["status"] == "SUCCEEDED"
    assert result["verification"]["tables_verified"] == 16
    assert result["verification"]["scope"] == "changed_keys"
    assert result["metrics"]["input_rows"] == sum(len(rows) for rows in data.values())
    assert result["metrics"]["output_rows"] == result["metrics"]["input_rows"]
    complete = [event["stage"] for event in result["events"] if event["status"] == "SUCCEEDED"]
    assert complete == ["SOURCE", "BRONZE", "SILVER", "GOLD", "PUBLISH", "VERIFY", "COMMIT"]
    checkpoint = store.read(CHECKPOINT_KEY)
    publication = store.read(PUBLICATION_KEY)
    replay = runner.run_execution(
        spark, store, settings, batch["batch_id"], catalog="lakehouse", execution_id="bootstrap"
    )
    assert replay == result
    assert store.read(CHECKPOINT_KEY) == checkpoint
    assert store.read(PUBLICATION_KEY) == publication


def test_failure_after_all_merges_retries_verification_and_preserves_frozen_plan(
    spark, operations, monkeypatch
):
    store, settings, data, _, _ = operations
    shipment = {**data["shipments"][0], "weight_kg": 9.75, "updated_at": "2026-02-07T09:00:00Z"}
    batch = write_batch(
        store,
        "operations-verification-recovery",
        "2026-02-07T12:00:00Z",
        [make_change("shipments", shipment, "UPDATE", 2, "2026-02-07T09:00:00Z")],
    )
    original = runner.verify_publication
    plans = []

    def interrupted(*args):
        report = original(*args)
        plans.append(args[2]["plan"]["sha256"])
        if len(plans) == 1:
            raise OSError("controlled verification storage failure after committed merges")
        return report

    monkeypatch.setattr(runner, "verify_publication", interrupted)
    result = runner.run_execution(
        spark,
        store,
        settings,
        batch["batch_id"],
        catalog="lakehouse",
        execution_id="verification-recovery",
        max_attempts=2,
        sleeper=lambda _: None,
    )
    assert [attempt["status"] for attempt in result["attempts"]] == ["FAILED", "SUCCEEDED"]
    assert plans[0] == plans[1]
    assert result["metrics"]["rows_updated"] == 1
    assert result["metrics"]["output_rows"] == 1
    assert result["verification"]["status"] == "SUCCEEDED"
    core = load_json(store, state_key(batch["batch_id"]))
    assert core["attempt"] == 2
    assert [row["status"] for row in core["history"]] == [
        "PENDING",
        "RUNNING",
        "FAILED",
        "RUNNING",
        "SUCCEEDED",
    ]
    assert load_json(store, CHECKPOINT_KEY)["active_batch_id"] is None
    fact = spark.table(f"lakehouse.{settings.database_prefix}_gold.fact_shipments")
    assert fact.filter(f"shipment_id = '{shipment['shipment_id']}'").first().weight_kg == 9.75


def test_quality_failure_is_not_retried_and_corrected_batch_recovers(spark, operations):
    store, settings, data, _, _ = operations
    shipment = {**data["shipments"][1], "assigned_driver_id": "MISSING-DRIVER"}
    batch = write_batch(
        store,
        "operations-quality-failure",
        "2026-02-08T12:00:00Z",
        [make_change("shipments", shipment, "UPDATE", 2, "2026-02-08T09:00:00Z")],
    )
    with pytest.raises(ValueError, match="REJECTION_THRESHOLD"):
        runner.run_execution(
            spark,
            store,
            settings,
            batch["batch_id"],
            catalog="lakehouse",
            execution_id="quality-failure",
            max_attempts=2,
        )
    failed = load_json(store, runner.execution_key("quality-failure"))
    assert len(failed["attempts"]) == 1
    assert failed["attempts"][0]["error_category"] == "DATA_QUALITY"
    assert failed["metrics"]["rows_rejected"] == 1
    assert failed["metrics"]["output_rows"] == 0
    assert not load_json(store, state_key(batch["batch_id"])).get("plan_key")
    good = {**data["shipments"][1], "weight_kg": 10.5}
    corrected = write_batch(
        store,
        "operations-corrected",
        "2026-02-08T12:00:00Z",
        [make_change("shipments", good, "UPDATE", 2, "2026-02-08T09:00:00Z")],
    )
    result = runner.run_execution(
        spark, store, settings, corrected["batch_id"], catalog="lakehouse", execution_id="corrected"
    )
    assert result["status"] == "SUCCEEDED"
    silver = {
        name: spark.table(f"lakehouse.{settings.database_prefix}_silver.{name}")
        for name in CONTRACTS
    }
    for name, expected in build_gold(spark, silver).items():
        actual = spark.table(f"lakehouse.{settings.database_prefix}_gold.{name}")
        assert compare_rows(expected.select(*actual.columns), actual, name) == actual.count()


def test_snapshot_execution_preserves_phase1_and_verifies_publication(spark, tmp_path):
    store, settings = context(spark, tmp_path)
    batch = write_landing(tmp_path)
    result = runner.run_execution(
        spark,
        store,
        settings,
        batch["batch_id"],
        mode="snapshot",
        catalog="lakehouse",
        execution_id="snapshot",
    )
    assert result["status"] == "SUCCEEDED"
    assert result["verification"]["tables_verified"] == 15
    assert result["verification"]["scope"] == "full_snapshot"
    assert result["metrics"]["rows_rejected"] == 10
    assert spark.table(f"lakehouse.{settings.database_prefix}_gold.fact_shipments").count() == 120


def test_event_only_batch_never_plans_or_rewrites_gold(spark, operations):
    store, settings, data, _, _ = operations
    tables = [
        "dim_station",
        "dim_driver",
        "dim_customer",
        "dim_date",
        "fact_shipments",
        "fact_delivery_attempts",
        "kpi_daily",
        "kpi_station_daily",
        "kpi_driver_daily",
    ]

    def snapshots():
        return {
            name: spark.sql(
                f"SELECT count(*) FROM lakehouse.{settings.database_prefix}_gold.{name}.snapshots"
            ).first()[0]
            for name in tables
        }

    before = snapshots()
    event = {**data["delivery_events"][0], "event_id": "OPERATIONS-LATE-EVENT"}
    batch = write_batch(
        store,
        "operations-event-only",
        "2026-02-09T12:00:00Z",
        [make_change("delivery_events", event, "INSERT", 1, "2026-01-01T09:00:00Z")],
    )
    result = runner.run_execution(
        spark, store, settings, batch["batch_id"], catalog="lakehouse", execution_id="event-only"
    )
    assert result["verification"]["tables_verified"] == 2
    assert result["metrics"]["output_rows"] == 1
    core = load_json(store, state_key(batch["batch_id"]))
    plan = load_json(store, core["plan_key"])
    assert not any(entry["layer"] == "gold" for entry in plan["entries"])
    assert before == snapshots()


@pytest.mark.parametrize("damage", ["duplicate", "missing", "changed", "schema"])
def test_post_write_verification_detects_mismatches(spark, damage):
    expected = spark.createDataFrame([(1, "a"), (2, "b")], "id int, value string")
    if damage == "duplicate":
        actual = expected.unionByName(expected.limit(1))
    elif damage == "missing":
        actual = expected.limit(1)
    elif damage == "changed":
        actual = spark.createDataFrame([(1, "a"), (2, "wrong")], "id int, value string")
    else:
        actual = spark.createDataFrame([(1, 2)], "id int, value int")
    with pytest.raises(QualityGateError, match="PUBLICATION_VERIFICATION_FAILED"):
        compare_rows(expected, actual, "fixture")
