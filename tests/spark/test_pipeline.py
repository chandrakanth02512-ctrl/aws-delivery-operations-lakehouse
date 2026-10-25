import json

import pytest
from pyspark.sql import functions as F

from delivery_lakehouse.config import Settings
from delivery_lakehouse.generator import generate
from delivery_lakehouse.pipeline import run_pipeline

pytestmark = pytest.mark.spark


def test_full_snapshot_counts_and_quarantine(spark, local_run):
    _, store, manifest, result = local_run
    assert result["status"] == "SUCCEEDED"
    assert len(result["tables"]) == 15
    reports = {r["dataset"]: r for r in result["quality"]["datasets"]}
    for r in reports.values():
        assert r["input_rows"] == r["accepted_rows"] + r["quarantined_rows"]
        assert r["reconciled"]
    assert reports["shipments"]["accepted_rows"] == 120
    assert reports["shipments"]["quarantined_rows"] == 6
    assert reports["shipments"]["reason_counts"]["DUPLICATE_PRIMARY_KEY"] == 1
    assert reports["shipments"]["reason_counts"]["FOREIGN_KEY:customer_id"] == 1
    rejected = spark.read.parquet(store.uri(f"quarantine/{manifest['batch_id']}/shipments"))
    assert rejected.filter(F.size("_errors") == 0).count() == 0
    assert rejected.filter(F.col("_raw_json").isNull()).count() == 0
    assert json.loads(store.read(f"quarantine/{manifest['batch_id']}/quality_report.json"))


def test_replay_is_noop(spark, local_run):
    _, store, manifest, result = local_run
    before = store.read("gold/_publication.json")
    assert run_pipeline(spark, store, Settings(), manifest["batch_id"]) == result
    assert store.read("gold/_publication.json") == before
    assert spark.read.parquet(result["tables"]["gold.fact_shipments"]).count() == 120


def test_curated_types_primary_keys_and_references(spark, local_run):
    _, _, _, result = local_run
    shipments = spark.read.parquet(result["tables"]["silver.shipments"])
    assert dict(shipments.dtypes)["promised_at"] == "timestamp"
    assert dict(shipments.dtypes)["weight_kg"] == "double"
    assert shipments.select("shipment_id").distinct().count() == shipments.count()
    attempts = spark.read.parquet(result["tables"]["silver.delivery_attempts"])
    assert attempts.join(shipments, "shipment_id", "left_anti").count() == 0


def test_gold_metrics_match_independent_python_oracle(spark, local_run):
    _, _, _, result = local_run
    source = generate(defects=False)
    from datetime import datetime

    delivered = [r for r in source["shipments"] if r["status"] == "DELIVERED"]
    on_time = sum(r["delivered_at"] <= r["promised_at"] for r in delivered)
    total_delay = sum(
        max(
            0,
            (
                datetime.fromisoformat(r["delivered_at"]) - datetime.fromisoformat(r["promised_at"])
            ).total_seconds()
            / 60,
        )
        for r in delivered
    )
    fact = spark.read.parquet(result["tables"]["gold.fact_shipments"])
    actual = fact.agg(
        F.count("*").alias("shipments"),
        F.sum("delivered_count").alias("delivered"),
        F.sum("on_time_count").alias("on_time"),
        F.sum("delivery_delay_minutes").alias("delay"),
        F.sum("attempt_count").alias("attempts"),
    ).first()
    assert actual.shipments == 120
    assert actual.delivered == len(delivered)
    assert actual.on_time == on_time
    assert actual.delay == pytest.approx(total_delay)
    assert actual.attempts == len(source["delivery_attempts"])
    daily = spark.read.parquet(result["tables"]["gold.kpi_daily"])
    assert daily.agg(F.sum("total_shipments")).first()[0] == 120
    assert fact.filter(~F.col("delivery_evidence_complete")).count() == 0
