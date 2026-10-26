from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from delivery_lakehouse.transforms.gold import build_kpis

pytestmark = pytest.mark.spark
QUERIES = Path(__file__).resolve().parents[2] / "sql/athena"


def query(spark, filename):
    sql = "\n".join(
        line for line in (QUERIES / filename).read_text().splitlines() if not line.startswith("--")
    )
    return spark.sql(sql.replace("delivery_ops_dev_gold.", "")).collect()


@pytest.fixture
def analytical_cohort(spark):
    promised = datetime(2026, 1, 1, 10, tzinfo=UTC)
    rows = [
        ("A", "DR1", "DELIVERED", 1, 1, 0, 0.0, 1, 0, 1, 1, promised),
        ("B", "DR2", "DELIVERED", 1, 0, 0, 60.0, 2, 1, 1, 0, promised + timedelta(hours=1)),
        ("C", "DR2", "FAILED", 0, 0, 1, None, 1, 1, 1, 0, None),
        ("D", "DR1", "IN_TRANSIT", 0, 0, 0, None, 0, 0, 0, 0, None),
        ("E", "DR1", "CANCELLED", 0, 0, 0, None, 0, 0, 0, 0, None),
    ]
    schema = (
        "shipment_id string, driver_id string, status string, delivered_count int, "
        "on_time_count int, failed_count int, delivery_delay_minutes double, "
        "attempt_count long, failed_attempts long, has_first_attempt int, "
        "first_attempt_success int, delivered_at timestamp, promised_at timestamp, "
        "promised_date date, station_id string, delivery_evidence_complete boolean, "
        "service_level string, weather string, traffic string, distance_km double"
    )
    values = [
        (*row, promised, date(2026, 1, 1), "ST1", True, "STANDARD", "RAIN", "HIGH", 10.0)
        for row in rows
    ]
    fact = spark.createDataFrame(values, schema)
    fact.createOrReplaceTempView("fact_shipments")
    spark.createDataFrame(
        [("ST1", "Synthetic Station", "NSW")],
        "station_id string, station_name string, region string",
    ).createOrReplaceTempView("dim_station")
    spark.createDataFrame(
        [("DR1", "Driver A", "VAN"), ("DR2", "Driver B", "VAN")],
        "driver_id string, driver_name string, vehicle_type string",
    ).createOrReplaceTempView("dim_driver")
    for name, frame in build_kpis(spark, fact).items():
        frame.createOrReplaceTempView(name)


def test_daily_rates_keep_terminal_and_attempt_denominators(spark, analytical_cohort):
    daily = query(spark, "07_daily_delivery_metrics.sql")[0]
    assert daily.total_shipments == 5
    assert daily.total_attempts == 4
    assert float(daily.on_time_delivery_pct) == 50
    assert float(daily.avg_delivery_delay_minutes) == 30
    assert float(daily.failed_delivery_pct) == pytest.approx(100 / 3)
    assert float(daily.first_attempt_success_pct) == pytest.approx(100 / 3)


def test_cancellation_and_delayed_details_are_distinct_from_failure(spark, analytical_cohort):
    cancellations = query(spark, "08_cancellation_rates.sql")[0]
    assert cancellations.cancelled_shipments == 1
    assert cancellations.total_shipments == 5
    assert float(cancellations.cancellation_pct) == 20
    delayed = query(spark, "09_delayed_shipments.sql")
    assert len(delayed) == 1
    assert delayed[0].shipment_id == "B"
    assert delayed[0].delivery_delay_minutes == 60
    assert delayed[0].attempt_count == 2


def test_station_and_driver_rates_reconcile_with_hand_calculated_cohort(spark, analytical_cohort):
    station = query(spark, "01_station_reliability.sql")[0]
    assert station.shipments == 5
    assert float(station.failed_delivery_pct) == pytest.approx(100 / 3)
    assert float(station.on_time_pct) == 50
    drivers = {row.driver_id: row for row in query(spark, "03_driver_performance.sql")}
    assert drivers["DR1"].assigned_shipments == 3
    assert float(drivers["DR1"].first_attempt_success_pct) == 100
    assert float(drivers["DR2"].first_attempt_success_pct) == 0


def test_trend_weights_counts_instead_of_averaging_percentages(spark):
    frame = spark.createDataFrame(
        [
            (date(2026, 1, 1), 5, 50.0, 1, 2),
            (date(2026, 1, 3), 1, 100.0, 1, 1),
        ],
        "promised_date date, total_shipments long, on_time_delivery_pct double, "
        "on_time_deliveries long, delivered_shipments long",
    )
    frame.createOrReplaceTempView("kpi_daily")
    rows = query(spark, "04_reliability_trend.sql")
    assert float(rows[-1].trailing_7_observed_days_on_time_pct) == pytest.approx(200 / 3)
