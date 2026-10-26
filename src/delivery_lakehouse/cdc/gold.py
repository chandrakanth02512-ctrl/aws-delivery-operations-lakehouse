"""Recompute changed shipment facts and only their old/new reporting cohorts."""

from pyspark.sql import functions as F

from delivery_lakehouse.cdc.frames import delta_for_keys
from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.transforms.gold import build_gold, build_kpis

GOLD_KEYS = {
    "dim_station": ["station_id"],
    "dim_driver": ["driver_id"],
    "dim_customer": ["customer_id"],
    "dim_date": ["date_key"],
    "fact_shipments": ["shipment_id"],
    "fact_delivery_attempts": ["attempt_id"],
    "kpi_daily": ["promised_date"],
    "kpi_station_daily": ["promised_date", "station_id"],
    "kpi_driver_daily": ["promised_date", "driver_id"],
}


def changed_keys(plan, dataset):
    return plan["winners"][dataset].select(CONTRACTS[dataset].primary_key).distinct()


def plan_gold(spark, silver_plan, before_silver, before_gold):
    after = silver_plan["prospective"]
    changed_attempts = changed_keys(silver_plan, "delivery_attempts")
    affected_shipments = (
        changed_keys(silver_plan, "shipments")
        .unionByName(
            before_silver["delivery_attempts"]
            .join(changed_attempts, "attempt_id")
            .select("shipment_id")
        )
        .unionByName(
            after["delivery_attempts"].join(changed_attempts, "attempt_id").select("shipment_id")
        )
        .distinct()
    )
    subset = {
        **after,
        "shipments": after["shipments"].join(affected_shipments, "shipment_id"),
        "delivery_attempts": after["delivery_attempts"].join(affected_shipments, "shipment_id"),
    }
    partial = build_gold(spark, subset)
    before_facts = before_gold["fact_shipments"].join(affected_shipments, "shipment_id")
    new_facts = partial["fact_shipments"]
    affected_dates = (
        before_facts.select("promised_date")
        .unionByName(new_facts.select("promised_date"))
        .distinct()
    )
    # Scan only impacted dates for aggregation, retaining all shipments in those cohorts.
    fact_cohorts = (
        before_gold["fact_shipments"]
        .join(affected_dates, "promised_date")
        .join(affected_shipments, "shipment_id", "left_anti")
        .unionByName(new_facts)
    )
    aggregates = build_kpis(spark, fact_cohorts)
    deltas = {
        "fact_shipments": delta_for_keys(
            before_gold["fact_shipments"].schema, new_facts, affected_shipments, ["shipment_id"]
        )
    }
    dimension_sources = {
        "dim_station": "delivery_stations",
        "dim_driver": "drivers",
        "dim_customer": "customers",
        "fact_delivery_attempts": "delivery_attempts",
    }
    for table, dataset in dimension_sources.items():
        keys = GOLD_KEYS[table]
        affected = changed_keys(silver_plan, dataset)
        rows = after[dataset].drop("_raw_json").join(affected, keys)
        deltas[table] = delta_for_keys(before_gold[table].schema, rows, affected, keys)
    for table, rows in aggregates.items():
        keys = GOLD_KEYS[table]
        affected = before_facts.select(*keys).unionByName(new_facts.select(*keys)).distinct()
        rows = rows.join(affected, keys)
        deltas[table] = delta_for_keys(before_gold[table].schema, rows, affected, keys)
    dates = fact_cohorts.select(F.col("promised_date").alias("date_key")).distinct()
    dates = dates.withColumn("year", F.year("date_key")).withColumn("month", F.month("date_key"))
    dates = dates.withColumn("day_of_week", F.dayofweek("date_key"))
    deltas["dim_date"] = delta_for_keys(
        before_gold["dim_date"].schema,
        dates,
        affected_dates.select(F.col("promised_date").alias("date_key")),
        ["date_key"],
    )
    return deltas
