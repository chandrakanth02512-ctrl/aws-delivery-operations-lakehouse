"""Star schema and additive KPI inputs; ratios are recomputed, never averaged."""

from pyspark.sql import functions as F


def build_gold(spark, silver):
    for name, frame in silver.items():
        frame.createOrReplaceTempView(f"silver_{name}")
    # Attempt aggregation precedes the shipment join, preventing fan-out of shipment KPIs.
    fact = spark.sql("""
        WITH attempts AS (
          SELECT shipment_id, COUNT(*) AS attempt_count,
                 SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_attempts,
                 MAX(CASE WHEN attempt_number = 1 THEN 1 ELSE 0 END) AS has_first_attempt,
                 MAX(CASE WHEN attempt_number = 1 AND status = 'DELIVERED'
                     THEN 1 ELSE 0 END) AS first_attempt_success,
                 SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS success_evidence
          FROM silver_delivery_attempts GROUP BY shipment_id
        )
        SELECT s.shipment_id, s.customer_id, s.station_id,
               s.assigned_driver_id AS driver_id, to_date(s.promised_at) AS promised_date,
               s.created_at, s.promised_at, s.delivered_at, s.status, s.service_level,
               s.weight_kg, s.distance_km, s.weather, s.traffic, s._batch_id,
               CASE WHEN s.status = 'DELIVERED' THEN 1 ELSE 0 END AS delivered_count,
               CASE WHEN s.status = 'DELIVERED' AND s.delivered_at <= s.promised_at
                    THEN 1 ELSE 0 END AS on_time_count,
               CASE WHEN s.status = 'FAILED' THEN 1 ELSE 0 END AS failed_count,
               CASE WHEN s.status = 'DELIVERED' THEN greatest(0.0,
                    (unix_millis(s.delivered_at) - unix_millis(s.promised_at)) / 60000.0)
                    END AS delivery_delay_minutes,
               coalesce(a.attempt_count, 0) AS attempt_count,
               coalesce(a.failed_attempts, 0) AS failed_attempts,
               coalesce(a.has_first_attempt, 0) AS has_first_attempt,
               coalesce(a.first_attempt_success, 0) AS first_attempt_success,
               CASE WHEN s.status = 'DELIVERED' THEN coalesce(a.success_evidence, 0) = 1
                    ELSE coalesce(a.success_evidence, 0) = 0 END AS delivery_evidence_complete
        FROM silver_shipments s LEFT JOIN attempts a ON s.shipment_id = a.shipment_id
    """)
    fact.createOrReplaceTempView("fact_shipments")
    tables = {
        "dim_station": silver["delivery_stations"].drop("_raw_json"),
        "dim_driver": silver["drivers"].drop("_raw_json"),
        "dim_customer": silver["customers"].drop("_raw_json"),
        "dim_date": fact.select(F.col("promised_date").alias("date_key"))
        .distinct()
        .withColumn("year", F.year("date_key"))
        .withColumn("month", F.month("date_key"))
        .withColumn("day_of_week", F.dayofweek("date_key")),
        "fact_shipments": fact,
        "fact_delivery_attempts": silver["delivery_attempts"].drop("_raw_json"),
    }
    tables.update(build_kpis(spark, fact))
    return tables


def build_kpis(spark, fact):
    """Shared formulas for complete snapshots and affected CDC reporting cohorts."""
    fact.createOrReplaceTempView("fact_shipments")
    tables = {}
    for name, dimensions in {
        "kpi_daily": ["promised_date"],
        "kpi_station_daily": ["promised_date", "station_id"],
        "kpi_driver_daily": ["promised_date", "driver_id"],
    }.items():
        grouping = ", ".join(dimensions)
        tables[name] = spark.sql(f"""
          SELECT {grouping}, COUNT(*) AS total_shipments,
                 SUM(delivered_count) AS delivered_shipments,
                 SUM(on_time_count) AS on_time_deliveries,
                 SUM(failed_count) AS failed_shipments,
                 SUM(has_first_attempt) AS shipments_with_first_attempt,
                 SUM(first_attempt_success) AS first_attempt_successes,
                 SUM(attempt_count) AS total_attempts,
                 SUM(failed_attempts) AS failed_attempts,
                 SUM(delivery_delay_minutes) AS total_delay_minutes,
                 100.0 * SUM(on_time_count) / nullif(SUM(delivered_count), 0)
                   AS on_time_delivery_pct,
                 AVG(delivery_delay_minutes) AS avg_delivery_delay_minutes,
                 100.0 * SUM(first_attempt_success) / nullif(SUM(has_first_attempt), 0)
                   AS first_attempt_success_pct,
                 100.0 * SUM(failed_count) / nullif(SUM(delivered_count + failed_count), 0)
                   AS failed_delivery_pct,
                 100.0 * SUM(failed_attempts) / nullif(SUM(attempt_count), 0)
                   AS failed_attempt_pct,
                 SUM(CASE WHEN NOT delivery_evidence_complete THEN 1 ELSE 0 END)
                   AS shipments_missing_delivery_evidence
          FROM fact_shipments GROUP BY {grouping}
        """)
    return tables
