"""Reproducible local demo + compact analytical evidence. Never creates an AWS client."""

import argparse
import json
from decimal import Decimal
from pathlib import Path

from delivery_lakehouse.config import Settings
from delivery_lakehouse.generator import write_landing
from delivery_lakehouse.ingestion.storage import LocalStore
from delivery_lakehouse.logging_config import configure_logging
from delivery_lakehouse.pipeline import run_pipeline
from delivery_lakehouse.spark import session


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("data"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/demo-summary.json"))
    args = parser.parse_args()
    configure_logging()
    settings = Settings.load("config/dev.json")
    batch = write_landing(
        args.root, profile=settings.profile, seed=settings.seed, start_date=settings.start_date
    )["batch_id"]
    spark = session()
    try:
        result = run_pipeline(spark, LocalStore(args.root), settings, batch)
        assert result == run_pipeline(spark, LocalStore(args.root), settings, batch)
        spark.read.parquet(result["tables"]["gold.fact_shipments"]).createOrReplaceTempView(
            "demo_fact"
        )
        metrics = (
            spark.sql("""
            SELECT count(*) AS total_shipments, sum(delivered_count) AS delivered_shipments,
              sum(on_time_count) AS on_time_deliveries, sum(failed_count) AS failed_shipments,
              sum(attempt_count) AS total_attempts, sum(failed_attempts) AS failed_attempts,
              100.0 * sum(on_time_count) / nullif(sum(delivered_count), 0) AS on_time_delivery_pct,
              avg(delivery_delay_minutes) AS avg_delivery_delay_minutes,
              100.0 * sum(first_attempt_success) / nullif(sum(has_first_attempt), 0)
                AS first_attempt_success_pct,
              100.0 * sum(failed_count) / nullif(sum(delivered_count + failed_count), 0)
                AS failed_delivery_pct
            FROM demo_fact
        """)
            .first()
            .asDict()
        )
        summary = {
            "source": "synthetic only",
            "batch_id": batch,
            "profile": settings.profile,
            "seed": settings.seed,
            "spark_version": spark.version,
            "publication_status": result["status"],
            "replay_identical": True,
            "curated_tables": sorted(result["tables"]),
            "quality": result["quality"]["datasets"],
            "overall_kpis": metrics,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(
                summary,
                indent=2,
                default=lambda value: float(value) if isinstance(value, Decimal) else str(value),
            )
            + "\n"
        )
        print(args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
