"""Run the four-batch CDC story locally, recording before/after facts and a replay."""

import argparse
import fcntl
import json
from pathlib import Path

from delivery_lakehouse.cdc.generator import write_sequence
from delivery_lakehouse.cdc.pipeline import run_cdc
from delivery_lakehouse.cdc.state import CHECKPOINT_KEY, PUBLICATION_KEY
from delivery_lakehouse.config import Settings
from delivery_lakehouse.ingestion.storage import LocalStore
from delivery_lakehouse.logging_config import configure_logging
from delivery_lakehouse.spark import session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data/cdc-demo"))
    parser.add_argument("--config", default="config/dev.json")
    parser.add_argument(
        "--iceberg-jar",
        type=Path,
        default=Path("artifacts/iceberg-spark-runtime-4.1_2.13-1.11.0.jar"),
    )
    args = parser.parse_args()
    store = LocalStore(args.root)
    settings = Settings.load(args.config)
    lock = store.path("gold/_cdc/local.lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    configure_logging()
    with lock.open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if store.exists(CHECKPOINT_KEY) or store.exists(PUBLICATION_KEY):
            parser.error(
                "Use a fresh --root for before/after observations; use the CDC CLI to replay"
            )
        batches = write_sequence(store, settings.profile, settings.seed)
        spark = session(
            iceberg_jar=args.iceberg_jar.resolve(), warehouse=(args.root / "_catalog").resolve()
        )
        try:
            for layer in ("silver", "gold"):
                spark.sql(f"CREATE NAMESPACE lakehouse.{settings.database_prefix}_{layer}")
            observations = []
            for batch in batches:
                result = run_cdc(spark, store, settings, batch["batch_id"], catalog="lakehouse")
                rows = (
                    spark.table(f"lakehouse.{settings.database_prefix}_gold.fact_shipments")
                    .filter("shipment_id = 'SHCDC0001'")
                    .collect()
                )
                observations.append(
                    {
                        "batch_name": batch["batch_name"],
                        "batch_id": batch["batch_id"],
                        "arrival": batch["created_at"],
                        "status": result["status"],
                        "source_event_watermark": result["watermark_after"],
                        "shipment": rows[0].asDict() if rows else None,
                    }
                )
            before = store.read(PUBLICATION_KEY), store.read(CHECKPOINT_KEY)
            run_cdc(spark, store, settings, batches[1]["batch_id"], catalog="lakehouse")
            replay_unchanged = before == (store.read(PUBLICATION_KEY), store.read(CHECKPOINT_KEY))
            summary = {"observations": observations, "old_batch_replay_unchanged": replay_unchanged}
            output = Path("artifacts/cdc-demo-summary.json")
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(summary, default=str, indent=2) + "\n")
            print(f"Wrote {output}; old-batch replay unchanged: {replay_unchanged}")
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
