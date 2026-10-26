"""Phase 1 full snapshot runner. No scheduling, CDC or hidden AWS execution."""

import json
import logging
from datetime import UTC, datetime

from delivery_lakehouse import __version__
from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.generator import canonical_json, digest
from delivery_lakehouse.ingestion.bronze import ingest
from delivery_lakehouse.quality.report import enforce
from delivery_lakehouse.tables import write_iceberg
from delivery_lakehouse.transforms.gold import build_gold
from delivery_lakehouse.transforms.silver import BRONZE_SCHEMA, transform

LOGGER = logging.getLogger(__name__)


def run_pipeline(spark, store, settings, batch_id, *, catalog=None):
    if store.exists("gold/_cdc/initialized.json"):
        raise ValueError(
            "CDC is initialized; snapshot replacement would invalidate source versions"
        )
    bronze_manifest = ingest(store, batch_id)
    run_id = digest(
        canonical_json(
            {
                "batch": batch_id,
                "version": __version__,
                "threshold": settings.max_reject_fraction,
                "catalog": catalog,
                "database_prefix": settings.database_prefix,
            }
        ).encode()
    )
    # A global publication record makes incomplete multi-table publication visible.
    # It is NOT a multi-table transaction. Consumers must check SUCCEEDED + batch IDs.
    publication_key = "gold/_publication.json"
    if store.exists(publication_key):
        previous = json.loads(store.read(publication_key))
        if previous.get("run_id") == run_id and previous.get("status") == "SUCCEEDED":
            LOGGER.info("pipeline_replay_noop", extra={"batch_id": batch_id})
            return previous
    state = {
        "batch_id": batch_id,
        "run_id": run_id,
        "pipeline_version": __version__,
        "status": "RUNNING",
        "started_at": datetime.now(UTC).isoformat(),
    }
    store.write(publication_key, canonical_json(state).encode())
    silver = None
    try:
        bronze = {
            name: spark.read.schema(BRONZE_SCHEMA).json(
                store.uri(bronze_manifest["files"][name]["key"])
            )
            for name in CONTRACTS
        }
        silver = transform(bronze)
        for name, frame in silver.quarantine.items():
            frame.write.mode("overwrite").parquet(store.uri(f"quarantine/{batch_id}/{name}"))
        quality = {"batch_id": batch_id, "run_id": run_id, "datasets": silver.reports}
        store.write(f"quarantine/{batch_id}/quality_report.json", canonical_json(quality).encode())
        enforce(silver.reports, settings.max_reject_fraction)
        gold = build_gold(spark, silver.tables)
        outputs = {}
        for layer, tables in (("silver", silver.tables), ("gold", gold)):
            database = f"{settings.database_prefix}_{layer}"
            for name, frame in tables.items():
                if catalog:
                    location = store.uri(f"{layer}/tables/{name}")
                    outputs[f"{layer}.{name}"] = write_iceberg(
                        spark, frame, catalog, database, name, location
                    )
                else:
                    location = store.uri(f"{layer}/{batch_id}/{name}")
                    frame.write.mode("overwrite").parquet(location)
                    outputs[f"{layer}.{name}"] = location
        state.update(
            status="SUCCEEDED",
            completed_at=datetime.now(UTC).isoformat(),
            tables=outputs,
            quality=quality,
        )
        store.write(f"gold/_commits/{run_id}.json", canonical_json(state).encode())
        store.write(publication_key, canonical_json(state).encode())
        LOGGER.info("snapshot_published", extra={"batch_id": batch_id})
        return state
    except Exception as exc:
        state.update(status="FAILED", error_type=type(exc).__name__, error=str(exc))
        store.write(publication_key, canonical_json(state).encode())
        LOGGER.exception("snapshot_failed", extra={"batch_id": batch_id})
        raise
    finally:
        if silver:
            silver.unpersist()
