"""Single-writer CDC runner with immutable plans and explicit recovery checkpoints."""

import json
import logging
import time

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from delivery_lakehouse import __version__
from delivery_lakehouse.cdc import CDC_VERSION
from delivery_lakehouse.cdc.bronze import ingest_changes
from delivery_lakehouse.cdc.contracts import event_time
from delivery_lakehouse.cdc.frames import RECORD_STATE_SCHEMA, business_schema
from delivery_lakehouse.cdc.gold import GOLD_KEYS, plan_gold
from delivery_lakehouse.cdc.merge import ensure_table, merge_delta, table_identifier
from delivery_lakehouse.cdc.silver import integrity_violations, plan_silver, release_silver
from delivery_lakehouse.cdc.state import (
    CHECKPOINT_KEY,
    INITIALIZED_KEY,
    PUBLICATION_KEY,
    finish_committed_batch,
    load_json,
    now,
    read_plan,
    save_json,
    save_state,
    state_key,
    transition,
)
from delivery_lakehouse.config import batch_identifier
from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.generator import canonical_json, digest
from delivery_lakehouse.operations.events import emit
from delivery_lakehouse.transforms.gold import build_gold

LOGGER = logging.getLogger(__name__)


def target_name(catalog, settings, layer, name):
    return table_identifier(catalog, f"{settings.database_prefix}_{layer}", name)


def read_current(spark, store, settings, catalog):
    silver = {}
    for name in CONTRACTS:
        empty = spark.createDataFrame([], business_schema(name))
        table = target_name(catalog, settings, "silver", name)
        if spark.catalog.tableExists(table):
            ensure_table(
                spark,
                empty,
                catalog,
                f"{settings.database_prefix}_silver",
                name,
                store.uri(f"silver/tables/{name}"),
            )
            silver[name] = spark.table(table)
        else:
            silver[name] = empty
    empty_gold = build_gold(spark, {name: frame.limit(0) for name, frame in silver.items()})
    gold = {}
    for name, empty in empty_gold.items():
        table = target_name(catalog, settings, "gold", name)
        if spark.catalog.tableExists(table):
            ensure_table(
                spark,
                empty,
                catalog,
                f"{settings.database_prefix}_gold",
                name,
                store.uri(f"gold/tables/{name}"),
            )
            gold[name] = spark.table(table)
        else:
            gold[name] = empty
    return silver, gold


def initialize(spark, store, settings, catalog, silver, *, adopt_snapshot=False):
    context = {
        "catalog": catalog,
        "database_prefix": settings.database_prefix,
        "cdc_version": CDC_VERSION,
    }
    marker = load_json(store, INITIALIZED_KEY)
    table = target_name(catalog, settings, "silver", "cdc_record_state")
    if marker:
        if any(marker.get(key) != value for key, value in context.items()):
            raise ValueError("CDC_CONTEXT_MISMATCH")
        if marker.get("ready"):
            return
    existing_data = any(frame.limit(1).count() for frame in silver.values())
    if existing_data and not adopt_snapshot:
        raise ValueError("EXPLICIT_PHASE1_BASELINE_REQUIRED: use --adopt-phase1-snapshot")
    if not marker:
        if spark.catalog.tableExists(table) and spark.table(table).limit(1).count():
            raise ValueError("CDC_RECORD_STATE_WITHOUT_INITIALIZATION_RECORD")
        if existing_data:
            publication = load_json(store, PUBLICATION_KEY, {})
            if publication.get("status") != "SUCCEEDED" or any(
                not spark.catalog.tableExists(target_name(catalog, settings, "gold", name))
                for name in GOLD_KEYS
            ):
                raise ValueError("COMPLETE_PHASE1_PUBLICATION_REQUIRED")
        marker = {**context, "ready": False, "adopted_snapshot": existing_data, "started_at": now()}
        save_json(store, INITIALIZED_KEY, marker)
    empty = spark.createDataFrame([], RECORD_STATE_SCHEMA)
    ensure_table(
        spark,
        empty,
        catalog,
        f"{settings.database_prefix}_silver",
        "cdc_record_state",
        store.uri("silver/tables/cdc_record_state"),
    )
    if existing_data:
        baseline = empty
        for dataset, frame in silver.items():
            key = CONTRACTS[dataset].primary_key
            rows = frame.select(
                F.lit(dataset).alias("dataset"),
                F.col(key).alias("record_key"),
                F.lit(0).cast("long").alias("source_version"),
                F.col("updated_at").alias("source_event_at"),
                F.sha2(
                    F.concat_ws(
                        "/",
                        F.lit("phase1-baseline"),
                        F.lit(dataset),
                        F.col(key),
                        F.col("_record_hash"),
                    ),
                    256,
                ).alias("change_id"),
                F.lit(False).alias("is_deleted"),
                F.col("_batch_id").alias("batch_id"),
            )
            baseline = baseline.unionByName(rows)
        # Materialize the union before MERGE: Spark 4.1 cannot plan these multiple
        # Iceberg TableReference branches directly inside an ordered MERGE source.
        baseline = baseline.localCheckpoint(eager=True)
        try:
            merge_delta(
                spark,
                table,
                baseline.withColumn("_delete", F.lit(False)),
                ["dataset", "record_key"],
                ordered=True,
            )
        finally:
            baseline.unpersist()
    save_json(store, INITIALIZED_KEY, {**marker, "ready": True, "completed_at": now()})


def save_quality(spark, store, state, silver_plan, threshold):
    prefix = f"quarantine/cdc/{state['batch_id']}/attempt-{state['attempt']}"
    state.update(
        counts=silver_plan["counts"],
        schema_drift=silver_plan["schema_drift"],
        quality_prefix=prefix,
    )
    for dataset, counts in silver_plan["counts"].items():
        if counts["input_rows"]:
            for category in ("rejected", "ignored"):
                silver_plan[category][dataset].write.mode("errorifexists").parquet(
                    store.uri(f"{prefix}/{category}/{dataset}")
                )
    save_json(
        store,
        f"{prefix}/quality.json",
        {
            "batch_id": state["batch_id"],
            "counts": state["counts"],
            "schema_drift": state["schema_drift"],
        },
    )
    save_state(store, state)
    for dataset, counts in state["counts"].items():
        if not counts["reconciled"]:
            raise ValueError(f"CDC_ROW_RECONCILIATION_FAILED: {dataset}")
        if counts["input_rows"] and counts["rows_rejected"] / counts["input_rows"] > threshold:
            raise ValueError(f"CDC_REJECTION_THRESHOLD_EXCEEDED: {dataset}")
        if counts["input_rows"] and counts["rows_rejected"] == counts["input_rows"]:
            raise ValueError(f"CDC_ALL_RECORDS_REJECTED: {dataset}")
    issues = {}
    if any(counts["accepted_changes"] for counts in state["counts"].values()):
        for dataset, invalid in integrity_violations(silver_plan["prospective"]).items():
            count = invalid.count()
            if count:
                invalid.write.mode("errorifexists").parquet(
                    store.uri(f"{prefix}/integrity/{dataset}")
                )
                issues[dataset] = count
    if issues:
        state["integrity_errors"] = issues
        save_json(store, f"{prefix}/integrity.json", issues)
        raise ValueError(f"DEPENDENT_RECORD_INTEGRITY_FAILED: {canonical_json(issues)}")


def stage_plan(
    spark, store, settings, catalog, state, silver_plan, before_silver, before_gold, checkpoint
):
    prefix = f"gold/_cdc/plans/{state['batch_id']}/attempt-{state['attempt']}"
    entries = []

    def stage(layer, name, frame, keys, *, ordered=False):
        target = target_name(catalog, settings, layer, name)
        count = frame.count()
        if not count and spark.catalog.tableExists(target):
            return
        staged_key = f"{prefix}/{layer}/{name}"
        frame.write.mode("errorifexists").parquet(store.uri(staged_key))
        entries.append(
            {
                "step": f"{layer}.{name}",
                "layer": layer,
                "name": name,
                "target": target,
                "keys": keys,
                "staged_key": staged_key,
                "rows": count,
                "schema": frame.schema.json(),
                "ordered": ordered,
            }
        )

    for dataset, winners in silver_plan["winners"].items():
        if not state["counts"][dataset]["accepted_changes"] and spark.catalog.tableExists(
            target_name(catalog, settings, "silver", dataset)
        ):
            continue
        frame = winners.select(
            *[field.name for field in business_schema(dataset)],
            (F.col("_operation") == "DELETE").alias("_delete"),
        )
        stage("silver", dataset, frame, [CONTRACTS[dataset].primary_key])
    stage(
        "silver",
        "cdc_record_state",
        silver_plan["state_delta"].withColumn("_delete", F.lit(False)),
        ["dataset", "record_key"],
        ordered=True,
    )
    gold = plan_gold(spark, silver_plan, before_silver, before_gold)
    for name, frame in gold.items():
        stage("gold", name, frame, GOLD_KEYS[name])
    # A sparse incremental plan still creates the complete model on first initialization.
    for name, keys in GOLD_KEYS.items():
        if name not in gold and not spark.catalog.tableExists(
            target_name(catalog, settings, "gold", name)
        ):
            stage(
                "gold", name, before_gold[name].limit(0).withColumn("_delete", F.lit(False)), keys
            )
    latest = silver_plan["state_delta"].agg(F.max("source_event_at")).first()[0]
    previous = checkpoint.get("source_event_watermark")
    watermark = max([event_time(previous)] if previous else [], default=None)
    if latest:
        # Spark returns naive Python datetimes for UTC-session timestamps.
        latest = event_time(latest.isoformat() + "+00:00")
        watermark = max(watermark, latest) if watermark else latest
    plan = {
        "batch_id": state["batch_id"],
        "manifest_sha256": state["manifest_sha256"],
        "cdc_version": CDC_VERSION,
        "package_version": __version__,
        "max_reject_fraction": settings.max_reject_fraction,
        "entries": entries,
        "counts": state["counts"],
        "schema_drift": state["schema_drift"],
        "context": {"catalog": catalog, "database_prefix": settings.database_prefix},
        "watermark_after": watermark.isoformat() if watermark else None,
    }
    plan["sha256"] = digest(canonical_json(plan).encode())
    key = f"{prefix}/manifest.json"
    save_json(store, key, plan, immutable=True)
    return key


def publish_plan(spark, store, settings, catalog, state, plan, failure_hook):
    if plan["context"] != {"catalog": catalog, "database_prefix": settings.database_prefix}:
        raise ValueError("PUBLICATION_PLAN_CONTEXT_MISMATCH")
    if (
        plan["manifest_sha256"] != state["manifest_sha256"]
        or plan["cdc_version"] != CDC_VERSION
        or plan["package_version"] != __version__
        or plan["max_reject_fraction"] != settings.max_reject_fraction
    ):
        raise ValueError("PUBLICATION_PLAN_SOURCE_OR_VERSION_MISMATCH")
    for entry in plan["entries"]:
        if entry["step"] in state["completed_steps"]:
            continue
        frame = spark.read.parquet(store.uri(entry["staged_key"]))
        # Parquet relaxes nullability, so compare column names/types rather than nullability flags.
        expected = StructType.fromJson(json.loads(entry["schema"]))
        if [(f.name, f.dataType) for f in frame.schema] != [(f.name, f.dataType) for f in expected]:
            raise ValueError("STAGED_SCHEMA_CHANGED")
        if frame.count() != entry["rows"]:
            raise ValueError("STAGED_ROW_COUNT_CHANGED")
        target = ensure_table(
            spark,
            frame.drop("_delete"),
            catalog,
            f"{settings.database_prefix}_{entry['layer']}",
            entry["name"],
            store.uri(f"{entry['layer']}/tables/{entry['name']}"),
        )
        if target != entry["target"]:
            raise ValueError("STAGED_TARGET_CHANGED")
        if entry["rows"]:
            merge_delta(spark, target, frame, entry["keys"], ordered=entry["ordered"])
        if failure_hook:
            failure_hook(entry["step"])
        state["completed_steps"].append(entry["step"])
        save_state(store, state)
        LOGGER.info(
            "cdc_publication_step_committed",
            extra={
                "batch_id": state["batch_id"],
                "stage": entry["step"],
                "rows": entry["rows"],
                "status": "RUNNING",
            },
        )


def run_cdc(
    spark,
    store,
    settings,
    batch_id,
    *,
    catalog="glue_catalog",
    adopt_snapshot=False,
    failure_hook=None,
    observer=None,
    verify_publication=None,
):
    batch_identifier(batch_id)
    binding = load_json(store, INITIALIZED_KEY, {})
    if binding and (
        binding["catalog"] != catalog or binding["database_prefix"] != settings.database_prefix
    ):
        raise ValueError("CDC_CONTEXT_MISMATCH")
    started = time.monotonic()
    checkpoint = load_json(store, CHECKPOINT_KEY, {})
    state = load_json(store, state_key(batch_id))
    active = checkpoint.get("active_batch_id")
    if active and active != batch_id:
        owner = load_json(store, state_key(active), {})
        if owner.get("status") == "SUCCEEDED":
            finish_committed_batch(store, owner, checkpoint)
            checkpoint = load_json(store, CHECKPOINT_KEY, {})
        elif owner.get("plan_key") or owner.get("status") == "RUNNING":
            raise RuntimeError(f"RECOVER_ACTIVE_BATCH_FIRST: {active}")
    if state and state["status"] == "SUCCEEDED":
        ingest_changes(store, batch_id)
        finish_committed_batch(store, state, checkpoint)
        LOGGER.info("cdc_batch_replay_noop", extra={"batch_id": batch_id, "status": "SUCCEEDED"})
        emit(observer, "REPLAY", "SUCCEEDED")
        return state
    if not state:
        state = transition(
            {
                "batch_id": batch_id,
                "attempt": 0,
                "completed_steps": [],
                "checkpoint_before": {**checkpoint, "active_batch_id": None},
                "source_manifest": store.uri(f"landing/cdc/{batch_id}/manifest.json"),
            },
            "PENDING",
        )
        save_state(store, state)
    if not state.get("plan_key"):
        # A validation-only failure may be retried after a different batch succeeded.
        # Its next plan must roll back to the current checkpoint, not the original one.
        state["checkpoint_before"] = {**checkpoint, "active_batch_id": None}
    state = transition(
        state,
        "RUNNING",
        attempt=state["attempt"] + 1,
        started_at=now(),
        ended_at=None,
        error=None,
        error_category=None,
        publication_status="NOT_STARTED",
        max_reject_fraction=settings.max_reject_fraction,
    )
    save_state(store, state)
    save_json(store, CHECKPOINT_KEY, {**checkpoint, "active_batch_id": batch_id})
    LOGGER.info(
        "cdc_batch_started", extra={"batch_id": batch_id, "status": "RUNNING", "stage": "BRONZE"}
    )
    stage = "BRONZE"
    silver_plan = None
    publication_started = False
    try:
        emit(observer, "BRONZE", "RUNNING")
        bronze = ingest_changes(store, batch_id)
        state["manifest_sha256"] = bronze["manifest_sha256"]
        emit(
            observer,
            "BRONZE",
            "SUCCEEDED",
            source_rows=sum(entry["rows"] for entry in bronze["files"].values()),
        )
        stage = "INITIALIZATION"
        if not state.get("plan_key"):
            before_silver, before_gold = read_current(spark, store, settings, catalog)
            initialize(
                spark, store, settings, catalog, before_silver, adopt_snapshot=adopt_snapshot
            )
        save_json(
            store, PUBLICATION_KEY, {"batch_id": batch_id, "phase": "cdc", "status": "RUNNING"}
        )
        publication_started = True
        stage = "VALIDATION"
        if not state.get("plan_key"):
            emit(observer, "SILVER", "RUNNING")
            record_state = spark.table(target_name(catalog, settings, "silver", "cdc_record_state"))
            silver_plan = plan_silver(spark, store, bronze, before_silver, record_state)
            save_quality(spark, store, state, silver_plan, settings.max_reject_fraction)
            emit(observer, "SILVER", "SUCCEEDED", counts=state["counts"])
            emit(observer, "GOLD", "RUNNING")
            state["plan_key"] = stage_plan(
                spark,
                store,
                settings,
                catalog,
                state,
                silver_plan,
                before_silver,
                before_gold,
                checkpoint,
            )
            save_state(store, state)
            emit(observer, "GOLD", "SUCCEEDED", plan_key=state["plan_key"])
        else:
            emit(observer, "SILVER", "SKIPPED", reason="RECOVER_FROZEN_PLAN")
            emit(observer, "GOLD", "SKIPPED", reason="RECOVER_FROZEN_PLAN")
        plan = read_plan(store, state["plan_key"])
        state.update(
            counts=plan["counts"],
            schema_drift=plan["schema_drift"],
            watermark_after=plan["watermark_after"],
            publication_status="PUBLISHING",
        )
        stage = "PUBLICATION"
        emit(observer, "PUBLISH", "RUNNING")
        if failure_hook:
            failure_hook("PLAN_COMMITTED")
        publish_plan(spark, store, settings, catalog, state, plan, failure_hook)
        emit(observer, "PUBLISH", "SUCCEEDED", completed_steps=state["completed_steps"])
        stage = "VERIFICATION"
        emit(observer, "VERIFY", "RUNNING")
        if verify_publication:
            verify_publication({"mode": "cdc", "plan": plan})
        emit(observer, "VERIFY", "SUCCEEDED", enabled=verify_publication is not None)
        stage = "PUBLICATION"
        emit(observer, "COMMIT", "RUNNING")
        save_json(
            store,
            PUBLICATION_KEY,
            {
                "batch_id": batch_id,
                "phase": "cdc",
                "status": "SUCCEEDED",
                "completed_at": now(),
                "plan_key": state["plan_key"],
                "counts": state["counts"],
            },
        )
        checkpoint = {
            **checkpoint,
            "active_batch_id": batch_id,
            "last_succeeded_batch": batch_id,
            "source_event_watermark": plan["watermark_after"],
        }
        save_json(store, CHECKPOINT_KEY, checkpoint)
        state = transition(
            state,
            "SUCCEEDED",
            publication_status="SUCCEEDED",
            ended_at=now(),
            duration_seconds=round(time.monotonic() - started, 3),
        )
        save_state(store, state)
        finish_committed_batch(store, state, checkpoint)
        emit(observer, "COMMIT", "SUCCEEDED")
        metrics = {
            key: sum(counts[key] for counts in state["counts"].values())
            for key in (
                "rows_inserted",
                "rows_updated",
                "rows_deleted",
                "rows_rejected",
                "rows_ignored",
            )
        }
        LOGGER.info(
            "cdc_batch_succeeded",
            extra={
                "batch_id": batch_id,
                "status": state["status"],
                "duration_seconds": state["duration_seconds"],
                **metrics,
            },
        )
        return state
    except Exception as exc:
        durable = load_json(store, state_key(batch_id), {})
        if durable.get("status") != "SUCCEEDED":
            if state["status"] == "SUCCEEDED":
                state["status"] = "RUNNING"
                state["history"] = state["history"][:-1]
            state = transition(
                state,
                "FAILED",
                ended_at=now(),
                error=str(exc),
                error_category=stage,
                publication_status="FAILED" if publication_started else "NOT_STARTED",
                duration_seconds=round(time.monotonic() - started, 3),
            )
            save_state(store, state)
            save_json(
                store, CHECKPOINT_KEY, {**state["checkpoint_before"], "active_batch_id": batch_id}
            )
            if publication_started:
                save_json(
                    store,
                    PUBLICATION_KEY,
                    {
                        "batch_id": batch_id,
                        "phase": "cdc",
                        "status": "FAILED",
                        "error_category": stage,
                        "plan_key": state.get("plan_key"),
                    },
                )
        LOGGER.exception(
            "cdc_batch_failed",
            extra={
                "batch_id": batch_id,
                "status": state["status"],
                "error_category": stage,
                "duration_seconds": round(time.monotonic() - started, 3),
            },
        )
        raise
    finally:
        if silver_plan:
            release_silver(silver_plan)
