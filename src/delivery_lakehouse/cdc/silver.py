"""Validate deltas, select source-version winners, and guard reverse relationships."""

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

from delivery_lakehouse.cdc.frames import (
    CHANGE_COLUMNS,
    RECORD_STATE_SCHEMA,
    audit_rows,
    business_schema,
    decode_bronze,
)
from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.transforms.silver import METADATA, business_rules, foreign_keys, normalize


def check_relationships(frame, dataset, parents):
    frame = business_rules(foreign_keys(frame, dataset, parents), dataset, parents)
    if dataset == "delivery_attempts":
        # An old attempt retains its actual driver after a shipment is reassigned.
        # Failed attempts can also precede an in-transit/cancelled current status.
        frame = frame.withColumn(
            "_errors",
            F.array_except(
                "_errors",
                F.array(
                    F.lit("RELATIONSHIP:shipment_id:driver_id"),
                    F.lit("ATTEMPT_FOR_UNATTEMPTED_SHIPMENT"),
                ),
            ),
        )
    return frame


def choose_changes(frame, record_state, dataset):
    previous = record_state.filter(F.col("dataset") == dataset).select(
        F.col("record_key").alias("_record_key"),
        F.col("source_version").alias("_previous_version"),
        F.col("source_event_at").alias("_previous_time"),
        F.col("change_id").alias("_previous_change"),
    )
    joined = frame.join(previous, "_record_key", "left")
    newer = F.col("_previous_version").isNull() | (
        F.struct("_source_version", "_source_event_at", "_change_id")
        > F.struct("_previous_version", "_previous_time", "_previous_change")
    )
    stale = audit_rows(
        joined.filter(~newer),
        F.array(
            F.when(F.col("_change_id") == F.col("_previous_change"), "REPLAYED_CHANGE").otherwise(
                "STALE_CHANGE"
            )
        ),
    )
    fresh = joined.filter(newer).drop("_previous_version", "_previous_time", "_previous_change")
    window = Window.partitionBy("_record_key").orderBy(
        F.col("_source_version").desc(),
        F.col("_source_event_at").desc(),
        F.col("_change_id").desc(),
        F.col("_record_id"),
    )
    ranked = fresh.withColumn("_rank", F.row_number().over(window)).withColumn(
        "_winner_id", F.first("_change_id").over(window)
    )
    ignored = audit_rows(
        ranked.filter("_rank > 1"),
        F.array(
            F.when(F.col("_change_id") == F.col("_winner_id"), "DUPLICATE_CHANGE").otherwise(
                "SUPERSEDED_CHANGE"
            )
        ),
    )
    return ranked.filter("_rank = 1").drop("_rank", "_winner_id"), stale.unionByName(ignored)


def summarize_reasons(frame):
    return {
        row.reason: row["count"]
        for row in frame.select(F.explode("_reasons").alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    }


def plan_silver(spark, store, bronze, current, record_state):
    decoded, rejected, drift = decode_bronze(spark, store, bronze)
    prospective, winners, ignored, counts = {}, {}, {}, {}
    for dataset, contract in CONTRACTS.items():
        key = contract.primary_key
        columns = [*METADATA, *contract.fields, *CHANGE_COLUMNS, "_errors"]
        if dataset not in bronze["files"]:
            winners[dataset] = normalize(decoded[dataset], dataset).select(*columns)
            ignored[dataset] = rejected[dataset]
            prospective[dataset] = current[dataset]
            counts[dataset] = {
                name: 0
                for name in (
                    "input_rows",
                    "accepted_changes",
                    "rows_inserted",
                    "rows_updated",
                    "rows_deleted",
                    "rows_rejected",
                    "rows_ignored",
                )
            }
            counts[dataset].update(rejection_reasons={}, ignored_reasons={}, reconciled=True)
            continue
        upserts = (
            decoded[dataset]
            .filter("_operation <> 'DELETE'")
            .withColumn("_original_payload", F.col("_raw_json"))
            .withColumn("_raw_json", F.col("_validation_json"))
        )
        upserts = normalize(upserts, dataset).withColumn("_raw_json", F.col("_original_payload"))
        upserts = check_relationships(upserts, dataset, prospective).select(*columns)
        deletes = decoded[dataset].filter("_operation = 'DELETE'")
        for name, field in contract.fields.items():
            deletes = deletes.withColumn(
                name, F.col("_record_key") if name == key else F.lit(None).cast(field.kind)
            )
        deletes = deletes.withColumn("_errors", F.array().cast("array<string>")).select(*columns)
        checked = upserts.unionByName(deletes).localCheckpoint(eager=True)
        invalid = audit_rows(checked.filter(F.size("_errors") > 0), F.col("_errors"))
        rejected[dataset] = rejected[dataset].unionByName(invalid).localCheckpoint(eager=True)
        winners[dataset], ignored[dataset] = choose_changes(
            checked.filter(F.size("_errors") == 0), record_state, dataset
        )
        winners[dataset] = winners[dataset].localCheckpoint(eager=True)
        ignored[dataset] = ignored[dataset].localCheckpoint(eager=True)
        business_columns = [field.name for field in business_schema(dataset)]
        affected = winners[dataset].select(key)
        after = winners[dataset].filter("_operation <> 'DELETE'").select(*business_columns)
        prospective[dataset] = current[dataset].join(affected, key, "left_anti").unionByName(after)
        before_keys = current[dataset].select(key)
        counts[dataset] = {
            "input_rows": bronze["files"].get(dataset, {}).get("rows", 0),
            "accepted_changes": winners[dataset].count(),
            "rows_inserted": after.join(before_keys, key, "left_anti").count(),
            "rows_updated": after.join(before_keys, key, "inner").count(),
            "rows_deleted": winners[dataset]
            .filter("_operation = 'DELETE'")
            .join(before_keys, key)
            .count(),
            "rows_rejected": rejected[dataset].count(),
            "rows_ignored": ignored[dataset].count(),
            "rejection_reasons": summarize_reasons(rejected[dataset]),
            "ignored_reasons": summarize_reasons(ignored[dataset]),
        }
        record = counts[dataset]
        record["reconciled"] = record["input_rows"] == (
            record["accepted_changes"] + record["rows_rejected"] + record["rows_ignored"]
        )
        checked.unpersist()
    states = [
        frame.select(
            F.lit(dataset).alias("dataset"),
            F.col("_record_key").alias("record_key"),
            F.col("_source_version").alias("source_version"),
            F.col("_source_event_at").alias("source_event_at"),
            F.col("_change_id").alias("change_id"),
            (F.col("_operation") == "DELETE").alias("is_deleted"),
            F.col("_batch_id").alias("batch_id"),
        )
        for dataset, frame in winners.items()
    ]
    state_delta = reduce(
        lambda left, right: left.unionByName(right),
        states,
        spark.createDataFrame([], RECORD_STATE_SCHEMA),
    )
    return {
        "prospective": prospective,
        "winners": winners,
        "rejected": rejected,
        "ignored": ignored,
        "counts": counts,
        "schema_drift": drift,
        "state_delta": state_delta,
    }


def integrity_violations(prospective):
    violations = {}
    for dataset, frame in prospective.items():
        checked = check_relationships(
            frame.withColumn("_errors", F.array().cast("array<string>")), dataset, prospective
        )
        violations[dataset] = checked.filter(F.size("_errors") > 0)
    attempts = prospective["delivery_attempts"]
    conflicts = attempts.groupBy("shipment_id", "attempt_number").count().filter("count > 1")
    duplicates = attempts.join(
        conflicts.select("shipment_id", "attempt_number"), ["shipment_id", "attempt_number"]
    ).withColumn("_errors", F.array(F.lit("DUPLICATE_ATTEMPT_NUMBER")))
    violations["delivery_attempts"] = violations["delivery_attempts"].unionByName(duplicates)
    return violations


def release_silver(plan):
    for section in ("winners", "rejected", "ignored"):
        for frame in plan[section].values():
            frame.unpersist()
