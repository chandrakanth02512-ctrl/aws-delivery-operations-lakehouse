"""Strict snapshot contracts with Spark expressions, deterministic winners and quarantine."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType

from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.quality.report import report

BRONZE_SCHEMA = StructType(
    [
        StructField(name, LongType() if name == "_source_line" else StringType(), False)
        for name in (
            "_raw_json",
            "_source_file",
            "_source_line",
            "_batch_id",
            "_ingested_at",
            "_record_hash",
            "_record_id",
        )
    ]
)
METADATA = [f.name for f in BRONZE_SCHEMA]


@dataclass
class SilverResult:
    tables: dict[str, DataFrame]
    quarantine: dict[str, DataFrame]
    reports: list[dict]

    def unpersist(self):
        for frame in [*self.tables.values(), *self.quarantine.values()]:
            frame.unpersist()


def add_error(frame, condition, reason):
    return frame.withColumn(
        "_errors",
        F.when(
            F.coalesce(condition, F.lit(False)),
            F.array_union(F.col("_errors"), F.array(F.lit(reason))),
        ).otherwise(F.col("_errors")),
    )


def normalize(frame: DataFrame, name: str) -> DataFrame:
    contract = CONTRACTS[name]
    frame = frame.withColumn("_errors", F.array().cast("array<string>"))
    frame = frame.withColumn(
        "_parsed",
        F.from_json(
            "_raw_json", MapType(StringType(), StringType()), {"allowNonNumericNumbers": "false"}
        ),
    )
    frame = add_error(frame, F.col("_parsed").isNull(), "MALFORMED_JSON_OBJECT")
    # Native Spark JSON shape inspection rejects nested structures in scalar contracts.
    # The temporary VARIANT expression is never stored in curated Iceberg tables.
    shape = F.schema_of_variant(F.try_parse_json("_raw_json"))
    frame = add_error(frame, shape.rlike("ARRAY<|: OBJECT<"), "NON_SCALAR_FIELD")
    keys = F.transform(
        F.map_keys("_parsed"), lambda k: F.lower(F.regexp_replace(F.trim(k), "[ -]+", "_"))
    )
    collision = F.size(keys) != F.size(F.array_distinct(keys))
    frame = add_error(frame, collision, "FIELD_NAME_COLLISION")
    # Guard transform_keys: Spark raises on duplicate map keys in ANSI mode.
    frame = frame.withColumn(
        "_parsed",
        F.when(
            ~collision,
            F.transform_keys(
                "_parsed", lambda k, v: F.lower(F.regexp_replace(F.trim(k), "[ -]+", "_"))
            ),
        ),
    )
    unknown = F.array_except(F.map_keys("_parsed"), F.array(*[F.lit(k) for k in contract.fields]))
    frame = add_error(frame, F.size(unknown) > 0, "UNEXPECTED_FIELDS")
    for column, spec in contract.fields.items():
        raw = F.trim(F.col("_parsed")[column])
        raw = F.when(raw != "", raw)
        frame = frame.withColumn(column, raw)
        if spec.kind != "string":
            frame = frame.withColumn(column, F.expr(f"try_cast(`{column}` AS {spec.kind})"))
            frame = add_error(
                frame, raw.isNotNull() & F.col(column).isNull(), f"INVALID_TYPE:{column}"
            )
        elif spec.allowed:
            frame = frame.withColumn(column, F.upper(F.col(column)))
        if spec.required:
            frame = add_error(frame, F.col(column).isNull(), f"REQUIRED:{column}")
        if spec.allowed:
            frame = add_error(
                frame,
                F.col(column).isNotNull() & ~F.col(column).isin(*spec.allowed),
                f"INVALID_ENUM:{column}",
            )
        if spec.minimum is not None:
            frame = add_error(
                frame,
                (F.col(column) < spec.minimum)
                | F.isnan(column)
                | (F.abs(F.col(column)) == float("inf")),
                f"OUT_OF_RANGE:{column}",
            )
        if spec.kind == "timestamp":
            frame = add_error(
                frame,
                raw.isNotNull() & ~raw.rlike(r"(Z|[+-]\d{2}:\d{2})$"),
                f"TIMEZONE_REQUIRED:{column}",
            )
    return frame.drop("_parsed")


def foreign_keys(frame, name, parents):
    for column, (parent, key) in CONTRACTS[name].foreign_keys.items():
        reference = parents[parent].select(F.col(key).alias("_reference_key"))
        frame = frame.join(reference, frame[column] == reference["_reference_key"], "left")
        frame = add_error(
            frame,
            frame[column].isNotNull() & F.col("_reference_key").isNull(),
            f"FOREIGN_KEY:{column}",
        ).drop("_reference_key")
    return frame


def relationship(frame, parents, parent, child_key, parent_key, comparisons):
    """Check composite business relationships after ordinary FK validation."""
    columns = {right for _, right in comparisons}
    reference = parents[parent].select(
        F.col(parent_key).alias("_join_id"), *[F.col(c).alias(f"_ref_{c}") for c in sorted(columns)]
    )
    frame = frame.join(reference, frame[child_key] == reference["_join_id"], "left")
    for left, right in comparisons:
        frame = add_error(
            frame,
            F.col("_join_id").isNotNull() & ~F.col(left).eqNullSafe(F.col(f"_ref_{right}")),
            f"RELATIONSHIP:{child_key}:{left}",
        )
    return frame.drop("_join_id", *[f"_ref_{c}" for c in columns])


def business_rules(frame, name, parents):
    if name == "shipments":
        frame = add_error(
            frame, F.col("promised_at") < F.col("created_at"), "PROMISE_BEFORE_CREATION"
        )
        frame = add_error(
            frame, F.col("delivered_at") < F.col("created_at"), "DELIVERY_BEFORE_CREATION"
        )
        frame = add_error(
            frame,
            (F.col("status") == "DELIVERED") != F.col("delivered_at").isNotNull(),
            "DELIVERY_STATUS_TIMESTAMP_MISMATCH",
        )
        frame = add_error(
            frame,
            F.col("updated_at") < F.greatest("created_at", "delivered_at"),
            "REVISION_BEFORE_EVENT",
        )
        frame = relationship(
            frame,
            parents,
            "drivers",
            "assigned_driver_id",
            "driver_id",
            [("station_id", "station_id")],
        )
    elif name == "delivery_attempts":
        frame = add_error(
            frame,
            (F.col("status") == "FAILED") != F.col("failure_reason").isNotNull(),
            "FAILURE_REASON_MISMATCH",
        )
        frame = relationship(
            frame, parents, "drivers", "driver_id", "driver_id", [("station_id", "station_id")]
        )
        frame = relationship(
            frame,
            parents,
            "shipments",
            "shipment_id",
            "shipment_id",
            [("station_id", "station_id"), ("driver_id", "assigned_driver_id")],
        )
        reference = parents["shipments"].select(
            "shipment_id",
            F.col("created_at").alias("_created"),
            F.col("delivered_at").alias("_delivered"),
            F.col("status").alias("_shipment_status"),
        )
        frame = frame.join(reference, "shipment_id", "left")
        frame = add_error(
            frame, F.col("attempted_at") < F.col("_created"), "ATTEMPT_BEFORE_CREATION"
        )
        frame = add_error(
            frame,
            (F.col("status") == "DELIVERED")
            & (
                (F.col("_shipment_status") != "DELIVERED")
                | ~F.col("attempted_at").eqNullSafe(F.col("_delivered"))
            ),
            "SUCCESS_EVIDENCE_MISMATCH",
        )
        frame = add_error(
            frame,
            F.col("_shipment_status").isin("CANCELLED", "IN_TRANSIT"),
            "ATTEMPT_FOR_UNATTEMPTED_SHIPMENT",
        )
        frame = frame.drop("_created", "_delivered", "_shipment_status")
    elif name == "delivery_events":
        frame = relationship(
            frame,
            parents,
            "shipments",
            "shipment_id",
            "shipment_id",
            [("station_id", "station_id")],
        )
        frame = relationship(
            frame,
            parents,
            "delivery_attempts",
            "attempt_id",
            "attempt_id",
            [("shipment_id", "shipment_id"), ("station_id", "station_id")],
        )
        frame = add_error(
            frame,
            (F.col("event_type") != "CREATED") & F.col("attempt_id").isNull(),
            "ATTEMPT_EVENT_REQUIRES_ATTEMPT",
        )
    return frame


def deduplicate(frame, partition_columns, reason):
    window = Window.partitionBy(*partition_columns).orderBy(
        F.col("updated_at").desc_nulls_last(), "_record_hash", "_source_file", "_source_line"
    )
    ranked = frame.withColumn("_rank", F.row_number().over(window))
    losers = add_error(ranked.filter("_rank > 1"), F.lit(True), reason).drop("_rank")
    return ranked.filter("_rank = 1").drop("_rank"), losers


def transform(bronze: dict[str, DataFrame]) -> SilverResult:
    tables, quarantine, reports = {}, {}, []
    for name in CONTRACTS:
        frame = normalize(bronze[name], name)
        frame = business_rules(foreign_keys(frame, name, tables), name, tables)
        # Cut expensive validation/join lineage once, before splitting accepted/rejected rows.
        frame = frame.localCheckpoint(eager=True)
        rejected = frame.filter(F.size("_errors") > 0)
        candidates = frame.filter(F.size("_errors") == 0)
        candidates, duplicates = deduplicate(
            candidates, [CONTRACTS[name].primary_key], "DUPLICATE_PRIMARY_KEY"
        )
        rejected = rejected.unionByName(duplicates)
        if name == "delivery_attempts":
            candidates, duplicates = deduplicate(
                candidates, ["shipment_id", "attempt_number"], "DUPLICATE_ATTEMPT_NUMBER"
            )
            rejected = rejected.unionByName(duplicates)
        tables[name] = candidates.drop("_errors").localCheckpoint(eager=True)
        quarantine[name] = rejected.localCheckpoint(eager=True)
        reports.append(report(name, bronze[name], tables[name], quarantine[name]))
        frame.unpersist()
    return SilverResult(tables, quarantine, reports)
