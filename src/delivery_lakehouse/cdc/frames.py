"""Small Spark schemas and conversion helpers for the CDC protocol."""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from delivery_lakehouse.cdc.contracts import (
    compare_schema,
    event_time,
    parse_json,
    validate_change,
)
from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.generator import canonical_json
from delivery_lakehouse.transforms.silver import BRONZE_SCHEMA, METADATA

TYPES = {
    "string": StringType(),
    "int": IntegerType(),
    "double": DoubleType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}
CHANGE_SCHEMA = StructType(
    [
        *BRONZE_SCHEMA.fields,
        StructField("_operation", StringType()),
        StructField("_source_version", LongType()),
        StructField("_source_event_at", TimestampType()),
        StructField("_change_id", StringType()),
        StructField("_record_key", StringType()),
        StructField("_cdc_raw", StringType()),
        StructField("_validation_json", StringType()),
    ]
)
CHANGE_COLUMNS = [field.name for field in CHANGE_SCHEMA if field.name not in METADATA]
AUDIT_SCHEMA = StructType([*BRONZE_SCHEMA.fields, StructField("_reasons", ArrayType(StringType()))])
RECORD_STATE_SCHEMA = StructType(
    [
        StructField("dataset", StringType()),
        StructField("record_key", StringType()),
        StructField("source_version", LongType()),
        StructField("source_event_at", TimestampType()),
        StructField("change_id", StringType()),
        StructField("is_deleted", BooleanType()),
        StructField("batch_id", StringType()),
    ]
)


def business_schema(dataset):
    return StructType(
        [
            *BRONZE_SCHEMA.fields,
            *[
                StructField(name, TYPES[field.kind])
                for name, field in CONTRACTS[dataset].fields.items()
            ],
        ]
    )


def decode_bronze(spark, store, bronze):
    changes, rejected, diagnostics = {}, {}, {}
    manifest = bronze["source_manifest"]
    for dataset in CONTRACTS:
        valid_rows, invalid_rows = [], []
        schema = manifest["schemas"].get(dataset, {})
        drift = compare_schema(dataset, schema) if dataset in manifest["files"] else []
        diagnostics[dataset] = drift
        breaking = any(item["classification"] == "breaking" for item in drift)
        if dataset in bronze["files"]:
            for line in store.read(bronze["files"][dataset]["key"]).decode().splitlines():
                envelope = parse_json(line)
                try:
                    change = parse_json(envelope["_raw_json"])
                    errors = validate_change(change, dataset, manifest["batch_id"], schema)
                except (TypeError, ValueError, KeyError):
                    errors = ["MALFORMED_CDC_ENVELOPE"]
                if breaking:
                    errors.append("BREAKING_SCHEMA_DRIFT")
                if errors:
                    invalid_rows.append({**envelope, "_reasons": sorted(set(errors))})
                    continue
                payload = change["data"] or {}
                known = {
                    key: value for key, value in payload.items() if key in CONTRACTS[dataset].fields
                }
                valid_rows.append(
                    {
                        **envelope,
                        "_raw_json": canonical_json(payload),
                        "_operation": change["operation"],
                        "_source_version": change["source_version"],
                        "_source_event_at": event_time(change["source_event_at"]),
                        "_change_id": change["change_id"],
                        "_record_key": change["record_id"],
                        "_cdc_raw": envelope["_raw_json"],
                        "_validation_json": canonical_json(known),
                    }
                )
        changes[dataset] = spark.createDataFrame(valid_rows, CHANGE_SCHEMA)
        rejected[dataset] = spark.createDataFrame(invalid_rows, AUDIT_SCHEMA)
    return changes, rejected, diagnostics


def audit_rows(frame, reasons):
    return frame.select(
        *[
            F.col(name) if name != "_raw_json" else F.col("_cdc_raw").alias(name)
            for name in METADATA
        ],
        reasons.alias("_reasons"),
    )


def delta_for_keys(current_schema, new_rows, affected, keys):
    """Include explicit delete rows for keys no longer present in the after-state."""
    marked = new_rows.withColumn("_present", F.lit(True))
    joined = affected.select(*keys).distinct().join(marked, keys, "left")
    return joined.select(
        *[F.col(field.name).cast(field.dataType).alias(field.name) for field in current_schema],
        F.col("_present").isNull().alias("_delete"),
    )
