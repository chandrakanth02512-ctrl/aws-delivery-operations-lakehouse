"""Explicit Iceberg v2 creation and atomic full-table snapshot replacement."""

from pathlib import Path
from urllib.parse import unquote, urlparse

from pyspark.sql import functions as F

from delivery_lakehouse.config import identifier

ICEBERG_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "snappy",
    "write.target-file-size-bytes": "134217728",
}


def normalized_location(location):
    parsed = urlparse(location)
    if parsed.scheme in {"", "file"}:
        return Path(unquote(parsed.path)).resolve().as_uri().rstrip("/")
    return location.rstrip("/")


def validate_existing_table(spark, frame, table, location):
    """Bind both snapshot and CDC writes to the reviewed schema and storage root."""
    properties = {row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    if properties.get("format-version") != "2":
        raise ValueError(f"ICEBERG_V2_REQUIRED: refusing non-v2 Iceberg table: {table}")
    actual = {field.name: field.dataType for field in spark.table(table).schema}
    expected = {field.name: field.dataType for field in frame.schema}
    if actual != expected:
        raise ValueError(f"EXPLICIT_SCHEMA_MIGRATION_REQUIRED: {table}")
    details = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    actual_location = next(row.data_type for row in details if row.col_name == "Location")
    if normalized_location(actual_location) != normalized_location(location):
        raise ValueError(f"TABLE_LOCATION_MISMATCH: {table}")


def write_iceberg(spark, frame, catalog, database, name, location):
    for component in (catalog, database, name):
        identifier(component)
    if "'" in location or ";" in location:
        raise ValueError("Unsafe table location")
    table = f"{catalog}.{database}.{name}"
    if spark.catalog.tableExists(table):
        validate_existing_table(spark, frame, table, location)
        # Iceberg performs one atomic metadata commit; empty frames clear old data too.
        frame.writeTo(table).overwrite(F.lit(True))
    else:
        writer = frame.writeTo(table).using("iceberg").tableProperty("location", location)
        for key, value in ICEBERG_PROPERTIES.items():
            writer = writer.tableProperty(key, value)
        writer.create()
    return table
