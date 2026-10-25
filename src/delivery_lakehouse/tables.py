"""Explicit Iceberg v2 creation and atomic full-table snapshot replacement."""

from pyspark.sql import functions as F

from delivery_lakehouse.config import identifier

ICEBERG_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "snappy",
    "write.target-file-size-bytes": "134217728",
}


def write_iceberg(spark, frame, catalog, database, name, location):
    for component in (catalog, database, name):
        identifier(component)
    if "'" in location or ";" in location:
        raise ValueError("Unsafe table location")
    table = f"{catalog}.{database}.{name}"
    if spark.catalog.tableExists(table):
        properties = {
            row["key"]: row["value"] for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        }
        if properties.get("format-version") != "2":
            raise ValueError(f"Refusing to write non-v2 Iceberg table: {table}")
        # Iceberg performs one atomic metadata commit; empty frames clear old data too.
        frame.writeTo(table).overwrite(F.lit(True))
    else:
        writer = frame.writeTo(table).using("iceberg").tableProperty("location", location)
        for key, value in ICEBERG_PROPERTIES.items():
            writer = writer.tableProperty(key, value)
        writer.create()
    return table
