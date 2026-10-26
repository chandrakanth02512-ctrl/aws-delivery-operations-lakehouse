"""Iceberg v2 MERGE primitives. Existing schemas require an explicit migration."""

import uuid
from pathlib import Path
from urllib.parse import unquote, urlparse

from delivery_lakehouse.config import identifier
from delivery_lakehouse.tables import write_iceberg


def table_identifier(catalog, database, name):
    return ".".join(identifier(part) for part in (catalog, database, name))


def normalized_location(location):
    parsed = urlparse(location)
    if parsed.scheme in {"", "file"}:
        return Path(unquote(parsed.path)).resolve().as_uri().rstrip("/")
    return location.rstrip("/")


def ensure_table(spark, schema_frame, catalog, database, name, location):
    table = table_identifier(catalog, database, name)
    if not spark.catalog.tableExists(table):
        write_iceberg(spark, schema_frame.limit(0), catalog, database, name, location)
    else:
        properties = {
            row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        }
        if properties.get("format-version") != "2":
            raise ValueError(f"ICEBERG_V2_REQUIRED: {table}")
        actual = {field.name: field.dataType for field in spark.table(table).schema}
        expected = {field.name: field.dataType for field in schema_frame.schema}
        if actual != expected:
            raise ValueError(f"EXPLICIT_SCHEMA_MIGRATION_REQUIRED: {table}")
        details = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
        actual_location = next(row.data_type for row in details if row.col_name == "Location")
        if normalized_location(actual_location) != normalized_location(location):
            raise ValueError(f"TABLE_LOCATION_MISMATCH: {table}")
    return table


def merge_delta(spark, target, delta, keys, *, ordered=False):
    for part in target.split("."):
        identifier(part)
    for key in keys:
        identifier(key)
    if delta.groupBy(*keys).count().filter("count > 1").limit(1).count():
        raise ValueError(f"MERGE_SOURCE_KEYS_NOT_UNIQUE: {target}")
    view = f"cdc_merge_{uuid.uuid4().hex}"
    delta.createOrReplaceTempView(view)
    columns = [field.name for field in spark.table(target).schema]
    join = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
    assignments = ", ".join(f"t.`{column}` = s.`{column}`" for column in columns)
    names = ", ".join(f"`{column}`" for column in columns)
    values = ", ".join(f"s.`{column}`" for column in columns)
    order_guard = ""
    if ordered:
        order_guard = (
            " AND struct(s.source_version, s.source_event_at, s.change_id) > "
            "struct(t.source_version, t.source_event_at, t.change_id)"
        )
    try:
        spark.sql(f"""
            MERGE INTO {target} t USING {view} s ON {join}
            WHEN MATCHED AND s._delete {order_guard} THEN DELETE
            WHEN MATCHED AND NOT s._delete {order_guard} THEN UPDATE SET {assignments}
            WHEN NOT MATCHED AND NOT s._delete THEN INSERT ({names}) VALUES ({values})
        """)
    finally:
        spark.catalog.dropTempView(view)
