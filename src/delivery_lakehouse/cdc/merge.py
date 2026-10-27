"""Iceberg v2 MERGE primitives. Existing schemas require an explicit migration."""

import uuid

from delivery_lakehouse.config import identifier
from delivery_lakehouse.tables import validate_existing_table, write_iceberg


def table_identifier(catalog, database, name):
    return ".".join(identifier(part) for part in (catalog, database, name))


def ensure_table(spark, schema_frame, catalog, database, name, location):
    table = table_identifier(catalog, database, name)
    if not spark.catalog.tableExists(table):
        write_iceberg(spark, schema_frame.limit(0), catalog, database, name, location)
    else:
        validate_existing_table(spark, schema_frame, table, location)
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
