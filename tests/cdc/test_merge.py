import os

import pytest

from delivery_lakehouse.cdc.merge import ensure_table, merge_delta

pytestmark = [
    pytest.mark.spark,
    pytest.mark.iceberg,
    pytest.mark.skipif(
        not os.environ.get("ICEBERG_SPARK_JAR"), reason="Local Iceberg JAR required"
    ),
]


def test_merge_is_repeatable_and_rejects_duplicate_source_keys(spark, cdc_context):
    store, settings = cdc_context
    database = f"{settings.database_prefix}_silver"
    rows = spark.createDataFrame([(1, "original")], "id int, value string")
    table = ensure_table(
        spark, rows, "lakehouse", database, "merge_guard", store.uri("silver/test")
    )
    first = spark.createDataFrame([(1, "original", False)], "id int, value string, _delete boolean")
    merge_delta(spark, table, first, ["id"])
    delta = spark.createDataFrame(
        [(1, None, True), (2, "new", False)], "id int, value string, _delete boolean"
    )
    merge_delta(spark, table, delta, ["id"])
    merge_delta(spark, table, delta, ["id"])
    assert [row.asDict() for row in spark.table(table).collect()] == [{"id": 2, "value": "new"}]
    with pytest.raises(ValueError, match="MERGE_SOURCE_KEYS_NOT_UNIQUE"):
        merge_delta(spark, table, delta.unionByName(delta), ["id"])


def test_existing_targets_require_v2_schema_and_location_match(spark, cdc_context):
    store, settings = cdc_context
    database = f"{settings.database_prefix}_silver"
    rows = spark.createDataFrame([], "id int")
    location = store.uri("silver/guard")
    table = ensure_table(spark, rows, "lakehouse", database, "target_guard", location)
    with pytest.raises(ValueError, match="TABLE_LOCATION_MISMATCH"):
        ensure_table(spark, rows, "lakehouse", database, "target_guard", store.uri("silver/other"))
    with pytest.raises(ValueError, match="EXPLICIT_SCHEMA_MIGRATION_REQUIRED"):
        ensure_table(
            spark,
            spark.createDataFrame([], "id string"),
            "lakehouse",
            database,
            "target_guard",
            location,
        )
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('format-version'='3')")
    with pytest.raises(ValueError, match="ICEBERG_V2_REQUIRED"):
        ensure_table(spark, rows, "lakehouse", database, "target_guard", location)


def test_local_catalog_reopens_with_an_independent_jdbc_connection(spark, cdc_context):
    _, settings = cdc_context
    catalog = spark._jvm.org.apache.iceberg.jdbc.JdbcCatalog()
    properties = spark._jvm.java.util.HashMap()
    for name in ("uri", "warehouse", "jdbc.schema-version"):
        properties.put(name, spark.conf.get(f"spark.sql.catalog.lakehouse.{name}"))
    try:
        catalog.initialize("lakehouse", properties)
        namespaces = {str(namespace) for namespace in catalog.listNamespaces()}
        assert f"{settings.database_prefix}_silver" in namespaces
    finally:
        catalog.close()
