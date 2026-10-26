"""Spark session configuration. Glue supplies its own Iceberg libraries."""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def session(*, local=True, iceberg_jar=None, warehouse=None):
    builder = SparkSession.builder.appName("enterprise-delivery-operations")
    if local:
        os.environ["PYSPARK_PYTHON"] = sys.executable
        builder = builder.master("local[2]").config("spark.ui.enabled", "false")
    builder = (
        builder.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2" if local else "8")
        .config("spark.sql.adaptive.enabled", "true")
    )
    if iceberg_jar:
        jdbc_jar = Path(iceberg_jar).parent / "h2-2.4.240.jar"
        if not jdbc_jar.is_file():
            raise ValueError(
                "Run scripts/fetch_iceberg.py to fetch the local H2 catalog dependency"
            )
        builder = (
            builder.config("spark.jars", f"{iceberg_jar},{jdbc_jar}")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config(
                "spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog"
            )
            # Iceberg probes JDBC metadata with lowercase identifiers when reopening.
            # H2's uppercase default makes an existing v1 schema look unmigrated.
            .config(
                "spark.sql.catalog.lakehouse.uri",
                f"jdbc:h2:{warehouse}/catalog;DATABASE_TO_LOWER=TRUE",
            )
            .config("spark.sql.catalog.lakehouse.warehouse", str(warehouse))
            .config("spark.sql.catalog.lakehouse.jdbc.schema-version", "V1")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
