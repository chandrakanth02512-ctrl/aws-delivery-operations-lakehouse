"""Verify written after-images before the pipeline publishes its success marker."""

from pyspark.sql import functions as F

from delivery_lakehouse.quality.report import QualityGateError


def compare_rows(expected, actual, label):
    if {f.name: f.dataType for f in expected.schema} != {f.name: f.dataType for f in actual.schema}:
        raise QualityGateError(f"PUBLICATION_VERIFICATION_FAILED: schema of {label}")
    actual = actual.select(*expected.columns)
    differences = expected.exceptAll(actual).unionByName(actual.exceptAll(expected))
    if differences.limit(1).count():
        raise QualityGateError(f"PUBLICATION_VERIFICATION_FAILED: rows of {label}")
    return expected.count()


def verify_publication(spark, store, context):
    verified = {}
    if context["mode"] == "cdc":
        for entry in context["plan"]["entries"]:
            staged = spark.read.parquet(store.uri(entry["staged_key"]))
            keys = entry["keys"]
            expected = staged.filter(~F.col("_delete")).drop("_delete")
            # Include deleted keys in the read so a surviving deleted row also fails verification.
            actual = spark.table(entry["target"]).join(staged.select(*keys).distinct(), keys)
            if not entry["rows"]:
                actual = spark.table(entry["target"])
            verified[entry["step"]] = compare_rows(expected, actual, entry["step"])
        scope = "changed_keys"
    else:
        for layer, tables in context["expected"].items():
            for name, expected in tables.items():
                label = f"{layer}.{name}"
                location = context["tables"][label]
                actual = (
                    spark.table(location) if context["catalog"] else spark.read.parquet(location)
                )
                verified[label] = compare_rows(expected, actual, label)
        scope = "full_snapshot"
    return {
        "status": "SUCCEEDED",
        "scope": scope,
        "tables": verified,
        "tables_verified": len(verified),
        "rows_verified": sum(verified.values()),
    }
