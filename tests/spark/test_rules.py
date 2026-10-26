import json

import pytest

from delivery_lakehouse.generator import generate
from delivery_lakehouse.transforms.silver import (
    BRONZE_SCHEMA,
    business_rules,
    deduplicate,
    foreign_keys,
    normalize,
)

pytestmark = pytest.mark.spark


def envelope(raw, index=1):
    return (raw, "fixture.jsonl", index, "a" * 64, "2026-02-06T00:00:00Z", str(index), str(index))


def test_malformed_and_strict_contract_validation(spark):
    base = generate(defects=False)["shipments"][0]
    records = [
        "not-json",
        json.dumps({**base, "status": "invalid"}),
        json.dumps({**base, "weight_kg": "NaN"}),
        json.dumps({**base, "weight_kg": "Infinity"}),
        json.dumps({**base, "created_at": "2026-01-01 00:00:00"}),
        json.dumps({**base, "extra": "unknown"}),
        json.dumps({**base, "Shipment ID": "collision"}),
        json.dumps({**base, "promised_at": "not-a-time"}),
        json.dumps({**base, "customer_id": " "}),
        json.dumps({**base, "service_level": {"nested": "EXPRESS"}}),
        json.dumps({**base, "service_level": ["EXPRESS"]}),
    ]
    frame = spark.createDataFrame(
        [envelope(raw, i) for i, raw in enumerate(records)], BRONZE_SCHEMA
    )
    actual = {r._source_line: set(r._errors) for r in normalize(frame, "shipments").collect()}
    for i, reason in enumerate(
        [
            "MALFORMED_JSON_OBJECT",
            "INVALID_ENUM:status",
            "OUT_OF_RANGE:weight_kg",
            "OUT_OF_RANGE:weight_kg",
            "TIMEZONE_REQUIRED:created_at",
            "UNEXPECTED_FIELDS",
            "FIELD_NAME_COLLISION",
            "INVALID_TYPE:promised_at",
            "REQUIRED:customer_id",
            "NON_SCALAR_FIELD",
            "NON_SCALAR_FIELD",
        ]
    ):
        assert reason in actual[i]


def test_field_normalization_and_missing_optional_values(spark):
    row = generate(defects=False)["customers"][0]
    row["Customer ID"] = f"  {row.pop('customer_id')} "
    row["region"] = " nsw "
    result = normalize(
        spark.createDataFrame([envelope(json.dumps(row))], BRONZE_SCHEMA), "customers"
    ).first()
    assert result.customer_id == "CU0000000"
    assert result.region == "NSW"
    assert result._errors == []


def test_deduplication_uses_newest_valid_source_revision(spark):
    row = generate(defects=False)["customers"][0]
    newer = {**row, "address_type": "BUSINESS", "updated_at": "2026-03-01T00:00:00Z"}
    frame = normalize(
        spark.createDataFrame(
            [envelope(json.dumps(row), 1), envelope(json.dumps(newer), 2)], BRONZE_SCHEMA
        ),
        "customers",
    )
    for input_frame in (frame, frame.orderBy(frame._source_line.desc())):
        winner, losers = deduplicate(input_frame, ["customer_id"], "DUPLICATE_PRIMARY_KEY")
        assert winner.first().address_type == "BUSINESS"
        assert losers.count() == 1
        assert losers.first()._errors == ["DUPLICATE_PRIMARY_KEY"]


def test_foreign_key_and_cross_station_assignment(spark, local_run):
    _, _, _, result = local_run
    parents = {
        name: spark.read.parquet(path)
        for table, path in result["tables"].items()
        if table.startswith("silver.")
        for name in [table.split(".")[1]]
    }
    row = generate(defects=False)["shipments"][0]
    row.update(customer_id="missing", assigned_driver_id="DR0001")
    frame = normalize(
        spark.createDataFrame([envelope(json.dumps(row))], BRONZE_SCHEMA), "shipments"
    )
    checked = business_rules(
        foreign_keys(frame, "shipments", parents), "shipments", parents
    ).first()
    assert "FOREIGN_KEY:customer_id" in checked._errors
    assert "RELATIONSHIP:assigned_driver_id:station_id" in checked._errors
