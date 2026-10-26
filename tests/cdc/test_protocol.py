from copy import deepcopy

import pytest

from delivery_lakehouse.cdc.contracts import (
    change_identifier,
    compare_schema,
    parse_json,
    source_schema,
    validate_change,
)
from delivery_lakehouse.cdc.generator import chronological_changes, make_change, write_sequence
from delivery_lakehouse.generator import generate
from delivery_lakehouse.ingestion.storage import LocalStore


def test_chronological_generator_is_deterministic_and_has_all_operations(tmp_path):
    assert chronological_changes() == chronological_changes()
    assert chronological_changes(seed=43) != chronological_changes(seed=42)
    store = LocalStore(tmp_path)
    first = write_sequence(store)
    before = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*.json*")}
    assert write_sequence(store) == first
    assert before == {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*.json*")}
    changes = [change for _, _, batch in chronological_changes() for change in batch]
    assert {change["operation"] for change in changes} == {"INSERT", "UPDATE", "DELETE"}
    assert any(change["data"] and change["data"].get("status") == "CANCELLED" for change in changes)
    assert len({change["change_id"] for change in changes}) < len(changes)


@pytest.mark.parametrize(
    "mutation,classification,reason",
    [
        ("nullable_addition", "compatible", "ADDED_COLUMN"),
        ("required_addition", "breaking", "ADDED_COLUMN"),
        ("missing_required", "breaking", "REMOVED_COLUMN"),
        ("rename", "breaking", "POSSIBLE_RENAME"),
        ("type", "breaking", "TYPE_OR_NULLABILITY_CHANGED"),
        ("nullable_required", "breaking", "TYPE_OR_NULLABILITY_CHANGED"),
        ("missing_optional", "compatible", "REMOVED_COLUMN"),
    ],
)
def test_schema_compatibility_policy(mutation, classification, reason):
    schema = source_schema("shipments")
    if mutation.endswith("addition"):
        schema["notes"] = {"type": "string", "nullable": mutation == "nullable_addition"}
    elif mutation == "missing_required":
        schema.pop("station_id")
    elif mutation == "rename":
        schema["station_code"] = schema.pop("station_id")
    elif mutation == "type":
        schema["weight_kg"]["type"] = "string"
    elif mutation == "nullable_required":
        schema["station_id"]["nullable"] = True
    else:
        schema.pop("delivered_at")
    diagnostics = compare_schema("shipments", schema)
    assert any(
        item["change"] == reason and item["classification"] == classification
        for item in diagnostics
    )
    assert not compare_schema("shipments", source_schema("shipments"))


@pytest.mark.parametrize(
    "mutation,reason",
    [
        ({"operation": "UPSERT"}, "INVALID_OPERATION"),
        ({"source_version": True}, "INVALID_SOURCE_VERSION"),
        ({"source_version": -1}, "INVALID_SOURCE_VERSION"),
        ({"source_version": 2**63}, "INVALID_SOURCE_VERSION"),
        ({"source_event_at": "2026-02-07T09:00:00"}, "INVALID_SOURCE_EVENT_TIME"),
        ({"batch_id": "b" * 64}, "SOURCE_IDENTITY_MISMATCH"),
        ({"record_id": "other"}, "PRIMARY_KEY_MISMATCH"),
    ],
)
def test_invalid_envelopes(mutation, reason):
    row = generate(defects=False)["shipments"][0]
    change = {
        **make_change("shipments", row, "UPDATE", 2, "2026-02-07T09:00:00Z"),
        "batch_id": "a" * 64,
    }
    change.update(mutation)
    assert reason in validate_change(change, "shipments", "a" * 64, source_schema("shipments"))


def test_change_identity_ignores_delivery_batch_but_detects_changed_payload():
    row = generate(defects=False)["shipments"][0]
    change = make_change("shipments", row, "UPDATE", 2, "2026-02-07T09:00:00Z")
    assert change_identifier({**change, "batch_id": "a" * 64}) == change["change_id"]
    corrupted = deepcopy(change)
    corrupted["data"]["weight_kg"] = 10.5
    corrupted["batch_id"] = "a" * 64
    assert "CHANGE_ID_CHECKSUM_MISMATCH" in validate_change(
        corrupted, "shipments", "a" * 64, source_schema("shipments")
    )


def test_key_whitespace_cannot_bypass_the_version_ledger():
    row = generate(defects=False)["shipments"][0]
    row["shipment_id"] = f" {row['shipment_id']} "
    change = {
        **make_change("shipments", row, "UPDATE", 2, "2026-02-07T09:00:00Z"),
        "batch_id": "a" * 64,
    }
    assert validate_change(change, "shipments", "a" * 64, source_schema("shipments")) == [
        "NON_CANONICAL_RECORD_ID"
    ]


@pytest.mark.parametrize("raw", ['{"id":1,"id":2}', '{"x":NaN}', '{"x":Infinity}', "not JSON"])
def test_json_does_not_silently_accept_duplicate_keys_or_nonstandard_numbers(raw):
    with pytest.raises(ValueError):
        parse_json(raw)
