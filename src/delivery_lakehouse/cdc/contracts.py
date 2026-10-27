"""Source protocol and explicit schema compatibility policy; no automatic DDL."""

import json
from datetime import UTC, datetime

from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.generator import canonical_json, digest

OPERATIONS = {"INSERT", "UPDATE", "DELETE"}
ENVELOPE_FIELDS = {
    "dataset",
    "record_id",
    "operation",
    "source_event_at",
    "source_version",
    "change_id",
    "batch_id",
    "data",
}


def source_schema(dataset):
    return {
        name: {"type": field.kind, "nullable": not field.required}
        for name, field in CONTRACTS[dataset].fields.items()
    }


def compare_schema(dataset, observed):
    expected = source_schema(dataset)
    diagnostics = []
    for name, definition in expected.items():
        if name not in observed:
            compatible = definition["nullable"]
            diagnostics.append(
                {
                    "field": name,
                    "change": "REMOVED_COLUMN",
                    "classification": "compatible" if compatible else "breaking",
                }
            )
        elif observed[name] != definition:
            diagnostics.append(
                {
                    "field": name,
                    "change": "TYPE_OR_NULLABILITY_CHANGED",
                    "classification": "breaking",
                    "expected": definition,
                    "observed": observed[name],
                }
            )
    for name, definition in observed.items():
        if name not in expected:
            compatible = (
                isinstance(definition, dict)
                and definition.get("nullable") is True
                and isinstance(definition.get("type"), str)
                and definition.get("type") in {"string", "int", "double", "date", "timestamp"}
            )
            diagnostics.append(
                {
                    "field": name,
                    "change": "ADDED_COLUMN",
                    "classification": "compatible" if compatible else "breaking",
                    "policy": "retain_raw_only" if compatible else "quarantine",
                }
            )
    removed = [d["field"] for d in diagnostics if d["change"] == "REMOVED_COLUMN"]
    added = [d["field"] for d in diagnostics if d["change"] == "ADDED_COLUMN"]
    if removed and added:
        diagnostics.append(
            {
                "change": "POSSIBLE_RENAME",
                "removed": removed,
                "added": added,
                "classification": "breaking",
                "policy": "explicit_migration_required",
            }
        )
    return diagnostics


def event_time(value):
    if not isinstance(value, str):
        raise ValueError("Source event timestamp must be an offset-aware string")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("Source event timestamp requires a timezone")
    return parsed.astimezone(UTC)


def change_identifier(change):
    identity = {key: value for key, value in change.items() if key not in {"batch_id", "change_id"}}
    return digest(canonical_json(identity).encode())


def unique_object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError(f"Duplicate JSON field: {name}")
        result[name] = value
    return result


def invalid_constant(value):
    raise ValueError(f"Non-standard JSON number: {value}")


def parse_json(raw):
    return json.loads(raw, object_pairs_hook=unique_object, parse_constant=invalid_constant)


def matches_type(value, kind):
    if kind in {"string", "date", "timestamp"}:
        return isinstance(value, str)
    if kind == "int":
        return type(value) is int
    if kind == "double":
        return type(value) in {int, float}
    return False


def validate_change(change, dataset, batch_id, schema):
    errors = []
    if not isinstance(change, dict) or set(change) != ENVELOPE_FIELDS:
        return ["INVALID_ENVELOPE_FIELDS"]
    if change["dataset"] != dataset or change["batch_id"] != batch_id:
        errors.append("SOURCE_IDENTITY_MISMATCH")
    if not isinstance(change["record_id"], str) or not change["record_id"].strip():
        errors.append("REQUIRED_RECORD_ID")
    elif change["record_id"] != change["record_id"].strip():
        # Silver trims strings; aliases must not create independent ordering ledgers.
        errors.append("NON_CANONICAL_RECORD_ID")
    if change["operation"] not in OPERATIONS:
        errors.append("INVALID_OPERATION")
    version = change["source_version"]
    if type(version) is not int or not 1 <= version <= 9223372036854775807:
        errors.append("INVALID_SOURCE_VERSION")
    try:
        event_time(change["source_event_at"])
    except (TypeError, ValueError):
        errors.append("INVALID_SOURCE_EVENT_TIME")
    if change["change_id"] != change_identifier(change):
        errors.append("CHANGE_ID_CHECKSUM_MISMATCH")
    if change["operation"] == "DELETE":
        if change["data"] is not None:
            errors.append("DELETE_REQUIRES_NULL_DATA")
        return errors
    payload = change["data"]
    if not isinstance(payload, dict):
        return [*errors, "AFTER_IMAGE_REQUIRED"]
    if payload.get(CONTRACTS[dataset].primary_key) != change["record_id"]:
        errors.append("PRIMARY_KEY_MISMATCH")
    for name, definition in schema.items():
        if not isinstance(definition, dict):
            errors.append(f"INVALID_SCHEMA_DEFINITION:{name}")
            continue
        value = payload.get(name)
        if value is None:
            if not definition.get("nullable", False):
                errors.append(f"REQUIRED_SOURCE_FIELD:{name}")
        elif not matches_type(value, definition.get("type")):
            errors.append(f"SOURCE_TYPE_CHANGED:{name}")
    errors.extend(f"UNDECLARED_SOURCE_FIELD:{name}" for name in payload if name not in schema)
    return errors
