"""Deterministic chronological batches; business time can precede arrival time."""

from copy import deepcopy

from delivery_lakehouse.cdc.contracts import change_identifier, source_schema
from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.generator import canonical_json, digest, generate


def make_change(dataset, record, operation, source_version, source_event_at, *, record_id=None):
    change = {
        "dataset": dataset,
        "record_id": record_id or record[CONTRACTS[dataset].primary_key],
        "operation": operation,
        "source_version": source_version,
        "source_event_at": source_event_at,
        "data": deepcopy(record),
    }
    change["change_id"] = change_identifier(change)
    return change


def write_batch(store, name, created_at, changes, *, schemas=None):
    datasets = sorted({change["dataset"] for change in changes})
    schemas = schemas or {dataset: source_schema(dataset) for dataset in datasets}
    # Batch identity is computed before adding batch_id to avoid a circular content hash.
    changes = [{k: v for k, v in change.items() if k != "batch_id"} for change in changes]
    identity = {"name": name, "created_at": created_at, "changes": changes, "schemas": schemas}
    batch_id = digest(canonical_json(identity).encode())
    files = {}
    for dataset in datasets:
        rows = [
            {**change, "batch_id": batch_id} for change in changes if change["dataset"] == dataset
        ]
        content = "".join(canonical_json(row) + "\n" for row in rows).encode()
        filename = f"{dataset}.jsonl"
        store.write(f"landing/cdc/{batch_id}/{filename}", content, immutable=True)
        files[dataset] = {"file": filename, "sha256": digest(content), "rows": len(rows)}
    manifest = {
        "manifest_version": 2,
        "source": "synthetic_delivery_ops",
        "batch_id": batch_id,
        "batch_name": name,
        "created_at": created_at,
        "schemas": schemas,
        "files": files,
    }
    manifest["manifest_sha256"] = digest(canonical_json(manifest).encode())
    store.write(
        f"landing/cdc/{batch_id}/manifest.json", canonical_json(manifest).encode(), immutable=True
    )
    return manifest


def chronological_changes(profile="small", seed=42):
    source = generate(profile=profile, seed=seed, defects=False)
    bootstrap = [
        make_change(name, row, "INSERT", 1, "2026-02-06T09:00:00Z")
        for name, rows in source.items()
        for row in rows
    ]
    first = {
        **source["shipments"][0],
        "shipment_id": "SHCDC0001",
        "status": "IN_TRANSIT",
        "created_at": "2026-02-07T01:00:00Z",
        "promised_at": "2026-02-09T07:00:00Z",
        "delivered_at": None,
        "updated_at": "2026-02-07T01:00:00Z",
        "assigned_driver_id": "DR0000",
    }
    second = {**first, "shipment_id": "SHCDC0002"}
    opened = [
        make_change("shipments", row, "INSERT", 1, "2026-02-07T01:00:00Z")
        for row in (first, second)
    ]
    opened.append(deepcopy(opened[0]))
    for row in (first, second):
        opened.append(
            make_change(
                "delivery_events",
                {
                    "event_id": f"{row['shipment_id']}-created",
                    "shipment_id": row["shipment_id"],
                    "attempt_id": None,
                    "station_id": "ST000",
                    "event_type": "CREATED",
                    "event_at": row["created_at"],
                    "updated_at": row["updated_at"],
                },
                "INSERT",
                1,
                "2026-02-07T01:00:00Z",
            )
        )
    attempt_correction = {
        **source["delivery_attempts"][0],
        "duration_minutes": 9.5,
        "updated_at": "2026-02-07T09:00:00Z",
    }
    opened.append(
        make_change("delivery_attempts", attempt_correction, "UPDATE", 2, "2026-02-07T09:00:00Z")
    )
    reassigned = {**first, "assigned_driver_id": "DR0003", "updated_at": "2026-02-08T09:00:00Z"}
    cancelled = {**second, "status": "CANCELLED", "updated_at": "2026-02-08T10:00:00Z"}
    changed = [
        make_change("shipments", reassigned, "UPDATE", 2, "2026-02-08T09:00:00Z"),
        make_change("shipments", cancelled, "UPDATE", 2, "2026-02-08T10:00:00Z"),
        make_change("shipments", None, "DELETE", 2, "2026-02-10T09:00:00Z", record_id="SH00000019"),
        make_change(
            "delivery_events", None, "DELETE", 2, "2026-02-10T09:00:00Z", record_id="EV00000019-00"
        ),
        deepcopy(opened[0]),
    ]
    delivered = {
        **reassigned,
        "status": "DELIVERED",
        "delivered_at": "2026-02-09T08:00:00Z",
        "updated_at": "2026-02-09T08:00:00Z",
        "mobile_sync_note": "Confirmation arrived Wednesday",
    }
    late = [make_change("shipments", delivered, "UPDATE", 3, "2026-02-09T08:00:00Z")]
    for number, status, hour in [(1, "FAILED", 6), (2, "DELIVERED", 8)]:
        attempted = f"2026-02-09T{hour:02}:00:00Z"
        attempt = {
            "attempt_id": f"ATCDC0001-{number}",
            "shipment_id": first["shipment_id"],
            "driver_id": "DR0003",
            "station_id": "ST000",
            "attempt_number": number,
            "attempted_at": attempted,
            "status": status,
            "failure_reason": "CUSTOMER_UNAVAILABLE" if status == "FAILED" else None,
            "duration_minutes": 8.0,
            "updated_at": attempted,
        }
        late.append(make_change("delivery_attempts", attempt, "INSERT", 1, attempted))
        late.append(
            make_change(
                "delivery_events",
                {
                    "event_id": f"EVCDC0001-{number}",
                    "shipment_id": first["shipment_id"],
                    "attempt_id": attempt["attempt_id"],
                    "station_id": "ST000",
                    "event_type": "ATTEMPT_FAILED" if status == "FAILED" else "DELIVERED",
                    "event_at": attempted,
                    "updated_at": attempted,
                },
                "INSERT",
                1,
                attempted,
            )
        )
    late.append(deepcopy(late[1]))
    return [
        ("bootstrap", "2026-02-06T12:00:00Z", bootstrap),
        ("new-shipments", "2026-02-07T12:00:00Z", opened),
        ("assignment-cancellation-delete", "2026-02-10T12:00:00Z", changed),
        ("late-monday-confirmation", "2026-02-11T12:00:00Z", late),
    ]


def write_sequence(store, profile="small", seed=42):
    manifests = []
    for name, created_at, changes in chronological_changes(profile, seed):
        schemas = {change["dataset"]: source_schema(change["dataset"]) for change in changes}
        if name == "late-monday-confirmation":
            schemas["shipments"]["mobile_sync_note"] = {"type": "string", "nullable": True}
        manifests.append(write_batch(store, name, created_at, changes, schemas=schemas))
    return manifests
