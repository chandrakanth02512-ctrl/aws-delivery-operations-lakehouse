"""Immutable CDC Bronze objects; partial ingestion resumes without replacing objects."""

from datetime import UTC, datetime

from delivery_lakehouse.cdc.contracts import event_time, parse_json
from delivery_lakehouse.config import batch_identifier
from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.generator import canonical_json, digest


def read_manifest(store, batch_id):
    batch_identifier(batch_id)
    manifest = parse_json(store.read(f"landing/cdc/{batch_id}/manifest.json"))
    if not isinstance(manifest, dict) or manifest.get("batch_id") != batch_id:
        raise ValueError("MANIFEST_IDENTITY_MISMATCH")
    expected = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    if digest(canonical_json(expected).encode()) != manifest.get("manifest_sha256"):
        raise ValueError("MANIFEST_CHECKSUM_MISMATCH")
    if manifest.get("manifest_version") != 2 or manifest.get("source") != "synthetic_delivery_ops":
        raise ValueError("UNSUPPORTED_SOURCE_PROTOCOL")
    event_time(manifest["created_at"])
    files = manifest["files"]
    if not files or not set(files) <= set(CONTRACTS) or set(files) != set(manifest["schemas"]):
        raise ValueError("INVALID_DATASET_MANIFEST")
    for dataset, entry in files.items():
        if (
            entry["file"] != f"{dataset}.jsonl"
            or type(entry["rows"]) is not int
            or entry["rows"] < 1
        ):
            raise ValueError("INVALID_SOURCE_FILE_CONTRACT")
        batch_identifier(entry["sha256"])
        if not isinstance(manifest["schemas"][dataset], dict):
            raise ValueError("INVALID_SCHEMA_DECLARATION")
    return manifest


def ingest_changes(store, batch_id):
    manifest = read_manifest(store, batch_id)
    # Validate every source before creating any Bronze object, including on replay.
    for entry in manifest["files"].values():
        content = store.read(f"landing/cdc/{batch_id}/{entry['file']}")
        if digest(content) != entry["sha256"] or len(content.splitlines()) != entry["rows"]:
            raise ValueError("SOURCE_FILE_INTEGRITY_FAILURE")
        content.decode("utf-8")
    prefix = f"bronze/cdc/{batch_id}"
    header_key = f"{prefix}/ingestion.json"
    if not store.exists(header_key):
        header = {
            "batch_id": batch_id,
            "manifest_sha256": manifest["manifest_sha256"],
            "ingested_at": datetime.now(UTC).isoformat(),
        }
        store.write(header_key, canonical_json(header).encode(), immutable=True)
    header = parse_json(store.read(header_key))
    if header["manifest_sha256"] != manifest["manifest_sha256"]:
        raise ValueError("BATCH_ID_REUSED_WITH_DIFFERENT_MANIFEST")
    commit_key = f"{prefix}/manifest.json"
    if store.exists(commit_key):
        committed = parse_json(store.read(commit_key))
        payload = {
            key: value for key, value in committed.items() if key != "bronze_manifest_sha256"
        }
        if (
            digest(canonical_json(payload).encode()) != committed.get("bronze_manifest_sha256")
            or committed.get("source_manifest") != manifest
            or any(committed.get(key) != value for key, value in header.items())
        ):
            raise ValueError("COMMITTED_BRONZE_MANIFEST_CORRUPTED")
        for entry in committed["files"].values():
            if digest(store.read(entry["key"])) != entry["sha256"]:
                raise ValueError("COMMITTED_BRONZE_CORRUPTED")
        return committed
    outputs = {}
    for dataset, entry in manifest["files"].items():
        source_key = f"landing/cdc/{batch_id}/{entry['file']}"
        envelopes = []
        for number, raw in enumerate(store.read(source_key).decode().splitlines(), 1):
            envelopes.append(
                {
                    "_raw_json": raw,
                    "_batch_id": batch_id,
                    "_source_file": store.uri(source_key),
                    "_source_line": number,
                    "_ingested_at": header["ingested_at"],
                    "_record_hash": digest(raw.encode()),
                    "_record_id": digest(f"{batch_id}/{dataset}/{number}".encode()),
                }
            )
        content = "".join(canonical_json(row) + "\n" for row in envelopes).encode()
        key = f"{prefix}/{dataset}.jsonl"
        store.write(key, content, immutable=True)
        outputs[dataset] = {"key": key, "sha256": digest(content), "rows": len(envelopes)}
    committed = {**header, "source_manifest": manifest, "files": outputs}
    committed["bronze_manifest_sha256"] = digest(canonical_json(committed).encode())
    store.write(commit_key, canonical_json(committed).encode(), immutable=True)
    return committed
