"""Validate batch integrity, retain every source line, commit a manifest last."""

import json
import logging
from datetime import UTC, datetime

from delivery_lakehouse.config import batch_identifier
from delivery_lakehouse.contracts import CONTRACT_VERSION, CONTRACTS
from delivery_lakehouse.generator import canonical_json, digest
from delivery_lakehouse.ingestion.storage import Store

LOGGER = logging.getLogger(__name__)


def validate_manifest(manifest: dict, batch_id: str):
    batch_identifier(batch_id)
    if manifest.get("batch_id") != batch_id:
        raise ValueError("Manifest batch identity mismatch")
    if manifest.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("Unsupported contract version")
    if set(manifest["files"]) != set(CONTRACTS):
        raise ValueError("Manifest must contain exactly the six contracted datasets")
    payload = {"contract_version": manifest["contract_version"], "files": manifest["files"]}
    if digest(canonical_json(payload).encode()) != batch_id:
        raise ValueError("Manifest content does not match batch digest")
    for name, entry in manifest["files"].items():
        if entry["file"] != f"{name}.jsonl" or not isinstance(entry["rows"], int):
            raise ValueError("Invalid source file contract")
        batch_identifier(entry["sha256"])
        if entry["rows"] < 1:
            raise ValueError("Empty source datasets are not supported in Phase 1")


def ingest(store: Store, batch_id: str) -> dict:
    """Serialized writer; retry repairs incomplete batches without appending rows.

    Check all landing hashes even on a no-op replay. A completed Bronze batch is
    immutable. A partial batch has no commit manifest and is safely rewritten.
    """
    batch_identifier(batch_id)
    manifest = json.loads(store.read(f"landing/{batch_id}/manifest.json"))
    validate_manifest(manifest, batch_id)
    for name, entry in manifest["files"].items():
        source = store.read(f"landing/{batch_id}/{entry['file']}")
        if digest(source) != entry["sha256"] or len(source.splitlines()) != entry["rows"]:
            raise ValueError(f"Landing integrity failure: {name}")
        source.decode("utf-8")  # Encoding errors fail the batch; no lossy replacement.
    commit_key = f"bronze/{batch_id}/manifest.json"
    if store.exists(commit_key):
        committed = json.loads(store.read(commit_key))
        if (
            not isinstance(committed, dict)
            or committed.get("batch_id") != batch_id
            or not isinstance(committed.get("files"), dict)
            or set(committed["files"]) != set(manifest["files"])
        ):
            raise ValueError("Committed Bronze manifest identity changed")
        for name, entry in committed["files"].items():
            if (
                not isinstance(entry, dict)
                or entry.get("key") != f"bronze/{batch_id}/{name}.jsonl"
                or type(entry.get("rows")) is not int
                or entry["rows"] != manifest["files"][name]["rows"]
            ):
                raise ValueError("Committed Bronze manifest file contract changed")
            if digest(store.read(entry["key"])) != entry["sha256"]:
                raise ValueError("Committed Bronze output changed; investigate before recovery")
        LOGGER.info("bronze_replay_noop", extra={"batch_id": batch_id})
        return committed
    ingested_at = datetime.now(UTC).isoformat()
    outputs = {}
    for name, entry in manifest["files"].items():
        source_key = f"landing/{batch_id}/{entry['file']}"
        # Bound by the documented largest source profile; one entity at a time.
        rows = []
        for line_number, line in enumerate(store.read(source_key).decode().splitlines(), 1):
            rows.append(
                canonical_json(
                    {
                        "_raw_json": line,
                        "_source_file": store.uri(source_key),
                        "_source_line": line_number,
                        "_batch_id": batch_id,
                        "_ingested_at": ingested_at,
                        "_record_hash": digest(line.encode()),
                        "_record_id": digest(f"{batch_id}/{name}/{line_number}".encode()),
                    }
                )
            )
        content = ("\n".join(rows) + "\n").encode()
        key = f"bronze/{batch_id}/{name}.jsonl"
        store.write(key, content)
        outputs[name] = {"key": key, "sha256": digest(content), "rows": len(rows)}
    committed = {"batch_id": batch_id, "ingested_at": ingested_at, "files": outputs}
    store.write(commit_key, (canonical_json(committed) + "\n").encode(), immutable=True)
    LOGGER.info("bronze_committed", extra={"batch_id": batch_id})
    return committed
