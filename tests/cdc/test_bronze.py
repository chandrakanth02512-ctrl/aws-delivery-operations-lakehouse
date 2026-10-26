import pytest

from delivery_lakehouse.cdc.bronze import ingest_changes
from delivery_lakehouse.cdc.contracts import parse_json
from delivery_lakehouse.cdc.generator import make_change, write_batch
from delivery_lakehouse.generator import canonical_json, digest, generate
from delivery_lakehouse.ingestion.storage import LocalStore


def batch_fixture(store):
    sources = generate(defects=False)
    changes = [
        make_change(name, rows[0], "INSERT", 1, "2026-02-06T09:00:00Z")
        for name, rows in sources.items()
    ]
    return write_batch(store, "fixture", "2026-02-06T12:00:00Z", changes)


def test_repeated_ingestion_is_immutable_and_traceable(tmp_path):
    store = LocalStore(tmp_path)
    manifest = batch_fixture(store)
    first = ingest_changes(store, manifest["batch_id"])
    before = {p: p.read_bytes() for p in (tmp_path / "bronze").rglob("*.json*")}
    assert ingest_changes(store, manifest["batch_id"]) == first
    assert before == {p: p.read_bytes() for p in (tmp_path / "bronze").rglob("*.json*")}
    raw = parse_json(store.read(first["files"]["shipments"]["key"]))
    assert (
        raw["_raw_json"].encode()
        == store.read(f"landing/cdc/{manifest['batch_id']}/shipments.jsonl").strip()
    )


@pytest.mark.parametrize(
    "damage", ["missing_file", "corrupt_file", "corrupt_manifest", "wrong_count"]
)
def test_integrity_failures_do_not_commit_bronze(tmp_path, damage):
    store = LocalStore(tmp_path)
    manifest = batch_fixture(store)
    prefix = f"landing/cdc/{manifest['batch_id']}"
    if damage == "missing_file":
        store.path(f"{prefix}/shipments.jsonl").unlink()
    elif damage == "corrupt_file":
        store.write(f"{prefix}/shipments.jsonl", b"{}\n")
    elif damage == "corrupt_manifest":
        manifest["batch_name"] = "tampered"
        store.write(f"{prefix}/manifest.json", canonical_json(manifest).encode())
    else:
        manifest["files"]["shipments"]["rows"] = 2
        manifest.pop("manifest_sha256")
        manifest["manifest_sha256"] = digest(canonical_json(manifest).encode())
        store.write(f"{prefix}/manifest.json", canonical_json(manifest).encode())
    with pytest.raises((FileNotFoundError, ValueError)):
        ingest_changes(store, manifest["batch_id"])
    assert not store.exists(f"bronze/cdc/{manifest['batch_id']}/manifest.json")


def test_partial_ingestion_reuses_original_time_and_preserves_written_objects(
    tmp_path, monkeypatch
):
    store = LocalStore(tmp_path)
    manifest = batch_fixture(store)
    write = store.write

    def interrupted(key, content, **options):
        if key.startswith("bronze/") and key.endswith("shipments.jsonl"):
            raise OSError("controlled interrupted upload")
        write(key, content, **options)

    monkeypatch.setattr(store, "write", interrupted)
    with pytest.raises(OSError):
        ingest_changes(store, manifest["batch_id"])
    partial = {p: p.read_bytes() for p in (tmp_path / "bronze").rglob("*.json*")}
    assert partial
    monkeypatch.setattr(store, "write", write)
    ingest_changes(store, manifest["batch_id"])
    assert all(path.read_bytes() == value for path, value in partial.items())


def test_changed_committed_bronze_is_detected(tmp_path):
    store = LocalStore(tmp_path)
    manifest = batch_fixture(store)
    committed = ingest_changes(store, manifest["batch_id"])
    store.write(committed["files"]["shipments"]["key"], b"modified")
    with pytest.raises(ValueError, match="COMMITTED_BRONZE_CORRUPTED"):
        ingest_changes(store, manifest["batch_id"])


def test_changed_committed_bronze_manifest_is_detected(tmp_path):
    store = LocalStore(tmp_path)
    manifest = batch_fixture(store)
    committed = ingest_changes(store, manifest["batch_id"])
    committed["source_manifest"]["batch_name"] = "corrupted"
    key = f"bronze/cdc/{manifest['batch_id']}/manifest.json"
    store.write(key, canonical_json(committed).encode())
    with pytest.raises(ValueError, match="COMMITTED_BRONZE_MANIFEST_CORRUPTED"):
        ingest_changes(store, manifest["batch_id"])
