import json

import boto3
import pytest
from botocore.stub import Stubber

from delivery_lakehouse.generator import write_landing
from delivery_lakehouse.ingestion.bronze import ingest
from delivery_lakehouse.ingestion.storage import LocalStore, S3Store, safe_key


def test_bronze_preserves_raw_and_idempotent(tmp_path):
    manifest = write_landing(tmp_path)
    store = LocalStore(tmp_path)
    first = ingest(store, manifest["batch_id"])
    first_bytes = store.read(first["files"]["shipments"]["key"])
    assert ingest(store, manifest["batch_id"]) == first
    assert store.read(first["files"]["shipments"]["key"]) == first_bytes
    rows = [json.loads(row) for row in first_bytes.splitlines()]
    source = store.read(f"landing/{manifest['batch_id']}/shipments.jsonl").decode().splitlines()
    assert [r["_raw_json"] for r in rows] == source
    assert len({r["_record_id"] for r in rows}) == len(rows)
    assert rows[0]["_source_line"] == 1
    assert rows[0]["_batch_id"] == manifest["batch_id"]


def test_modified_source_rejected_even_on_replay(tmp_path):
    batch = write_landing(tmp_path)["batch_id"]
    store = LocalStore(tmp_path)
    ingest(store, batch)
    store.write(f"landing/{batch}/shipments.jsonl", b"{}\n")
    with pytest.raises(ValueError, match="integrity"):
        ingest(store, batch)


def test_modified_bronze_is_not_silently_repaired(tmp_path):
    batch = write_landing(tmp_path)["batch_id"]
    store = LocalStore(tmp_path)
    ingest(store, batch)
    store.write(f"bronze/{batch}/drivers.jsonl", b"{}\n")
    with pytest.raises(ValueError, match="Committed Bronze"):
        ingest(store, batch)


def test_partial_failure_can_retry_without_duplicates(tmp_path, monkeypatch):
    batch = write_landing(tmp_path)["batch_id"]
    store = LocalStore(tmp_path)
    original_write = store.write

    def fail_on_shipments(key, content, **kwargs):
        if key.endswith("shipments.jsonl"):
            raise OSError("simulated interrupted upload")
        original_write(key, content, **kwargs)

    monkeypatch.setattr(store, "write", fail_on_shipments)
    with pytest.raises(OSError):
        ingest(store, batch)
    assert not store.exists(f"bronze/{batch}/manifest.json")
    monkeypatch.setattr(store, "write", original_write)
    completed = ingest(store, batch)
    assert len(store.read(completed["files"]["shipments"]["key"]).splitlines()) == 126
    assert ingest(store, batch) == completed


@pytest.mark.parametrize("key", ["../secret", "/etc/passwd", "x/../../y", "x\\y", ""])
def test_unsafe_keys_rejected(key):
    with pytest.raises(ValueError):
        safe_key(key)


def test_local_immutable_conflicts(tmp_path):
    store = LocalStore(tmp_path)
    store.write("test", b"one", immutable=True)
    store.write("test", b"one", immutable=True)
    with pytest.raises(ValueError, match="Immutable"):
        store.write("test", b"two", immutable=True)


def test_s3_conditional_put_and_access_denied_not_treated_as_missing():
    client = boto3.client(
        "s3",
        region_name="ap-southeast-2",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    store = S3Store("example", "ap-southeast-2", client=client)
    with Stubber(client) as stub:
        stub.add_response(
            "put_object",
            {},
            {"Bucket": "example", "Key": "landing/file", "Body": b"a", "IfNoneMatch": "*"},
        )
        store.write("landing/file", b"a", immutable=True)
        stub.add_client_error(
            "head_object",
            "AccessDenied",
            http_status_code=403,
            expected_params={"Bucket": "example", "Key": "private"},
        )
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError):
            store.exists("private")
        stub.assert_no_pending_responses()
