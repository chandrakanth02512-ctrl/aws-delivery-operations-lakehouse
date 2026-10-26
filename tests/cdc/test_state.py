import json
import sys

import pytest

from delivery_lakehouse.cdc.cli import main
from delivery_lakehouse.cdc.pipeline import run_cdc
from delivery_lakehouse.cdc.state import (
    CHECKPOINT_KEY,
    PUBLICATION_KEY,
    finish_committed_batch,
    load_json,
    read_plan,
    save_json,
    state_key,
    transition,
)
from delivery_lakehouse.config import Settings
from delivery_lakehouse.generator import canonical_json, digest
from delivery_lakehouse.ingestion.storage import LocalStore
from delivery_lakehouse.logging_config import configure_logging


def test_missing_source_records_failed_state_and_keeps_previous_publication(tmp_path):
    store = LocalStore(tmp_path)
    publication = {"status": "SUCCEEDED", "batch_id": "previous"}
    checkpoint = {
        "last_succeeded_batch": "previous",
        "source_event_watermark": "2026-02-10T00:00:00Z",
    }
    save_json(store, PUBLICATION_KEY, publication)
    save_json(store, CHECKPOINT_KEY, checkpoint)
    batch = "a" * 64
    with pytest.raises(FileNotFoundError):
        run_cdc(None, store, Settings(), batch)
    state = load_json(store, state_key(batch))
    assert [entry["status"] for entry in state["history"]] == ["PENDING", "RUNNING", "FAILED"]
    assert state["error_category"] == "BRONZE"
    assert state["started_at"] <= state["ended_at"]
    assert state["duration_seconds"] >= 0
    assert state["source_manifest"].endswith("manifest.json")
    assert state["publication_status"] == "NOT_STARTED"
    assert load_json(store, PUBLICATION_KEY) == publication
    assert (
        load_json(store, CHECKPOINT_KEY)["source_event_watermark"]
        == checkpoint["source_event_watermark"]
    )
    assert "Error category: BRONZE" in store.read(f"gold/_cdc/reports/{batch}.md").decode()


def test_plan_checksum_rejects_mutation(tmp_path):
    store = LocalStore(tmp_path)
    plan = {"batch_id": "a" * 64, "entries": []}
    plan["sha256"] = digest(canonical_json(plan).encode())
    save_json(store, "gold/plan.json", plan)
    assert read_plan(store, "gold/plan.json") == plan
    plan["entries"].append({"rows": 100})
    save_json(store, "gold/plan.json", plan)
    with pytest.raises(ValueError, match="PLAN_CHECKSUM"):
        read_plan(store, "gold/plan.json")


def test_checkpoint_cleanup_is_safe_after_committed_batch(tmp_path):
    store = LocalStore(tmp_path)
    state = {"batch_id": "current", "status": "SUCCEEDED"}
    checkpoint = {"active_batch_id": "current", "last_succeeded_batch": "current"}
    save_json(store, CHECKPOINT_KEY, checkpoint)
    finish_committed_batch(store, state, checkpoint)
    assert load_json(store, CHECKPOINT_KEY) == {**checkpoint, "active_batch_id": None}
    checkpoint = {"active_batch_id": "newer", "last_succeeded_batch": "newer"}
    save_json(store, CHECKPOINT_KEY, checkpoint)
    finish_committed_batch(store, state, checkpoint)
    assert load_json(store, CHECKPOINT_KEY) == checkpoint


def test_success_is_terminal_and_pending_cannot_skip_execution():
    with pytest.raises(ValueError, match="Invalid batch transition"):
        transition({"status": "SUCCEEDED"}, "RUNNING")
    with pytest.raises(ValueError, match="Invalid batch transition"):
        transition({"status": "PENDING"}, "SUCCEEDED")


def test_cli_generation_is_local_and_s3_upload_is_explicit(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        sys, "argv", ["delivery-lakehouse-cdc", "generate", "--root", str(tmp_path)]
    )
    main()
    batches = json.loads(capsys.readouterr().out)
    assert len(batches) == 4
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "delivery-lakehouse-cdc",
            "upload-landing",
            "--root",
            str(tmp_path),
            "--batch-id",
            "a" * 64,
        ],
    )
    with pytest.raises(SystemExit):
        main()
    assert "requires explicit --approve-s3-write" in capsys.readouterr().err


def test_json_logs_include_cdc_metrics(capsys):
    import logging

    configure_logging()
    logging.getLogger("cdc-test").info(
        "cdc_batch_succeeded",
        extra={
            "batch_id": "example",
            "status": "SUCCEEDED",
            "duration_seconds": 1.5,
            "rows_inserted": 2,
            "rows_updated": 3,
            "rows_deleted": 1,
            "rows_rejected": 4,
            "error_category": None,
        },
    )
    record = json.loads(capsys.readouterr().err)
    assert record["status"] == "SUCCEEDED"
    assert record["rows_inserted"] == 2
    assert record["rows_updated"] == 3
    assert record["rows_deleted"] == 1
    assert record["rows_rejected"] == 4
    assert record["duration_seconds"] == 1.5
