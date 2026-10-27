import json
import sys

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from delivery_lakehouse.cdc.generator import make_change, write_batch
from delivery_lakehouse.cdc.state import load_json
from delivery_lakehouse.config import Settings
from delivery_lakehouse.generator import generate
from delivery_lakehouse.ingestion.storage import LocalStore
from delivery_lakehouse.operations import runner
from delivery_lakehouse.operations.cli import main
from delivery_lakehouse.operations.requests import (
    aws_execution_request,
    default_execution_id,
    execution_identifier,
    execution_request,
)
from delivery_lakehouse.operations.retry import classify_failure
from delivery_lakehouse.quality.report import QualityGateError


@pytest.fixture
def source(tmp_path):
    store = LocalStore(tmp_path)
    station = generate(defects=False)["delivery_stations"][0]
    batch = write_batch(
        store,
        "operation-test",
        "2026-02-06T12:00:00Z",
        [make_change("delivery_stations", station, "INSERT", 1, "2026-02-06T09:00:00Z")],
    )
    return store, batch["batch_id"]


def result(batch_id):
    return {
        "status": "SUCCEEDED",
        "batch_id": batch_id,
        "counts": {
            "delivery_stations": {
                "input_rows": 3,
                "rows_inserted": 2,
                "rows_updated": 0,
                "rows_deleted": 0,
                "rows_rejected": 1,
                "rows_ignored": 0,
                "accepted_changes": 2,
            }
        },
    }


def fake_success(*args, observer, verify_publication, **kwargs):
    for stage in ("BRONZE", "SILVER", "GOLD", "PUBLISH", "VERIFY", "COMMIT"):
        observer(stage, "RUNNING", {})
        if stage == "VERIFY":
            verify_publication({"mode": "cdc", "plan": {"entries": []}})
        observer(stage, "SUCCEEDED", {})
    return result(args[3])


def test_execution_order_metrics_report_and_duplicate_prevention(source, monkeypatch):
    store, batch = source
    calls = []

    def pipeline(*args, **kwargs):
        calls.append(args[3])
        return fake_success(*args, **kwargs)

    monkeypatch.setattr(runner, "run_cdc", pipeline)
    first = runner.run_execution(None, store, Settings(), batch, execution_id="ordered")
    assert [event["stage"] for event in first["events"] if event["status"] == "SUCCEEDED"] == [
        "SOURCE",
        "BRONZE",
        "SILVER",
        "GOLD",
        "PUBLISH",
        "VERIFY",
        "COMMIT",
    ]
    assert first["metrics"]["input_rows"] == 3
    assert first["metrics"]["output_rows"] == 2
    assert first["metrics"]["rows_rejected"] == 1
    assert first["metrics"]["quality_reject_fraction"] == pytest.approx(1 / 3)
    assert first["started_at"] <= first["completed_at"]
    before = store.read(runner.execution_key("ordered"))
    assert runner.run_execution(None, store, Settings(), batch, execution_id="ordered") == first
    assert store.read(runner.execution_key("ordered")) == before
    assert calls == [batch]
    assert "SUCCEEDED" in store.read("gold/_operations/reports/ordered.md").decode()


def test_transient_retry_is_bounded_and_retains_failure_details(source, monkeypatch):
    store, batch = source
    calls, delays = [], []

    def pipeline(*args, **kwargs):
        calls.append(args[3])
        if len(calls) == 1:
            raise OSError("controlled temporary storage failure")
        return fake_success(*args, **kwargs)

    monkeypatch.setattr(runner, "run_cdc", pipeline)
    state = runner.run_execution(
        None,
        store,
        Settings(),
        batch,
        execution_id="retry-once",
        max_attempts=2,
        retry_delay_seconds=3,
        sleeper=delays.append,
    )
    assert [attempt["status"] for attempt in state["attempts"]] == ["FAILED", "SUCCEEDED"]
    assert state["attempts"][0]["error_category"] == "TRANSIENT_IO"
    assert "temporary" in state["attempts"][0]["error"]
    assert calls == [batch, batch]
    assert delays == [3]


@pytest.mark.parametrize(
    "error,category",
    [
        (QualityGateError("bad required data"), "DATA_QUALITY"),
        (ValueError("bad schema"), "CONTRACT"),
        (FileNotFoundError("missing batch"), "SOURCE_INTEGRITY"),
        (RuntimeError("RECOVER_ACTIVE_BATCH_FIRST: other"), "RECOVERY_REQUIRED"),
    ],
)
def test_permanent_failures_never_retry_and_resume_is_explicit(
    source, monkeypatch, error, category
):
    store, batch = source
    calls = []

    def pipeline(*args, **kwargs):
        calls.append(1)
        raise error

    monkeypatch.setattr(runner, "run_cdc", pipeline)
    with pytest.raises(type(error)):
        runner.run_execution(None, store, Settings(), batch, execution_id="failed", max_attempts=2)
    state = load_json(store, runner.execution_key("failed"))
    assert len(calls) == len(state["attempts"]) == 1
    assert state["status"] == "FAILED"
    assert state["attempts"][0]["error_category"] == category
    assert state["metrics"]["output_rows"] == 0
    with pytest.raises(RuntimeError, match="EXPLICIT_RESUME"):
        runner.run_execution(None, store, Settings(), batch, execution_id="failed", max_attempts=2)
    monkeypatch.setattr(runner, "run_cdc", fake_success)
    recovered = runner.run_execution(
        None, store, Settings(), batch, execution_id="failed", max_attempts=2, resume=True
    )
    assert recovered["status"] == "SUCCEEDED"
    assert len(recovered["attempts"]) == 2


def test_default_disables_retries_and_retry_exhaustion_is_failed(source, monkeypatch):
    store, batch = source
    calls = []

    def pipeline(*args, **kwargs):
        calls.append(1)
        raise OSError("still unavailable")

    monkeypatch.setattr(runner, "run_cdc", pipeline)
    for limit in (1, 2):
        with pytest.raises(OSError):
            runner.run_execution(
                None,
                store,
                Settings(),
                batch,
                execution_id=f"limit-{limit}",
                max_attempts=limit,
                sleeper=lambda _: None,
            )
        assert len(load_json(store, runner.execution_key(f"limit-{limit}"))["attempts"]) == limit
    assert len(calls) == 3


def test_execution_id_cannot_be_reused_with_other_parameters(source, monkeypatch):
    store, batch = source
    monkeypatch.setattr(runner, "run_cdc", fake_success)
    runner.run_execution(None, store, Settings(), batch, execution_id="fixed")
    with pytest.raises(ValueError, match="DIFFERENT_PARAMETERS"):
        runner.run_execution(None, store, Settings(), batch, execution_id="fixed", max_attempts=2)


def test_source_failure_does_not_report_counts_from_previous_batch_execution(source, monkeypatch):
    store, batch = source
    store.write(f"gold/_cdc/batches/{batch}.json", json.dumps(result(batch)).encode())

    def invalid_source(*args):
        raise ValueError("changed source manifest")

    monkeypatch.setattr(runner, "source_preflight", invalid_source)
    with pytest.raises(ValueError, match="changed source"):
        runner.run_execution(None, store, Settings(), batch, execution_id="bad-source")
    state = load_json(store, runner.execution_key("bad-source"))
    assert state["metrics"] == {"input_rows": 0, "output_rows": 0, "rows_rejected": 0}


def test_failure_is_logged_when_state_storage_also_fails(source, monkeypatch, caplog):
    store, batch = source
    original = runner.save_execution

    def unavailable_state(store, state):
        if state["status"] == "FAILED":
            raise OSError("state storage unavailable")
        original(store, state)

    def invalid_source(*args):
        raise ValueError("original source error")

    monkeypatch.setattr(runner, "save_execution", unavailable_state)
    monkeypatch.setattr(runner, "source_preflight", invalid_source)
    with pytest.raises(OSError, match="state storage"):
        runner.run_execution(None, store, Settings(), batch, execution_id="failed-state")
    assert "pipeline_attempt_failed" in caplog.text
    assert "original source error" in caplog.text


def test_interrupted_final_execution_record_recovers_without_losing_applied_counts(
    source, monkeypatch
):
    store, batch = source
    calls = []

    def pipeline(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            return fake_success(*args, **kwargs)
        kwargs["observer"]("REPLAY", "SUCCEEDED", {})
        return result(batch)

    monkeypatch.setattr(runner, "run_cdc", pipeline)
    original = store.write
    failed = False

    def write(key, content, **options):
        nonlocal failed
        if (
            key == runner.execution_key("commit-loss")
            and json.loads(content)["status"] == "SUCCEEDED"
            and not failed
        ):
            failed = True
            raise OSError("lost final execution record")
        return original(key, content, **options)

    monkeypatch.setattr(store, "write", write)
    state = runner.run_execution(
        None,
        store,
        Settings(),
        batch,
        execution_id="commit-loss",
        max_attempts=2,
        sleeper=lambda _: None,
    )
    assert state["status"] == "SUCCEEDED"
    assert [row["status"] for row in state["attempts"]] == ["FAILED", "SUCCEEDED"]
    assert state["metrics"]["rows_inserted"] == 2


@pytest.mark.parametrize(
    "options",
    [
        {"mode": "unknown"},
        {"max_attempts": 0},
        {"max_attempts": 3},
        {"max_attempts": True},
        {"retry_delay_seconds": -1},
        {"retry_delay_seconds": 31},
        {"retry_delay_seconds": float("nan")},
        {"mode": "snapshot", "adopt_snapshot": True},
    ],
)
def test_invalid_execution_requests(options):
    with pytest.raises(ValueError):
        execution_request(Settings(), "a" * 64, **options)


def test_aws_request_is_deterministic_and_preparation_never_initializes_aws(monkeypatch, capsys):
    import boto3

    request = execution_request(Settings(), "a" * 64)
    assert default_execution_id(request) == default_execution_id(
        execution_request(Settings(), "a" * 64)
    )
    assert default_execution_id(request) == default_execution_id(
        execution_request(Settings(), "a" * 64, retry_delay_seconds=1.0)
    )
    prepared = aws_execution_request(request)
    assert prepared["input"]["maxAttempts"] == 1
    assert prepared["input"]["adoptPhase1Snapshot"] == "false"

    def forbidden(*args, **kwargs):
        raise AssertionError("AWS client must not be constructed")

    monkeypatch.setattr(boto3, "client", forbidden)
    monkeypatch.setattr(sys, "argv", ["delivery-lakehouse-ops", "plan-aws", "--batch-id", "a" * 64])
    main()
    assert json.loads(capsys.readouterr().out) == prepared
    with pytest.raises(ValueError):
        execution_identifier("../unsafe")


@pytest.mark.parametrize(
    "error,expected",
    [
        (EndpointConnectionError(endpoint_url="https://example.invalid"), ("TRANSIENT_IO", True)),
        (
            ClientError({"Error": {"Code": "SlowDown", "Message": "slow"}}, "GetObject"),
            ("TRANSIENT_IO", True),
        ),
        (
            ClientError({"Error": {"Code": "AccessDenied", "Message": "denied"}}, "GetObject"),
            ("AWS_AccessDenied", False),
        ),
        (PermissionError("denied"), ("AUTHORIZATION", False)),
        (RuntimeError("unknown JVM failure"), ("UNCLASSIFIED", False)),
    ],
)
def test_conservative_error_classification(error, expected):
    assert classify_failure(error) == expected


def test_local_writer_lock_prevents_a_second_execution(tmp_path, monkeypatch):
    import fcntl

    from delivery_lakehouse import spark as spark_module

    lock = tmp_path / "gold/_cdc/local.lock"
    lock.parent.mkdir(parents=True)
    jar = tmp_path / "test.jar"
    jar.touch()

    def forbidden(*args, **kwargs):
        raise AssertionError("A competing writer must not start Spark")

    monkeypatch.setattr(spark_module, "session", forbidden)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "delivery-lakehouse-ops",
            "run-local",
            "--batch-id",
            "a" * 64,
            "--root",
            str(tmp_path),
            "--iceberg-jar",
            str(jar),
        ],
    )
    with lock.open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            main()


def test_resume_closes_interrupted_attempt_without_inventing_a_completion_time(source, monkeypatch):
    store, batch = source

    class ProcessStopped(BaseException):
        pass

    def interrupted(*args, **kwargs):
        kwargs["observer"]("PUBLISH", "RUNNING", {})
        raise ProcessStopped()

    monkeypatch.setattr(runner, "run_cdc", interrupted)
    with pytest.raises(ProcessStopped):
        runner.run_execution(None, store, Settings(), batch, execution_id="process-stopped")
    saved = load_json(store, runner.execution_key("process-stopped"))
    assert saved["status"] == saved["attempts"][0]["status"] == "RUNNING"
    monkeypatch.setattr(runner, "run_cdc", fake_success)
    recovered = runner.run_execution(
        None, store, Settings(), batch, execution_id="process-stopped", resume=True
    )
    assert [attempt["status"] for attempt in recovered["attempts"]] == ["INTERRUPTED", "SUCCEEDED"]
    interrupted_attempt = recovered["attempts"][0]
    assert interrupted_attempt["error_category"] == "PROCESS_INTERRUPTED"
    assert interrupted_attempt["interruption_observed_at"] >= interrupted_attempt["started_at"]
    assert interrupted_attempt.get("completed_at") is None
    assert interrupted_attempt.get("duration_seconds") is None
