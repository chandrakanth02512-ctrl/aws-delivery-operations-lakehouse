"""Single-writer execution control around the established snapshot and CDC runners."""

import logging
import time

from delivery_lakehouse.cdc.bronze import read_manifest
from delivery_lakehouse.cdc.pipeline import run_cdc
from delivery_lakehouse.cdc.state import load_json, now, save_json
from delivery_lakehouse.generator import canonical_json
from delivery_lakehouse.operations.requests import (
    default_execution_id,
    execution_identifier,
    execution_request,
    request_hash,
)
from delivery_lakehouse.operations.retry import classify_failure
from delivery_lakehouse.operations.verification import verify_publication
from delivery_lakehouse.pipeline import run_pipeline

LOGGER = logging.getLogger(__name__)


def execution_key(execution_id):
    execution_identifier(execution_id)
    return f"gold/_operations/executions/{execution_id}.json"


def metrics(result, mode, replayed):
    if mode == "cdc":
        counts = result["counts"].values()
        totals = {
            key: sum(row[key] for row in counts)
            for key in (
                "input_rows",
                "rows_inserted",
                "rows_updated",
                "rows_deleted",
                "rows_rejected",
                "rows_ignored",
                "accepted_changes",
            )
        }
    else:
        reports = result["quality"]["datasets"]
        totals = {
            "input_rows": sum(row["input_rows"] for row in reports),
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
            "rows_rejected": sum(row["quarantined_rows"] for row in reports),
            "rows_ignored": 0,
            "accepted_changes": sum(row["accepted_rows"] for row in reports),
        }
    source_rows = totals["input_rows"]
    if replayed:
        totals = dict.fromkeys(totals, 0)
    totals["source_rows"] = source_rows
    totals["output_rows"] = (
        totals["accepted_changes"]
        if mode == "snapshot"
        else totals["rows_inserted"] + totals["rows_updated"]
    )
    totals["planned_output_rows"] = totals["output_rows"]
    totals["quality_reject_fraction"] = (
        totals["rows_rejected"] / totals["input_rows"] if totals["input_rows"] else None
    )
    return totals


def save_execution(store, state):
    lines = [
        f"# Execution {state['execution_id']}",
        "",
        f"Status: **{state['status']}**; batch: `{state['batch_id']}`; mode: `{state['mode']}`.",
        f"Started: {state.get('started_at')}; completed: {state.get('completed_at')}.",
        f"Duration: {state.get('duration_seconds', 0)} seconds; "
        f"attempts: {len(state['attempts'])}.",
        "Counts describe accepted changes; failed publication can contain mixed table versions.",
        "",
        "## Metrics",
        "",
        "```json",
        canonical_json(state.get("metrics", {})),
        "```",
        "",
        "## Attempts",
        "",
        "| Attempt | Status | Error category | Error |",
        "|---:|---|---|---|",
    ]
    for attempt in state["attempts"]:
        error = str(attempt.get("error", "")).replace("|", "/").replace("\n", " ")
        lines.append(
            f"| {attempt['number']} | {attempt['status']} | "
            f"{attempt.get('error_category', '')} | {error} |"
        )
    store.write(
        f"gold/_operations/reports/{state['execution_id']}.md", ("\n".join(lines) + "\n").encode()
    )
    save_json(store, execution_key(state["execution_id"]), state)


def source_preflight(store, request):
    if request["pipeline_mode"] == "cdc":
        return read_manifest(store, request["batch_id"])
    # Snapshot ingestion performs the full protocol/checksum validation before any table write.
    return load_json(store, f"landing/{request['batch_id']}/manifest.json")


def run_execution(
    spark,
    store,
    settings,
    batch_id,
    *,
    mode="cdc",
    catalog="glue_catalog",
    execution_id=None,
    adopt_snapshot=False,
    max_attempts=1,
    retry_delay_seconds=1,
    resume=False,
    failure_hook=None,
    sleeper=time.sleep,
):
    request = execution_request(
        settings,
        batch_id,
        mode,
        catalog=catalog,
        adopt_snapshot=adopt_snapshot,
        max_attempts=max_attempts,
        retry_delay_seconds=retry_delay_seconds,
    )
    execution_id = execution_identifier(execution_id or default_execution_id(request))
    key = execution_key(execution_id)
    previous = load_json(store, key)
    if previous and previous["request_hash"] != request_hash(request):
        raise ValueError("EXECUTION_ID_REUSED_WITH_DIFFERENT_PARAMETERS")
    if previous and previous["status"] == "SUCCEEDED":
        # The underlying runner validates immutable source content on its own replay path.
        if mode == "cdc":
            from delivery_lakehouse.cdc.bronze import ingest_changes

            ingest_changes(store, batch_id)
        else:
            from delivery_lakehouse.ingestion.bronze import ingest

            ingest(store, batch_id)
        LOGGER.info(
            "execution_replay_noop", extra={"execution_id": execution_id, "batch_id": batch_id}
        )
        return previous
    if previous and not resume:
        raise RuntimeError("EXECUTION_REQUIRES_EXPLICIT_RESUME")
    started = time.monotonic()
    state = previous or {
        "execution_id": execution_id,
        "batch_id": batch_id,
        "mode": mode,
        "request": request,
        "request_hash": request_hash(request),
        "status": "PENDING",
        "created_at": now(),
        "attempts": [],
        "events": [],
        "work_started": False,
    }
    # Resume is permitted only after the previous writer has stopped. Its precise
    # stop time is unknown, so record when the interruption was observed instead.
    for attempt in state["attempts"]:
        if attempt["status"] == "RUNNING":
            attempt.update(
                status="INTERRUPTED",
                interruption_observed_at=now(),
                error_category="PROCESS_INTERRUPTED",
                error="Previous invocation stopped before recording its outcome",
            )
    save_execution(store, state)

    def observe(stage, status, details):
        state["current_stage"] = stage
        state["events"].append({"stage": stage, "status": status, "at": now(), **details})
        if stage == "PUBLISH" and status == "RUNNING":
            state["work_started"] = True
        if stage == "REPLAY":
            state["replayed"] = True
        save_execution(store, state)
        LOGGER.info(
            "pipeline_stage",
            extra={
                "execution_id": execution_id,
                "batch_id": batch_id,
                "stage": stage,
                "status": status,
            },
        )

    def verify(context):
        report = verify_publication(spark, store, context)
        state["verification"] = report
        save_json(
            store,
            f"gold/_operations/verification/{execution_id}/attempt-{len(state['attempts'])}.json",
            report,
        )

    for invocation_attempt in range(max_attempts):
        attempt = {"number": len(state["attempts"]) + 1, "status": "RUNNING", "started_at": now()}
        state["attempts"].append(attempt)
        state.update(
            status="RUNNING", started_at=state.get("started_at") or now(), completed_at=None
        )
        save_execution(store, state)
        attempt_started = time.monotonic()
        LOGGER.info(
            "pipeline_execution_started",
            extra={
                "execution_id": execution_id,
                "batch_id": batch_id,
                "status": "RUNNING",
                "attempt": attempt["number"],
                "started_at": attempt["started_at"],
            },
        )
        core_started = False
        try:
            observe("SOURCE", "RUNNING", {})
            if source_preflight(store, request) is None:
                raise FileNotFoundError("Source manifest is missing")
            observe("SOURCE", "SUCCEEDED", {})
            options = {"catalog": catalog, "observer": observe, "verify_publication": verify}
            core_started = True
            if mode == "cdc":
                result = run_cdc(
                    spark,
                    store,
                    settings,
                    batch_id,
                    adopt_snapshot=adopt_snapshot,
                    failure_hook=failure_hook,
                    **options,
                )
            else:
                result = run_pipeline(spark, store, settings, batch_id, **options)
            replayed = state.get("replayed", False) and not state["work_started"]
            state["metrics"] = metrics(result, mode, replayed)
            state["pipeline_result"] = {
                key: result.get(key) for key in ("status", "batch_id", "plan_key", "run_id")
            }
            attempt.update(
                status="SUCCEEDED",
                completed_at=now(),
                duration_seconds=round(time.monotonic() - attempt_started, 3),
            )
            state.update(
                status="SUCCEEDED",
                completed_at=now(),
                duration_seconds=round(time.monotonic() - started, 3),
            )
            save_execution(store, state)
            LOGGER.info(
                "pipeline_execution_completed",
                extra={
                    "execution_id": execution_id,
                    "batch_id": batch_id,
                    "status": "SUCCEEDED",
                    "duration_seconds": state["duration_seconds"],
                    "started_at": state["started_at"],
                    "completed_at": state["completed_at"],
                    **state["metrics"],
                },
            )
            return state
        except Exception as error:
            category, retryable = classify_failure(error)
            attempt.update(
                status="FAILED",
                completed_at=now(),
                error_type=type(error).__name__,
                error=str(error),
                error_category=category,
                retryable=retryable,
                duration_seconds=round(time.monotonic() - attempt_started, 3),
            )
            state.update(
                status="FAILED",
                completed_at=now(),
                duration_seconds=round(time.monotonic() - started, 3),
            )
            # Emit diagnostics before persistence: storage itself may be the failing dependency.
            LOGGER.exception(
                "pipeline_attempt_failed",
                extra={
                    "execution_id": execution_id,
                    "batch_id": batch_id,
                    "error_category": category,
                    "status": "FAILED",
                },
            )
            # Retrieve quality counts only for processing entered by this attempt.
            failed = (
                load_json(store, f"gold/_cdc/batches/{batch_id}.json", {})
                if core_started and mode == "cdc"
                else None
            )
            if failed and failed.get("counts"):
                state["metrics"] = metrics(failed, mode, False)
            elif (
                core_started
                and mode == "snapshot"
                and store.exists(f"quarantine/{batch_id}/quality_report.json")
            ):
                quality = load_json(store, f"quarantine/{batch_id}/quality_report.json")
                state["metrics"] = metrics({"quality": quality}, mode, False)
            else:
                state["metrics"] = {"input_rows": 0, "output_rows": 0, "rows_rejected": 0}
            state["metrics"]["output_rows"] = None if state["work_started"] else 0
            save_execution(store, state)
            retry = retryable and invocation_attempt + 1 < max_attempts
            if not retry:
                LOGGER.info(
                    "pipeline_execution_completed",
                    extra={
                        "execution_id": execution_id,
                        "batch_id": batch_id,
                        "status": "FAILED",
                        "error_category": category,
                        "duration_seconds": state["duration_seconds"],
                        "started_at": state["started_at"],
                        "completed_at": state["completed_at"],
                        **state["metrics"],
                    },
                )
                raise
            sleeper(retry_delay_seconds)
    raise AssertionError("Execution attempts exhausted without a result")
