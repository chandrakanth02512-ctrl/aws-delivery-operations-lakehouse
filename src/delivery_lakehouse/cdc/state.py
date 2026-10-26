"""Durable operational state. Deployment must enforce a single writer."""

from datetime import UTC, datetime

from delivery_lakehouse.cdc.contracts import parse_json
from delivery_lakehouse.generator import canonical_json, digest

CHECKPOINT_KEY = "gold/_cdc/checkpoint.json"
INITIALIZED_KEY = "gold/_cdc/initialized.json"
PUBLICATION_KEY = "gold/_publication.json"


def now():
    return datetime.now(UTC).isoformat()


def load_json(store, key, default=None):
    return parse_json(store.read(key)) if store.exists(key) else default


def save_json(store, key, value, *, immutable=False):
    store.write(key, canonical_json(value).encode(), immutable=immutable)


def state_key(batch_id):
    return f"gold/_cdc/batches/{batch_id}.json"


def transition(state, status, **details):
    allowed = {
        None: {"PENDING"},
        "PENDING": {"RUNNING"},
        "RUNNING": {"RUNNING", "FAILED", "SUCCEEDED"},
        "FAILED": {"RUNNING"},
        "SUCCEEDED": set(),
    }
    if status not in allowed[state.get("status")]:
        raise ValueError(f"Invalid batch transition: {state.get('status')} -> {status}")
    return {
        **state,
        **details,
        "status": status,
        "history": [*state.get("history", []), {"status": status, "at": now()}],
    }


def read_plan(store, key):
    plan = load_json(store, key)
    payload = {name: value for name, value in plan.items() if name != "sha256"}
    if digest(canonical_json(payload).encode()) != plan.get("sha256"):
        raise ValueError("PUBLICATION_PLAN_CHECKSUM_MISMATCH")
    return plan


def report_markdown(state):
    lines = [
        f"# CDC batch {state['batch_id']}",
        "",
        f"Status: **{state['status']}**",
        f"Publication: **{state.get('publication_status', 'NOT_STARTED')}**",
        f"Source manifest: `{state.get('source_manifest', 'unknown')}`",
        f"Manifest checksum: `{state.get('manifest_sha256', 'not yet verified')}`",
        f"Attempt: {state.get('attempt', 0)}; "
        f"duration: {state.get('duration_seconds', 0)} seconds.",
        "",
        "Counts describe the accepted plan; publication is complete only at SUCCEEDED.",
        "",
        "| Dataset | Input | Accepted changes | Insert | Update | Delete | Reject | Ignore |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for dataset, counts in state.get("counts", {}).items():
        values = [
            counts[key]
            for key in (
                "input_rows",
                "accepted_changes",
                "rows_inserted",
                "rows_updated",
                "rows_deleted",
                "rows_rejected",
                "rows_ignored",
            )
        ]
        lines.append(f"| {dataset} | " + " | ".join(str(value) for value in values) + " |")
    lines.extend(
        [
            "",
            f"Source event watermark: {state.get('watermark_after') or 'not advanced'}",
            f"Completed publication steps: {', '.join(state.get('completed_steps', [])) or 'none'}",
            "",
        ]
    )
    if state.get("error"):
        lines.extend([f"Error category: {state['error_category']}", f"Error: {state['error']}", ""])
    for dataset, changes in state.get("schema_drift", {}).items():
        for change in changes:
            lines.append(f"- {dataset}: {canonical_json(change)}")
    for dataset, counts in state.get("counts", {}).items():
        for field in ("rejection_reasons", "ignored_reasons"):
            if counts[field]:
                lines.append(f"- {dataset} {field}: {canonical_json(counts[field])}")
    return "\n".join(lines) + "\n"


def save_state(store, state):
    store.write(f"gold/_cdc/reports/{state['batch_id']}.md", report_markdown(state).encode())
    # The durable state is the commit record, so write its human-readable companion first.
    save_json(store, state_key(state["batch_id"]), state)


def finish_committed_batch(store, state, checkpoint):
    """Repair the final checkpoint without rewinding publication for an old replay."""
    if checkpoint.get("active_batch_id") == state["batch_id"]:
        save_json(store, CHECKPOINT_KEY, {**checkpoint, "active_batch_id": None})
