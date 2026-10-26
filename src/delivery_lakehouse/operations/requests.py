"""Portable execution requests; preparing one never contacts AWS."""

import math
import re

from delivery_lakehouse import __version__
from delivery_lakehouse.config import batch_identifier, identifier
from delivery_lakehouse.generator import canonical_json, digest


def execution_identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", value):
        raise ValueError("Execution ID must be 1–80 letters, digits, underscores or hyphens")
    return value


def execution_request(
    settings,
    batch_id,
    mode="cdc",
    *,
    catalog="glue_catalog",
    adopt_snapshot=False,
    max_attempts=1,
    retry_delay_seconds=1,
):
    batch_identifier(batch_id)
    identifier(catalog)
    if mode not in {"snapshot", "cdc"}:
        raise ValueError("Pipeline mode must be snapshot or cdc")
    if type(adopt_snapshot) is not bool or (adopt_snapshot and mode != "cdc"):
        raise ValueError("Snapshot adoption is an explicit CDC-only option")
    if type(max_attempts) is not int or not 1 <= max_attempts <= 2:
        raise ValueError("max_attempts must be 1 or 2; retries are disabled by default")
    if (
        type(retry_delay_seconds) not in {int, float}
        or not math.isfinite(retry_delay_seconds)
        or not 0 <= retry_delay_seconds <= 30
    ):
        raise ValueError("Retry delay must be between 0 and 30 seconds")
    return {
        "batch_id": batch_id,
        "pipeline_mode": mode,
        "catalog": catalog,
        "bucket": settings.bucket,
        "region": settings.region,
        "database_prefix": settings.database_prefix,
        "max_reject_fraction": settings.max_reject_fraction,
        "adopt_snapshot": adopt_snapshot,
        "max_attempts": max_attempts,
        "retry_delay_seconds": float(retry_delay_seconds),
        "package_version": __version__,
    }


def request_hash(request):
    return digest(canonical_json(request).encode())


def default_execution_id(request):
    return f"ops-{request_hash(request)}"


def aws_execution_request(request, *, execution_id=None):
    if request["catalog"] != "glue_catalog":
        raise ValueError("AWS execution requires the Glue Catalog")
    name = execution_identifier(execution_id or default_execution_id(request))
    return {
        "name": name,
        "input": {
            "batchId": request["batch_id"],
            "pipelineMode": request["pipeline_mode"],
            "adoptPhase1Snapshot": str(request["adopt_snapshot"]).lower(),
            "maxAttempts": request["max_attempts"],
            "retryDelaySeconds": request["retry_delay_seconds"],
        },
    }
