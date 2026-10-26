"""Conservative retry classification; data errors and unknown JVM errors are not retried."""

import errno

from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

from delivery_lakehouse.quality.report import QualityGateError


def classify_failure(error):
    message = str(error)
    if isinstance(error, QualityGateError) or any(
        term in message
        for term in (
            "REJECTION_THRESHOLD",
            "ALL_RECORDS_REJECTED",
            "ROW_RECONCILIATION",
            "DEPENDENT_RECORD_INTEGRITY",
            "PUBLICATION_VERIFICATION_FAILED",
        )
    ):
        return "DATA_QUALITY", False
    if isinstance(error, FileNotFoundError):
        return "SOURCE_INTEGRITY", False
    if isinstance(error, PermissionError):
        return "AUTHORIZATION", False
    if isinstance(error, ClientError):
        code = error.response.get("Error", {}).get("Code", "UNKNOWN")
        if code in {
            "SlowDown",
            "Throttling",
            "ThrottlingException",
            "RequestTimeout",
            "InternalError",
            "ServiceUnavailable",
        }:
            return "TRANSIENT_IO", True
        return f"AWS_{code}", False
    if isinstance(error, (ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError)):
        return "TRANSIENT_IO", True
    if isinstance(error, OSError) and error.errno in {
        None,
        errno.EIO,
        errno.ETIMEDOUT,
        errno.ECONNRESET,
        errno.ECONNREFUSED,
    }:
        return "TRANSIENT_IO", True
    if "RECOVER_ACTIVE_BATCH_FIRST" in message:
        return "RECOVERY_REQUIRED", False
    if isinstance(error, ValueError):
        return "CONTRACT", False
    return "UNCLASSIFIED", False
