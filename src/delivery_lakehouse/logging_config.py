import json
import logging
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in (
            "batch_id",
            "execution_id",
            "attempt",
            "started_at",
            "completed_at",
            "dataset",
            "rows",
            "stage",
            "status",
            "duration_seconds",
            "rows_inserted",
            "rows_updated",
            "rows_deleted",
            "rows_rejected",
            "rows_ignored",
            "error_category",
            "input_rows",
            "output_rows",
            "planned_output_rows",
            "source_rows",
            "accepted_changes",
            "quality_reject_fraction",
        ):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    logging.getLogger("py4j").setLevel(logging.WARNING)
