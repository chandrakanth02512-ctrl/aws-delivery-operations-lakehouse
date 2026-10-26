"""Plan executions or run them locally. This CLI never starts an AWS workload."""

import argparse
import fcntl
import json
from pathlib import Path

from delivery_lakehouse.config import Settings
from delivery_lakehouse.ingestion.storage import LocalStore
from delivery_lakehouse.logging_config import configure_logging
from delivery_lakehouse.operations.requests import aws_execution_request, execution_request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["plan-aws", "run-local"])
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--mode", choices=["snapshot", "cdc"], default="cdc")
    parser.add_argument("--config", default="config/dev.json")
    parser.add_argument("--root", type=Path, default=Path("data/operations"))
    parser.add_argument("--execution-id")
    parser.add_argument("--max-attempts", type=int, default=1)
    parser.add_argument("--retry-delay-seconds", type=float, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--adopt-phase1-snapshot", action="store_true")
    parser.add_argument(
        "--iceberg-jar",
        type=Path,
        default=Path("artifacts/iceberg-spark-runtime-4.1_2.13-1.11.0.jar"),
    )
    args = parser.parse_args()
    settings = Settings.load(args.config)
    catalog = "glue_catalog" if args.command == "plan-aws" else "lakehouse"
    options = {
        "catalog": catalog,
        "adopt_snapshot": args.adopt_phase1_snapshot,
        "max_attempts": args.max_attempts,
        "retry_delay_seconds": args.retry_delay_seconds,
    }
    request = execution_request(settings, args.batch_id, args.mode, **options)
    if args.command == "plan-aws":
        print(json.dumps(aws_execution_request(request, execution_id=args.execution_id), indent=2))
        return
    if not args.iceberg_jar.is_file():
        parser.error("Run scripts/fetch_iceberg.py before local execution")
    from delivery_lakehouse.operations.runner import run_execution
    from delivery_lakehouse.spark import session

    configure_logging()
    store = LocalStore(args.root)
    lock = store.path("gold/_cdc/local.lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        spark = session(
            iceberg_jar=args.iceberg_jar.resolve(), warehouse=(args.root / "_catalog").resolve()
        )
        try:
            for layer in ("silver", "gold"):
                spark.sql(
                    f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{settings.database_prefix}_{layer}"
                )
            result = run_execution(
                spark,
                store,
                settings,
                args.batch_id,
                mode=args.mode,
                execution_id=args.execution_id,
                resume=args.resume,
                **options,
            )
            print(json.dumps(result, indent=2))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
