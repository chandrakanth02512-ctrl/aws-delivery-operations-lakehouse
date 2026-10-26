"""Explicit local CDC commands and an approval-gated S3 source upload."""

import argparse
import json
from pathlib import Path

from delivery_lakehouse.cdc.bronze import read_manifest
from delivery_lakehouse.cdc.generator import write_sequence
from delivery_lakehouse.config import Settings, batch_identifier
from delivery_lakehouse.generator import digest
from delivery_lakehouse.ingestion.storage import LocalStore, S3Store
from delivery_lakehouse.logging_config import configure_logging


def main():
    parser = argparse.ArgumentParser(
        description="CDC processing; local by default, no AWS execution"
    )
    parser.add_argument("command", choices=["generate", "run-local", "upload-landing"])
    parser.add_argument("--config", default="config/dev.json")
    parser.add_argument("--root", type=Path, default=Path("data/cdc"))
    parser.add_argument("--batch-id")
    parser.add_argument(
        "--iceberg-jar",
        type=Path,
        default=Path("artifacts/iceberg-spark-runtime-4.1_2.13-1.11.0.jar"),
    )
    parser.add_argument("--adopt-phase1-snapshot", action="store_true")
    parser.add_argument("--approve-s3-write", action="store_true")
    args = parser.parse_args()
    settings = Settings.load(args.config)
    configure_logging()
    store = LocalStore(args.root)
    if args.command == "generate":
        print(json.dumps(write_sequence(store, settings.profile, settings.seed), indent=2))
        return
    if not args.batch_id:
        parser.error("--batch-id is required")
    batch_identifier(args.batch_id)
    if args.command == "upload-landing":
        if not args.approve_s3_write:
            parser.error("S3 upload requires explicit --approve-s3-write")
        manifest = read_manifest(store, args.batch_id)
        prefix = f"landing/cdc/{args.batch_id}"
        for entry in manifest["files"].values():
            content = store.read(f"{prefix}/{entry['file']}")
            if digest(content) != entry["sha256"] or len(content.splitlines()) != entry["rows"]:
                raise ValueError("CDC_SOURCE_INTEGRITY_FAILURE")
        remote = S3Store(settings.bucket, settings.region)
        for entry in manifest["files"].values():
            key = f"{prefix}/{entry['file']}"
            remote.write(key, store.read(key), immutable=True)
        key = f"{prefix}/manifest.json"
        remote.write(key, store.read(key), immutable=True)
        print(f"Uploaded CDC batch {args.batch_id}")
        return
    if not args.iceberg_jar.is_file():
        parser.error("Run scripts/fetch_iceberg.py before local CDC execution")
    import fcntl

    from delivery_lakehouse.cdc.pipeline import run_cdc
    from delivery_lakehouse.spark import session

    # Enforce the same single-writer contract locally as Glue's MaxConcurrentRuns=1.
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
            result = run_cdc(
                spark,
                store,
                settings,
                args.batch_id,
                catalog="lakehouse",
                adopt_snapshot=args.adopt_phase1_snapshot,
            )
            print(json.dumps(result, indent=2))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
