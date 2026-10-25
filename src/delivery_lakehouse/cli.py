import argparse
import json
from pathlib import Path

from delivery_lakehouse.config import Settings, batch_identifier
from delivery_lakehouse.generator import write_landing
from delivery_lakehouse.ingestion.bronze import ingest, validate_manifest
from delivery_lakehouse.ingestion.storage import LocalStore, S3Store
from delivery_lakehouse.logging_config import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Synthetic delivery lakehouse; local by default")
    parser.add_argument("command", choices=["generate", "bronze", "run-local", "upload-landing"])
    parser.add_argument("--config", default="config/dev.json")
    parser.add_argument("--root", type=Path, default=Path("data"))
    parser.add_argument("--batch-id")
    parser.add_argument("--profile", choices=["small", "medium", "large"])
    parser.add_argument("--approve-s3-write", action="store_true")
    args = parser.parse_args()
    configure_logging()
    settings = Settings.load(args.config)
    local = LocalStore(args.root)
    if args.command == "generate":
        manifest = write_landing(
            args.root,
            profile=args.profile or settings.profile,
            seed=settings.seed,
            start_date=settings.start_date,
        )
        print(manifest["batch_id"])
        return
    if not args.batch_id:
        parser.error("--batch-id is required")
    batch_identifier(args.batch_id)
    if args.command == "bronze":
        print(json.dumps(ingest(local, args.batch_id), indent=2))
    elif args.command == "run-local":
        from delivery_lakehouse.pipeline import run_pipeline
        from delivery_lakehouse.spark import session

        spark = session()
        try:
            print(json.dumps(run_pipeline(spark, local, settings, args.batch_id), indent=2))
        finally:
            spark.stop()
    else:
        if not args.approve_s3_write:
            parser.error("S3 uploads require explicit --approve-s3-write after AWS review")
        prefix = f"landing/{args.batch_id}"
        manifest_bytes = local.read(f"{prefix}/manifest.json")
        manifest = json.loads(manifest_bytes)
        validate_manifest(manifest, args.batch_id)
        # Validate checksums before the first remote write.
        from delivery_lakehouse.generator import digest

        for entry in manifest["files"].values():
            content = local.read(f"{prefix}/{entry['file']}")
            if digest(content) != entry["sha256"]:
                raise ValueError("Landing checksum mismatch")
        remote = S3Store(settings.bucket, settings.region)
        for entry in manifest["files"].values():
            key = f"{prefix}/{entry['file']}"
            remote.write(key, local.read(key), immutable=True)
        remote.write(f"{prefix}/manifest.json", manifest_bytes, immutable=True)
        print(f"Uploaded verified batch {args.batch_id}")


if __name__ == "__main__":
    main()
