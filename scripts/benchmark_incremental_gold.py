"""Measure event-only Gold planning against the preserved Phase 2 implementation, locally."""

import argparse
import json
import platform
import re
import statistics
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

from pyspark.sql import functions as F

from delivery_lakehouse.cdc.gold import plan_gold
from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.generator import digest, write_landing
from delivery_lakehouse.ingestion.bronze import ingest
from delivery_lakehouse.ingestion.storage import LocalStore
from delivery_lakehouse.spark import session
from delivery_lakehouse.transforms.gold import build_gold
from delivery_lakehouse.transforms.silver import BRONZE_SCHEMA, transform

ROOT = Path(__file__).resolve().parents[1]
BASELINE = "f0e4467a3cee75e3cdc9a5044d5f2c7a330022e4"


def baseline_planner(revision):
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise ValueError("Baseline must be a full, reviewed Git commit SHA")
    source = subprocess.check_output(
        ["git", "show", f"{revision}:src/delivery_lakehouse/cdc/gold.py"], cwd=ROOT, text=True
    )
    namespace = {}
    exec(compile(source, f"git:{revision}:cdc/gold.py", "exec"), namespace)
    return namespace["plan_gold"]


def measure(spark, planner, plan, silver, gold):
    group = f"gold-benchmark-{uuid.uuid4().hex}"
    spark.sparkContext.setJobGroup(group, "Event-only Gold planning benchmark")
    started = time.perf_counter()
    deltas = planner(spark, plan, silver, gold)
    rows = sum(frame.count() for frame in deltas.values())
    elapsed = (time.perf_counter() - started) * 1000
    jobs = spark.sparkContext.statusTracker().getJobIdsForGroup(group)
    return {
        "elapsed_ms": round(elapsed, 3),
        "spark_jobs": len(jobs),
        "output_rows": rows,
        "planned_tables": len(deltas),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", default=BASELINE)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 10:
        parser.error("Use 1–10 repetitions")
    baseline = baseline_planner(args.baseline)
    spark = session()
    try:
        with tempfile.TemporaryDirectory(prefix="delivery-gold-benchmark-") as temporary:
            store = LocalStore(Path(temporary))
            manifest = write_landing(Path(temporary))
            bronze = ingest(store, manifest["batch_id"])
            frames = {
                name: spark.read.schema(BRONZE_SCHEMA).json(store.uri(entry["key"]))
                for name, entry in bronze["files"].items()
            }
            silver = transform(frames)
            gold = build_gold(spark, silver.tables)
            for name, frame in gold.items():
                gold[name] = frame.cache()
                gold[name].count()
            event = (
                silver.tables["delivery_events"]
                .limit(1)
                .withColumn("event_id", F.lit("BENCHMARK-EVENT"))
            )
            winners = {name: frame.limit(0) for name, frame in silver.tables.items()}
            winners["delivery_events"] = event
            after = {
                **silver.tables,
                "delivery_events": silver.tables["delivery_events"].unionByName(event),
            }
            plan = {
                "winners": winners,
                "prospective": after,
                "counts": {
                    name: {"accepted_changes": int(name == "delivery_events")} for name in CONTRACTS
                },
            }
            measurements = {"phase2": [], "phase3": []}
            for _ in range(args.repetitions):
                measurements["phase2"].append(measure(spark, baseline, plan, silver.tables, gold))
                measurements["phase3"].append(measure(spark, plan_gold, plan, silver.tables, gold))
            assert all(
                sample["output_rows"] == 0 for values in measurements.values() for sample in values
            )
            report = {
                "workload": "one new delivery event; 120 shipments; cached Gold; no affected facts",
                "baseline_commit": args.baseline,
                "candidate_source_sha256": digest(
                    (ROOT / "src/delivery_lakehouse/cdc/gold.py").read_bytes()
                ),
                "python": platform.python_version(),
                "spark": spark.version,
                "java": spark._jvm.java.lang.System.getProperty("java.version"),
                "measurements": measurements,
                "median_ms": {
                    name: statistics.median(row["elapsed_ms"] for row in rows)
                    for name, rows in measurements.items()
                },
                "scope": (
                    "Gold planning and materializing deltas only; "
                    "not full-pipeline or AWS throughput"
                ),
            }
            output = ROOT / "artifacts/gold-planner-benchmark.json"
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(report, indent=2))
            for frame in gold.values():
                frame.unpersist()
            silver.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
