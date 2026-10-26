from pyspark.sql import functions as F


class QualityGateError(ValueError):
    """Quarantine is persisted, but curated publication is blocked."""


def report(name, original, accepted, rejected):
    input_rows = original.count()
    accepted_rows = accepted.count()
    rejected_rows = rejected.count()
    if input_rows != accepted_rows + rejected_rows:
        raise QualityGateError(f"Row reconciliation failed for {name}")
    reasons = {
        row["reason"]: row["count"]
        for row in rejected.select(F.explode("_errors").alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    }
    return {
        "dataset": name,
        "input_rows": input_rows,
        "accepted_rows": accepted_rows,
        "quarantined_rows": rejected_rows,
        "reject_fraction": rejected_rows / input_rows if input_rows else 0.0,
        "reason_counts": dict(sorted(reasons.items())),
        "reconciled": True,
    }


def enforce(reports, max_reject_fraction):
    failed = [
        r["dataset"]
        for r in reports
        if r["reject_fraction"] > max_reject_fraction or r["accepted_rows"] == 0
    ]
    if failed:
        raise QualityGateError(f"Publication blocked by quality gate: {', '.join(failed)}")
