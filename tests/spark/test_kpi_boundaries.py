import json

import pytest

from delivery_lakehouse.generator import generate
from delivery_lakehouse.transforms.gold import build_gold
from delivery_lakehouse.transforms.silver import BRONZE_SCHEMA, normalize

pytestmark = pytest.mark.spark


def test_exact_deadline_redelivery_failure_and_zero_denominator(spark):
    data = generate(defects=False)
    shipments = data["shipments"][:5]
    for i, row in enumerate(shipments):
        row.update(
            created_at="2026-01-01T00:00:00Z",
            promised_at="2026-01-02T12:00:00Z",
            delivered_at=None,
            status="DELIVERED",
        )
        if i == 0:
            row["delivered_at"] = "2026-01-02T12:00:00Z"
        elif i == 1:
            row["delivered_at"] = "2026-01-02T13:00:00Z"
        elif i == 2:
            row["status"] = "FAILED"
        else:
            row["status"] = "IN_TRANSIT" if i == 3 else "CANCELLED"
            row["promised_at"] = "2026-01-03T12:00:00Z"
    template = data["delivery_attempts"][0]
    attempts = []
    for i, (shipment, number, status) in enumerate(
        [(0, 1, "DELIVERED"), (1, 1, "FAILED"), (1, 2, "DELIVERED"), (2, 1, "FAILED")]
    ):
        attempts.append(
            {
                **template,
                "attempt_id": f"T{i}",
                "shipment_id": shipments[shipment]["shipment_id"],
                "attempt_number": number,
                "status": status,
            }
        )
    data.update(shipments=shipments, delivery_attempts=attempts)
    frames = {}
    for name, rows in data.items():
        envelopes = [
            (json.dumps(row), "fixture", i, "a" * 64, "2026-02-06T00:00:00Z", str(i), str(i))
            for i, row in enumerate(rows)
        ]
        frames[name] = normalize(spark.createDataFrame(envelopes, BRONZE_SCHEMA), name).drop(
            "_errors"
        )
    daily = build_gold(spark, frames)["kpi_daily"].orderBy("promised_date").collect()
    completed, pending = daily
    assert completed.total_shipments == 3
    assert completed.on_time_delivery_pct == pytest.approx(50)
    assert completed.avg_delivery_delay_minutes == pytest.approx(30)
    assert float(completed.first_attempt_success_pct) == pytest.approx(100 / 3)
    assert float(completed.failed_delivery_pct) == pytest.approx(100 / 3)
    assert completed.failed_attempt_pct == pytest.approx(50)
    assert completed.total_attempts == 4
    assert pending.total_shipments == 2
    assert pending.on_time_delivery_pct is None
    assert pending.avg_delivery_delay_minutes is None
    assert pending.first_attempt_success_pct is None
    assert pending.failed_delivery_pct is None
