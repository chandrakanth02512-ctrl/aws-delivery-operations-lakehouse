from datetime import UTC, datetime

import pytest
from pyspark.sql import functions as F

from delivery_lakehouse.cdc.frames import CHANGE_SCHEMA, RECORD_STATE_SCHEMA
from delivery_lakehouse.cdc.silver import choose_changes

pytestmark = pytest.mark.spark


def event(version, instant, change_id, line):
    return {
        "_raw_json": "{}",
        "_source_file": "fixture.jsonl",
        "_source_line": line,
        "_batch_id": "a" * 64,
        "_ingested_at": "2026-02-12T00:00:00Z",
        "_record_hash": str(line),
        "_record_id": str(line),
        "_operation": "UPDATE",
        "_source_version": version,
        "_source_event_at": datetime.fromisoformat(instant),
        "_change_id": change_id,
        "_record_key": "SH1",
        "_cdc_raw": "{}",
        "_validation_json": "{}",
    }


@pytest.mark.parametrize("deleted", [False, True])
def test_source_version_precedes_event_time_and_tombstones_keep_ordering(spark, deleted):
    state = spark.createDataFrame(
        [("shipments", "SH1", 5, datetime(2026, 2, 10, tzinfo=UTC), "b", deleted, "old")],
        RECORD_STATE_SCHEMA,
    )
    rows = [
        event(4, "2026-02-12T00:00:00+00:00", "z", 1),
        event(5, "2026-02-09T00:00:00+00:00", "z", 2),
        event(5, "2026-02-10T00:00:00+00:00", "a", 3),
        event(5, "2026-02-10T00:00:00+00:00", "b", 4),
        event(5, "2026-02-11T00:00:00+00:00", "z", 5),
        event(6, "2026-02-08T00:00:00+00:00", "x", 6),
        event(6, "2026-02-08T00:00:00+00:00", "x", 7),
    ]
    frame = spark.createDataFrame(rows, CHANGE_SCHEMA)
    winner, ignored = choose_changes(frame, state, "shipments")
    assert winner.first()._source_version == 6
    assert winner.count() == 1
    assert ignored.count() == 6
    reasons = {
        row.reason for row in ignored.select(F.explode("_reasons").alias("reason")).collect()
    }
    assert reasons == {"STALE_CHANGE", "REPLAYED_CHANGE", "SUPERSEDED_CHANGE", "DUPLICATE_CHANGE"}


def test_conflicting_changes_have_a_stable_tie_breaker(spark):
    state = spark.createDataFrame([], RECORD_STATE_SCHEMA)
    rows = [
        event(3, "2026-02-09T00:00:00+00:00", "a", 1),
        event(3, "2026-02-09T00:00:00+00:00", "z", 2),
    ]
    for ordered in (rows, list(reversed(rows))):
        winner, ignored = choose_changes(
            spark.createDataFrame(ordered, CHANGE_SCHEMA), state, "shipments"
        )
        assert winner.first()._change_id == "z"
        assert ignored.first()._reasons == ["SUPERSEDED_CHANGE"]
