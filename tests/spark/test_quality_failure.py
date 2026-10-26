import json

import pytest

from delivery_lakehouse.config import Settings
from delivery_lakehouse.generator import write_landing
from delivery_lakehouse.ingestion.storage import LocalStore
from delivery_lakehouse.pipeline import run_pipeline
from delivery_lakehouse.quality.report import QualityGateError

pytestmark = pytest.mark.spark


def test_failed_quality_gate_persists_evidence_before_publication(spark, tmp_path):
    batch = write_landing(tmp_path)["batch_id"]
    store = LocalStore(tmp_path)
    with pytest.raises(QualityGateError, match="Publication blocked"):
        run_pipeline(spark, store, Settings(max_reject_fraction=0), batch)
    assert store.exists(f"quarantine/{batch}/quality_report.json")
    assert json.loads(store.read("gold/_publication.json"))["status"] == "FAILED"
    assert not (tmp_path / "silver").exists()
    assert not (tmp_path / "gold" / batch).exists()
    repaired = run_pipeline(spark, store, Settings(), batch)
    assert repaired["status"] == "SUCCEEDED"
    assert spark.read.parquet(repaired["tables"]["gold.fact_shipments"]).count() == 120
