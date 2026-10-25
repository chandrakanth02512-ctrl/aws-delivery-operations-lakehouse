import pytest

from delivery_lakehouse.config import Settings, batch_identifier, identifier
from delivery_lakehouse.quality.report import QualityGateError, enforce


@pytest.mark.parametrize(
    "kwargs",
    [
        {"region": "us-east-1"},
        {"bucket": "unreviewed"},
        {"database_prefix": "db;DROP"},
        {"max_reject_fraction": 2},
        {"profile": "huge"},
    ],
)
def test_invalid_settings(kwargs):
    with pytest.raises(ValueError):
        Settings(**kwargs)


def test_identifiers_reject_traversal_and_sql():
    with pytest.raises(ValueError):
        identifier("db;DROP")
    with pytest.raises(ValueError):
        batch_identifier("../../")


def test_quality_gate_boundary_and_empty_dataset():
    enforce([{"dataset": "test", "reject_fraction": 0.15, "accepted_rows": 10}], 0.15)
    for fraction, accepted in [(0.151, 10), (0, 0)]:
        with pytest.raises(QualityGateError):
            enforce(
                [{"dataset": "test", "reject_fraction": fraction, "accepted_rows": accepted}], 0.15
            )
