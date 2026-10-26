import json

import pytest

from delivery_lakehouse.contracts import CONTRACTS
from delivery_lakehouse.generator import PROFILES, generate, write_landing


def test_determinism_and_seed_isolation():
    assert generate() == generate()
    assert generate(seed=43) != generate(seed=42)
    assert generate() == generate(seed=42)


def test_all_schemas_and_foreign_keys_without_defects():
    data = generate(defects=False)
    for name, contract in CONTRACTS.items():
        keys = [row[contract.primary_key] for row in data[name]]
        assert len(keys) == len(set(keys))
        for row in data[name]:
            assert set(row) == set(contract.fields)
            for column, spec in contract.fields.items():
                if spec.required:
                    assert row[column] is not None
                if spec.allowed and row[column] is not None:
                    assert row[column] in spec.allowed
            for column, (parent, key) in contract.foreign_keys.items():
                if row[column] is not None:
                    assert row[column] in {r[key] for r in data[parent]}


def test_business_scenarios_and_intentional_defects():
    data = generate()
    assert {r["status"] for r in data["shipments"]} >= {
        "DELIVERED",
        "FAILED",
        "IN_TRANSIT",
        "CANCELLED",
        "TELEPORTED",
    }
    assert any(r["attempt_number"] > 1 for r in data["delivery_attempts"])
    assert len({r["shipment_id"] for r in data["shipments"]}) < len(data["shipments"])
    assert any(r["delivered_at"] > r["promised_at"] for r in data["shipments"] if r["delivered_at"])
    assert any(
        r["delivered_at"] <= r["promised_at"] for r in data["shipments"] if r["delivered_at"]
    )


@pytest.mark.parametrize("profile,count", PROFILES.items())
def test_profile_sizes(profile, count):
    assert len(generate(profile, defects=False)["shipments"]) == count


def test_repeated_generation_same_files(tmp_path):
    first = write_landing(tmp_path)
    files = {p.name: p.read_bytes() for p in (tmp_path / "landing" / first["batch_id"]).iterdir()}
    assert write_landing(tmp_path) == first
    assert files == {
        p.name: p.read_bytes() for p in (tmp_path / "landing" / first["batch_id"]).iterdir()
    }
    assert json.loads(files["manifest.json"])["batch_id"] == first["batch_id"]
