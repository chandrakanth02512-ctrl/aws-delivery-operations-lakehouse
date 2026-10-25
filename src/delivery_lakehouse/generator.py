"""Reproducible snapshot sources with correlated operational conditions and defects."""

import hashlib
import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

from delivery_lakehouse.contracts import CONTRACT_VERSION, CONTRACTS

PROFILES = {"small": 120, "medium": 10_000, "large": 100_000}


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def iso(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def generate(profile="small", seed=42, start_date="2026-01-01", *, defects=True):
    """Return one full snapshot, with no dependency on wall-clock time or global RNG."""
    count = PROFILES[profile]
    rng = random.Random(seed)
    start = datetime.fromisoformat(start_date).replace(tzinfo=UTC)
    revised = iso(start + timedelta(days=35))
    data = {name: [] for name in CONTRACTS}
    for i, (region, zone) in enumerate(
        [("NSW", "Australia/Sydney"), ("VIC", "Australia/Melbourne"), ("QLD", "Australia/Brisbane")]
    ):
        data["delivery_stations"].append(
            {
                "station_id": f"ST{i:03}",
                "station_name": f"Synthetic {region} Hub",
                "region": region,
                "timezone": zone,
                "daily_capacity": 600 + i * 200,
                "updated_at": revised,
            }
        )
    for i in range(18):
        data["drivers"].append(
            {
                "driver_id": f"DR{i:04}",
                "station_id": f"ST{i % 3:03}",
                "driver_name": f"Synthetic Driver {i:04}",
                "hire_date": "2024-01-01",
                "vehicle_type": ["VAN", "ELECTRIC_VAN", "BIKE"][i % 3],
                "status": "ACTIVE",
                "updated_at": revised,
            }
        )
    for i in range(max(30, count // 2)):
        data["customers"].append(
            {
                "customer_id": f"CU{i:07}",
                "region": ["NSW", "VIC", "QLD"][i % 3],
                "postcode": ["2000", "3000", "4000"][i % 3],
                "address_type": rng.choice(["RESIDENTIAL", "RESIDENTIAL", "BUSINESS"]),
                "access_difficulty": rng.choice(["LOW", "LOW", "MEDIUM", "HIGH"]),
                "updated_at": revised,
            }
        )
    for i in range(count):
        customer = data["customers"][i % len(data["customers"])]
        station_index = ["NSW", "VIC", "QLD"].index(customer["region"])
        driver = data["drivers"][station_index + 3 * rng.randrange(6)]
        created = start + timedelta(days=i % 28, hours=rng.randrange(6))
        service_level = "EXPRESS" if i % 5 == 0 else "STANDARD"
        # Next-day afternoon/evening promises in Australia, earlier for express.
        promised = (created + timedelta(days=1)).replace(
            hour=4 if service_level == "EXPRESS" else 7, minute=0
        )
        weather = rng.choices(["CLEAR", "RAIN", "STORM"], [70, 25, 5])[0]
        traffic = rng.choice(["LOW", "MEDIUM", "HIGH"])
        # Seeded cohorts guarantee all business outcomes, even in the small profile.
        scenario = i % 20
        status = (
            "CANCELLED"
            if scenario == 19
            else ("IN_TRANSIT" if scenario == 18 else "FAILED" if scenario == 17 else "DELIVERED")
        )
        n_attempts = (
            0
            if status in {"CANCELLED", "IN_TRANSIT"}
            else (3 if status == "FAILED" else 2 if scenario in {3, 7, 11} else 1)
        )
        delay = rng.randint(15, 180) if scenario % 4 == 0 else -rng.randint(30, 180)
        delay += (90 if weather == "STORM" else 0) + (60 if traffic == "HIGH" else 0)
        delay += 45 if customer["access_difficulty"] == "HIGH" else 0
        delivered = promised + timedelta(minutes=delay, hours=24 * (n_attempts - 1))
        sid = f"SH{i:08}"
        station_id = driver["station_id"]
        data["shipments"].append(
            {
                "shipment_id": sid,
                "customer_id": customer["customer_id"],
                "station_id": station_id,
                "assigned_driver_id": driver["driver_id"],
                "created_at": iso(created),
                "promised_at": iso(promised),
                "delivered_at": iso(delivered) if status == "DELIVERED" else None,
                "status": status,
                "service_level": service_level,
                "weight_kg": round(rng.uniform(0.1, 20), 2),
                "distance_km": round(rng.uniform(1, 90), 2),
                "weather": weather,
                "traffic": traffic,
                "updated_at": revised,
            }
        )
        data["delivery_events"].append(
            {
                "event_id": f"EV{i:08}-00",
                "shipment_id": sid,
                "attempt_id": None,
                "station_id": station_id,
                "event_type": "CREATED",
                "event_at": iso(created),
                "updated_at": revised,
            }
        )
        for number in range(1, n_attempts + 1):
            success = status == "DELIVERED" and number == n_attempts
            attempted = delivered - timedelta(days=n_attempts - number)
            aid = f"AT{i:08}-{number}"
            data["delivery_attempts"].append(
                {
                    "attempt_id": aid,
                    "shipment_id": sid,
                    "driver_id": driver["driver_id"],
                    "station_id": station_id,
                    "attempt_number": number,
                    "attempted_at": iso(attempted),
                    "status": "DELIVERED" if success else "FAILED",
                    "failure_reason": None
                    if success
                    else (
                        "WEATHER"
                        if weather == "STORM"
                        else rng.choice(
                            ["CUSTOMER_UNAVAILABLE", "ACCESS_ISSUE", "VEHICLE_BREAKDOWN"]
                        )
                    ),
                    "duration_minutes": round(rng.uniform(2, 18), 1),
                    "updated_at": revised,
                }
            )
            for suffix, kind, when in [
                ("O", "OUT_FOR_DELIVERY", attempted - timedelta(hours=3)),
                ("R", "DELIVERED" if success else "ATTEMPT_FAILED", attempted),
            ]:
                data["delivery_events"].append(
                    {
                        "event_id": f"EV{i:08}-{number}{suffix}",
                        "shipment_id": sid,
                        "attempt_id": aid,
                        "station_id": station_id,
                        "event_type": kind,
                        "event_at": iso(when),
                        "updated_at": revised,
                    }
                )
    if defects:
        # Append isolated defects so the valid operational snapshot stays intact.
        for name in ("shipments", "delivery_attempts", "delivery_events"):
            rows = data[name]
            rows.append(dict(rows[0]))
            missing = dict(rows[1])
            missing[CONTRACTS[name].primary_key] = None
            rows.append(missing)
        for suffix, changes in {
            "status": {"status": "TELEPORTED"},
            "customer": {"customer_id": "CU-DOES-NOT-EXIST"},
            "weight": {"weight_kg": "not-a-number"},
            "time": {"promised_at": "2020-01-01T00:00:00Z"},
        }.items():
            data["shipments"].append(
                {**data["shipments"][0], "shipment_id": f"BAD-{suffix}", **changes}
            )
    return data


def write_landing(root: Path, **kwargs) -> dict:
    """Content-addressed source files + manifest, written only if absent/identical."""
    data = generate(**kwargs)
    files = {}
    encoded = {}
    for name, rows in data.items():
        content = "".join(canonical_json(row) + "\n" for row in rows).encode()
        encoded[name] = content
        files[name] = {"file": f"{name}.jsonl", "sha256": digest(content), "rows": len(rows)}
    payload = {"contract_version": CONTRACT_VERSION, "files": files}
    batch_id = digest(canonical_json(payload).encode())
    manifest = {**payload, "batch_id": batch_id}
    directory = root / "landing" / batch_id
    directory.mkdir(parents=True, exist_ok=True)
    for name, content in encoded.items():
        path = directory / files[name]["file"]
        if path.exists() and path.read_bytes() != content:
            raise ValueError(f"Immutable landing object changed: {path}")
        if not path.exists():
            path.write_bytes(content)
    (directory / "manifest.json").write_text(canonical_json(manifest) + "\n")
    return manifest
