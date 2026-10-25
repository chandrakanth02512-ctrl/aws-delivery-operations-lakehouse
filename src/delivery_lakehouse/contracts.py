"""Ordered source contracts shared by generation, validation and documentation."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Field:
    kind: str = "string"
    required: bool = True
    allowed: tuple[str, ...] = ()
    minimum: float | None = None
    description: str = ""


@dataclass(frozen=True)
class Contract:
    primary_key: str
    fields: dict[str, Field]
    foreign_keys: dict[str, tuple[str, str]] = field(default_factory=dict)


def text(description="", *, required=True, allowed=()):
    return Field(required=required, allowed=allowed, description=description)


def timestamp(description="", *, required=True):
    return Field("timestamp", required=required, description=description)


CONTRACT_VERSION = 1
CONTRACTS = {
    "delivery_stations": Contract(
        "station_id",
        {
            "station_id": text("Synthetic station business key"),
            "station_name": text("Fictional station name"),
            "region": text("Australian state", allowed=("NSW", "VIC", "QLD")),
            "timezone": text("IANA business timezone"),
            "daily_capacity": Field("int", minimum=1, description="Planned parcels per day"),
            "updated_at": timestamp("Source record revision time"),
        },
    ),
    "drivers": Contract(
        "driver_id",
        {
            "driver_id": text("Synthetic driver business key"),
            "station_id": text("Assigned station"),
            "driver_name": text("Fictional display name; no personal data"),
            "hire_date": Field("date", description="Employment start date"),
            "vehicle_type": text(allowed=("VAN", "ELECTRIC_VAN", "BIKE")),
            "status": text(allowed=("ACTIVE", "INACTIVE")),
            "updated_at": timestamp(),
        },
        {"station_id": ("delivery_stations", "station_id")},
    ),
    "customers": Contract(
        "customer_id",
        {
            "customer_id": text("Synthetic customer business key"),
            "region": text(allowed=("NSW", "VIC", "QLD")),
            "postcode": text("Synthetic region-compatible postcode"),
            "address_type": text(allowed=("RESIDENTIAL", "BUSINESS")),
            "access_difficulty": text(allowed=("LOW", "MEDIUM", "HIGH")),
            "updated_at": timestamp(),
        },
    ),
    "shipments": Contract(
        "shipment_id",
        {
            "shipment_id": text("One parcel / shipment business key"),
            "customer_id": text(),
            "station_id": text(),
            "assigned_driver_id": text(),
            "created_at": timestamp(),
            "promised_at": timestamp("Inclusive delivery deadline"),
            "delivered_at": timestamp(
                "Actual delivery time; delivered shipments only", required=False
            ),
            "status": text(allowed=("DELIVERED", "FAILED", "IN_TRANSIT", "CANCELLED")),
            "service_level": text(allowed=("STANDARD", "EXPRESS")),
            "weight_kg": Field("double", minimum=0.01),
            "distance_km": Field("double", minimum=0),
            "weather": text(allowed=("CLEAR", "RAIN", "STORM")),
            "traffic": text(allowed=("LOW", "MEDIUM", "HIGH")),
            "updated_at": timestamp(),
        },
        {
            "customer_id": ("customers", "customer_id"),
            "station_id": ("delivery_stations", "station_id"),
            "assigned_driver_id": ("drivers", "driver_id"),
        },
    ),
    "delivery_attempts": Contract(
        "attempt_id",
        {
            "attempt_id": text("Unique delivery attempt"),
            "shipment_id": text(),
            "driver_id": text(),
            "station_id": text(),
            "attempt_number": Field("int", minimum=1),
            "attempted_at": timestamp(),
            "status": text(allowed=("DELIVERED", "FAILED")),
            "failure_reason": text(
                required=False,
                allowed=("CUSTOMER_UNAVAILABLE", "ACCESS_ISSUE", "WEATHER", "VEHICLE_BREAKDOWN"),
            ),
            "duration_minutes": Field("double", minimum=0),
            "updated_at": timestamp(),
        },
        {
            "shipment_id": ("shipments", "shipment_id"),
            "driver_id": ("drivers", "driver_id"),
            "station_id": ("delivery_stations", "station_id"),
        },
    ),
    "delivery_events": Contract(
        "event_id",
        {
            "event_id": text("Unique operational event"),
            "shipment_id": text(),
            "attempt_id": text(required=False),
            "station_id": text(),
            "event_type": text(
                allowed=("CREATED", "OUT_FOR_DELIVERY", "DELIVERED", "ATTEMPT_FAILED")
            ),
            "event_at": timestamp(),
            "updated_at": timestamp(),
        },
        {
            "shipment_id": ("shipments", "shipment_id"),
            "attempt_id": ("delivery_attempts", "attempt_id"),
            "station_id": ("delivery_stations", "station_id"),
        },
    ),
}
