# Source data dictionary

Contract version: 1.

Generated from `src/delivery_lakehouse/contracts.py`. Run `uv run python scripts/data_dictionary.py` after an intentional contract change.

UTF-8 newline-delimited JSON, one object per physical line. Business keys are synthetic strings; timestamps require explicit UTC/offset information. Silver timestamps are UTC Spark `timestamp` values. Numeric strings may be cast; invalid casts are quarantined. Empty/whitespace-only strings become null. Optional fields may be omitted. Additional fields are rejected in Phase 1.

## delivery_stations

Primary key: `station_id`.

| Field | Silver type | Required | Constraints / relationship | Meaning |
|---|---|---|---|---|
| `station_id` | string | yes | — | Synthetic station business key |
| `station_name` | string | yes | — | Fictional station name |
| `region` | string | yes | NSW, VIC, QLD | Australian state |
| `timezone` | string | yes | — | IANA business timezone |
| `daily_capacity` | int | yes | >= 1; finite | Planned parcels per day |
| `updated_at` | timestamp | yes | — | Source record revision time |

## drivers

Primary key: `driver_id`.

| Field | Silver type | Required | Constraints / relationship | Meaning |
|---|---|---|---|---|
| `driver_id` | string | yes | — | Synthetic driver business key |
| `station_id` | string | yes | FK → delivery_stations.station_id | Assigned station |
| `driver_name` | string | yes | — | Fictional display name; no personal data |
| `hire_date` | date | yes | — | Employment start date |
| `vehicle_type` | string | yes | VAN, ELECTRIC_VAN, BIKE | See layer rules |
| `status` | string | yes | ACTIVE, INACTIVE | See layer rules |
| `updated_at` | timestamp | yes | — | See layer rules |

## customers

Primary key: `customer_id`.

| Field | Silver type | Required | Constraints / relationship | Meaning |
|---|---|---|---|---|
| `customer_id` | string | yes | — | Synthetic customer business key |
| `region` | string | yes | NSW, VIC, QLD | See layer rules |
| `postcode` | string | yes | — | Synthetic region-compatible postcode |
| `address_type` | string | yes | RESIDENTIAL, BUSINESS | See layer rules |
| `access_difficulty` | string | yes | LOW, MEDIUM, HIGH | See layer rules |
| `updated_at` | timestamp | yes | — | See layer rules |

## shipments

Primary key: `shipment_id`.

| Field | Silver type | Required | Constraints / relationship | Meaning |
|---|---|---|---|---|
| `shipment_id` | string | yes | — | One parcel / shipment business key |
| `customer_id` | string | yes | FK → customers.customer_id | See layer rules |
| `station_id` | string | yes | FK → delivery_stations.station_id | See layer rules |
| `assigned_driver_id` | string | yes | FK → drivers.driver_id | See layer rules |
| `created_at` | timestamp | yes | — | See layer rules |
| `promised_at` | timestamp | yes | — | Inclusive delivery deadline |
| `delivered_at` | timestamp | no | — | Actual delivery time; delivered shipments only |
| `status` | string | yes | DELIVERED, FAILED, IN_TRANSIT, CANCELLED | See layer rules |
| `service_level` | string | yes | STANDARD, EXPRESS | See layer rules |
| `weight_kg` | double | yes | >= 0.01; finite | See layer rules |
| `distance_km` | double | yes | >= 0; finite | See layer rules |
| `weather` | string | yes | CLEAR, RAIN, STORM | See layer rules |
| `traffic` | string | yes | LOW, MEDIUM, HIGH | See layer rules |
| `updated_at` | timestamp | yes | — | See layer rules |

## delivery_attempts

Primary key: `attempt_id`.

| Field | Silver type | Required | Constraints / relationship | Meaning |
|---|---|---|---|---|
| `attempt_id` | string | yes | — | Unique delivery attempt |
| `shipment_id` | string | yes | FK → shipments.shipment_id | See layer rules |
| `driver_id` | string | yes | FK → drivers.driver_id | See layer rules |
| `station_id` | string | yes | FK → delivery_stations.station_id | See layer rules |
| `attempt_number` | int | yes | >= 1; finite | See layer rules |
| `attempted_at` | timestamp | yes | — | See layer rules |
| `status` | string | yes | DELIVERED, FAILED | See layer rules |
| `failure_reason` | string | no | CUSTOMER_UNAVAILABLE, ACCESS_ISSUE, WEATHER, VEHICLE_BREAKDOWN | See layer rules |
| `duration_minutes` | double | yes | >= 0; finite | See layer rules |
| `updated_at` | timestamp | yes | — | See layer rules |

## delivery_events

Primary key: `event_id`.

| Field | Silver type | Required | Constraints / relationship | Meaning |
|---|---|---|---|---|
| `event_id` | string | yes | — | Unique operational event |
| `shipment_id` | string | yes | FK → shipments.shipment_id | See layer rules |
| `attempt_id` | string | no | FK → delivery_attempts.attempt_id | See layer rules |
| `station_id` | string | yes | FK → delivery_stations.station_id | See layer rules |
| `event_type` | string | yes | CREATED, OUT_FOR_DELIVERY, DELIVERED, ATTEMPT_FAILED | See layer rules |
| `event_at` | timestamp | yes | — | See layer rules |
| `updated_at` | timestamp | yes | — | See layer rules |

## Bronze and Silver lineage

Every input row retains these fields in Bronze, Silver and quarantine. Gold shipment facts carry `_batch_id`; attempt facts and source dimensions retain all lineage except `_raw_json`.

| Field | Type | Meaning |
|---|---|---|
| `_raw_json` | string | Original physical line, excluding newline delimiter |
| `_source_file` | string | Absolute local path or full S3 URI |
| `_source_line` | long | One-based physical line number |
| `_batch_id` | string | SHA-256 of the canonical manifest payload |
| `_ingested_at` | string | First successful Bronze ingestion time, UTC ISO-8601 |
| `_record_hash` | string | SHA-256 of raw line |
| `_record_id` | string | SHA-256 of batch/entity/line identity; duplicates stay distinct |
| `_errors` | array<string> | Quarantine only: all validation reasons |

Gold schemas and grains are described in [layers.md](layers.md); KPI formulas are defined in [kpis.md](kpis.md).
