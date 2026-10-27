# Layer contracts and quality policy

The snapshot rules below remain the Phase 1 contract. [Phase 2 CDC](cdc-architecture.md) adds full
after-images, source-version ordering, immutable incremental Bronze, explicit dependent deletes and
historical corrections. Both modes share these business schemas and Gold formulas. The CDC guide
documents its narrower rule changes for historical driver assignments and failed attempts.

## Landing and Bronze

Generate six UTF-8 JSONL files and a manifest under `landing/<batch-id>/`. A manifest contains contract
version, filename, checksum and row count for each entity. Only the six contracted filenames are
accepted. Landing objects are immutable; the uploader uses conditional S3 writes and uploads the
manifest last. Generating the same seed/profile/date produces the same bytes and batch ID.

Bronze validates the entire source batch before writing. It preserves every physical source line
in `_raw_json`, including malformed JSON, and adds filename, line number, UTC ingestion time, batch
ID, raw-line hash and record identity. It does not trim, cast or repair business values. UTF-8 decoding
and checksum failures fail the whole batch; they are source integrity failures, not row rejections.

Output objects are deterministic per batch/entity. A Bronze manifest is committed only after all
objects are written. Retries rewrite incomplete batches, while completed batches are read-only and
hash-verified. Replays also bind the committed manifest to the source batch, dataset set, expected
Bronze object keys and row counts. This depends on serialized writers; it is not a distributed ingestion lock.

## Silver

Processing order is stations → drivers → customers → shipments → attempts → events. Rejected parent
keys do not become valid parents. Children with absent accepted parents are quarantined.

1. Parse JSON objects, retain the raw line and lineage, normalize names to lower snake case (spaces
   and hyphens to underscores), reject collisions, nested values and unexpected fields. The fixed Phase 1 contract
   does not silently absorb schema evolution. Bare NaN/Infinity JSON constants are malformed even
   when they appear in a text field; quoted numeric values still follow the field's cast/domain rules.
2. Trim values and turn empty strings into null. Uppercase enumerations. Use ANSI-safe `try_cast`
   for dates, timestamps, integers and doubles; require an explicit timezone for timestamps.
3. Quarantine missing required values, invalid enums, invalid casts, nonfinite/negative metrics,
   promises/deliveries before creation, and revisions before their shipment lifecycle timestamps.
4. Require delivery timestamp iff shipment status is DELIVERED. Failed attempts require a valid
   failure reason; successful attempts must not have one. Attempt success must agree with the
   shipment delivery time/status. Cancelled/in-transit snapshots cannot have attempts in this model.
5. Check all FKs plus station/driver/shipment assignments and event-to-attempt shipment/station
   consistency. Driver reassignment within a shipment is deliberately outside Phase 1's contract.
6. Among otherwise valid duplicate primary keys, keep the greatest `updated_at`; ties use raw hash,
   source filename and line number. Quarantine all losers with `DUPLICATE_PRIMARY_KEY`. Also enforce
   unique `(shipment_id, attempt_number)` with `DUPLICATE_ATTEMPT_NUMBER`. Invalid newer versions do
   not evict valid records. This is snapshot conflict resolution, not CDC.
7. Materialize all six quarantine datasets and structured JSON quality reports **before** enforcing
   a configurable maximum rejection fraction, default 15% per dataset. Zero accepted rows also fail.
   Empty required source entities are rejected at the manifest contract. Every input row must satisfy
   `input_rows = accepted_rows + quarantined_rows`. A row can carry several reasons, so the sum of
   reason counts can exceed the number of rejected rows.

No customer, station or driver key is imputed. Optional nulls remain null. The quality gate controls
publication; failures leave reports available and set publication status FAILED. Exceptions are logged
as structured JSON. No raw customer rows are emitted to logs (all customers are synthetic anyway).

Quality checks do not claim exhaustive event-stream consistency: first-attempt evidence and
successful delivery evidence are exposed in Gold. Phase 2 handles late/out-of-order after-images;
it does not infer missing evidence. Source status remains the shipment-level outcome authority.

## Gold table dictionary

Dimensions use stable synthetic natural keys; they represent the current snapshot (Type 1), not SCD2.
No event-to-shipment fan-out is allowed: attempts are aggregated before joining the shipment fact.

| Table | Grain/key | Columns |
|---|---|---|
| `dim_station` | station_id | All Silver station fields and lineage except `_raw_json` |
| `dim_driver` | driver_id | All Silver driver fields and lineage except `_raw_json` |
| `dim_customer` | customer_id | All Silver customer fields and lineage except `_raw_json` |
| `dim_date` | date_key | Observed promised UTC date (`date`), year/month/day_of_week (`int`, Sunday=1) |
| `fact_shipments` | shipment_id | See detailed dictionary below |
| `fact_delivery_attempts` | attempt_id | All Silver attempt fields and lineage except `_raw_json` |
| `kpi_daily` | promised_date | Additive counters and ratios defined below |
| `kpi_station_daily` | promised_date, station_id | Same measures, station aggregation |
| `kpi_driver_daily` | promised_date, driver_id | Same measures, assigned-driver aggregation |

`dim_date` contains observed days, not a dense calendar. Reporting week/month trends can group by
this dimension; seven-row windows represent seven observed dates, which may not be consecutive.

### fact_shipments

| Columns | Type / definition |
|---|---|
| shipment_id, customer_id, station_id, driver_id | string; shipment and dimension keys |
| promised_date | date; UTC date of promised_at |
| created_at, promised_at, delivered_at | timestamp; from Silver, delivered_at nullable |
| status, service_level, weather, traffic | string; source contract enums |
| weight_kg, distance_km | double; from Silver |
| _batch_id | string; source snapshot or last accepted shipment after-image batch identity |
| delivered_count, on_time_count, failed_count | int; 0/1 indicator columns |
| delivery_delay_minutes | numeric; positive delay clamped at zero, null for non-delivered |
| attempt_count, failed_attempts | long; counts of accepted attempts |
| has_first_attempt, first_attempt_success | int; 0/1 evidence flags |
| delivery_evidence_complete | boolean; delivered shipments have exactly one accepted success; others none |

### KPI aggregate columns

`total_shipments`, `delivered_shipments`, `on_time_deliveries`, `failed_shipments`,
`shipments_with_first_attempt`, `first_attempt_successes`, `total_attempts`, `failed_attempts`,
`shipments_missing_delivery_evidence` are integral counters. `total_delay_minutes` is the sum of
nonnegative delivered delays (null if no delivered rows). `on_time_delivery_pct`,
`avg_delivery_delay_minutes`, `first_attempt_success_pct`, `failed_delivery_pct`,
`failed_attempt_pct` are numeric/decimal results from Spark SQL and nullable when denominators are
zero. Do not coerce unknown rates to zero or sum percentages. Exact formulas: [kpis.md](kpis.md).
