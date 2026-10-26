# Delivery operations analytics

The analytical model remains the established Gold star: `fact_shipments`, `fact_delivery_attempts`,
station/driver/customer/date dimensions and daily, station-daily and driver-daily KPI tables.
All data is synthetic. Business definitions are in [KPI definitions](kpis.md); these are portfolio
choices, not Amazon-internal formulas. Accepted deletes and late corrections restate historical cohorts.

## Query map

| Business question | Athena SQL | Interpretation |
|---|---|---|
| On-time delivery percentage | [07 daily metrics](../sql/athena/07_daily_delivery_metrics.sql), [01 stations](../sql/athena/01_station_reliability.sql) | On-time / delivered; deadline equality counts as on time |
| Average delivery delay | [07 daily metrics](../sql/athena/07_daily_delivery_metrics.sql), [02 conditions](../sql/athena/02_delay_conditions.sql) | Nonnegative delay / delivered, including on-time zeros |
| Failed delivery percentage | [07 daily metrics](../sql/athena/07_daily_delivery_metrics.sql), [01 stations](../sql/athena/01_station_reliability.sql) | Failed / (delivered + failed); cancelled and pending excluded |
| Driver performance | [03 drivers](../sql/athena/03_driver_performance.sql) | Assigned workload, attempts, first-attempt success and failed attempts |
| Delivery station performance | [01 stations](../sql/athena/01_station_reliability.sql) | Recomputed weighted rates by station; explicit date range |
| First-attempt delivery success | [07 daily metrics](../sql/athena/07_daily_delivery_metrics.sql), [03 drivers](../sql/athena/03_driver_performance.sql) | Successful first attempts / shipments with accepted first-attempt evidence |
| Delivery trends by date | [04 trends](../sql/athena/04_reliability_trend.sql) | Trailing seven **observed** dates, not seven calendar days |
| Cancellation rates | [08 cancellations](../sql/athena/08_cancellation_rates.sql) | Cancelled / all accepted shipments by date and station |
| Delayed shipment investigation | [09 late shipments](../sql/athena/09_delayed_shipments.sql) | Completed late deliveries, delay, attempts, conditions and assigned station/driver |

[05 attempts](../sql/athena/05_redelivery_and_failure_reasons.sql) separates failure reasons and redelivery counts.
[06 verification](../sql/athena/06_verification.sql) checks uniqueness, orphan attempts, evidence gaps
and snapshots. Adjust the explicit station query's January 2026 date filter for later CDC cohorts.
The SQL uses the configured `delivery_ops_dev_gold` database and Athena engine 3 conventions.
No query was submitted to AWS during implementation.

## Interpreting the results

Reporting dates are UTC promised-delivery dates. Current shipment assignment controls station/driver
shipment KPIs; attempt facts retain their actual historical driver. Compare workload, route distance,
traffic and weather before ranking drivers. Generated condition correlations are not causal findings.

Cancellation is deliberately separate from terminal failure: a five-shipment boundary cohort with
two delivered, one failed, one pending and one cancelled has a **20% cancellation rate** and a
**33.33% terminal delivery failure rate**. If one delivery is exactly on time and the other 60 minutes
late, on-time is **50%** and mean delivery delay is **30 minutes**. With one successful first attempt
among three shipments with first-attempt evidence, first-attempt success is **33.33%**. These exact
values are asserted by the Phase 3 tests.

For trends, a day with 1/2 deliveries on time followed by a day with 1/1 has a combined on-time rate
of **2/3**, not the average of 50% and 100%. The window query uses additive numerators and denominators;
gaps between observed dates do not create calendar rows. Zero denominators remain NULL, which means
insufficient applicable evidence rather than zero performance. Cancelled or pending shipments with
historical attempts still participate in attempt-evidence denominators.

The delayed-shipment query deliberately filters completed positive-delay deliveries. Pending overdue
shipments need an explicitly supplied as-of time and a separate SLA definition; this query does not
pretend a missing completion timestamp is zero delay. No real-time alert or dashboard is provisioned.

## Local SQL evidence and AWS limits

`tests/spark/test_analytics_sql.py` executes every ordinary query statement over generated local Gold
views. The Athena-specific `$snapshots` metadata query is excluded from that execution.
`tests/operations/test_analytics.py` independently asserts the hand-worked rates, station/driver
aggregation, cancellation details, late shipment selection and weighted trends with date gaps.
Phase 2 and operations integration tests compare incremental results against full recomputation.

Local Spark execution proves the shared relational expressions and business calculations. It does
not prove Athena parser/catalog/permissions behavior, query scan costs or managed Iceberg compatibility.
After approval, run the same SQL through the bounded Athena workgroup, retain QueryExecutionIds,
results and scanned bytes, and record that as separate live acceptance evidence.
