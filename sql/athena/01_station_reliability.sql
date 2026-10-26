-- Use workgroup delivery-ops-dev (Athena engine 3).
-- Recompute ratios from additive counts; do not average daily percentages.
SELECT s.station_id, s.station_name, s.region,
       sum(k.total_shipments) AS shipments,
       100.0 * sum(k.on_time_deliveries) / nullif(sum(k.delivered_shipments), 0) AS on_time_pct,
       100.0 * sum(k.failed_shipments) /
           nullif(sum(k.delivered_shipments + k.failed_shipments), 0) AS failed_delivery_pct,
       sum(k.total_delay_minutes) / nullif(sum(k.delivered_shipments), 0) AS avg_delay_minutes
FROM delivery_ops_dev_gold.kpi_station_daily k
JOIN delivery_ops_dev_gold.dim_station s ON k.station_id = s.station_id
WHERE k.promised_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
GROUP BY s.station_id, s.station_name, s.region
ORDER BY failed_delivery_pct DESC;
