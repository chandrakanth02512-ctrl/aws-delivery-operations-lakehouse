-- Compare workload, distance and condition mix before interpreting driver differences.
SELECT d.driver_id, d.driver_name, d.vehicle_type,
       sum(k.total_shipments) AS assigned_shipments,
       sum(k.total_attempts) AS attempts,
       100.0 * sum(k.first_attempt_successes) /
           nullif(sum(k.shipments_with_first_attempt), 0) AS first_attempt_success_pct,
       100.0 * sum(k.failed_attempts) / nullif(sum(k.total_attempts), 0) AS failed_attempt_pct
FROM delivery_ops_dev_gold.kpi_driver_daily k
JOIN delivery_ops_dev_gold.dim_driver d ON k.driver_id = d.driver_id
GROUP BY d.driver_id, d.driver_name, d.vehicle_type
ORDER BY first_attempt_success_pct;
