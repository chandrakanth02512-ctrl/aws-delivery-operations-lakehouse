-- Association, not a causal claim. Synthetic data deliberately correlates these conditions.
SELECT weather, traffic, service_level, count(*) AS delivered_shipments,
       avg(delivery_delay_minutes) AS avg_delay_minutes,
       100.0 * sum(on_time_count) / count(*) AS on_time_pct
FROM delivery_ops_dev_gold.fact_shipments
WHERE status = 'DELIVERED'
GROUP BY weather, traffic, service_level
HAVING count(*) >= 5
ORDER BY avg_delay_minutes DESC;
