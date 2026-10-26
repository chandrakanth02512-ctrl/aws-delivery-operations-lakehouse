-- Cancellation is distinct from terminal delivery failure; all current shipments form its denominator.
SELECT promised_date, station_id, count(*) AS total_shipments,
       sum(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_shipments,
       100.0 * sum(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) /
           nullif(count(*), 0) AS cancellation_pct
FROM delivery_ops_dev_gold.fact_shipments
GROUP BY promised_date, station_id
ORDER BY promised_date, station_id;
