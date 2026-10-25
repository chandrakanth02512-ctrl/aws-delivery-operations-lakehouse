SELECT a.failure_reason, s.station_id, count(*) AS failed_attempts,
       count(DISTINCT a.shipment_id) AS affected_shipments
FROM delivery_ops_dev_gold.fact_delivery_attempts a
JOIN delivery_ops_dev_gold.fact_shipments s ON a.shipment_id = s.shipment_id
WHERE a.status = 'FAILED'
GROUP BY a.failure_reason, s.station_id
ORDER BY failed_attempts DESC;

SELECT attempt_count, status, count(*) AS shipments
FROM delivery_ops_dev_gold.fact_shipments
GROUP BY attempt_count, status
ORDER BY attempt_count, status;
