-- Completed late deliveries; pending shipments need a separately defined as-of-time SLA analysis.
SELECT f.shipment_id, f.promised_date, s.station_name, d.driver_name,
       f.promised_at, f.delivered_at, f.delivery_delay_minutes, f.attempt_count,
       f.failed_attempts, f.service_level, f.weather, f.traffic, f.distance_km
FROM delivery_ops_dev_gold.fact_shipments f
JOIN delivery_ops_dev_gold.dim_station s ON f.station_id = s.station_id
JOIN delivery_ops_dev_gold.dim_driver d ON f.driver_id = d.driver_id
WHERE f.status = 'DELIVERED' AND f.delivery_delay_minutes > 0
ORDER BY f.delivery_delay_minutes DESC, f.shipment_id;
