-- The cohort is the UTC promised-delivery date. Rates retain their correct denominators.
SELECT promised_date, total_shipments, delivered_shipments, failed_shipments,
       total_attempts, on_time_delivery_pct, avg_delivery_delay_minutes,
       first_attempt_success_pct, failed_delivery_pct,
       shipments_missing_delivery_evidence
FROM delivery_ops_dev_gold.kpi_daily
ORDER BY promised_date;
