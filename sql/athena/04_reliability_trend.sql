-- Window the additive numerator and denominator, not the percentages.
SELECT promised_date, total_shipments, on_time_delivery_pct,
       100.0 * sum(on_time_deliveries) OVER (
           ORDER BY promised_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) /
       nullif(sum(delivered_shipments) OVER (
           ORDER BY promised_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0)
       AS trailing_7_observed_days_on_time_pct
FROM delivery_ops_dev_gold.kpi_daily
ORDER BY promised_date;
