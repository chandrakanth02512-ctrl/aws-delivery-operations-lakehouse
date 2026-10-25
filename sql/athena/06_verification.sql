-- Run only after gold/_publication.json reports SUCCEEDED. This file executes nothing automatically.
SELECT count(*) AS shipments, count(DISTINCT shipment_id) AS distinct_shipments,
       count(DISTINCT _batch_id) AS snapshot_batches
FROM delivery_ops_dev_gold.fact_shipments;

SELECT count(*) AS orphan_attempts
FROM delivery_ops_dev_gold.fact_delivery_attempts a
LEFT JOIN delivery_ops_dev_gold.fact_shipments s ON a.shipment_id = s.shipment_id
WHERE s.shipment_id IS NULL;

SELECT sum(shipments_missing_delivery_evidence) AS evidence_gaps
FROM delivery_ops_dev_gold.kpi_daily;

-- Athena metadata table syntax. Inspect format-version in S3 metadata JSON as well.
SELECT * FROM "delivery_ops_dev_gold"."fact_shipments$snapshots";
