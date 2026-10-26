# Performance review and bounded benchmark evidence

Phase 3 changes the Gold planner to return only tables affected by accepted changes. For an
event-only CDC batch, shipment/attempt facts and their KPI groups are unchanged, so the planner now
returns no Gold deltas. A dimension-only change builds only its dimension delta. Initial publication
still creates the complete dimensional model, including empty tables where appropriate.

This preserves the Phase 2 current-state model and avoids forcing empty Spark computations simply
to establish that nothing changed. Real integration tests verify unchanged snapshots for all nine
Gold tables, unchanged facts/KPI counts and agreement with full recomputation.

## Executed local comparison

```bash
uv run python scripts/benchmark_incremental_gold.py
```

The script reads the reviewed Phase 2 Gold planner from commit
`f0e4467a3cee75e3cdc9a5044d5f2c7a330022e4` and compares it with the current planner on identical
cached Gold data: 120 shipments and one new delivery event, with no affected shipment/attempt keys.
It forces each returned delta with `count()`, matching the publication planner's empty-delta check.
The baseline commit must exist locally; fetch that reviewed commit if using a shallow checkout.

Runtime: Python 3.13.5, Spark 4.1.1, Java 21.0.12.1; three repetitions after preparing/cache-warming
the shared input. Other local verification work ran concurrently, so wall times are descriptive,
not isolated throughput estimates. Job/table counts are the useful structural evidence.

| Measurement | Phase 2 baseline | Phase 3 |
|---|---:|---:|
| Gold tables planned per repetition | 9 | 0 |
| Spark jobs per repetition | 9 | 0 |
| Gold output rows per repetition | 0 | 0 |
| Median measured Gold planning/materialization time | 5,149.756 ms | 0.016 ms |

Raw measurements and candidate source SHA-256 are retained in
[phase3-gold-benchmark.json](evidence/phase3-gold-benchmark.json). The generated output also lives in
`artifacts/gold-planner-benchmark.json`. This proves elimination of unnecessary Gold work for this
specific case. It is **not** a whole-pipeline speedup, AWS DPU saving, managed Glue benchmark or
large-volume scalability claim. Source ingestion, validation, catalog checks and Silver writes still run.

## Review decisions and remaining work

- Keep complete prospective relationship validation before publication. Removing those scans without
  a proven affected-dependency model risks allowing parent changes to invalidate unchanged children.
- Recompute affected date cohorts from all their shipment facts. A delta-only denominator would
  produce wrong rates; attempts are aggregated before joining shipments to avoid fan-out.
- Retain small unpartitioned tables for the 120-shipment default. A future measured larger workload
  can evaluate Iceberg day transforms on shipment promise dates, attempt/event timestamps and
  station-oriented sorting. Do not introduce many tiny partitions without file/scan evidence.
- Curated MERGE may rewrite shared data files even when only a few keys change. No copy-on-write
  amplification or Athena scan-cost estimate has been measured on AWS.
- Snapshot verification intentionally reads all published rows. CDC post-verification restricts its
  logical comparisons to affected keys; physical pruning depends on file/partition layout.
- Python source decoding/generation remains bounded driver-side work. Medium/large CDC ingestion,
  distributed parsing, compaction and retention operations require a separate benchmark/review.
- No tombstone expiry or snapshot vacuum is enabled. Retention must preserve pending plans and
  replay protection; storage/request costs accumulate until an explicitly reviewed cleanup.
