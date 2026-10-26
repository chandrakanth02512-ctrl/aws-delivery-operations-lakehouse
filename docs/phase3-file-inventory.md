# Phase 3 file inventory

Changes relative to Phase 2 commit `f0e4467a3cee75e3cdc9a5044d5f2c7a330022e4`.
The implementation extends the existing runners; no Phase 1/2 test file is removed or rewritten.

## Added

- [`docs/analytics.md`](../docs/analytics.md)
- [`docs/evidence/phase3-gold-benchmark.json`](../docs/evidence/phase3-gold-benchmark.json)
- [`docs/evidence/phase3-local-verification.json`](../docs/evidence/phase3-local-verification.json)
- [`docs/monitoring.md`](../docs/monitoring.md)
- [`docs/operations.md`](../docs/operations.md)
- [`docs/performance.md`](../docs/performance.md)
- [`docs/phase3-file-inventory.md`](../docs/phase3-file-inventory.md)
- [`docs/phase3-verification.md`](../docs/phase3-verification.md)
- [`docs/release-checklist.md`](../docs/release-checklist.md)
- [`infrastructure/operations.yaml`](../infrastructure/operations.yaml)
- [`scripts/benchmark_incremental_gold.py`](../scripts/benchmark_incremental_gold.py)
- [`sql/athena/07_daily_delivery_metrics.sql`](../sql/athena/07_daily_delivery_metrics.sql)
- [`sql/athena/08_cancellation_rates.sql`](../sql/athena/08_cancellation_rates.sql)
- [`sql/athena/09_delayed_shipments.sql`](../sql/athena/09_delayed_shipments.sql)
- [`src/delivery_lakehouse/operations/__init__.py`](../src/delivery_lakehouse/operations/__init__.py)
- [`src/delivery_lakehouse/operations/cli.py`](../src/delivery_lakehouse/operations/cli.py)
- [`src/delivery_lakehouse/operations/events.py`](../src/delivery_lakehouse/operations/events.py)
- [`src/delivery_lakehouse/operations/requests.py`](../src/delivery_lakehouse/operations/requests.py)
- [`src/delivery_lakehouse/operations/retry.py`](../src/delivery_lakehouse/operations/retry.py)
- [`src/delivery_lakehouse/operations/runner.py`](../src/delivery_lakehouse/operations/runner.py)
- [`src/delivery_lakehouse/operations/verification.py`](../src/delivery_lakehouse/operations/verification.py)
- [`tests/operations/test_analytics.py`](../tests/operations/test_analytics.py)
- [`tests/operations/test_control.py`](../tests/operations/test_control.py)
- [`tests/operations/test_glue_adapter.py`](../tests/operations/test_glue_adapter.py)
- [`tests/operations/test_integration.py`](../tests/operations/test_integration.py)
- [`tests/operations/test_operations_infrastructure.py`](../tests/operations/test_operations_infrastructure.py)

## Modified

- [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
- [`README.md`](../README.md)
- [`docs/costs.md`](../docs/costs.md)
- [`docs/deployment.md`](../docs/deployment.md)
- [`docs/kpis.md`](../docs/kpis.md)
- [`docs/testing.md`](../docs/testing.md)
- [`docs/troubleshooting.md`](../docs/troubleshooting.md)
- [`jobs/phase1.py`](../jobs/phase1.py)
- [`pyproject.toml`](../pyproject.toml)
- [`src/delivery_lakehouse/__init__.py`](../src/delivery_lakehouse/__init__.py)
- [`src/delivery_lakehouse/cdc/gold.py`](../src/delivery_lakehouse/cdc/gold.py)
- [`src/delivery_lakehouse/cdc/pipeline.py`](../src/delivery_lakehouse/cdc/pipeline.py)
- [`src/delivery_lakehouse/logging_config.py`](../src/delivery_lakehouse/logging_config.py)
- [`src/delivery_lakehouse/pipeline.py`](../src/delivery_lakehouse/pipeline.py)
- [`uv.lock`](../uv.lock)

The existing bootstrap/main infrastructure, deployment workflow, source contracts and published
Phase 1/2 verification evidence are preserved. Generated lakehouse data, JVM dependencies, builds,
raw test logs and local runtime state remain ignored by Git.
