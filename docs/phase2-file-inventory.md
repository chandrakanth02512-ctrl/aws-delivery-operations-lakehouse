# Phase 2 file inventory

Relative to Phase 1 commit `158986db17fff50d1280aca5d9d4021f28cfdc22`.

25 files created; 17 files modified. No existing files deleted.

## Created

- [`docs/cdc-architecture.md`](../docs/cdc-architecture.md)
- [`docs/cdc-operations.md`](../docs/cdc-operations.md)
- [`docs/phase2-file-inventory.md`](../docs/phase2-file-inventory.md)
- [`docs/phase2-verification.md`](../docs/phase2-verification.md)
- [`scripts/demo_cdc.py`](../scripts/demo_cdc.py)
- [`src/delivery_lakehouse/cdc/__init__.py`](../src/delivery_lakehouse/cdc/__init__.py)
- [`src/delivery_lakehouse/cdc/bronze.py`](../src/delivery_lakehouse/cdc/bronze.py)
- [`src/delivery_lakehouse/cdc/cli.py`](../src/delivery_lakehouse/cdc/cli.py)
- [`src/delivery_lakehouse/cdc/contracts.py`](../src/delivery_lakehouse/cdc/contracts.py)
- [`src/delivery_lakehouse/cdc/frames.py`](../src/delivery_lakehouse/cdc/frames.py)
- [`src/delivery_lakehouse/cdc/generator.py`](../src/delivery_lakehouse/cdc/generator.py)
- [`src/delivery_lakehouse/cdc/gold.py`](../src/delivery_lakehouse/cdc/gold.py)
- [`src/delivery_lakehouse/cdc/merge.py`](../src/delivery_lakehouse/cdc/merge.py)
- [`src/delivery_lakehouse/cdc/pipeline.py`](../src/delivery_lakehouse/cdc/pipeline.py)
- [`src/delivery_lakehouse/cdc/silver.py`](../src/delivery_lakehouse/cdc/silver.py)
- [`src/delivery_lakehouse/cdc/state.py`](../src/delivery_lakehouse/cdc/state.py)
- [`tests/cdc/conftest.py`](../tests/cdc/conftest.py)
- [`tests/cdc/test_bronze.py`](../tests/cdc/test_bronze.py)
- [`tests/cdc/test_integrity.py`](../tests/cdc/test_integrity.py)
- [`tests/cdc/test_merge.py`](../tests/cdc/test_merge.py)
- [`tests/cdc/test_ordering.py`](../tests/cdc/test_ordering.py)
- [`tests/cdc/test_protocol.py`](../tests/cdc/test_protocol.py)
- [`tests/cdc/test_recovery.py`](../tests/cdc/test_recovery.py)
- [`tests/cdc/test_sequence.py`](../tests/cdc/test_sequence.py)
- [`tests/cdc/test_state.py`](../tests/cdc/test_state.py)

## Modified

- [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
- [`README.md`](../README.md)
- [`docs/architecture.md`](../docs/architecture.md)
- [`docs/costs.md`](../docs/costs.md)
- [`docs/deployment.md`](../docs/deployment.md)
- [`docs/kpis.md`](../docs/kpis.md)
- [`docs/layers.md`](../docs/layers.md)
- [`docs/testing.md`](../docs/testing.md)
- [`docs/troubleshooting.md`](../docs/troubleshooting.md)
- [`jobs/phase1.py`](../jobs/phase1.py)
- [`pyproject.toml`](../pyproject.toml)
- [`src/delivery_lakehouse/__init__.py`](../src/delivery_lakehouse/__init__.py)
- [`src/delivery_lakehouse/logging_config.py`](../src/delivery_lakehouse/logging_config.py)
- [`src/delivery_lakehouse/pipeline.py`](../src/delivery_lakehouse/pipeline.py)
- [`src/delivery_lakehouse/spark.py`](../src/delivery_lakehouse/spark.py)
- [`src/delivery_lakehouse/transforms/gold.py`](../src/delivery_lakehouse/transforms/gold.py)
- [`uv.lock`](../uv.lock)

Generated datasets, local catalogs, JARs, coverage, JUnit evidence and build artifacts are gitignored.
IAM/CloudFormation templates and `.github/workflows/deploy.yml` are unchanged.
