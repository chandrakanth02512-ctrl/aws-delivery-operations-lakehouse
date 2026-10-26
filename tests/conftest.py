import os
from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addoption("--run-aws", action="store_true", help="Allow read-only live AWS checks")


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "aws" in item.keywords and not config.getoption("--run-aws"):
            item.add_marker(pytest.mark.skip(reason="Live AWS access not authorized/configured"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delivery_lakehouse.spark import session

    jar = os.environ.get("ICEBERG_SPARK_JAR")
    if jar and not Path(jar).is_file():
        raise ValueError("ICEBERG_SPARK_JAR must reference a downloaded local JAR")
    instance = session(iceberg_jar=jar, warehouse=tmp_path_factory.mktemp("warehouse"))
    yield instance
    instance.stop()


@pytest.fixture(scope="session")
def local_run(spark, tmp_path_factory):
    from delivery_lakehouse.config import Settings
    from delivery_lakehouse.generator import write_landing
    from delivery_lakehouse.ingestion.storage import LocalStore
    from delivery_lakehouse.pipeline import run_pipeline

    root = tmp_path_factory.mktemp("lakehouse")
    manifest = write_landing(root)
    store = LocalStore(root)
    result = run_pipeline(spark, store, Settings(), manifest["batch_id"])
    return root, store, manifest, result
