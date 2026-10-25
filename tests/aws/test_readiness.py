"""Opt-in READ-ONLY probes. Never start Glue jobs or Athena queries in this suite."""

import boto3
import pytest

from delivery_lakehouse.config import Settings

pytestmark = pytest.mark.aws


def test_existing_bucket_region():
    settings = Settings()
    client = boto3.client("s3", region_name=settings.region)
    assert (
        client.get_bucket_location(Bucket=settings.bucket)["LocationConstraint"] == settings.region
    )
