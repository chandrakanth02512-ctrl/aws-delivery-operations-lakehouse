"""AWS Glue 6.0 entry point. Job invocation requires separate owner approval."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from delivery_lakehouse.config import Settings, batch_identifier
from delivery_lakehouse.ingestion.storage import S3Store
from delivery_lakehouse.logging_config import configure_logging
from delivery_lakehouse.pipeline import run_pipeline


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "bucket", "region", "database_prefix", "batch_id", "max_reject_fraction"],
    )
    batch_identifier(args["batch_id"])
    settings = Settings(
        bucket=args["bucket"],
        region=args["region"],
        database_prefix=args["database_prefix"],
        max_reject_fraction=float(args["max_reject_fraction"]),
    )
    configure_logging()
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    job = Job(glue)
    job.init(args["JOB_NAME"], args)
    run_pipeline(
        spark,
        S3Store(settings.bucket, settings.region),
        settings,
        args["batch_id"],
        catalog="glue_catalog",
    )
    job.commit()


if __name__ == "__main__":
    main()
