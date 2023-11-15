import sys
import logging
from typing import Generator
from contextlib import contextmanager

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.sql.session import SparkSession


@contextmanager
def glue_job_tx() -> Generator[tuple[logging.Logger, GlueContext, Job], None, None]:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job_name = args["JOB_NAME"]

    spark = SparkSession.builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    context = GlueContext(spark.sparkContext)
    job = Job(context)
    job.init(job_name, args)

    logger = logging.getLogger()
    logger.info('Start glue job')
    yield logger, context, job

    job.commit()
    logger.info("Finish glue job")
