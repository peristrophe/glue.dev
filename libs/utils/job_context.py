import sys
import logging
from typing import Generator
from contextlib import contextmanager

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.sql.session import SparkSession

from utils.dynamicframe import DynamicFrameReaderWriter


@contextmanager
def glue_job_tx() -> Generator[tuple[logging.Logger, DynamicFrameReaderWriter, Job], None, None]:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job_name = args['JOB_NAME']

    spark = SparkSession.builder.config(
        'spark.serializer', 'org.apache.spark.serializer.KryoSerializer').getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init(job_name, args)

    loader = DynamicFrameReaderWriter(glue_context)

    logger = logging.getLogger()
    logger.info('Start glue job')
    yield logger, loader, job

    job.commit()
    logger.info("Finish glue job")
