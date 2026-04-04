"""
glue/etl_job.py
===============
AWS Glue ETL job that:
  1. Reads raw data from Kinesis (via S3 landing zone)
  2. Applies transformations and builds custom tables
  3. Writes output to Apache Iceberg format on S3

This script is designed to be submitted as a Glue job
and orchestrated by Apache Airflow.

Author: Biswajit Praharaj
GitHub: github.com/Biswajit107927
"""

import sys
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Initialise Glue Context ───────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_path",        # S3 path of raw data (from Kinesis Firehose)
    "iceberg_database",   # Glue catalog database name
    "iceberg_table",      # Target Iceberg table name
    "iceberg_warehouse",  # S3 path for Iceberg warehouse
    "partition_field",    # Field to partition by (e.g. date)
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── Configure Iceberg ─────────────────────────────────────────────────────────

spark.conf.set("spark.sql.extensions",
               "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog",
               "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args["iceberg_warehouse"])
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
               "org.apache.iceberg.aws.s3.S3FileIO")


# ── Step 1: Read Raw Data from S3 Landing Zone ───────────────────────────────

def read_raw_data(source_path: str):
    """Read raw JSON data from S3 landing zone."""
    logger.info(f"Reading raw data from: {source_path}")

    df = spark.read.json(source_path)

    logger.info(f"Raw records loaded: {df.count()}")
    return df


# ── Step 2: Apply Transformations ────────────────────────────────────────────

def transform(df):
    """
    Apply business transformations:
    - Cast types
    - Add derived columns
    - Deduplicate
    - Add audit columns
    """
    logger.info("Applying transformations...")

    transformed = (
        df
        # Deduplicate by record_id
        .dropDuplicates(["_record_id"])

        # Drop records with missing critical fields
        .filter(
            F.col("_record_id").isNotNull() &
            F.col("_ingested_at").isNotNull()
        )

        # Cast ingestion timestamp
        .withColumn(
            "_ingested_at",
            F.to_timestamp(F.col("_ingested_at"))
        )

        # Add partition date column
        .withColumn(
            "partition_date",
            F.to_date(F.col("_ingested_at"))
        )

        # Add ETL audit columns
        .withColumn("_etl_job_name", F.lit(args["JOB_NAME"]))
        .withColumn("_etl_processed_at", F.lit(datetime.utcnow().isoformat()))
    )

    logger.info(f"Records after transformation: {transformed.count()}")
    return transformed


# ── Step 3: Write to Iceberg ──────────────────────────────────────────────────

def write_to_iceberg(df, database: str, table: str, partition_field: str):
    """
    Write transformed data to Apache Iceberg table.
    Creates table if it doesn't exist.
    Uses MERGE for upsert to handle late-arriving data.
    """
    full_table_name = f"glue_catalog.{database}.{table}"
    logger.info(f"Writing to Iceberg table: {full_table_name}")

    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database}")

    # Write to Iceberg — append mode with partition
    (
        df.write
        .format("iceberg")
        .mode("append")
        .option("write.distribution-mode", "hash")
        .partitionBy(partition_field)
        .saveAsTable(full_table_name)
    )

    # Run Iceberg maintenance — expire snapshots older than 7 days
    spark.sql(f"""
        CALL glue_catalog.system.expire_snapshots(
            '{full_table_name}',
            TIMESTAMP '{datetime.utcnow().isoformat()}',
            100
        )
    """)

    # Compact small files
    spark.sql(f"""
        CALL glue_catalog.system.rewrite_data_files(
            '{full_table_name}'
        )
    """)

    record_count = spark.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()[0][0]
    logger.info(f"Iceberg table {full_table_name} now has {record_count} records")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info(f"Glue job started: {args['JOB_NAME']}")
    logger.info(f"Source: {args['source_path']}")
    logger.info(f"Target: {args['iceberg_database']}.{args['iceberg_table']}")

    # Step 1 — Read
    raw_df = read_raw_data(args["source_path"])

    # Step 2 — Transform
    transformed_df = transform(raw_df)

    # Step 3 — Write to Iceberg
    write_to_iceberg(
        transformed_df,
        args["iceberg_database"],
        args["iceberg_table"],
        args["partition_field"]
    )

    logger.info("Glue job completed successfully")
    job.commit()


if __name__ == "__main__":
    main()
