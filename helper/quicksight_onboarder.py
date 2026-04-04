"""
lambda/quicksight_onboarder.py
==============================
AWS Lambda function that automatically onboards new Iceberg/Redshift
tables to Amazon QuickSight.

Triggered by: S3 Event (new permission config file uploaded)

What it does:
  1. Reads permission config from S3
  2. Creates a QS folder named after the database
  3. Creates a QS dataset using Redshift direct query
  4. Adds the dataset to the folder
  5. Applies user/group permissions from config

Refactored to use helpers for clean separation of concerns.

Author: Biswajit Praharaj
GitHub: github.com/Biswajit107927
"""

import json
import os
from helpers.quicksight_helper import QuickSightHelper
from helpers.s3_helper import S3Helper
from helpers.redshift_helper import RedshiftHelper
from helpers.aws_clients import get_account_id
from helpers.logger import get_logger

logger = get_logger(__name__)

# ── Environment Variables ─────────────────────────────────────────────────────
AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID") or get_account_id()
REDSHIFT_CLUSTER_ID = os.environ["REDSHIFT_CLUSTER_ID"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
REDSHIFT_IAM_ROLE = os.environ["REDSHIFT_IAM_ROLE"]
QS_DATASOURCE_ID = os.environ["QS_DATASOURCE_ID"]

# ── Initialise Helpers ────────────────────────────────────────────────────────
qs = QuickSightHelper(account_id=AWS_ACCOUNT_ID)
s3 = S3Helper()
rs = RedshiftHelper(
    cluster_id=REDSHIFT_CLUSTER_ID,
    database=REDSHIFT_DATABASE,
    secret_arn=REDSHIFT_SECRET_ARN
)


def lambda_handler(event, context):
    """
    Lambda entry point.
    Triggered by S3 PutObject when a permission config is uploaded.
    """
    logger.info(f"Lambda triggered: {len(event.get('Records', []))} record(s)")
    results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        try:
            result = process_config(bucket, key)
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to process {key}: {str(e)}")
            results.append({"key": key, "status": "failed", "error": str(e)})

    return {"statusCode": 200, "body": json.dumps(results)}


def process_config(bucket: str, key: str) -> dict:
    """Process a single permission config file."""
    config = s3.read_json(bucket, key)

    database = config["database"]
    table = config["table"]
    schema = config.get("redshift_schema", database)
    folder_name = config.get("folder_name", database)
    permissions = config.get("permissions", [])

    logger.info(f"Processing: {database}.{table}")

    # Register Iceberg schema in Redshift if not exists
    if not rs.schema_exists(schema):
        rs.register_iceberg_schema(schema, database, REDSHIFT_IAM_ROLE)

    # Get QS datasource ARN
    datasource_arn = qs.get_datasource_arn(QS_DATASOURCE_ID)

    # Create or get folder (named after database)
    folder_arn = qs.get_or_create_folder(folder_name)

    # Create dataset using Redshift direct query
    dataset_arn = qs.create_dataset(
        schema=schema,
        table=table,
        datasource_arn=datasource_arn,
        dataset_name=config.get("dataset_display_name")
    )

    # Add to folder
    qs.add_to_folder(folder_arn, dataset_arn)

    # Apply permissions
    qs.apply_permissions(dataset_arn, permissions)

    logger.info(f"Successfully onboarded: {database}.{table}")

    return {
        "table": f"{database}.{table}",
        "status": "success",
        "dataset_arn": dataset_arn,
        "folder_arn": folder_arn,
        "permissions_applied": len(permissions)
    }
