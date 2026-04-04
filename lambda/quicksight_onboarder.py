"""
lambda/quicksight_onboarder.py
==============================
AWS Lambda function that automatically onboards new Iceberg/Redshift
tables to Amazon QuickSight.

Triggered by: S3 Event (new table config file uploaded)

What it does:
  1. Reads permission config from S3
  2. Creates a QS folder named after the database
  3. Creates a QS dataset using Redshift direct query
  4. Adds the dataset to the folder
  5. Applies user/group permissions from config

Author: Biswajit Praharaj
GitHub: github.com/Biswajit107927
"""

import boto3
import json
import logging
import os
from typing import Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Environment Variables ─────────────────────────────────────────────────────
AWS_ACCOUNT_ID = os.environ["AWS_ACCOUNT_ID"]
QS_NAMESPACE = os.environ.get("QS_NAMESPACE", "default")
REDSHIFT_CLUSTER_ID = os.environ["REDSHIFT_CLUSTER_ID"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
REDSHIFT_IAM_ROLE = os.environ["REDSHIFT_IAM_ROLE"]

# ── AWS Clients ───────────────────────────────────────────────────────────────
qs_client = boto3.client("quicksight")
s3_client = boto3.client("s3")


# ── Helper: Read Permission Config from S3 ────────────────────────────────────

def read_permission_config(bucket: str, key: str) -> dict:
    """
    Read and parse the permission config JSON from S3.

    Expected config format:
    {
        "database": "marketing_db",
        "table": "campaign_spend",
        "folder_name": "Marketing Analytics",
        "permissions": [
            {
                "principal": "arn:aws:quicksight:us-east-1:123:user/default/john",
                "actions": ["quicksight:DescribeDataSet", "quicksight:QueryDataSet"]
            }
        ]
    }
    """
    logger.info(f"Reading permission config: s3://{bucket}/{key}")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    config = json.loads(response["Body"].read().decode("utf-8"))
    logger.info(f"Config loaded for table: {config.get('table')}")
    return config


# ── Step 1: Create or Get QS Folder ──────────────────────────────────────────

def get_or_create_folder(folder_name: str) -> str:
    """
    Create a QuickSight folder if it doesn't exist.
    Returns the folder ARN.

    Args:
        folder_name: Name of the folder (same as database name)

    Returns:
        Folder ARN
    """
    folder_id = folder_name.lower().replace(" ", "-")

    try:
        # Check if folder already exists
        response = qs_client.describe_folder(
            AwsAccountId=AWS_ACCOUNT_ID,
            FolderId=folder_id
        )
        folder_arn = response["Folder"]["Arn"]
        logger.info(f"Folder already exists: {folder_arn}")
        return folder_arn

    except qs_client.exceptions.ResourceNotFoundException:
        # Create new folder
        response = qs_client.create_folder(
            AwsAccountId=AWS_ACCOUNT_ID,
            FolderId=folder_id,
            Name=folder_name,
            FolderType="SHARED",
        )
        folder_arn = response["Arn"]
        logger.info(f"Folder created: {folder_arn}")
        return folder_arn


# ── Step 2: Create QS Dataset ─────────────────────────────────────────────────

def create_dataset(
    database: str,
    schema: str,
    table: str,
    dataset_name: Optional[str] = None
) -> str:
    """
    Create a QuickSight dataset using Redshift direct query mode.

    Args:
        database: Redshift database name
        schema: Redshift schema name
        table: Redshift table name
        dataset_name: Display name for the dataset

    Returns:
        Dataset ARN
    """
    dataset_id = f"{schema}-{table}".lower().replace("_", "-")
    dataset_name = dataset_name or f"{schema}.{table}"

    try:
        # Check if dataset already exists
        existing = qs_client.describe_data_set(
            AwsAccountId=AWS_ACCOUNT_ID,
            DataSetId=dataset_id
        )
        dataset_arn = existing["DataSet"]["Arn"]
        logger.info(f"Dataset already exists: {dataset_arn}")
        return dataset_arn

    except qs_client.exceptions.ResourceNotFoundException:
        pass

    # Create new dataset
    response = qs_client.create_data_set(
        AwsAccountId=AWS_ACCOUNT_ID,
        DataSetId=dataset_id,
        Name=dataset_name,
        ImportMode="DIRECT_QUERY",  # Live query — no SPICE import needed
        PhysicalTableMap={
            f"{schema}_{table}": {
                "RelationalTable": {
                    "DataSourceArn": f"arn:aws:quicksight:{os.environ.get('AWS_REGION', 'us-east-1')}:{AWS_ACCOUNT_ID}:datasource/redshift-{REDSHIFT_CLUSTER_ID}",
                    "Catalog": "ExternalCatalog",
                    "Schema": schema,
                    "Name": table,
                    "InputColumns": [
                        # Columns are auto-detected in direct query mode
                        # Add explicit columns here for column-level security
                    ]
                }
            }
        }
    )

    dataset_arn = response["Arn"]
    logger.info(f"Dataset created: {dataset_arn}")
    return dataset_arn


# ── Step 3: Add Dataset to Folder ────────────────────────────────────────────

def add_to_folder(folder_arn: str, dataset_arn: str):
    """
    Add a dataset to a QuickSight folder.

    Args:
        folder_arn: Target folder ARN
        dataset_arn: Dataset ARN to add
    """
    try:
        qs_client.create_folder_membership(
            AwsAccountId=AWS_ACCOUNT_ID,
            FolderId=folder_arn.split("/")[-1],
            MemberId=dataset_arn.split("/")[-1],
            MemberType="DATASET"
        )
        logger.info(f"Dataset added to folder: {folder_arn}")

    except qs_client.exceptions.ResourceExistsException:
        logger.info("Dataset already in folder — skipping")


# ── Step 4: Apply Permissions ─────────────────────────────────────────────────

def apply_permissions(dataset_arn: str, permissions: list[dict]):
    """
    Apply user/group permissions to a QuickSight dataset.

    Permission actions reference:
        Read:  quicksight:DescribeDataSet, quicksight:QueryDataSet,
               quicksight:ListTagsForResource, quicksight:DescribeDataSetPermissions
        Write: quicksight:UpdateDataSet, quicksight:DeleteDataSet,
               quicksight:GrantIngestion, quicksight:UpdateDataSetPermissions

    Args:
        dataset_arn: Dataset ARN to apply permissions to
        permissions: List of permission dicts from config file
    """
    if not permissions:
        logger.warning("No permissions specified — dataset will be private")
        return

    dataset_id = dataset_arn.split("/")[-1]

    qs_client.update_data_set_permissions(
        AwsAccountId=AWS_ACCOUNT_ID,
        DataSetId=dataset_id,
        GrantPermissions=[
            {
                "Principal": perm["principal"],
                "Actions": perm.get("actions", [
                    "quicksight:DescribeDataSet",
                    "quicksight:QueryDataSet",
                    "quicksight:ListTagsForResource",
                    "quicksight:DescribeDataSetPermissions"
                ])
            }
            for perm in permissions
        ]
    )

    logger.info(f"Permissions applied to {len(permissions)} principals")


# ── Main Lambda Handler ───────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Lambda entry point.
    Triggered by S3 PutObject event when a new permission config is uploaded.

    Event format (S3 trigger):
    {
        "Records": [{
            "s3": {
                "bucket": {"name": "my-config-bucket"},
                "object": {"key": "configs/marketing_db/campaign_spend.json"}
            }
        }]
    }
    """
    logger.info(f"Event received: {json.dumps(event)}")

    results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        try:
            # Step 1 — Read config
            config = read_permission_config(bucket, key)

            database = config["database"]
            table = config["table"]
            schema = config.get("redshift_schema", database)
            folder_name = config.get("folder_name", database)
            permissions = config.get("permissions", [])

            logger.info(f"Processing: {database}.{table}")

            # Step 2 — Create or get folder
            folder_arn = get_or_create_folder(folder_name)

            # Step 3 — Create dataset
            dataset_arn = create_dataset(
                database=REDSHIFT_DATABASE,
                schema=schema,
                table=table,
                dataset_name=config.get("dataset_display_name")
            )

            # Step 4 — Add to folder
            add_to_folder(folder_arn, dataset_arn)

            # Step 5 — Apply permissions
            apply_permissions(dataset_arn, permissions)

            results.append({
                "table": f"{database}.{table}",
                "status": "success",
                "dataset_arn": dataset_arn,
                "folder_arn": folder_arn
            })

            logger.info(f"Successfully onboarded: {database}.{table}")

        except Exception as e:
            logger.error(f"Failed to process {key}: {str(e)}")
            results.append({
                "key": key,
                "status": "failed",
                "error": str(e)
            })

    logger.info(f"Processing complete: {len(results)} configs processed")
    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }
