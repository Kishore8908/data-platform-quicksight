"""
helpers/aws_clients.py
======================
Centralised boto3 client factory.

Why centralise clients?
  - Consistent retry configuration across all services
  - Single place to manage region, credentials, and timeouts
  - Easy to mock in unit tests
  - Avoids creating multiple clients for the same service

Usage:
    from helpers.aws_clients import get_client, get_resource

    qs  = get_client("quicksight")
    s3  = get_client("s3")
    sts = get_client("sts")

Author: Biswajit Praharaj
GitHub: github.com/Biswajit107927
"""

import os
import boto3
from botocore.config import Config
from functools import lru_cache
from helpers.logger import get_logger

logger = get_logger(__name__)

# ── Default Configuration ─────────────────────────────────────────────────────

DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Retry config — exponential backoff with up to 3 retries
RETRY_CONFIG = Config(
    region_name=DEFAULT_REGION,
    retries={
        "max_attempts": 3,
        "mode": "adaptive"          # adaptive mode backs off on throttling
    },
    connect_timeout=10,
    read_timeout=30,
)


# ── Client Factory ────────────────────────────────────────────────────────────

@lru_cache(maxsize=None)
def get_client(service_name: str, region: str = None) -> boto3.client:
    """
    Get a cached boto3 client for the given AWS service.

    Clients are cached per service+region combination using
    lru_cache — avoids creating new connections on every call.

    Args:
        service_name: AWS service name (e.g. 's3', 'quicksight', 'glue')
        region: AWS region (default: AWS_REGION env var or us-east-1)

    Returns:
        Configured boto3 client

    Example:
        qs_client = get_client("quicksight")
        s3_client = get_client("s3", region="eu-west-1")
    """
    config = Config(
        region_name=region or DEFAULT_REGION,
        retries={
            "max_attempts": 3,
            "mode": "adaptive"
        },
        connect_timeout=10,
        read_timeout=30,
    )

    client = boto3.client(service_name, config=config)
    logger.debug(f"boto3 client created: {service_name} in {region or DEFAULT_REGION}")
    return client


@lru_cache(maxsize=None)
def get_resource(service_name: str, region: str = None):
    """
    Get a cached boto3 resource for the given AWS service.

    Use resources for higher-level operations (e.g. S3 bucket/object).
    Use clients for lower-level API calls.

    Args:
        service_name: AWS service name (e.g. 's3', 'dynamodb')
        region: AWS region

    Returns:
        Configured boto3 resource
    """
    resource = boto3.resource(
        service_name,
        region_name=region or DEFAULT_REGION,
        config=RETRY_CONFIG
    )
    logger.debug(f"boto3 resource created: {service_name}")
    return resource


def get_account_id() -> str:
    """
    Get the current AWS account ID using STS.

    Returns:
        AWS account ID string

    Example:
        account_id = get_account_id()
        # "123456789012"
    """
    sts = get_client("sts")
    identity = sts.get_caller_identity()
    account_id = identity["Account"]
    logger.info(f"AWS Account ID: {account_id}")
    return account_id


def get_current_region() -> str:
    """
    Get the current AWS region from environment or instance metadata.

    Returns:
        AWS region string
    """
    return DEFAULT_REGION
