"""
helpers/s3_helper.py
====================
S3 read/write/list utility functions.

Usage:
    from helpers.s3_helper import S3Helper

    s3 = S3Helper()
    config = s3.read_json("my-bucket", "configs/table.json")
    s3.write_json("my-bucket", "output/result.json", data)
    keys = s3.list_keys("my-bucket", prefix="configs/")

Author: Biswajit Praharaj
GitHub: github.com/Biswajit107927
"""

import json
from typing import Optional
from helpers.aws_clients import get_client
from helpers.logger import get_logger

logger = get_logger(__name__)


class S3Helper:
    """Utility wrapper for common S3 operations."""

    def __init__(self, region: Optional[str] = None):
        self.client = get_client("s3", region)
        self.resource = None

    # ── Read ──────────────────────────────────────────────────────────────────

    def read_json(self, bucket: str, key: str) -> dict:
        """
        Read and parse a JSON file from S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Parsed JSON as dict

        Raises:
            ValueError: If file is not valid JSON
        """
        logger.info(f"Reading s3://{bucket}/{key}")
        response = self.client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")

        try:
            data = json.loads(content)
            logger.info(f"Successfully parsed JSON from s3://{bucket}/{key}")
            return data
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON at s3://{bucket}/{key}: {e}")

    def read_text(self, bucket: str, key: str) -> str:
        """
        Read a text file from S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            File content as string
        """
        logger.info(f"Reading text from s3://{bucket}/{key}")
        response = self.client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    # ── Write ─────────────────────────────────────────────────────────────────

    def write_json(
        self,
        bucket: str,
        key: str,
        data: dict,
        indent: int = 2
    ):
        """
        Write a dict as a JSON file to S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            data: Dict to serialise as JSON
            indent: JSON indentation (default: 2)
        """
        content = json.dumps(data, indent=indent, default=str)
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="application/json"
        )
        logger.info(f"Written JSON to s3://{bucket}/{key}")

    def write_text(self, bucket: str, key: str, content: str):
        """
        Write a string as a text file to S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            content: String content to write
        """
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="text/plain"
        )
        logger.info(f"Written text to s3://{bucket}/{key}")

    # ── List ──────────────────────────────────────────────────────────────────

    def list_keys(
        self,
        bucket: str,
        prefix: str = "",
        suffix: str = ""
    ) -> list[str]:
        """
        List all object keys in an S3 bucket matching prefix/suffix.

        Args:
            bucket: S3 bucket name
            prefix: Key prefix filter
            suffix: Key suffix filter (e.g. ".json")

        Returns:
            List of matching S3 keys
        """
        keys = []
        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if suffix and not key.endswith(suffix):
                    continue
                keys.append(key)

        logger.info(
            f"Found {len(keys)} keys in s3://{bucket}/{prefix} "
            f"with suffix '{suffix}'"
        )
        return keys

    def key_exists(self, bucket: str, key: str) -> bool:
        """
        Check if an S3 object exists.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            True if object exists, False otherwise
        """
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except self.client.exceptions.ClientError:
            return False

    # ── Delete ────────────────────────────────────────────────────────────────

    def delete_key(self, bucket: str, key: str):
        """
        Delete an S3 object.

        Args:
            bucket: S3 bucket name
            key: S3 object key to delete
        """
        self.client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Deleted s3://{bucket}/{key}")
