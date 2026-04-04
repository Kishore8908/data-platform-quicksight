"""
kinesis/producer.py
===================
Kinesis Data Stream producer for ingesting raw records
into the data platform.

Usage:
    python producer.py --stream my-stream --records sample_data.json

Author: Biswajit Praharaj
GitHub: github.com/Biswajit107927
"""

import boto3
import json
import logging
import argparse
import uuid
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def get_kinesis_client(region: str = "us-east-1"):
    """Initialise and return a Kinesis client."""
    return boto3.client("kinesis", region_name=region)


def publish_record(
    client,
    stream_name: str,
    record: dict,
    partition_key: Optional[str] = None
) -> dict:
    """
    Publish a single record to a Kinesis Data Stream.

    Args:
        client: Boto3 Kinesis client
        stream_name: Name of the Kinesis stream
        record: Dict to publish as JSON
        partition_key: Optional partition key (default: random UUID)

    Returns:
        Kinesis PutRecord response
    """
    # Add ingestion metadata
    record["_ingested_at"] = datetime.utcnow().isoformat()
    record["_record_id"] = str(uuid.uuid4())

    response = client.put_record(
        StreamName=stream_name,
        Data=json.dumps(record).encode("utf-8"),
        PartitionKey=partition_key or str(uuid.uuid4())
    )

    logger.info(
        f"Published record {record['_record_id']} "
        f"to shard {response['ShardId']}"
    )
    return response


def publish_batch(
    client,
    stream_name: str,
    records: list[dict],
    partition_key_field: Optional[str] = None
) -> dict:
    """
    Publish a batch of records to Kinesis (max 500 per batch).

    Args:
        client: Boto3 Kinesis client
        stream_name: Name of the Kinesis stream
        records: List of dicts to publish
        partition_key_field: Field to use as partition key

    Returns:
        Summary of successful and failed records
    """
    if not records:
        logger.warning("No records to publish")
        return {"success": 0, "failed": 0}

    # Kinesis batch limit is 500 records
    batch_size = 500
    total_success = 0
    total_failed = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]

        kinesis_records = []
        for record in batch:
            record["_ingested_at"] = datetime.utcnow().isoformat()
            record["_record_id"] = str(uuid.uuid4())

            partition_key = (
                str(record.get(partition_key_field, ""))
                if partition_key_field
                else str(uuid.uuid4())
            )

            kinesis_records.append({
                "Data": json.dumps(record).encode("utf-8"),
                "PartitionKey": partition_key
            })

        response = client.put_records(
            StreamName=stream_name,
            Records=kinesis_records
        )

        success = len(batch) - response["FailedRecordCount"]
        total_success += success
        total_failed += response["FailedRecordCount"]

        logger.info(
            f"Batch {i // batch_size + 1}: "
            f"{success} succeeded, {response['FailedRecordCount']} failed"
        )

    logger.info(
        f"Publish complete: {total_success} succeeded, {total_failed} failed"
    )
    return {"success": total_success, "failed": total_failed}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kinesis producer")
    parser.add_argument("--stream", required=True, help="Kinesis stream name")
    parser.add_argument("--records", required=True, help="Path to JSON records file")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--partition-key", help="Field to use as partition key")
    args = parser.parse_args()

    with open(args.records) as f:
        records = json.load(f)

    client = get_kinesis_client(args.region)
    result = publish_batch(client, args.stream, records, args.partition_key)
    print(f"\nResult: {result}")
