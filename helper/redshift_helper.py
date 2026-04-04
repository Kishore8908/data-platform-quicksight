"""
helpers/redshift_helper.py
==========================
Redshift connection and query utilities.

Provides a clean interface for:
  - Running SQL queries via Redshift Data API (no VPC needed)
  - Registering Iceberg tables as external schemas
  - Running data quality checks
  - Managing external schemas

Usage:
    from helpers.redshift_helper import RedshiftHelper

    rs = RedshiftHelper(
        cluster_id="my-cluster",
        database="analytics",
        secret_arn="arn:aws:secretsmanager:..."
    )
    results = rs.execute_query("SELECT COUNT(*) FROM marketing.spend")
    rs.register_iceberg_schema("marketing", "marketing_db", iam_role)

Author: Biswajit Praharaj
GitHub: github.com/Biswajit107927
"""

import os
import time
from typing import Optional
from helpers.aws_clients import get_client
from helpers.logger import get_logger

logger = get_logger(__name__)


class RedshiftHelper:
    """
    Wrapper for Redshift Data API.
    Uses Data API — no need for VPC, bastion hosts, or direct connections.
    """

    def __init__(
        self,
        cluster_id: Optional[str] = None,
        database: Optional[str] = None,
        secret_arn: Optional[str] = None,
        region: Optional[str] = None
    ):
        self.cluster_id = cluster_id or os.environ["REDSHIFT_CLUSTER_ID"]
        self.database = database or os.environ["REDSHIFT_DATABASE"]
        self.secret_arn = secret_arn or os.environ["REDSHIFT_SECRET_ARN"]
        self.client = get_client("redshift-data", region)

        logger.info(
            f"RedshiftHelper initialised — "
            f"cluster: {self.cluster_id}, database: {self.database}"
        )

    # ── Query Execution ───────────────────────────────────────────────────────

    def execute_query(
        self,
        sql: str,
        wait: bool = True,
        timeout_seconds: int = 300
    ) -> Optional[list[dict]]:
        """
        Execute a SQL query on Redshift via Data API.

        Args:
            sql: SQL query string
            wait: Wait for completion (default: True)
            timeout_seconds: Max wait time in seconds

        Returns:
            List of result rows as dicts, or None if not waiting
        """
        logger.info(f"Executing query: {sql[:100]}...")

        response = self.client.execute_statement(
            ClusterIdentifier=self.cluster_id,
            Database=self.database,
            SecretArn=self.secret_arn,
            Sql=sql
        )

        statement_id = response["Id"]
        logger.info(f"Statement submitted: {statement_id}")

        if not wait:
            return None

        return self._wait_and_fetch(statement_id, timeout_seconds)

    def _wait_and_fetch(
        self,
        statement_id: str,
        timeout_seconds: int
    ) -> list[dict]:
        """
        Wait for a Redshift statement to complete and fetch results.

        Args:
            statement_id: Redshift Data API statement ID
            timeout_seconds: Max wait time

        Returns:
            List of result rows as dicts
        """
        elapsed = 0
        poll_interval = 5

        while elapsed < timeout_seconds:
            status_response = self.client.describe_statement(Id=statement_id)
            status = status_response["Status"]

            if status == "FINISHED":
                break
            elif status in ("FAILED", "ABORTED"):
                error = status_response.get("Error", "Unknown error")
                raise RuntimeError(f"Redshift query failed: {error}")

            logger.info(f"Query status: {status} — waiting {poll_interval}s...")
            time.sleep(poll_interval)
            elapsed += poll_interval

        if elapsed >= timeout_seconds:
            raise TimeoutError(
                f"Redshift query timed out after {timeout_seconds}s: {statement_id}"
            )

        # Fetch results
        results_response = self.client.get_statement_result(Id=statement_id)
        columns = [col["name"] for col in results_response["ColumnMetadata"]]

        rows = []
        for record in results_response.get("Records", []):
            row = {}
            for col_name, field in zip(columns, record):
                value = list(field.values())[0] if field else None
                row[col_name] = value
            rows.append(row)

        logger.info(f"Query returned {len(rows)} rows")
        return rows

    # ── Schema Management ─────────────────────────────────────────────────────

    def register_iceberg_schema(
        self,
        redshift_schema: str,
        glue_database: str,
        iam_role: str
    ):
        """
        Register a Glue/Iceberg database as an external schema in Redshift.

        Args:
            redshift_schema: Schema name to create in Redshift
            glue_database: Source Glue catalog database name
            iam_role: IAM role ARN for Redshift to access Glue
        """
        sql = f"""
            CREATE EXTERNAL SCHEMA IF NOT EXISTS {redshift_schema}
            FROM DATA CATALOG
            DATABASE '{glue_database}'
            IAM_ROLE '{iam_role}'
            CREATE EXTERNAL DATABASE IF NOT EXISTS;
        """

        logger.info(
            f"Registering Iceberg schema: {glue_database} → {redshift_schema}"
        )
        self.execute_query(sql)
        logger.info(f"Schema registered: {redshift_schema}")

    def schema_exists(self, schema_name: str) -> bool:
        """
        Check if a schema exists in Redshift.

        Args:
            schema_name: Schema name to check

        Returns:
            True if schema exists
        """
        sql = f"""
            SELECT COUNT(*)
            FROM information_schema.schemata
            WHERE schema_name = '{schema_name}';
        """
        results = self.execute_query(sql)
        count = int(results[0]["count"]) if results else 0
        return count > 0

    # ── Data Quality ──────────────────────────────────────────────────────────

    def get_row_count(self, schema: str, table: str) -> int:
        """
        Get the row count for a table.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Row count as integer
        """
        sql = f"SELECT COUNT(*) as cnt FROM {schema}.{table};"
        results = self.execute_query(sql)
        count = int(results[0]["cnt"]) if results else 0
        logger.info(f"{schema}.{table} row count: {count}")
        return count

    def check_duplicates(
        self,
        schema: str,
        table: str,
        key_column: str
    ) -> int:
        """
        Check for duplicate records by key column.

        Args:
            schema: Schema name
            table: Table name
            key_column: Column to check for duplicates

        Returns:
            Number of duplicate records (0 = clean)
        """
        sql = f"""
            SELECT COUNT(*) - COUNT(DISTINCT {key_column}) as duplicates
            FROM {schema}.{table};
        """
        results = self.execute_query(sql)
        duplicates = int(results[0]["duplicates"]) if results else 0

        if duplicates > 0:
            logger.warning(
                f"Found {duplicates} duplicates in {schema}.{table} "
                f"on column {key_column}"
            )
        else:
            logger.info(f"No duplicates found in {schema}.{table}")

        return duplicates
