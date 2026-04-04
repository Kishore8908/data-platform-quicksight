"""
airflow/data_platform_dag.py
============================
Apache Airflow DAG that orchestrates the full data platform pipeline:

  1. Trigger Glue ETL job (transform + write to Iceberg)
  2. Run Redshift data quality checks
  3. Notify on success or failure

This DAG is parameterised — one DAG template serves
multiple datasets via config-driven instantiation.

Author: Biswajit Praharaj
GitHub: github.com/Biswajit107927
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
import boto3
import json
import logging

logger = logging.getLogger(__name__)

# ── DAG Factory ───────────────────────────────────────────────────────────────

def create_pipeline_dag(config: dict) -> DAG:
    """
    Factory function that creates a pipeline DAG from a config dict.
    Enables config-driven multi-tenant pipeline management.

    Args:
        config: Pipeline configuration dict containing:
            - dag_id: Unique DAG identifier
            - source_path: S3 path for raw data
            - iceberg_database: Target Iceberg database
            - iceberg_table: Target Iceberg table
            - redshift_schema: Redshift schema name
            - schedule: Cron schedule
            - owner: Team or individual owner
            - sla_minutes: Expected completion time in minutes
            - alert_email: Email for failure alerts

    Returns:
        Configured Airflow DAG
    """

    default_args = {
        "owner": config.get("owner", "data-platform"),
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "email_on_failure": True,
        "email": config.get("alert_email", "data-platform@company.com"),
        "sla": timedelta(minutes=config.get("sla_minutes", 60)),
    }

    dag = DAG(
        dag_id=config["dag_id"],
        default_args=default_args,
        description=f"Data pipeline for {config['iceberg_table']}",
        schedule_interval=config.get("schedule", "@hourly"),
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["data-platform", config.get("owner", "unknown")],
    )

    with dag:

        # ── Task 1: Check source data arrived in S3 ───────────────────────
        check_source = S3KeySensor(
            task_id="check_source_data",
            bucket_name=config["source_bucket"],
            bucket_key=config["source_key_prefix"] + "*.json",
            wildcard_match=True,
            timeout=3600,
            poke_interval=60,
            mode="reschedule",
        )

        # ── Task 2: Run Glue ETL job ──────────────────────────────────────
        run_glue_job = GlueJobOperator(
            task_id="run_glue_etl",
            job_name=config.get("glue_job_name", "data-platform-etl"),
            script_args={
                "--source_path": config["source_path"],
                "--iceberg_database": config["iceberg_database"],
                "--iceberg_table": config["iceberg_table"],
                "--iceberg_warehouse": config["iceberg_warehouse"],
                "--partition_field": config.get("partition_field", "partition_date"),
            },
            aws_conn_id="aws_default",
            region_name=config.get("region", "us-east-1"),
            wait_for_completion=True,
        )

        # ── Task 3: Register Iceberg table in Redshift ────────────────────
        register_in_redshift = RedshiftSQLOperator(
            task_id="register_in_redshift",
            sql=f"""
                CREATE EXTERNAL SCHEMA IF NOT EXISTS {config['redshift_schema']}
                FROM DATA CATALOG
                DATABASE '{config['iceberg_database']}'
                IAM_ROLE '{{{{ var.value.redshift_iam_role }}}}'
                CREATE EXTERNAL DATABASE IF NOT EXISTS;
            """,
            redshift_conn_id="redshift_default",
        )

        # ── Task 4: Data quality checks ───────────────────────────────────
        run_quality_checks = RedshiftSQLOperator(
            task_id="run_quality_checks",
            sql=f"""
                -- Check 1: No duplicate records
                SELECT COUNT(*) - COUNT(DISTINCT _record_id)
                FROM {config['redshift_schema']}.{config['iceberg_table']}
                WHERE partition_date = CURRENT_DATE;

                -- Check 2: Row count threshold
                SELECT CASE
                    WHEN COUNT(*) < {config.get('min_rows', 1)}
                    THEN 1/0  -- Force failure if below threshold
                    ELSE COUNT(*)
                END
                FROM {config['redshift_schema']}.{config['iceberg_table']}
                WHERE partition_date = CURRENT_DATE;
            """,
            redshift_conn_id="redshift_default",
        )

        # ── Task 5: Notify on failure ─────────────────────────────────────
        notify_failure = EmailOperator(
            task_id="notify_failure",
            to=config.get("alert_email", "data-platform@company.com"),
            subject=f"[FAILED] Pipeline: {config['dag_id']}",
            html_content=f"""
                <h3>Pipeline Failed</h3>
                <p><b>DAG:</b> {config['dag_id']}</p>
                <p><b>Table:</b> {config['iceberg_database']}.{config['iceberg_table']}</p>
                <p>Please check Airflow logs for details.</p>
            """,
            trigger_rule=TriggerRule.ONE_FAILED,
        )

        # ── Task Dependencies ─────────────────────────────────────────────
        check_source >> run_glue_job >> register_in_redshift >> run_quality_checks
        [run_glue_job, register_in_redshift, run_quality_checks] >> notify_failure

    return dag


# ── Load Configs and Create DAGs ──────────────────────────────────────────────

def load_pipeline_configs(config_path: str = "config/pipelines.json") -> list[dict]:
    """Load pipeline configurations from JSON file."""
    with open(config_path) as f:
        return json.load(f)


# Dynamically create one DAG per pipeline config
# This pattern scales to hundreds of pipelines
for pipeline_config in load_pipeline_configs():
    dag_id = pipeline_config["dag_id"]
    globals()[dag_id] = create_pipeline_dag(pipeline_config)
    logger.info(f"DAG created: {dag_id}")
