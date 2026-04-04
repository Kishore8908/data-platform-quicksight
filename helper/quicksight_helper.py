"""
helpers/quicksight_helper.py
============================
QuickSight API wrapper functions.

Wraps the boto3 QuickSight client to provide:
  - Clean, readable function signatures
  - Consistent error handling
  - Idempotent operations (safe to call multiple times)
  - Logging at every step

Usage:
    from helpers.quicksight_helper import QuickSightHelper

    qs = QuickSightHelper(account_id="123456789012")
    folder_arn = qs.get_or_create_folder("Marketing Analytics")
    dataset_arn = qs.create_dataset(schema="marketing", table="spend")
    qs.add_to_folder(folder_arn, dataset_arn)
    qs.apply_permissions(dataset_arn, permissions)

Author: Biswajit Praharaj
GitHub: github.com/Biswajit107927
"""

import os
from typing import Optional
from helpers.aws_clients import get_client
from helpers.logger import get_logger

logger = get_logger(__name__)

# ── Permission Action Sets ────────────────────────────────────────────────────

READ_ACTIONS = [
    "quicksight:DescribeDataSet",
    "quicksight:QueryDataSet",
    "quicksight:ListTagsForResource",
    "quicksight:DescribeDataSetPermissions",
]

WRITE_ACTIONS = READ_ACTIONS + [
    "quicksight:UpdateDataSet",
    "quicksight:DeleteDataSet",
    "quicksight:UpdateDataSetPermissions",
    "quicksight:GrantIngestion",
]

FOLDER_READ_ACTIONS = [
    "quicksight:DescribeFolder",
    "quicksight:ListFolderMembers",
]

FOLDER_WRITE_ACTIONS = FOLDER_READ_ACTIONS + [
    "quicksight:UpdateFolder",
    "quicksight:DeleteFolder",
    "quicksight:CreateFolderMembership",
    "quicksight:DeleteFolderMembership",
    "quicksight:UpdateFolderPermissions",
]


class QuickSightHelper:
    """
    Helper class wrapping QuickSight boto3 API calls.
    All operations are idempotent — safe to run multiple times.
    """

    def __init__(
        self,
        account_id: Optional[str] = None,
        namespace: str = "default",
        region: Optional[str] = None
    ):
        self.account_id = account_id or os.environ["AWS_ACCOUNT_ID"]
        self.namespace = namespace
        self.client = get_client("quicksight", region)
        logger.info(
            f"QuickSightHelper initialised — "
            f"account: {self.account_id}, namespace: {self.namespace}"
        )

    # ── Folders ───────────────────────────────────────────────────────────────

    def get_or_create_folder(self, folder_name: str) -> str:
        """
        Create a QuickSight folder if it doesn't exist.
        Returns the folder ARN.

        Args:
            folder_name: Display name for the folder

        Returns:
            Folder ARN string
        """
        folder_id = self._to_id(folder_name)

        try:
            response = self.client.describe_folder(
                AwsAccountId=self.account_id,
                FolderId=folder_id
            )
            arn = response["Folder"]["Arn"]
            logger.info(f"Folder exists: {arn}")
            return arn

        except self.client.exceptions.ResourceNotFoundException:
            response = self.client.create_folder(
                AwsAccountId=self.account_id,
                FolderId=folder_id,
                Name=folder_name,
                FolderType="SHARED",
            )
            arn = response["Arn"]
            logger.info(f"Folder created: {arn}")
            return arn

    def add_to_folder(self, folder_arn: str, member_arn: str, member_type: str = "DATASET"):
        """
        Add a member (dataset, analysis) to a QuickSight folder.

        Args:
            folder_arn: Target folder ARN
            member_arn: Member ARN to add
            member_type: DATASET or ANALYSIS
        """
        folder_id = folder_arn.split("/")[-1]
        member_id = member_arn.split("/")[-1]

        try:
            self.client.create_folder_membership(
                AwsAccountId=self.account_id,
                FolderId=folder_id,
                MemberId=member_id,
                MemberType=member_type
            )
            logger.info(f"Added {member_type} {member_id} to folder {folder_id}")

        except self.client.exceptions.ResourceExistsException:
            logger.info(f"{member_type} already in folder — skipping")

    # ── Datasets ──────────────────────────────────────────────────────────────

    def create_dataset(
        self,
        schema: str,
        table: str,
        datasource_arn: str,
        dataset_name: Optional[str] = None,
        import_mode: str = "DIRECT_QUERY"
    ) -> str:
        """
        Create a QuickSight dataset from a Redshift table.
        Uses DIRECT_QUERY mode by default — no SPICE import.

        Args:
            schema: Redshift schema name
            table: Redshift table name
            datasource_arn: ARN of the QuickSight Redshift datasource
            dataset_name: Display name (default: schema.table)
            import_mode: DIRECT_QUERY or SPICE

        Returns:
            Dataset ARN string
        """
        dataset_id = self._to_id(f"{schema}-{table}")
        dataset_name = dataset_name or f"{schema}.{table}"

        try:
            existing = self.client.describe_data_set(
                AwsAccountId=self.account_id,
                DataSetId=dataset_id
            )
            arn = existing["DataSet"]["Arn"]
            logger.info(f"Dataset exists: {arn}")
            return arn

        except self.client.exceptions.ResourceNotFoundException:
            pass

        response = self.client.create_data_set(
            AwsAccountId=self.account_id,
            DataSetId=dataset_id,
            Name=dataset_name,
            ImportMode=import_mode,
            PhysicalTableMap={
                f"{schema}_{table}": {
                    "RelationalTable": {
                        "DataSourceArn": datasource_arn,
                        "Schema": schema,
                        "Name": table,
                        "InputColumns": []
                    }
                }
            }
        )

        arn = response["Arn"]
        logger.info(f"Dataset created: {arn}")
        return arn

    def delete_dataset(self, dataset_id: str):
        """
        Delete a QuickSight dataset by ID.

        Args:
            dataset_id: Dataset ID to delete
        """
        try:
            self.client.delete_data_set(
                AwsAccountId=self.account_id,
                DataSetId=dataset_id
            )
            logger.info(f"Dataset deleted: {dataset_id}")

        except self.client.exceptions.ResourceNotFoundException:
            logger.warning(f"Dataset not found — skipping delete: {dataset_id}")

    # ── Permissions ───────────────────────────────────────────────────────────

    def apply_permissions(self, dataset_arn: str, permissions: list[dict]):
        """
        Apply permissions to a QuickSight dataset.

        Args:
            dataset_arn: Dataset ARN
            permissions: List of permission dicts:
                [{
                    "principal": "arn:aws:quicksight:...",
                    "actions": ["quicksight:QueryDataSet", ...]
                }]
        """
        if not permissions:
            logger.warning("No permissions provided — dataset remains private")
            return

        dataset_id = dataset_arn.split("/")[-1]

        grant_permissions = [
            {
                "Principal": perm["principal"],
                "Actions": perm.get("actions", READ_ACTIONS)
            }
            for perm in permissions
        ]

        self.client.update_data_set_permissions(
            AwsAccountId=self.account_id,
            DataSetId=dataset_id,
            GrantPermissions=grant_permissions
        )

        logger.info(
            f"Permissions applied: {len(permissions)} principals "
            f"on dataset {dataset_id}"
        )

    def revoke_permissions(self, dataset_arn: str, principals: list[str]):
        """
        Revoke permissions from a QuickSight dataset.

        Args:
            dataset_arn: Dataset ARN
            principals: List of principal ARNs to revoke
        """
        dataset_id = dataset_arn.split("/")[-1]

        self.client.update_data_set_permissions(
            AwsAccountId=self.account_id,
            DataSetId=dataset_id,
            RevokePermissions=[
                {"Principal": p, "Actions": WRITE_ACTIONS}
                for p in principals
            ]
        )

        logger.info(f"Permissions revoked for {len(principals)} principals")

    # ── Datasources ───────────────────────────────────────────────────────────

    def get_datasource_arn(self, datasource_id: str) -> str:
        """
        Get the ARN of an existing QuickSight datasource.

        Args:
            datasource_id: Datasource ID

        Returns:
            Datasource ARN string
        """
        response = self.client.describe_data_source(
            AwsAccountId=self.account_id,
            DataSourceId=datasource_id
        )
        return response["DataSource"]["Arn"]

    # ── Utilities ─────────────────────────────────────────────────────────────

    def list_datasets(self) -> list[dict]:
        """
        List all QuickSight datasets in the account.

        Returns:
            List of dataset summary dicts
        """
        response = self.client.list_data_sets(AwsAccountId=self.account_id)
        datasets = response.get("DataSetSummaries", [])
        logger.info(f"Found {len(datasets)} datasets")
        return datasets

    def list_folders(self) -> list[dict]:
        """
        List all QuickSight folders in the account.

        Returns:
            List of folder summary dicts
        """
        response = self.client.list_folders(AwsAccountId=self.account_id)
        folders = response.get("FolderSummaryList", [])
        logger.info(f"Found {len(folders)} folders")
        return folders

    @staticmethod
    def _to_id(name: str) -> str:
        """Convert a display name to a valid QS resource ID."""
        return name.lower().replace(" ", "-").replace("_", "-").replace(".", "-")
