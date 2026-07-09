"""Unit tests for S3 Table Bucket support in dbt-athena."""

from multiprocessing import get_context

import boto3
import pytest
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from dbt.adapters.athena import AthenaAdapter
from dbt.adapters.athena import Plugin as AthenaPlugin

from .constants import (
    ATHENA_WORKGROUP,
    AWS_REGION,
    DATABASE_NAME,
    S3_STAGING_DIR,
)
from .utils import config_from_parts_or_dicts, inject_adapter

S3TB_CATALOG_NAME = "my_s3tb_catalog"
S3TB_CATALOG_ID = f"{DEFAULT_ACCOUNT_ID}:s3tablescatalog/my-table-bucket"


@pytest.mark.usefixtures("aws_credentials")
class TestS3TableBucket:
    """Tests for S3 Table Bucket detection and behavior."""

    def setup_method(self, _):
        self.config = self._config_from_settings()
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = AthenaAdapter(self.config, get_context("spawn"))
            inject_adapter(self._adapter, AthenaPlugin)
        return self._adapter

    @staticmethod
    def _config_from_settings(settings=None):
        if settings is None:
            settings = {}
        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "athena",
                    "s3_staging_dir": S3_STAGING_DIR,
                    "region_name": AWS_REGION,
                    "database": S3TB_CATALOG_NAME,
                    "work_group": ATHENA_WORKGROUP,
                    "schema": DATABASE_NAME,
                    **settings,
                }
            },
            "target": "test",
        }
        return config_from_parts_or_dicts(project_cfg, profile_cfg)

    # is_s3_table_bucket detection

    @mock_aws
    def test_is_s3_table_bucket_true(self):
        """S3TB catalog with 's3tablescatalog/' in catalog-id returns True."""
        self._create_s3tb_catalog()
        self.adapter.acquire_connection("dummy")
        assert self.adapter.is_s3_table_bucket(S3TB_CATALOG_NAME) is True

    @mock_aws
    def test_is_s3_table_bucket_regular_glue(self):
        """Regular GLUE catalog (AwsDataCatalog) returns False."""
        self._create_regular_catalog()
        self.adapter.acquire_connection("dummy")
        assert self.adapter.is_s3_table_bucket("awsdatacatalog") is False

    @mock_aws
    def test_is_s3_table_bucket_lambda_catalog(self):
        """LAMBDA catalog returns False."""
        conn = boto3.client("athena", region_name=AWS_REGION)
        conn.create_data_catalog(
            Name="lambda_catalog",
            Type="LAMBDA",
            Parameters={"function": "arn:aws:lambda:us-east-1:123456789012:function:my-func"},
        )
        self.adapter.acquire_connection("dummy")
        assert self.adapter.is_s3_table_bucket("lambda_catalog") is False

    @mock_aws
    def test_is_s3_table_bucket_hive_catalog(self):
        """HIVE catalog returns False."""
        conn = boto3.client("athena", region_name=AWS_REGION)
        conn.create_data_catalog(
            Name="hive_catalog",
            Type="HIVE",
            Parameters={
                "metadata-function": "arn:aws:lambda:us-east-1:123456789012:function:my-func"
            },
        )
        self.adapter.acquire_connection("dummy")
        assert self.adapter.is_s3_table_bucket("hive_catalog") is False

    def test_is_s3_table_bucket_none_database(self):
        """None database returns False without API calls."""
        self.adapter.acquire_connection("dummy")
        assert self.adapter.is_s3_table_bucket(None) is False

    def test_is_s3_table_bucket_empty_database(self):
        """Empty string database returns False without API calls."""
        self.adapter.acquire_connection("dummy")
        assert self.adapter.is_s3_table_bucket("") is False

    @mock_aws
    def test_is_s3_table_bucket_glue_without_s3tablescatalog(self):
        """GLUE catalog with a regular (non-S3TB) catalog-id returns False."""
        conn = boto3.client("athena", region_name=AWS_REGION)
        conn.create_data_catalog(
            Name="shared_glue",
            Type="GLUE",
            Parameters={"catalog-id": "987654321098"},
        )
        self.adapter.acquire_connection("dummy")
        assert self.adapter.is_s3_table_bucket("shared_glue") is False

    # list_schemas with CatalogId

    @mock_aws
    def test_list_schemas_passes_catalog_id(self):
        """list_schemas passes CatalogId for non-default catalogs."""
        # Create a GLUE catalog with a shared account catalog-id
        conn = boto3.client("athena", region_name=AWS_REGION)
        conn.create_data_catalog(
            Name="shared_catalog",
            Type="GLUE",
            Parameters={"catalog-id": DEFAULT_ACCOUNT_ID},
        )
        # Create databases in the default catalog (moto limitation: can't create cross-account)
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": "ns_alpha"})
        glue.create_database(DatabaseInput={"Name": "ns_beta"})

        self.adapter.acquire_connection("dummy")
        result = self.adapter.list_schemas("shared_catalog")
        assert sorted(result) == ["ns_alpha", "ns_beta"]

    # _get_data_catalog caching

    @mock_aws
    def test_get_data_catalog_is_cached(self):
        """Repeated calls to _get_data_catalog return cached result."""
        self._create_s3tb_catalog()
        self.adapter.acquire_connection("dummy")
        result1 = self.adapter._get_data_catalog(S3TB_CATALOG_NAME)
        result2 = self.adapter._get_data_catalog(S3TB_CATALOG_NAME)
        assert result1 is result2  # same object, not just equal

    # multi-catalog detection

    @mock_aws
    def test_multi_catalog_detection(self):
        """Different databases return different S3TB detection results."""
        self._create_s3tb_catalog()
        self._create_regular_catalog()
        self.adapter.acquire_connection("dummy")
        assert self.adapter.is_s3_table_bucket(S3TB_CATALOG_NAME) is True
        assert self.adapter.is_s3_table_bucket("awsdatacatalog") is False

    # helpers

    def _create_s3tb_catalog(self):
        conn = boto3.client("athena", region_name=AWS_REGION)
        conn.create_data_catalog(
            Name=S3TB_CATALOG_NAME,
            Type="GLUE",
            Parameters={"catalog-id": S3TB_CATALOG_ID},
        )

    def _create_regular_catalog(self):
        conn = boto3.client("athena", region_name=AWS_REGION)
        conn.create_data_catalog(
            Name="awsdatacatalog",
            Type="GLUE",
            Parameters={"catalog-id": DEFAULT_ACCOUNT_ID},
        )
