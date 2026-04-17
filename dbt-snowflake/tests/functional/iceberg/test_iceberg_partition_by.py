import os
import re
import pytest

from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file


def get_cleaned_model_ddl_from_file(file_name: str) -> str:
    with open(f"target/run/test/models/{file_name}", "r") as ddl_file:
        return re.sub(r"\s+", " ", ddl_file.read())


_SEED_PARTITION_BY = """
order_id,order_date,region,amount
1001,2025-10-01,North,120.50
1002,2025-10-01,North,85.00
1003,2025-10-01,West,42.10
1004,2025-10-02,West,110.00
1005,2025-10-02,East,67.30
1006,2025-10-02,East,134.40
1007,2025-10-03,South,210.00
1008,2025-10-03,South,95.00
1009,2025-10-03,North,55.00
1010,2025-10-04,North,33.30
1011,2025-10-04,West,145.20
1012,2025-10-04,East,60.00
1013,2025-10-05,West,88.90
1014,2025-10-05,South,75.50
1015,2025-10-05,East,99.00
1016,2025-10-06,North,122.00
1017,2025-10-06,South,140.00
1018,2025-10-06,West,57.50
1019,2025-10-07,North,200.00
1020,2025-10-07,East,180.00
""".strip()

_MODEL_BUILTIN_TABLE = """
{{
  config(
    materialized='table',
    partition_by=['order_date']
) }}

select
  order_date,
  region,
  count(*) as num_orders,
  sum(amount) as total_sales,
  avg(amount) as avg_order_value
from {{ ref('seed') }}
group by 1, 2
order by 1, 2
"""

_MODEL_ICEBERG_BUILTIN_TABLE = """
{{
  config(
    materialized='table',
    catalog_name='iceberg_snowflake_managed_catalog',
    partition_by=['order_date']
) }}

select
  order_date,
  region,
  count(*) as num_orders,
  sum(amount) as total_sales,
  avg(amount) as avg_order_value
from {{ ref('seed') }}
group by 1, 2
order by 1, 2
"""

_MODEL_ICEBERG_REST_TABLE = """
{{
  config(
    materialized='table',
    catalog_name='iceberg_rest_catalog',
    partition_by=['order_date']
) }}

select
  order_date,
  region,
  count(*) as num_orders,
  sum(amount) as total_sales,
  avg(amount) as avg_order_value
from {{ ref('seed') }}
group by 1, 2
order by 1, 2
"""

_MODEL_GLUE_ICEBERG_REST_TABLE = """
{{
  config(
    materialized='table',
    catalog_name='glue_iceberg_rest_catalog',
    partition_by=['order_date']
) }}

select
  order_date,
  region,
  count(*) as num_orders,
  sum(amount) as total_sales,
  avg(amount) as avg_order_value
from {{ ref('seed') }}
group by 1, 2
order by 1, 2
"""


class TestIcebergPartitionBy(BaseCatalogIntegrationValidation):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": _SEED_PARTITION_BY,
        }

    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "iceberg_snowflake_managed_catalog",
                    "active_write_integration": "iceberg_managed_test",
                    "write_integrations": [
                        {
                            "name": "iceberg_managed_test",
                            "external_volume": "s3_iceberg_snow",
                            "catalog_type": "built_in",
                            "table_format": "iceberg",
                            "adapter_properties": {
                                # No catalog_linked_database_type means standard CTAS is used
                                "change_tracking": "true",
                            },
                        }
                    ],
                },
                {
                    "name": "iceberg_rest_catalog",
                    "active_write_integration": "iceberg_rest_test",
                    "write_integrations": [
                        {
                            "name": "iceberg_rest_test",
                            "catalog_type": "iceberg_rest",
                            "table_format": "iceberg",
                            "adapter_properties": {
                                "catalog_linked_database": os.getenv(
                                    "SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE"
                                ),
                                "max_data_extension_time_in_days": 1,
                                "target_file_size": "AUTO",
                                "auto_refresh": "true",
                            },
                        }
                    ],
                },
                {
                    "name": "glue_iceberg_rest_catalog",
                    "active_write_integration": "glue_iceberg_rest_catalog_integration",
                    "write_integrations": [
                        {
                            "name": "glue_iceberg_rest_catalog_integration",
                            "catalog_type": "iceberg_rest",
                            "table_format": "iceberg",
                            "adapter_properties": {
                                "catalog_linked_database": os.getenv(
                                    "SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE_GLUE"
                                ),
                                "catalog_linked_database_type": "glue",  # Glue requires 4-step process
                                "max_data_extension_time_in_days": 1,
                                "target_file_size": "AUTO",
                                "auto_refresh": "true",
                            },
                        }
                    ],
                },
            ]
        }

    # can only use alphanumeric characters in schema names in catalog linked databases
    # until it's GA
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"{prefix}_{test_file}"
        return unique_schema.replace("_", "")


class TestPartitionByIgnoredIfNotIceberg(TestIcebergPartitionBy):
    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "standard_table.sql": _MODEL_BUILTIN_TABLE,
        }

    def test_partition_by_ignored_if_not_iceberg(self, project, setup_class):
        run_dbt(["run"])
        standard_table_sql = get_cleaned_model_ddl_from_file("standard_table.sql")
        assert "partition by" not in standard_table_sql


class TestPartitionByIcebergBuiltinCatalog(TestIcebergPartitionBy):
    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "builtin_table.sql": _MODEL_ICEBERG_BUILTIN_TABLE,
        }

    def test_partition_by_iceberg_builtin_catalog(self, project, setup_class):
        run_dbt(["run"])
        iceberg_sql = get_cleaned_model_ddl_from_file("builtin_table.sql")
        assert "partition by (order_date)" in iceberg_sql


class TestPartitionByIcebergRestCatalog(TestIcebergPartitionBy):
    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "rest_table.sql": _MODEL_ICEBERG_REST_TABLE,
        }

    def test_partition_by_iceberg_rest_catalog(self, project, setup_class):
        run_dbt(["run"])
        iceberg_sql = get_cleaned_model_ddl_from_file("rest_table.sql")
        assert "partition by (order_date)" in iceberg_sql


class TestPartitionByIcebergRestGlueCatalog(TestIcebergPartitionBy):
    # For some reason, when using Glue, `dbt seed` doesn't create the seed schema.
    @pytest.fixture(scope="class", autouse=True)
    def setup_glue_seed(self, project):
        """Pre-create schema with quoted lowercase identifier for Glue CLD"""
        adapter = project.adapter
        seed_database = os.getenv("SNOWFLAKE_TEST_DATABASE")
        schema_name = project.test_schema.lower()

        # Create schema with quoted identifier to preserve lowercase
        create_schema_sql = f'CREATE SCHEMA IF NOT EXISTS {seed_database}."{schema_name}"'
        adapter.execute(create_schema_sql, fetch=False)

        yield

        # Cleanup: drop schema after test
        drop_schema_sql = f'DROP SCHEMA IF EXISTS {seed_database}."{schema_name}"'
        try:
            adapter.execute(drop_schema_sql, fetch=False)
        except:
            pass  # Ignore cleanup errors

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed", "--log-level", "debug"])
        yield

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Force quoting for Glue CLD compatibility
        return {
            "quoting": {
                "database": False,
                "schema": True,
                "identifier": True,
            }
        }

    # AWS Glue requires lowercase identifiers and alphanumeric characters only
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"{prefix}_{test_file}_glue"
        # Remove underscores and convert to lowercase for Glue compatibility
        return unique_schema.replace("_", "").lower()

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "glue_table.sql": _MODEL_GLUE_ICEBERG_REST_TABLE,
        }

    def test_partition_by_glue_iceberg_rest_catalog(self, project, setup_class):
        run_dbt(["run", "--log-level", "debug"])
        iceberg_sql = get_cleaned_model_ddl_from_file("glue_table.sql")
        assert 'partition by ("order_date")' in iceberg_sql
