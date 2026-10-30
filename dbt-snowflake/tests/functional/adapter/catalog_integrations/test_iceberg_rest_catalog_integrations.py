import os
import pytest
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt

MODEL__BASIC_ICEBERG_TABLE = """
                            {{ config(materialized='table',
                             catalog_name='basic_iceberg_rest_catalog') }}
                            select 1 as id
                            """

MODEL__ICEBERG_TABLE_WITH_CATALOG_CONFIG = """
                            {{ config(materialized='table', catalog_name='basic_iceberg_rest_catalog',
                            target_file_size='16MB', max_data_extension_time_in_days=1, auto_refresh='true') }}
                            select 1 as id
                            """

MODEL__INCREMENTAL_ICEBERG_REST = """
{{
  config(
    materialized='incremental',
    catalog_name='basic_iceberg_rest_catalog',
    incremental_strategy='merge',
    unique_key="id",
  )
}}
select * from {{ ref('basic_iceberg_table') }}

{% if is_incremental() %}
where id > 2
{% endif %}
"""

MODEL__INCREMENTAL_ICEBERG_REST_INSERT_OVERWRITE = """
{{
  config(
    materialized='incremental',
    catalog_name='basic_iceberg_rest_catalog',
    incremental_strategy='insert_overwrite',
    unique_key="id",
  )
}}
select * from {{ ref('basic_iceberg_table') }}

{% if is_incremental() %}
where id > 2
{% endif %}
"""


MODEL__GLUE_DECIMAL_TABLE = """
{{ config(materialized='table', catalog_name='glue_iceberg_rest_catalog') }}
select
    1 as id,
    cast(150.50 as decimal(38,2)) as price,
    cast(10 as decimal(38,0)) as amount
"""

MODEL__GLUE_DECIMAL_INCREMENTAL = """
{{
  config(
    materialized='incremental',
    catalog_name='glue_iceberg_rest_catalog',
    incremental_strategy='append',
  )
}}
select * from {{ ref('glue_decimal_table') }}
"""

MODEL__GLUE_INT_UPGRADE_INCREMENTAL = """
{{
  config(
    materialized='incremental',
    catalog_name='glue_iceberg_rest_catalog',
    incremental_strategy='append',
    on_schema_change='fail',
  )
}}
select * from {{ ref('glue_decimal_table') }}
"""


class TestSnowflakeIcebergRestCatalogIntegration(BaseCatalogIntegrationValidation):

    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "basic_iceberg_rest_catalog",
                    "active_write_integration": "iceberg_rest_catalog_with_linked_db_integration",
                    "write_integrations": [
                        {
                            "name": "iceberg_rest_catalog_with_linked_db_integration",
                            "catalog_type": "iceberg_rest",
                            "table_format": "iceberg",
                            "adapter_properties": {
                                "catalog_linked_database": os.getenv(
                                    "SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE"
                                ),
                                # No catalog_linked_database_type means standard CTAS is used
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

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models": {
                "basic_iceberg_table.sql": MODEL__BASIC_ICEBERG_TABLE,
                "iceberg_table_with_catalog_config.sql": MODEL__ICEBERG_TABLE_WITH_CATALOG_CONFIG,
                "incremental_iceberg_rest.sql": MODEL__INCREMENTAL_ICEBERG_REST,
                "incremental_iceberg_rest_insert_overwrite.sql": MODEL__INCREMENTAL_ICEBERG_REST_INSERT_OVERWRITE,
            }
        }

    def test_basic_iceberg_rest_catalog_integration(self, project):
        result = run_dbt(["run"])
        assert len(result) == 4
        run_dbt(["run"])


class TestSnowflakeIcebergRestGlueCatalogIntegration(BaseCatalogIntegrationValidation):

    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
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

    # @pytest.fixture(scope="class", autouse=True)
    # def setup_glue_schema(self, project):
    #     """Pre-create schema with quoted lowercase identifier for Glue CLD"""
    #     adapter = project.adapter
    #     glue_database = os.getenv("SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE_GLUE")
    #     schema_name = project.test_schema.lower()

    #     # Create schema with quoted identifier to preserve lowercase
    #     create_schema_sql = f'CREATE SCHEMA IF NOT EXISTS {glue_database}."{schema_name}"'
    #     adapter.execute(create_schema_sql, fetch=False)

    #     yield

    #     # Cleanup: drop schema after test
    #     drop_schema_sql = f'DROP SCHEMA IF EXISTS {glue_database}."{schema_name}"'
    #     try:
    #         adapter.execute(drop_schema_sql, fetch=False)
    #     except:
    #         pass  # Ignore cleanup errors

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
        # Use different catalog name for Glue models
        return {
            "models": {
                "glue_basic_iceberg_table.sql": """
                    {{ config(materialized='table', schema=target.schema.upper(),
                     catalog_name='glue_iceberg_rest_catalog') }}
                    select 1 as id, 'test' as name, 1.0 as price, '2021-01-01' as test_date
                """,
                "glue_iceberg_table_with_catalog_config.sql": """
                    {{ config(materialized='table', catalog_name='glue_iceberg_rest_catalog',
                    target_file_size='16MB', max_data_extension_time_in_days=1, auto_refresh='true') }}
                    select 1 as id
                """,
                "glue_decimal_table.sql": MODEL__GLUE_DECIMAL_TABLE,
                "glue_decimal_incremental.sql": MODEL__GLUE_DECIMAL_INCREMENTAL,
                "glue_int_upgrade_incremental.sql": MODEL__GLUE_INT_UPGRADE_INCREMENTAL,
            }
        }

    def test_glue_iceberg_rest_catalog_integration(self, project, models):
        """Test Glue CLD with 4-step table creation process"""
        result = run_dbt(["run"])
        assert len(result) == len(models["models"])
        # Run again to test update path
        run_dbt(["run"])

    def test_glue_iceberg_decimal_keeps_precision_and_scale(self, project):
        # The second run appends through a __dbt_tmp table, which is built the same way
        for _ in range(2):
            run_dbt(["run", "--select", "glue_decimal_table", "glue_decimal_incremental"])

        glue_database = os.getenv("SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE_GLUE")
        for identifier in ("glue_decimal_table", "glue_decimal_incremental"):
            relation = f'{glue_database}."{project.test_schema}"."{identifier}"'
            rows = project.run_sql(f'select "price", "amount" from {relation}', fetch="all")
            assert rows
            for price, amount in rows:
                assert str(price) == "150.50"
                assert str(amount) == "10"

    def test_glue_iceberg_int_table_needs_full_refresh(self, project):
        # Tables built before this fix have INT where the model now has DECIMAL(p,s), so
        # on_schema_change='fail' stops the next run until the table is rebuilt
        run_dbt(["run", "--select", "glue_decimal_table"])
        glue_database = os.getenv("SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE_GLUE")
        relation = f'{glue_database}."{project.test_schema}"."glue_int_upgrade_incremental"'
        project.run_sql(f"drop table if exists {relation}")
        project.run_sql(f'create iceberg table {relation} ("id" INT, "price" INT, "amount" INT)')

        results = run_dbt(["run", "--select", "glue_int_upgrade_incremental"], expect_pass=False)
        assert "out of sync" in results[0].message

        run_dbt(["run", "--select", "glue_int_upgrade_incremental", "--full-refresh"])
        rows = project.run_sql(f'select "price", "amount" from {relation}', fetch="all")
        assert [(str(price), str(amount)) for price, amount in rows] == [("150.50", "10")]
