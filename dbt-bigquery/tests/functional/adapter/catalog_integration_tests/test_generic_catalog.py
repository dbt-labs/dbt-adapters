import os

import pytest

from dbt.tests.util import run_dbt


MODEL__INFO_SCHEMA_DEFAULT = """
select 1 as id
"""
MODEL__INFO_SCHEMA_TABLE = """
{{ config(materialized='table') }}
select 1 as id
"""
MODEL__MANAGED_ICEBERG_TABLE = (
    """
{{ config(
    materialized='table',
    catalog='managed_iceberg',
    storage_uri='gs://"""
    + os.getenv("BIGQUERY_TEST_ICEBERG_BUCKET")
    + """/managed_iceberg_table'
) }}
select 1 as id
"""
)


class TestGenericCatalog:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "info_schema_default.sql": MODEL__INFO_SCHEMA_DEFAULT,
            "info_schema_table.sql": MODEL__INFO_SCHEMA_TABLE,
            "managed_iceberg_table.sql": MODEL__MANAGED_ICEBERG_TABLE,
        }

    def test_generic_catalog(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
