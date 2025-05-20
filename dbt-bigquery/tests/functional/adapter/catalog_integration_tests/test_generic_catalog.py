import os

import pytest


MODEL__INFO_SCHEMA_DEFAULT = """
select 1 as id
"""
MODEL__INFO_SCHEMA_TABLE = """
{{ config(materialized='table') }}
select 1 as id
"""
MODEL__ICEBERG_TABLE = f"""
{{ config(
    materialized='table',
    catalog='managed_iceberg',
    storage_uri='gs://{os.getenv("BIGQUERY_TEST_ICEBERG_BUCKET")}/table'
) }}
select 1 as id
"""


class TestGenericCatalog:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "info_schema_default.sql": MODEL__INFO_SCHEMA_DEFAULT,
            "info_schema_table.sql": MODEL__INFO_SCHEMA_TABLE,
            "iceberg_table.sql": MODEL__ICEBERG_TABLE,
        }

    def test_generic_catalog(self, project):
        results = project.run_dbt(["run"])
        assert len(results) == 3
