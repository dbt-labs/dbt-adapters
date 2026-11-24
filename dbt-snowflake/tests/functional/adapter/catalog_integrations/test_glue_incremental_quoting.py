import os

import pytest
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt

MODEL__GLUE_SOURCE = """
{{ config(materialized='table', catalog_name='glue_iceberg_rest_catalog') }}
select 1 as id, 'a' as name
"""

MODEL__GLUE_MERGE = """
{{
  config(
    materialized='incremental',
    catalog_name='glue_iceberg_rest_catalog',
    incremental_strategy='merge',
    unique_key='id',
  )
}}
select * from {{ ref('glue_source') }}
"""

MODEL__GLUE_DELETE_INSERT = """
{{
  config(
    materialized='incremental',
    catalog_name='glue_iceberg_rest_catalog',
    incremental_strategy='delete+insert',
    unique_key=['id'],
  )
}}
select * from {{ ref('glue_source') }}
"""

MODEL__GLUE_MERGE_UPDATE_COLUMNS = """
{{
  config(
    materialized='incremental',
    catalog_name='glue_iceberg_rest_catalog',
    incremental_strategy='merge',
    unique_key='id',
    merge_update_columns=['name'],
  )
}}
select * from {{ ref('glue_source') }}
"""

MODEL__GLUE_EVENTS = """
{{ config(materialized='table', catalog_name='glue_iceberg_rest_catalog') }}
select 1 as id, '2026-10-28 10:00:00'::timestamp_ntz as event_ts
union all
select 2 as id, '2026-10-29 10:00:00'::timestamp_ntz as event_ts
"""

# Yearly batches keep a run without --event-time-start bounded as CI dates move on
MODEL__GLUE_MICROBATCH = """
{{
  config(
    materialized='incremental',
    incremental_strategy='microbatch',
    catalog_name='glue_iceberg_rest_catalog',
    event_time='event_ts',
    begin='2026-01-01',
    batch_size='year',
    concurrent_batches=false,
  )
}}
select * from {{ ref('glue_events') }}
"""


class TestSnowflakeGlueIncrementalQuoting(BaseCatalogIntegrationValidation):
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
                                "catalog_linked_database_type": "glue",
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

    # AWS Glue requires lowercase identifiers and alphanumeric characters only
    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__.split(".")[-1]
        return f"{prefix}_{test_file}".replace("_", "").lower()

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models": {
                "glue_source.sql": MODEL__GLUE_SOURCE,
                "glue_merge.sql": MODEL__GLUE_MERGE,
                "glue_delete_insert.sql": MODEL__GLUE_DELETE_INSERT,
                "glue_merge_update_columns.sql": MODEL__GLUE_MERGE_UPDATE_COLUMNS,
                "glue_events.sql": MODEL__GLUE_EVENTS,
                "glue_microbatch.sql": MODEL__GLUE_MICROBATCH,
            }
        }

    def test_unique_key_and_merge_update_columns(self, project):
        # Glue columns are quoted lowercase, so the second run only matches the existing
        # row if the unquoted unique_key and merge_update_columns are quoted for it
        incremental_models = ["glue_merge", "glue_delete_insert", "glue_merge_update_columns"]
        for _ in range(2):
            run_dbt(["run", "--select", "glue_source", *incremental_models])

        glue_database = os.getenv("SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE_GLUE")
        for identifier in incremental_models:
            relation = f'{glue_database}."{project.test_schema}"."{identifier}"'
            rows = project.run_sql(f'select "id", "name" from {relation}', fetch="all")
            assert [(int(id), name) for id, name in rows] == [(1, "a")]

    def test_microbatch_event_time(self, project):
        # Each run's batch deletes its window from the table before inserting it again, which
        # only matches rows if the unquoted event_time is quoted for Glue
        window = ["--event-time-start", "2026-10-28", "--event-time-end", "2026-10-30"]
        for _ in range(2):
            run_dbt(["run", "--select", "glue_events", "glue_microbatch", *window])

        glue_database = os.getenv("SNOWFLAKE_TEST_CATALOG_LINKED_DATABASE_GLUE")
        relation = f'{glue_database}."{project.test_schema}"."glue_microbatch"'
        rows = project.run_sql(f'select "id" from {relation} order by "id"', fetch="all")
        assert [int(id) for (id,) in rows] == [1, 2]
