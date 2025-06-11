from datetime import datetime
import os
import random

from dbt.tests.util import get_connection, run_dbt
import pytest

from dbt.adapters.bigquery import BigQueryRelation


def prefix():
    # create a directory name that will be unique per test session
    _randint = random.randint(0, 9999)
    _runtime_timedelta = datetime.utcnow() - datetime(1970, 1, 1, 0, 0, 0)
    _runtime = (int(_runtime_timedelta.total_seconds() * 1e6)) + _runtime_timedelta.microseconds
    return f"test{_runtime}{_randint:04}"


# make sure this is static
PREFIX = prefix()


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
    catalog_name='managed_iceberg',
    storage_uri='gs://"""
    + os.getenv("BIGQUERY_TEST_ICEBERG_BUCKET")
    + """/"""
    + PREFIX
    + """__managed_iceberg_table'
) }}
select 1 as id
"""
)
MODEL__MANAGED_ICEBERG_INCREMENTAL = (
    """
{{ config(
    materialized="incremental",
    unique_key="id",
    catalog_name='managed_iceberg',
    storage_uri='gs://"""
    + os.getenv("BIGQUERY_TEST_ICEBERG_BUCKET")
    + """/"""
    + PREFIX
    + """__managed_iceberg_incremental'
) }}

with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-01' as datetime) as date_time

    {% else %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-02' as datetime) as date_time union all
        select 5 as id, cast('2020-01-02' as datetime) as date_time union all
        select 6 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data

{% if is_incremental() %}
where id >= (select max(id) from {{ this }})
{% endif %}
"""
)


class TestGenericCatalog:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "info_schema_default.sql": MODEL__INFO_SCHEMA_DEFAULT,
            "info_schema_table.sql": MODEL__INFO_SCHEMA_TABLE,
            "managed_iceberg_table.sql": MODEL__MANAGED_ICEBERG_TABLE,
            "managed_iceberg_incremental.sql": MODEL__MANAGED_ICEBERG_INCREMENTAL,
        }

    def test_generic_catalog(self, project):
        results = run_dbt(["run"])
        assert len(results) == 4
        results = run_dbt(["run", "--select", "managed_iceberg_incremental"])
        assert len(results) == 1

        schema = BigQueryRelation.create(project.database, project.test_schema)
        with get_connection(project.adapter):
            relations = project.adapter.list_relations_without_caching(schema)
        assert len(relations) == 4
