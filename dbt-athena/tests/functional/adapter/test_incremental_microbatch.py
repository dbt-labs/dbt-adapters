import pytest

from dbt.tests.util import run_dbt_and_capture
from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
    patch_microbatch_end_time,
)

_hive_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, CAST(from_iso8601_timestamp('2020-01-01T00:00:00.000000Z') as timestamp) as event_time
union all
select 2 as id, CAST(from_iso8601_timestamp('2020-01-02T00:00:00.000000Z') as timestamp) as event_time
union all
select 3 as id, CAST(from_iso8601_timestamp('2020-01-03T00:00:00.000000Z') as timestamp) as event_time
"""

_iceberg_input_model_sql = """
{{ config(materialized='table', event_time='event_time', table_type='iceberg') }}
select 1 as id, CAST(from_iso8601_timestamp('2020-01-01T00:00:00.000000Z') as timestamp(6)) as event_time
union all
select 2 as id, CAST(from_iso8601_timestamp('2020-01-02T00:00:00.000000Z') as timestamp(6)) as event_time
union all
select 3 as id, CAST(from_iso8601_timestamp('2020-01-03T00:00:00.000000Z') as timestamp(6)) as event_time
"""

_hive_microbatch_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0),
    partitioned_by=['event_time'],
    table_type='hive'
    )
}}
with
hive_input_model as (
    select * from {{ ref('input_model') }}
),
iceberg_input_model as (
    select * from {{ ref('iceberg_input_model') }}
)
select
    hive_input_model.id,
    cast(iceberg_input_model.event_time as timestamp) as iceberg_event_time,
    hive_input_model.event_time
from hive_input_model
left join iceberg_input_model
    on hive_input_model.id = iceberg_input_model.id
"""

_iceberg_microbatch_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0),
    unique_key='id',
    table_type='iceberg'
    )
}}
with
hive_input_model as (
    select * from {{ ref('hive_input_model') }}
),
iceberg_input_model as (
    select * from {{ ref('input_model') }}
)
select
    iceberg_input_model.id,
    iceberg_input_model.event_time,
    cast(hive_input_model.event_time as timestamp(6)) as hive_event_time
from iceberg_input_model
left join hive_input_model
    on iceberg_input_model.id = hive_input_model.id
"""

_iceberg_microbatch_model_no_unique_key_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0),
    table_type='iceberg'
    )
}}
select * from {{ ref('input_model') }}
"""

_microbatch_model_no_partitioned_by_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)
    )
}}
select * from {{ ref('input_model') }}
"""


class TestAthenaHiveMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": _hive_input_model_sql,
            "iceberg_input_model.sql": _iceberg_input_model_sql,
            "microbatch_model.sql": _hive_microbatch_model_sql,
        }


class TestAthenaHiveMicrobatchMissingPartitionBy:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "microbatch.sql": _microbatch_model_no_partitioned_by_sql,
            "input_model.sql": _hive_input_model_sql,
        }

    def test_execution_failure_no_partitioned_by(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, stdout = run_dbt_and_capture(["run"], expect_pass=False)
        assert (
            "dbt-athena 'microbatch' incremental strategy for hive tables requires a `partitioned_by` config."
            in stdout
        )


class TestAthenaIcebergMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hive_input_model.sql": _hive_input_model_sql,
            "input_model.sql": _iceberg_input_model_sql,
            "microbatch_model.sql": _iceberg_microbatch_model_sql,
        }


class TestAthenaIcebergMicrobatchMissingUniqueKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "microbatch.sql": _iceberg_microbatch_model_no_unique_key_sql,
            "input_model.sql": _iceberg_input_model_sql,
        }

    def test_execution_failure_no_unique_key(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, stdout = run_dbt_and_capture(["run"], expect_pass=False)
        assert (
            "Microbatch strategy for iceberg tables must implement unique_key as a single column or a list of columns."
            in stdout
        )
