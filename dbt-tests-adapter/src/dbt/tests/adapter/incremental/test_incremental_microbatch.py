from pprint import pformat

import pytest

from dbt.tests.util import relation_from_name, run_dbt

try:
    # patch_microbatch_end_time introduced in dbt 1.9.0
    from dbt.tests.util import patch_microbatch_end_time
except ImportError:
    from freezegun import freeze_time as patch_microbatch_end_time

_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2020-01-01 00:00:00-0' as event_time
union all
select 2 as id, TIMESTAMP '2020-01-02 00:00:00-0' as event_time
union all
select 3 as id, TIMESTAMP '2020-01-03 00:00:00-0' as event_time
"""

_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)) }}
select * from {{ ref('input_model') }}
"""


class BaseMicrobatch:
    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        """
        This is the SQL that defines the microbatch model, including any {{ config(..) }}
        """
        return _microbatch_model_sql

    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        """
        This is the SQL that defines the input model to the microbatch model, including any {{ config(..) }}.
        event_time is a required configuration of this input
        """
        return _input_model_sql

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return f"insert into {test_schema_relation}.input_model (id, event_time) values (4, TIMESTAMP '2020-01-04 00:00:00-0'), (5, TIMESTAMP '2020-01-05 00:00:00-0')"

    @pytest.fixture(scope="class")
    def models(self, microbatch_model_sql, input_model_sql):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_sql,
        }

    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select * from {relation}", fetch="all")

        assert len(result) == expected_row_count, f"{relation_name}:{pformat(result)}"

    def test_run_with_event_time(self, project, insert_two_rows_sql):
        # initial run -- backfills all data
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # our partition grain is "day" so running the same day without new data should produce the same results
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # add next two days of data
        project.run_sql(insert_two_rows_sql)

        self.assert_row_count(project, "input_model", 5)

        # re-run without changing current time => no insert
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 3)

        # re-run by advancing time by one day changing current time => insert 1 row
        with patch_microbatch_end_time("2020-01-04 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 4)

        # re-run by advancing time by one more day changing current time => insert 1 more row
        with patch_microbatch_end_time("2020-01-05 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 5)
