"""
Tests for BigQuery snapshots with ARRAY columns using strategy='check'.

BigQuery does not support `!=` on ARRAY types, so the default snapshot_check_strategy
generates invalid SQL when check_cols includes an ARRAY column. The
bigquery__snapshot_check_column_values override fixes this by wrapping comparisons
in TO_JSON_STRING().
"""

import pytest

from dbt.tests.adapter.simple_snapshot import common
from dbt.tests.adapter.simple_snapshot.test_snapshot import BaseSimpleSnapshotBase
from dbt.tests.util import relation_from_name, run_dbt

SOURCE_DATA_SQL = """
{{ config(materialized="table") }}

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    [gender, ip_address] as tags
from (
    select 1 as id, 'Judith' as first_name, 'Kennedy' as last_name, 'jkennedy0@phpbb.com' as email, 'Female' as gender, '54.60.24.128' as ip_address, timestamp('2015-12-24') as updated_at union all
    select 2, 'Arthur', 'Kelly', 'akelly1@eepurl.com', 'Male', '62.56.24.215', timestamp('2015-10-28') union all
    select 3, 'Rachel', 'Moreno', 'rmoreno2@msu.edu', 'Female', '31.222.249.23', timestamp('2016-04-05') union all
    select 4, 'Ralph', 'Turner', 'rturner3@hp.com', 'Male', '157.83.76.114', timestamp('2016-08-08') union all
    select 5, 'Laura', 'Gonzales', 'lgonzales4@howstuffworks.com', 'Female', '30.54.105.168', timestamp('2016-09-01') union all
    select 6, 'Katherine', 'Lopez', 'klopez5@yahoo.co.jp', 'Female', '169.138.46.89', timestamp('2016-08-30') union all
    select 7, 'Jeremy', 'Hamilton', 'jhamilton6@mozilla.org', 'Male', '231.189.13.133', timestamp('2016-07-17') union all
    select 8, 'Heather', 'Rose', 'hrose7@goodreads.com', 'Female', '87.165.201.65', timestamp('2015-12-29') union all
    select 9, 'Gregory', 'Kelly', 'gkelly8@trellian.com', 'Male', '154.209.99.7', timestamp('2016-03-24') union all
    select 10, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', timestamp('2016-08-20')
)
"""

FACT_SQL = """
{{ config(materialized="table") }}

select * from {{ ref('source_data') }}
where id between 1 and 5
"""

SNAPSHOT_CHECK_ALL_SQL = """
{% snapshot snapshot %}
    {{ config(
        target_database=database,
        target_schema=schema,
        unique_key='id',
        strategy='check',
        check_cols='all',
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""


class TestSnapshotCheckArrayColumns(BaseSimpleSnapshotBase):
    """Test that snapshots with strategy='check' work when ARRAY columns are present."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_data.sql": SOURCE_DATA_SQL,
            "fact.sql": FACT_SQL,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_CHECK_ALL_SQL}

    @pytest.fixture(scope="class", autouse=True)
    def _setup_class(self, project):
        run_dbt(["run"])

    @pytest.fixture(scope="function", autouse=True)
    def _setup_method(self, project):
        self.project = project
        self.create_fact_from_seed("id between 1 and 5")
        run_dbt(["snapshot"])
        yield
        self.delete_snapshot_records()
        self.delete_fact_records()

    def create_fact_from_seed(self, where=None):
        common.clone_table(self.project, "fact", "source_data", "*", where)

    def insert_fact_records(self, where=None):
        common.insert_records(self.project, "fact", "source_data", "*", where)

    def test_updates_are_captured_by_snapshot(self, project):
        """
        Update scalar columns on the last 2 records. The check strategy should detect
        the change even though ARRAY columns are present in check_cols='all'.
        """
        self.update_fact_records({"email": "'updated@test.com'"}, "id between 4 and 5")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 6),
            ids_with_closed_out_snapshot_records=range(4, 6),
        )

    def test_array_updates_are_captured_by_snapshot(self, project):
        """
        Update ARRAY column values. The check strategy must detect the change
        via TO_JSON_STRING wrapping.
        """
        fact = relation_from_name(project.adapter, "fact")
        project.run_sql(
            f"""
            update {fact}
            set tags = ['updated_tag1', 'updated_tag2']
            where id between 4 and 5
            """
        )
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 6),
            ids_with_closed_out_snapshot_records=range(4, 6),
        )

    def test_inserts_are_captured_by_snapshot(self, project):
        """
        Insert 5 new records. All 10 ids are current, none are closed out.
        """
        self.insert_fact_records("id between 6 and 10")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 11),
            ids_with_closed_out_snapshot_records=[],
        )

    def test_deletes_are_captured_by_snapshot(self, project):
        """
        Hard-delete the last 2 records. With invalidate_hard_deletes (default),
        deleted ids get their records closed out.
        """
        self.delete_fact_records("id between 4 and 5")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 4),
            ids_with_closed_out_snapshot_records=range(4, 6),
        )
