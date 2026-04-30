"""
Tests for BigQuery snapshots with STRUCT and ARRAY columns using hard_deletes='new_record'.

The default snapshot_staging_table macro has two issues on BigQuery:
1. get_column_schema_from_query() flattens STRUCT fields, causing column count mismatches.
2. Bare `NULL as col` is typed as INT64, incompatible with STRUCT/ARRAY columns.

The bigquery__snapshot_staging_table override fixes both by using get_columns_in_query()
(top-level names only) and source_data.<col> instead of NULL for new columns.
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
    struct(first_name as first, last_name as last, email) as contact,
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

SNAPSHOT_HARD_DELETES_SQL = """
{% snapshot snapshot %}
    {{ config(
        target_database=database,
        target_schema=schema,
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
        hard_deletes='new_record',
    ) }}
    select *, timestamp(updated_at) as updated_at_ts from {{ ref('fact') }}
{% endsnapshot %}
"""


class TestSnapshotStructArrayHardDeletes(BaseSimpleSnapshotBase):
    """Test that snapshots with STRUCT/ARRAY columns work with hard_deletes='new_record'."""

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
        return {"snapshot.sql": SNAPSHOT_HARD_DELETES_SQL}

    @pytest.fixture(scope="class", autouse=True)
    def _setup_class(self, project):
        # Build source_data and fact models (replaces `run_dbt(["seed"])` from base).
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
        Update the last 2 records. All ids are current, but the last 2 reflect updates.
        """
        date_add_expression = "timestamp_add(updated_at, interval 1 day)"
        self.update_fact_records({"updated_at": date_add_expression}, "id between 4 and 5")
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
        Hard-delete the last 2 records. With hard_deletes='new_record', each deleted id
        gets its original record closed out AND a new deletion marker inserted (dbt_valid_to
        is null, dbt_is_deleted='True'). So deleted ids appear in both current and closed out.
        """
        self.delete_fact_records("id between 4 and 5")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 6),
            ids_with_closed_out_snapshot_records=range(4, 6),
        )
        # Additionally verify the deletion markers exist
        deleted_rows = self.get_snapshot_records("id", "dbt_is_deleted = 'True'")
        assert sorted(deleted_rows) == [(4,), (5,)]

    def test_revives_are_captured_by_snapshot(self, project):
        """
        Delete the last 2 records, snapshot, then re-insert one of them.
        """
        self.delete_fact_records("id between 4 and 5")
        run_dbt(["snapshot"])
        self.insert_fact_records("id = 4")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 6),
            ids_with_closed_out_snapshot_records=range(4, 6),
        )

    def test_new_struct_column_with_deletes(self, project):
        """
        Add a new STRUCT column to fact, then delete rows and re-snapshot.

        This exercises Change 2 of the fix: when a column exists in the source but
        not yet in the snapshot, the default macro emits `NULL as col` which BigQuery
        types as INT64 — incompatible with the STRUCT type in the other UNION ALL arms.
        Our override uses `source_data.col` instead, preserving the correct type.
        """
        # Recreate fact with an additional STRUCT column not present in the snapshot.
        fact = relation_from_name(project.adapter, "fact")
        source_data = relation_from_name(project.adapter, "source_data")
        project.run_sql(f"drop table if exists {fact}")
        project.run_sql(
            f"""
            create table {fact} as
            select
                *,
                struct(first_name as name, id as rank) as extra_info
            from {source_data}
            where id between 1 and 5
            """
        )

        # Delete rows so the deletion_records CTE must handle the new column.
        self.delete_fact_records("id between 4 and 5")

        # This snapshot would fail without Change 2.
        run_dbt(["snapshot"])

        self._assert_results(
            ids_with_current_snapshot_records=range(1, 6),
            ids_with_closed_out_snapshot_records=range(4, 6),
        )
