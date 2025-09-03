import pytest

from dbt.tests.util import run_dbt

_seed_new_record_mode_statements = [
    "create table {database}.{schema}.seed (id INTEGER, first_name VARCHAR(50));",
    "insert into {database}.{schema}.seed (id, first_name) values (1, 'Judith'), (2, 'Arthur');",
]

_snapshot_actual_sql = """
{% snapshot snapshot_actual %}
    select * from {{target.database}}.{{target.schema}}.seed
{% endsnapshot %}
"""

_delete_sql = """
delete from {database}.{schema}.seed where id = 1
"""

# If the deletion worked correctly, this should return one row (and not more) where dbt_is_deleted is True
_delete_check_sql = """
select dbt_scd_id from {schema}.snapshot_actual where id = 1 and dbt_is_deleted = 'True'
"""

_snapshots_yml = """
snapshots:
  - name: snapshot_actual
    config:
      unique_key: id
      strategy: check
      check_cols: all
      hard_deletes: new_record
      dbt_valid_to_current: "date('9999-12-31')"
"""


class BaseSnapshotNewRecordDbtValidToCurrent:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"snapshots.yml": _snapshots_yml}

    @pytest.fixture(scope="class")
    def seed_new_record_mode_statements(self):
        return _seed_new_record_mode_statements

    @pytest.fixture(scope="class")
    def delete_sql(self):
        return _delete_sql

    def test_snapshot_new_record_mode(
        self,
        project,
        seed_new_record_mode_statements,
        delete_sql,
    ):
        for stmt in seed_new_record_mode_statements:
            project.run_sql(stmt)

        # Snapshot once to get the initial snapshot
        run_dbt(["snapshot"])

        # Remove the record from the source data
        project.run_sql(delete_sql)

        # Snapshot twice in a row checking that the deleted record is not duplicated in the snapshot
        run_dbt(["snapshot"])
        run_dbt(["snapshot"])

        check_result = project.run_sql(_delete_check_sql, fetch="all")
        assert len(check_result) == 1
