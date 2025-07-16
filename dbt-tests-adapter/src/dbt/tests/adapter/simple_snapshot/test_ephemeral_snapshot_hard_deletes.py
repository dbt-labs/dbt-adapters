import pytest

from dbt.tests.util import run_dbt


# Source table creation statement
_source_create_sql = """
create table {database}.{schema}.src_customers (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    updated_at TIMESTAMP
);
"""

# Initial data for source table
_source_insert_sql = """
insert into {database}.{schema}.src_customers (id, first_name, last_name, email, updated_at) values
(1, 'John', 'Doe', 'john.doe@example.com', '2023-01-01 10:00:00'),
(2, 'Jane', 'Smith', 'jane.smith@example.com', '2023-01-02 11:00:00'),
(3, 'Bob', 'Johnson', 'bob.johnson@example.com', '2023-01-03 12:00:00');
"""

# SQL to add a dummy column to source table (simulating schema change)
_source_alter_sql = """
alter table {database}.{schema}.src_customers add column dummy_column VARCHAR(50) default 'dummy_value';
"""

# Sources YAML configuration
_sources_yml = """
version: 2

sources:
  - name: test_source
    schema: "{{ target.schema }}"
    tables:
      - name: src_customers
"""

# Ephemeral model that references the source
_ephemeral_customers_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ source('test_source', 'src_customers') }}
"""

# Snapshots YAML configuration with hard_deletes: new_record
_snapshots_yml = """
snapshots:
  - name: snapshot_customers
    relation: ref('ephemeral_customers')
    config:
      unique_key: id
      strategy: check
      check_cols: all
      hard_deletes: new_record
"""

# Test model to query the snapshot (for verification)
_ref_snapshot_sql = """
select * from {{ ref('snapshot_customers') }}
"""


class BaseSnapshotEphemeralHardDeletes:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "_sources.yml": _sources_yml,
            "ephemeral_customers.sql": _ephemeral_customers_sql,
            "snapshots.yml": _snapshots_yml,
            "ref_snapshot.sql": _ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def source_create_sql(self):
        return _source_create_sql

    @pytest.fixture(scope="class")
    def source_insert_sql(self):
        return _source_insert_sql

    @pytest.fixture(scope="class")
    def source_alter_sql(self):
        return _source_alter_sql

    def test_ephemeral_snapshot_hard_deletes(
        self, project, source_create_sql, source_insert_sql, source_alter_sql
    ):

        project.run_sql(
            source_create_sql.format(database=project.database, schema=project.test_schema)
        )
        project.run_sql(
            source_insert_sql.format(database=project.database, schema=project.test_schema)
        )

        results = run_dbt(["snapshot"])
        assert results is not None
        assert len(results) == 1  # type: ignore

        snapshot_result = project.run_sql(
            "select count(*) as row_count from snapshot_customers", fetch="one"
        )
        assert snapshot_result[0] == 3  # Should have 3 rows from initial data

        project.run_sql(
            source_alter_sql.format(database=project.database, schema=project.test_schema)
        )

        results = run_dbt(["snapshot"])
        assert len(results) == 1  # type: ignore
