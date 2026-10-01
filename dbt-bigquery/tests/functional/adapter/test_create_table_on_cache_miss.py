"""Incremental and snapshot first builds emit `create table`, not `create or replace table`.

They take that branch only when the relation cache says the target does not exist. If the cache is
wrong and the table exists, the run must fail instead of silently replacing it. The stale cache is
simulated by overriding the macro each materialization uses to look the target up.
"""

import re

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

SIMULATE_CACHE_MISS = ["--vars", "{simulate_cache_miss: true}"]

INCREMENTAL_MODEL = """
{{ config(materialized="incremental", unique_key="id") }}
select 1 as id
{% if is_incremental() %}
union all select 2 as id
{% endif %}
"""

# BigQuery's incremental materialization looks its target up with `load_relation(this)`.
LOAD_RELATION_OVERRIDE = """
{% macro load_relation(relation) %}
  {% if var('simulate_cache_miss', false) %}
    {{ return(none) }}
  {% endif %}
  {{ return(dbt.load_relation(relation)) }}
{% endmacro %}
"""

SNAPSHOT_SOURCE = """
select 1 as id, timestamp('2024-01-01') as updated_at
union all select 2 as id, timestamp('2024-01-01') as updated_at
"""

SNAPSHOT = """
{% snapshot my_snapshot %}
{{ config(
    target_database=database,
    target_schema=schema,
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
) }}
select * from {{ ref('snapshot_source') }}
{% endsnapshot %}
"""

# The default snapshot materialization looks its target up with `get_or_create_relation`.
GET_OR_CREATE_RELATION_OVERRIDE = """
{% macro get_or_create_relation(database, schema, identifier, type) %}
  {% if var('simulate_cache_miss', false) %}
    {{ return((false, api.Relation.create(database=database, schema=schema, identifier=identifier, type=type))) }}
  {% endif %}
  {{ return(dbt.get_or_create_relation(database, schema, identifier, type)) }}
{% endmacro %}
"""


def _normalize(logs):
    return re.sub(r"\s+", " ", logs)


def _target(project, identifier):
    return f"`{project.database}`.`{project.test_schema}`.`{identifier}`"


def _row_count(project, identifier):
    return project.run_sql(f"select count(*) from {_target(project, identifier)}", fetch="one")[0]


class TestIncrementalFirstBuildCreatesTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_model.sql": INCREMENTAL_MODEL}

    def test_first_build_uses_create_table(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"])
        target = _target(project, "incremental_model")
        assert f"create table {target}" in _normalize(logs)
        assert f"create or replace table {target}" not in _normalize(logs)

        # The next run takes the incremental branch as usual.
        run_dbt(["run"])
        assert _row_count(project, "incremental_model") == 2


class TestIncrementalStaleCacheFailsInsteadOfReplacing:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_model.sql": INCREMENTAL_MODEL}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"load_relation.sql": LOAD_RELATION_OVERRIDE}

    def test_stale_cache_fails(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        assert _row_count(project, "incremental_model") == 2

        results, logs = run_dbt_and_capture(["run", *SIMULATE_CACHE_MISS], expect_pass=False)
        assert results[0].status == "error"
        assert "Already Exists" in logs
        assert _row_count(project, "incremental_model") == 2


class TestSnapshotFirstBuildCreatesTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {"snapshot_source.sql": SNAPSHOT_SOURCE}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"my_snapshot.sql": SNAPSHOT}

    def test_first_build_uses_create_table(self, project):
        run_dbt(["run"])
        _, logs = run_dbt_and_capture(["--debug", "snapshot"])
        target = _target(project, "my_snapshot")
        assert f"create table {target}" in _normalize(logs)
        assert f"create or replace table {target}" not in _normalize(logs)

        # Later runs still build the temporary staging table with `create or replace`.
        _, logs = run_dbt_and_capture(["--debug", "snapshot"])
        assert "create or replace table" in _normalize(logs)
        assert _row_count(project, "my_snapshot") == 2


class TestSnapshotStaleCacheFailsInsteadOfReplacing:
    @pytest.fixture(scope="class")
    def models(self):
        return {"snapshot_source.sql": SNAPSHOT_SOURCE}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"my_snapshot.sql": SNAPSHOT}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"get_or_create_relation.sql": GET_OR_CREATE_RELATION_OVERRIDE}

    def test_stale_cache_fails(self, project):
        run_dbt(["run"])
        run_dbt(["snapshot"])
        assert _row_count(project, "my_snapshot") == 2

        results, logs = run_dbt_and_capture(["snapshot", *SIMULATE_CACHE_MISS], expect_pass=False)
        assert results[0].status == "error"
        assert "Already Exists" in logs
        assert _row_count(project, "my_snapshot") == 2
