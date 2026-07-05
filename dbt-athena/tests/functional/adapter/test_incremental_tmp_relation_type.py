"""
Functional tests for the `tmp_relation_type` config on incremental models.

`tmp_relation_type: view` stages the model query as a view instead of a
temporary table, so the data is only written once (by the MERGE/INSERT into
the target) instead of twice (CTAS into a tmp table, then again by the merge).
"""

import json

import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import check_relations_equal, run_dbt

models__merge_default_tmp_sql = """
{{ config(
        table_type='iceberg',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['id']
    )
}}

{% if is_incremental() %}

select * from (
    values
    (2, 'v2-updated')
    , (3, 'v3')
) as t (id, value)

{% else %}

select * from (
    values
    (1, 'v1')
    , (2, 'v2')
) as t (id, value)

{% endif %}
"""

models__merge_view_tmp_sql = """
{{ config(
        table_type='iceberg',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['id'],
        tmp_relation_type='view'
    )
}}

{% if is_incremental() %}

select * from (
    values
    (2, 'v2-updated')
    , (3, 'v3')
) as t (id, value)

{% else %}

select * from (
    values
    (1, 'v1')
    , (2, 'v2')
) as t (id, value)

{% endif %}
"""

models__merge_table_tmp_sql = """
{{ config(
        table_type='iceberg',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['id'],
        tmp_relation_type='table'
    )
}}

{% if is_incremental() %}

select * from (
    values
    (2, 'v2-updated')
    , (3, 'v3')
) as t (id, value)

{% else %}

select * from (
    values
    (1, 'v1')
    , (2, 'v2')
) as t (id, value)

{% endif %}
"""

seeds__expected_merge_view_tmp_csv = """id,value
1,v1
2,v2-updated
3,v3
"""

models__merge_view_unsupported_type_sql = """
{{ config(
        table_type='iceberg',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['id']
    )
}}

{% if is_incremental() %}

select * from (
    values
    (2, 'v2-updated', cast('2024-01-02 00:00:00.123456' as timestamp(6)))
    , (3, 'v3', cast('2024-01-03 00:00:00.123456' as timestamp(6)))
) as t (id, value, ts)

{% else %}

select * from (
    values
    (1, 'v1', cast('2024-01-01 00:00:00.123456' as timestamp(6)))
    , (2, 'v2', cast('2024-01-02 00:00:00.000000' as timestamp(6)))
) as t (id, value, ts)

{% endif %}
"""

models__view_tmp_insert_overwrite_sql = """
{{ config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partitioned_by=['id'],
        tmp_relation_type='view'
    )
}}

select * from (
    values
    (1, 'v1')
) as t (id, value)
"""

models__view_tmp_force_batch_sql = """
{{ config(
        table_type='iceberg',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['id'],
        tmp_relation_type='view',
        force_batch=true
    )
}}

select * from (
    values
    (1, 'v1')
) as t (id, value)
"""


def extract_athena_queries(dbt_run_capsys_output: str, relation_name: str) -> list:
    """Collect every executed statement for the model from debug json logs.

    Queries surface either as 'Running Athena query:' debug messages or as
    SQLQuery events carrying the statement in data.sql.
    """
    queries = []
    for events_msg in dbt_run_capsys_output.split("\n")[1:]:
        try:
            data = json.loads(events_msg).get("data", {})
        except (json.JSONDecodeError, AttributeError):
            continue
        if not isinstance(data, dict):
            continue
        base_msg = data.get("base_msg")
        if base_msg and "Running Athena query:" in str(base_msg):
            queries.append(str(base_msg))
        elif data.get("conn_name") == f"model.test.{relation_name}" and "sql" in data:
            queries.append(str(data["sql"]))
    return queries


class BaseMergeTmpRelationType:
    """Runs a merge model twice and returns the staging statements of the
    incremental run so subclasses can assert on the tmp relation type."""

    def run_and_get_tmp_queries(self, project, capsys, relation_name):
        expected_seed_name = "expected_merge_view_tmp"
        run_dbt(["seed", "--select", expected_seed_name, "--full-refresh"])

        model_run = run_dbt(["run", "--select", relation_name])
        assert model_run.results[0].status == RunStatus.Success
        capsys.readouterr()  # discard first-run logs

        incremental_run = run_dbt(
            [
                "run",
                "--select",
                relation_name,
                "--log-level",
                "debug",
                "--log-format",
                "json",
            ]
        )
        assert incremental_run.results[0].status == RunStatus.Success

        out, _ = capsys.readouterr()
        queries = extract_athena_queries(out, relation_name)
        tmp_queries = [q for q in queries if "__dbt_tmp" in q]

        check_relations_equal(project.adapter, [relation_name, expected_seed_name])
        return tmp_queries


class TestIcebergMergeDefaultTmpRelationIsView(BaseMergeTmpRelationType):
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_default_tmp.sql": models__merge_default_tmp_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_merge_view_tmp.csv": seeds__expected_merge_view_tmp_csv}

    def test__merge_defaults_to_view_tmp_relation(self, project, capsys):
        """With no tmp_relation_type set, merge stages the source as a view
        (same default as dbt-trino and dbt-snowflake)."""

        tmp_queries = self.run_and_get_tmp_queries(project, capsys, "merge_default_tmp")

        view_creates = [q for q in tmp_queries if "create or replace view" in q.lower()]
        table_creates = [q for q in tmp_queries if "create table" in q.lower()]

        assert view_creates, "expected merge to stage the source as a view by default"
        assert not table_creates, "expected no tmp table creation for the default merge path"


class TestIcebergMergeViewTmpRelation(BaseMergeTmpRelationType):
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_view_tmp.sql": models__merge_view_tmp_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_merge_view_tmp.csv": seeds__expected_merge_view_tmp_csv}

    def test__merge_view_tmp_relation(self, project, capsys):
        """tmp_relation_type=view stages the source as a view, not a tmp table."""

        tmp_queries = self.run_and_get_tmp_queries(project, capsys, "merge_view_tmp")

        view_creates = [q for q in tmp_queries if "create or replace view" in q.lower()]
        table_creates = [q for q in tmp_queries if "create table" in q.lower()]

        assert view_creates, "expected the tmp relation to be created as a view"
        assert not table_creates, "expected no tmp table creation when tmp_relation_type=view"


class TestIcebergMergeTableTmpRelationOptOut(BaseMergeTmpRelationType):
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_table_tmp.sql": models__merge_table_tmp_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_merge_view_tmp.csv": seeds__expected_merge_view_tmp_csv}

    def test__merge_table_tmp_relation(self, project, capsys):
        """tmp_relation_type=table opts out of view staging, e.g. for models
        projecting types views cannot hold (timestamp with time zone)."""

        tmp_queries = self.run_and_get_tmp_queries(project, capsys, "merge_table_tmp")

        view_creates = [q for q in tmp_queries if "create or replace view" in q.lower()]
        table_creates = [q for q in tmp_queries if "create table" in q.lower()]

        assert table_creates, "expected the tmp relation to be created as a table"
        assert not view_creates, "expected no view creation when tmp_relation_type=table"


class TestIcebergMergeViewFallbackOnUnsupportedType:
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_view_unsupported_type.sql": models__merge_view_unsupported_type_sql}

    def test__falls_back_to_table_staging(self, project, capsys):
        """timestamp(6) columns cannot live in an Athena view, so the default
        view staging must fall back to a table and the run still succeed."""

        relation_name = "merge_view_unsupported_type"
        model_run = run_dbt(["run", "--select", relation_name])
        assert model_run.results[0].status == RunStatus.Success
        capsys.readouterr()  # discard first-run logs

        incremental_run = run_dbt(
            [
                "run",
                "--select",
                relation_name,
                "--log-level",
                "debug",
                "--log-format",
                "json",
            ]
        )
        assert incremental_run.results[0].status == RunStatus.Success

        out, _ = capsys.readouterr()
        queries = extract_athena_queries(out, relation_name)
        tmp_queries = [q for q in queries if "__dbt_tmp" in q]
        table_creates = [q for q in tmp_queries if "create table" in q.lower()]
        assert table_creates, "expected fallback to table staging for unsupported view types"

        rows = project.run_sql(
            f"select count(*) from {project.test_schema}.{relation_name}", fetch="one"
        )
        assert rows[0] == 3


class TestViewTmpRelationInvalidWithInsertOverwrite:
    @pytest.fixture(scope="class")
    def models(self):
        return {"view_tmp_insert_overwrite.sql": models__view_tmp_insert_overwrite_sql}

    def test__view_tmp_relation_rejected(self, project):
        """insert_overwrite runs multiple statements against the staged data,
        so a view staging relation must be rejected on any run (fail fast)."""

        result = run_dbt(["run", "--select", "view_tmp_insert_overwrite"], expect_pass=False)
        assert result.results[0].status == RunStatus.Error
        assert "tmp_relation_type" in result.results[0].message


class TestViewTmpRelationInvalidWithForceBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"view_tmp_force_batch.sql": models__view_tmp_force_batch_sql}

    def test__view_tmp_relation_rejected(self, project):
        """force_batch merges partition-by-partition (multiple reads of the
        staged data), so a view staging relation must be rejected (fail fast)."""

        result = run_dbt(["run", "--select", "view_tmp_force_batch"], expect_pass=False)
        assert result.results[0].status == RunStatus.Error
        assert "tmp_relation_type" in result.results[0].message
