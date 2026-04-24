import pytest
from dbt.tests.util import run_dbt, relation_from_name


_MODEL_TRANSIENT_TMP = """
{{ config(
    materialized='incremental',
    unique_key='id',
    tmp_relation_type='transient',
) }}

select 1 as id, 'alice' as name
{% if is_incremental() %}
union all
select 2 as id, 'bob' as name
{% endif %}
"""

_MODEL_TRANSIENT_TMP_DELETE_INSERT = """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
    tmp_relation_type='transient',
) }}

select 1 as id, 'alice' as name
{% if is_incremental() %}
union all
select 2 as id, 'bob' as name
{% endif %}
"""


class TestIncrementalTransientTmpRelation:
    """tmp_relation_type='transient' creates a transient (not session-scoped) staging
    table, enabling Snowflake lineage tracking while avoiding permanent-table
    fail-safe storage costs."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"transient_incremental.sql": _MODEL_TRANSIENT_TMP}

    def test_incremental_transient_runs(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        run_dbt(["test"])

    def test_incremental_transient_initial_row_count(self, project):
        run_dbt(["run"])
        result = project.run_sql(
            "select count(*) as cnt from {database}.{schema}.transient_incremental",
            fetch="one",
        )
        assert result[0] == 1

    def test_incremental_transient_second_run_row_count(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        result = project.run_sql(
            "select count(*) as cnt from {database}.{schema}.transient_incremental",
            fetch="one",
        )
        assert result[0] == 2


class TestIncrementalTransientTmpRelationDeleteInsert:
    """transient tmp_relation_type is allowed for delete+insert strategy since
    transient tables are stable across multiple statements, unlike views."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"transient_delete_insert.sql": _MODEL_TRANSIENT_TMP_DELETE_INSERT}

    def test_incremental_transient_delete_insert_runs(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        run_dbt(["test"])
