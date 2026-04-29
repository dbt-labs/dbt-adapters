import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt


models__iceberg_merge_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge',
    table_type = 'iceberg',
    build_strategy = 'subquery',
) }}

{% if not is_incremental() %}

select 1 as id, 'hello' as msg, 'blue' as color
union all
select 2 as id, 'goodbye' as msg, 'red' as color

{% else %}

select 1 as id, 'hey' as msg, 'blue' as color
union all
select 2 as id, 'yo' as msg, 'green' as color
union all
select 3 as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

models__iceberg_merge_force_batch_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge',
    table_type = 'iceberg',
    build_strategy = 'subquery',
    force_batch = True,
) }}

select 1 as id, 'hello' as msg
"""

models__iceberg_append_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    table_type = 'iceberg',
    build_strategy = 'subquery',
) }}

select 1 as id, 'hello' as msg, 'blue' as color
"""

models__iceberg_append_force_batch_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    table_type = 'iceberg',
    build_strategy = 'subquery',
    force_batch = True,
) }}

select 1 as id, 'hello' as msg
"""

models__hive_append_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    table_type = 'hive',
    build_strategy = 'subquery',
) }}

select 1 as id, 'hello' as msg, 'blue' as color
"""


class TestIcebergMergeWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_merge.sql": models__iceberg_merge_sql}

    def test_full_refresh(self, project):
        result = run_dbt(["run", "--select", "iceberg_merge", "--full-refresh"])
        assert result.results[0].status == RunStatus.Success

        row_count = project.run_sql(
            f"select count(*) from {project.test_schema}.iceberg_merge", fetch="one"
        )[0]
        assert row_count == 2

    def test_incremental_merge(self, project):
        run_dbt(["run", "--select", "iceberg_merge", "--full-refresh"])
        result = run_dbt(["run", "--select", "iceberg_merge"])
        assert result.results[0].status == RunStatus.Success

        row_count = project.run_sql(
            f"select count(*) from {project.test_schema}.iceberg_merge", fetch="one"
        )[0]
        assert row_count == 3

        updated = project.run_sql(
            f"select msg from {project.test_schema}.iceberg_merge where id = 1", fetch="one"
        )[0]
        assert updated == "hey"


class TestIcebergMergeWithSubqueryIncompatibleForceBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_merge_force_batch.sql": models__iceberg_merge_force_batch_sql}

    def test_force_batch_fails(self, project):
        run_dbt(["run", "--select", "iceberg_merge_force_batch", "--full-refresh"])
        results = run_dbt(
            ["run", "--select", "iceberg_merge_force_batch"],
            expect_pass=False,
        )
        assert len(results) == 1
        assert "incompatible with force_batch" in results[0].message


class TestIcebergAppendWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_append.sql": models__iceberg_append_sql}

    def test_full_refresh(self, project):
        result = run_dbt(["run", "--select", "iceberg_append", "--full-refresh"])
        assert result.results[0].status == RunStatus.Success

    def test_incremental_append(self, project):
        run_dbt(["run", "--select", "iceberg_append", "--full-refresh"])
        result = run_dbt(["run", "--select", "iceberg_append"])
        assert result.results[0].status == RunStatus.Success

        row_count = project.run_sql(
            f"select count(*) from {project.test_schema}.iceberg_append", fetch="one"
        )[0]
        assert row_count == 2


class TestIcebergAppendWithSubqueryIncompatibleForceBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_append_force_batch.sql": models__iceberg_append_force_batch_sql}

    def test_force_batch_fails(self, project):
        run_dbt(["run", "--select", "iceberg_append_force_batch", "--full-refresh"])
        results = run_dbt(
            ["run", "--select", "iceberg_append_force_batch"],
            expect_pass=False,
        )
        assert len(results) == 1
        assert "incompatible with force_batch" in results[0].message


models__iceberg_table_sql = """
{{ config(
    materialized = 'table',
    table_type = 'iceberg',
    build_strategy = 'subquery',
) }}

select 1 as id, 'hello' as msg, 'blue' as color
union all
select 2 as id, 'goodbye' as msg, 'red' as color
"""

models__iceberg_table_force_batch_sql = """
{{ config(
    materialized = 'table',
    table_type = 'iceberg',
    build_strategy = 'subquery',
    force_batch = True,
) }}

select 1 as id, 'hello' as msg
"""

models__hive_table_sql = """
{{ config(
    materialized = 'table',
    table_type = 'hive',
    build_strategy = 'subquery',
) }}

select 1 as id, 'hello' as msg, 'blue' as color
"""


class TestIcebergTableWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_table.sql": models__iceberg_table_sql}

    def test_create_table(self, project):
        result = run_dbt(["run", "--select", "iceberg_table"])
        assert result.results[0].status == RunStatus.Success

        row_count = project.run_sql(
            f"select count(*) from {project.test_schema}.iceberg_table", fetch="one"
        )[0]
        assert row_count == 2

    def test_recreate_table(self, project):
        run_dbt(["run", "--select", "iceberg_table"])
        result = run_dbt(["run", "--select", "iceberg_table"])
        assert result.results[0].status == RunStatus.Success

        row_count = project.run_sql(
            f"select count(*) from {project.test_schema}.iceberg_table", fetch="one"
        )[0]
        assert row_count == 2


class TestIcebergTableWithSubqueryIncompatibleForceBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_table_force_batch.sql": models__iceberg_table_force_batch_sql}

    def test_force_batch_fails(self, project):
        results = run_dbt(
            ["run", "--select", "iceberg_table_force_batch"],
            expect_pass=False,
        )
        assert len(results) == 1
        assert "incompatible with force_batch" in results[0].message


class TestHiveTableWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hive_table.sql": models__hive_table_sql}

    def test_create_table(self, project):
        result = run_dbt(["run", "--select", "hive_table"])
        assert result.results[0].status == RunStatus.Success

        row_count = project.run_sql(
            f"select count(*) from {project.test_schema}.hive_table", fetch="one"
        )[0]
        assert row_count == 2


class TestHiveAppendWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hive_append.sql": models__hive_append_sql}

    def test_full_refresh(self, project):
        result = run_dbt(["run", "--select", "hive_append", "--full-refresh"])
        assert result.results[0].status == RunStatus.Success

    def test_incremental_append(self, project):
        run_dbt(["run", "--select", "hive_append", "--full-refresh"])
        result = run_dbt(["run", "--select", "hive_append"])
        assert result.results[0].status == RunStatus.Success

        row_count = project.run_sql(
            f"select count(*) from {project.test_schema}.hive_append", fetch="one"
        )[0]
        assert row_count == 2
