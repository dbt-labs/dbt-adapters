import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt


# --- incremental: iceberg merge ---

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


class TestIcebergMergeWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_merge.sql": models__iceberg_merge_sql}

    def test_full_refresh(self, project):
        result = run_dbt(["run", "--select", "iceberg_merge", "--full-refresh"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.iceberg_merge",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "blue"),
            (2, "goodbye", "red"),
        ]

    def test_incremental_merge(self, project):
        run_dbt(["run", "--select", "iceberg_merge", "--full-refresh"])
        result = run_dbt(["run", "--select", "iceberg_merge"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.iceberg_merge",
                fetch="all",
            )
        )
        assert records == [
            (1, "hey", "blue"),
            (2, "yo", "green"),
            (3, "anyway", "purple"),
        ]


class TestIcebergMergeWithSubqueryIncompatibleForceBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_merge_force_batch.sql": models__iceberg_merge_force_batch_sql}

    def test_force_batch_fails(self, project):
        results = run_dbt(
            ["run", "--select", "iceberg_merge_force_batch"],
            expect_pass=False,
        )
        assert len(results) == 1
        assert "incompatible with force_batch" in results[0].message


# --- incremental: iceberg append ---

models__iceberg_append_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    table_type = 'iceberg',
    build_strategy = 'subquery',
) }}

{% if not is_incremental() %}
select 1 as id, 'hello' as msg, 'blue' as color
{% else %}
select 2 as id, 'world' as msg, 'red' as color
{% endif %}
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


class TestIcebergAppendWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_append.sql": models__iceberg_append_sql}

    def test_full_refresh(self, project):
        result = run_dbt(["run", "--select", "iceberg_append", "--full-refresh"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.iceberg_append",
                fetch="all",
            )
        )
        assert records == [(1, "hello", "blue")]

    def test_incremental_append(self, project):
        run_dbt(["run", "--select", "iceberg_append", "--full-refresh"])
        result = run_dbt(["run", "--select", "iceberg_append"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.iceberg_append",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "blue"),
            (2, "world", "red"),
        ]


class TestIcebergAppendWithSubqueryIncompatibleForceBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_append_force_batch.sql": models__iceberg_append_force_batch_sql}

    def test_force_batch_fails(self, project):
        results = run_dbt(
            ["run", "--select", "iceberg_append_force_batch"],
            expect_pass=False,
        )
        assert len(results) == 1
        assert "incompatible with force_batch" in results[0].message


# --- table materialization ---

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
union all
select 2 as id, 'goodbye' as msg, 'red' as color
"""


class TestIcebergTableWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_table.sql": models__iceberg_table_sql}

    def test_create_table(self, project):
        result = run_dbt(["run", "--select", "iceberg_table"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.iceberg_table",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "blue"),
            (2, "goodbye", "red"),
        ]

    def test_recreate_table(self, project):
        run_dbt(["run", "--select", "iceberg_table"])
        result = run_dbt(["run", "--select", "iceberg_table"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.iceberg_table",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "blue"),
            (2, "goodbye", "red"),
        ]


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

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.hive_table",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "blue"),
            (2, "goodbye", "red"),
        ]


# --- incremental: hive append ---

models__hive_append_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    table_type = 'hive',
    build_strategy = 'subquery',
) }}

{% if not is_incremental() %}
select 1 as id, 'hello' as msg, 'blue' as color
{% else %}
select 2 as id, 'world' as msg, 'red' as color
{% endif %}
"""

models__hive_append_force_batch_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    table_type = 'hive',
    build_strategy = 'subquery',
    force_batch = True,
) }}

select 1 as id, 'hello' as msg
"""


class TestHiveAppendWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hive_append.sql": models__hive_append_sql}

    def test_full_refresh(self, project):
        result = run_dbt(["run", "--select", "hive_append", "--full-refresh"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.hive_append",
                fetch="all",
            )
        )
        assert records == [(1, "hello", "blue")]

    def test_incremental_append(self, project):
        run_dbt(["run", "--select", "hive_append", "--full-refresh"])
        result = run_dbt(["run", "--select", "hive_append"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.hive_append",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "blue"),
            (2, "world", "red"),
        ]


class TestHiveAppendWithSubqueryIncompatibleForceBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hive_append_force_batch.sql": models__hive_append_force_batch_sql}

    def test_force_batch_fails(self, project):
        results = run_dbt(
            ["run", "--select", "hive_append_force_batch"],
            expect_pass=False,
        )
        assert len(results) == 1
        assert "incompatible with force_batch" in results[0].message


# --- incremental: hive insert_overwrite ---

models__hive_insert_overwrite_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    table_type = 'hive',
    partitioned_by = ['date_col'],
    build_strategy = 'subquery',
) }}

{% if not is_incremental() %}
select id, cast(msg as varchar) as msg, date_col from (values
    (1, 'hello',     date '2024-01-01'),
    (2, 'goodbye',   date '2024-01-02')
) as t(id, msg, date_col)
{% else %}
select id, cast(msg as varchar) as msg, date_col from (values
    (3, 'overwrite', date '2024-01-02'),
    (4, 'new',       date '2024-01-03')
) as t(id, msg, date_col)
{% endif %}
"""


class TestHiveInsertOverwriteWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hive_insert_overwrite.sql": models__hive_insert_overwrite_sql}

    def test_full_refresh(self, project):
        result = run_dbt(["run", "--select", "hive_insert_overwrite", "--full-refresh"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                "select id, msg, cast(date_col as varchar) "
                f"from {project.test_schema}.hive_insert_overwrite",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "2024-01-01"),
            (2, "goodbye", "2024-01-02"),
        ]

    def test_incremental_overwrites_partition(self, project):
        run_dbt(["run", "--select", "hive_insert_overwrite", "--full-refresh"])
        result = run_dbt(["run", "--select", "hive_insert_overwrite"])
        assert result.results[0].status == RunStatus.Success

        # Partition 2024-01-01 untouched, 2024-01-02 overwritten, 2024-01-03 added
        records = sorted(
            project.run_sql(
                "select id, msg, cast(date_col as varchar) "
                f"from {project.test_schema}.hive_insert_overwrite",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "2024-01-01"),
            (3, "overwrite", "2024-01-02"),
            (4, "new", "2024-01-03"),
        ]


# --- compiler error cases ---

models__invalid_build_strategy_sql = """
{{ config(
    materialized = 'table',
    build_strategy = 'invalid_value',
) }}

select 1 as id
"""


class TestInvalidBuildStrategy:
    @pytest.fixture(scope="class")
    def models(self):
        return {"invalid_build_strategy.sql": models__invalid_build_strategy_sql}

    def test_invalid_value_raises_compiler_error(self, project):
        results = run_dbt(
            ["run", "--select", "invalid_build_strategy"],
            expect_pass=False,
        )
        assert len(results) == 1
        assert "Invalid build_strategy" in results[0].message
        assert "invalid_value" in results[0].message


models__python_subquery_sql = """
def model(dbt, spark_session):
    dbt.config(
        materialized='table',
        build_strategy='subquery',
    )
    return spark_session.createDataFrame([(1, 'hello')], ['id', 'msg'])
"""


class TestSubqueryWithPythonModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {"python_subquery.py": models__python_subquery_sql}

    def test_python_model_rejects_subquery(self, project):
        results = run_dbt(
            ["run", "--select", "python_subquery"],
            expect_pass=False,
        )
        assert len(results) == 1
        assert "not supported with Python models" in results[0].message


# --- partitioned variants ---

models__hive_table_partitioned_sql = """
{{ config(
    materialized = 'table',
    table_type = 'hive',
    partitioned_by = ['color'],
    build_strategy = 'subquery',
) }}

select * from (values
    (1, 'hello',   'blue'),
    (2, 'goodbye', 'red'),
    (3, 'hi',      'blue')
) as t(id, msg, color)
"""


class TestHiveTablePartitionedWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hive_table_partitioned.sql": models__hive_table_partitioned_sql}

    def test_create_partitioned_table(self, project):
        result = run_dbt(["run", "--select", "hive_table_partitioned"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.hive_table_partitioned",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "blue"),
            (2, "goodbye", "red"),
            (3, "hi", "blue"),
        ]


models__iceberg_table_partitioned_sql = """
{{ config(
    materialized = 'table',
    table_type = 'iceberg',
    partitioned_by = ['color'],
    build_strategy = 'subquery',
) }}

select * from (values
    (1, 'hello',   'blue'),
    (2, 'goodbye', 'red'),
    (3, 'hi',      'blue')
) as t(id, msg, color)
"""


class TestIcebergTablePartitionedWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_table_partitioned.sql": models__iceberg_table_partitioned_sql}

    def test_create_partitioned_table(self, project):
        result = run_dbt(["run", "--select", "iceberg_table_partitioned"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.iceberg_table_partitioned",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "blue"),
            (2, "goodbye", "red"),
            (3, "hi", "blue"),
        ]


models__hive_append_partitioned_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    table_type = 'hive',
    partitioned_by = ['color'],
    build_strategy = 'subquery',
) }}

{% if not is_incremental() %}
select id, cast(msg as varchar) as msg, cast(color as varchar) as color from (values
    (1, 'hello',   'blue'),
    (2, 'goodbye', 'red')
) as t(id, msg, color)
{% else %}
select id, cast(msg as varchar) as msg, cast(color as varchar) as color from (values
    (3, 'hi',      'blue'),
    (4, 'bye',     'green')
) as t(id, msg, color)
{% endif %}
"""


class TestHiveAppendPartitionedWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hive_append_partitioned.sql": models__hive_append_partitioned_sql}

    def test_incremental_append(self, project):
        run_dbt(["run", "--select", "hive_append_partitioned", "--full-refresh"])
        result = run_dbt(["run", "--select", "hive_append_partitioned"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.hive_append_partitioned",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "blue"),
            (2, "goodbye", "red"),
            (3, "hi", "blue"),
            (4, "bye", "green"),
        ]


models__iceberg_merge_partitioned_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id',
    table_type = 'iceberg',
    partitioned_by = ['color'],
    build_strategy = 'subquery',
) }}

{% if not is_incremental() %}
select id, cast(msg as varchar) as msg, cast(color as varchar) as color from (values
    (1, 'hello',   'blue'),
    (2, 'goodbye', 'red')
) as t(id, msg, color)
{% else %}
select id, cast(msg as varchar) as msg, cast(color as varchar) as color from (values
    (1, 'hey',     'blue'),
    (3, 'anyway',  'purple')
) as t(id, msg, color)
{% endif %}
"""


class TestIcebergMergePartitionedWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_merge_partitioned.sql": models__iceberg_merge_partitioned_sql}

    def test_incremental_merge(self, project):
        run_dbt(["run", "--select", "iceberg_merge_partitioned", "--full-refresh"])
        result = run_dbt(["run", "--select", "iceberg_merge_partitioned"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.iceberg_merge_partitioned",
                fetch="all",
            )
        )
        assert records == [
            (1, "hey", "blue"),
            (2, "goodbye", "red"),
            (3, "anyway", "purple"),
        ]


models__iceberg_append_partitioned_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    table_type = 'iceberg',
    partitioned_by = ['color'],
    build_strategy = 'subquery',
) }}

{% if not is_incremental() %}
select id, cast(msg as varchar) as msg, cast(color as varchar) as color from (values
    (1, 'hello',   'blue'),
    (2, 'goodbye', 'red')
) as t(id, msg, color)
{% else %}
select id, cast(msg as varchar) as msg, cast(color as varchar) as color from (values
    (3, 'hi',      'blue'),
    (4, 'bye',     'green')
) as t(id, msg, color)
{% endif %}
"""


class TestIcebergAppendPartitionedWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_append_partitioned.sql": models__iceberg_append_partitioned_sql}

    def test_incremental_append(self, project):
        run_dbt(["run", "--select", "iceberg_append_partitioned", "--full-refresh"])
        result = run_dbt(["run", "--select", "iceberg_append_partitioned"])
        assert result.results[0].status == RunStatus.Success

        records = sorted(
            project.run_sql(
                f"select id, msg, color from {project.test_schema}.iceberg_append_partitioned",
                fetch="all",
            )
        )
        assert records == [
            (1, "hello", "blue"),
            (2, "goodbye", "red"),
            (3, "hi", "blue"),
            (4, "bye", "green"),
        ]
