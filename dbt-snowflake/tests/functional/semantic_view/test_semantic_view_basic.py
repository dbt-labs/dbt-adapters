import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

_SEED__DATA = """id,value\n1,100\n2,200\n3,300\n"""

_MODEL__BASE_TABLE_SQL = """
{{ config(materialized='table') }}
select * from {{ ref('my_seed') }}
"""

_MODEL__SEMANTIC_VIEW_SQL = """
{{ config(materialized='semantic_view') }}
TABLES(t1 AS {{ ref('base_table') }}, t2 as {{ source('seed_sources', 'base_table2') }})
DIMENSIONS(t1.count as value, t2.volume as value)
METRICS(t1.total_rows AS SUM(t1.count), t2.max_volume as max(t2.volume))
COMMENT='test semantic view'
"""

_MODEL__TABLE_REFER_SEMANTIC_VIEW_SQL = """
{{ config(materialized='table') }}
select * from semantic_view({{ ref('my_semantic_view') }} metrics total_rows)
"""

_MODEL__TABLE_REFER_RAW_SEMANTIC_VIEW_SQL = """
{{ config(materialized='table') }}
select * from semantic_view({{ source('seed_sources', 'raw_semantic_view') }} metrics total_rows)
"""

_MODEL__SEMANTIC_VIEW_WITH_COPY_SQL = """
{{ config(materialized='semantic_view') }}
TABLES(t1 AS {{ ref('base_table') }})
DIMENSIONS(t1.count as value)
METRICS(t1.total_rows AS SUM(t1.value))
COMMENT='test semantic view explicit copy grants'
COPY GRANTS
"""

_MODEL__SEMANTIC_VIEW_WITHOUT_COPY_SQL = """
{{ config(materialized='semantic_view') }}
TABLES(t1 AS {{ ref('base_table') }})
DIMENSIONS(t1.count as value)
METRICS(t1.total_rows AS SUM(t1.value))
COMMENT='test semantic view yaml copy grants'
"""

_MODEL__SEMANTIC_VIEW_WITH_EXTENSION_SQL = """
{{ config(materialized='semantic_view') }}
TABLES(t1 AS {{ ref('base_table') }}, t2 as {{ source('seed_sources', 'base_table2') }})
DIMENSIONS(t1.count as value, t2.volume as value)
METRICS(t1.total_rows AS SUM(t1.count), t2.max_volume as max(t2.volume))
with extension (CA = '{"verified_queries":[{"name":"hi", "question": "hello"}]')
"""

_SCHEMA_YML = """
version: 2
models:
  - name: my_semantic_view
    description: "Semantic view description for persist_docs"
  - name: my_semantic_view_without_copy
    config:
      copy_grants: true
  - name: my_semantic_view_with_copy
"""

_SOURCES_YML = """
version: 2
sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: raw_semantic_view
      - name: base_table2
"""


class TestSemanticViewBasic:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": _SEED__DATA}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {
            "base_table.sql": _MODEL__BASE_TABLE_SQL,
            "my_semantic_view.sql": _MODEL__SEMANTIC_VIEW_SQL,
            "my_semantic_view_with_copy.sql": _MODEL__SEMANTIC_VIEW_WITH_COPY_SQL,
            "my_semantic_view_without_copy.sql": _MODEL__SEMANTIC_VIEW_WITHOUT_COPY_SQL,
            "table_refer_to_semantic_view.sql": _MODEL__TABLE_REFER_SEMANTIC_VIEW_SQL,
            "table_refer_to_raw_semantic_view.sql": _MODEL__TABLE_REFER_RAW_SEMANTIC_VIEW_SQL,
            "table_with_ca_extension.sql": _MODEL__SEMANTIC_VIEW_WITH_EXTENSION_SQL,
            "sources.yml": _SOURCES_YML,
            "schema.yml": _SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                    }
                }
            }
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "base_table"])

        database = project.database
        schema = project.test_schema

        # Create a physical source table with different values than base_table
        project.run_sql(
            f"""
            create or replace table {database}.{schema}.base_table2 as
            select id, value + 1000 as value from {database}.{schema}.base_table
        """
        )

        project.run_sql(
            f"""
            create or replace semantic view raw_semantic_view
            tables(t1 as {database}.{schema}.base_table)
            dimensions(t1.count as value)
            metrics(t1.total_rows as sum(t1.value))
        """
        )

    def test_create_semantic_view(self, project):
        database = project.database
        schema = project.test_schema

        qualified = f"{database}.{schema}.my_semantic_view"

        # Create the semantic view and assert DDL in logs
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "my_semantic_view.sql"])
        assert f"create or replace semantic view {qualified}" in logs.lower()

        # Verify existence via SHOW SEMANTIC VIEWS (preferred for semantic views)
        exists_sql = (
            f"show semantic views like 'MY_SEMANTIC_VIEW' in schema " f"{database}.{schema}"
        )
        rows = project.run_sql(exists_sql, fetch="all")
        assert rows and len(rows) >= 1, "semantic view not found via SHOW SEMANTIC VIEWS"

        # verify the select-star result of the table which refer to the semantic view and the semantic view are the same
        table_select_star_sql = f"select sum(value) from {database}.{schema}.BASE_TABLE"
        table_select_result = project.run_sql(table_select_star_sql, fetch="all")
        semantic_view_select_star_sql = f"select * from semantic_view({database}.{schema}.MY_SEMANTIC_VIEW metrics total_rows);"
        semantic_view_select_result = project.run_sql(semantic_view_select_star_sql, fetch="all")
        assert table_select_result[0][0] == semantic_view_select_result[0][0]

    def test_table_refer_to_semantic_view(self, project):
        database = project.database
        schema = project.test_schema

        qualified = f"{database}.{schema}.table_refer_to_semantic_view"

        # Create the semantic view and assert DDL in logs
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--select", "+table_refer_to_semantic_view.sql"]
        )
        assert f"create or replace transient table {qualified}" in logs.lower()

        # verify the select-star result of the table which refer to the semantic view and the semantic view are the same
        table_select_star_sql = f"select * from {database}.{schema}.TABLE_REFER_TO_SEMANTIC_VIEW"
        table_select_result = project.run_sql(table_select_star_sql, fetch="all")
        semantic_view_select_star_sql = f"select * from semantic_view({database}.{schema}.MY_SEMANTIC_VIEW metrics total_rows);"
        semantic_view_select_result = project.run_sql(semantic_view_select_star_sql, fetch="all")
        assert table_select_result[0][0] == semantic_view_select_result[0][0]

    def test_table_refer_to_raw_semantic_view(self, project):
        database = project.database
        schema = project.test_schema

        qualified = f"{database}.{schema}.table_refer_to_raw_semantic_view"

        # Create the semantic view and assert DDL in logs
        _, logs = run_dbt_and_capture(
            [
                "--debug",
                "run",
                "--select",
                "+table_refer_to_raw_semantic_view.sql",
            ]
        )
        assert f"create or replace transient table {qualified}" in logs.lower()
        # verify the select-star result of the table which refer to the semantic view and the semantic view are the same
        table_select_star_sql = (
            f"select * from {database}.{schema}.table_refer_to_raw_semantic_view"
        )
        table_select_result = project.run_sql(table_select_star_sql, fetch="all")
        semantic_view_select_star_sql = f"select * from semantic_view({database}.{schema}.raw_semantic_view metrics total_rows);"
        semantic_view_select_result = project.run_sql(semantic_view_select_star_sql, fetch="all")
        assert table_select_result[0][0] == semantic_view_select_result[0][0]

    def test_semantic_view_comment(self, project):
        database = project.database
        schema = project.test_schema

        qualified = f"{database}.{schema}.my_semantic_view"

        # Create the semantic view and assert DDL in logs
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "my_semantic_view.sql"])
        assert f"comment on semantic view {qualified}" in logs.lower()
        # Verify existence via SHOW SEMANTIC VIEWS (preferred for semantic views)
        exists_sql = (
            f"show semantic views like 'MY_SEMANTIC_VIEW' in schema " f"{database}.{schema}"
        )
        rows = project.run_sql(exists_sql, fetch="all")
        assert "semantic view description for persist_docs" in rows[0][4].lower()

    def test_semantic_view_copy_grants_logic(self, project):
        database = project.database
        schema = project.test_schema

        # Case 1: SQL already ends with COPY GRANTS -> should not rely on yaml, appears in DDL
        qualified_with = f"{database}.{schema}.my_semantic_view_with_copy"
        _, logs_with = run_dbt_and_capture(
            ["--debug", "run", "--select", "my_semantic_view_with_copy.sql"]
        )
        assert f"create or replace semantic view {qualified_with}" in logs_with.lower()
        assert "copy grants" in logs_with.lower()

        # Case 2: SQL does not end with COPY GRANTS but YAML sets copy_grants: true -> appended
        qualified_without = f"{database}.{schema}.my_semantic_view_without_copy"
        _, logs_without = run_dbt_and_capture(
            ["--debug", "run", "--select", "my_semantic_view_without_copy.sql"]
        )
        assert f"create or replace semantic view {qualified_without}" in logs_without.lower()
        assert "copy grants" in logs_without.lower()

        # Case 3: SQL does not end with COPY GRANTS and YAML does not set copy_grants: true -> no copy grants
        qualified_without_sql_or_yaml = f"{database}.{schema}.my_semantic_view"
        _, logs_without_sql_or_yaml = run_dbt_and_capture(
            ["--debug", "run", "--select", "my_semantic_view.sql"]
        )
        assert (
            f"create or replace semantic view {qualified_without_sql_or_yaml}"
            in logs_without.lower()
        )
        assert "copy grants" not in logs_without_sql_or_yaml.lower()

    def test_semantic_view_with_ca_extension(self, project):
        database = project.database
        schema = project.test_schema

        qualified = f"{database}.{schema}.table_with_ca_extension"

        # Create the semantic view and assert DDL in logs
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--select", "table_with_ca_extension.sql"]
        )
        assert f"create or replace semantic view {qualified}" in logs.lower()

        desc_semantic_view_sql = f"describe semantic view {qualified}"
        desc_remantic_view_res = project.run_sql(desc_semantic_view_sql, fetch="all")
        for row in desc_remantic_view_res:
            if row[0].lower() == "extension":
                assert row[1].lower() == "ca"
                assert row[4].lower() == '{"verified_queries":[{"name":"hi", "question": "hello"}]'
                break
        else:
            assert False, "Extension not found"
