import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture


_SEED__DATA = """id,value\n1,100\n2,200\n3,300\n"""

_MODEL__BASE_TABLE_SQL = """
{{ config(materialized='table') }}
select * from {{ ref('my_seed') }}
"""

_MODEL__SEMANTIC_VIEW_SQL = """
{{ config(materialized='semantic_view') }}

TABLES(t1 AS {{ ref('base_table') }})
METRICS(t1.total_rows AS COUNT(*))
COMMENT='test semantic view'
COPY GRANTS
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
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])  # load seed for base table
        run_dbt(["run", "--select", "base_table"])  # create the base table

    def test_create_semantic_view(self, project):
        qualified = f"{project.database}.{project.test_schema}.my_semantic_view"

        # Create the semantic view and assert DDL in logs
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "my_semantic_view.sql"])
        assert f"create semantic view {qualified}" in logs.lower()

        # Verify existence via SHOW SEMANTIC VIEWS (preferred for semantic views)
        exists_sql = (
            f"show semantic views like 'MY_SEMANTIC_VIEW' in schema "
            f"{project.database}.{project.test_schema}"
        )
        rows = project.run_sql(exists_sql, fetch="all")
        assert rows and len(rows) >= 1, "semantic view not found via SHOW SEMANTIC VIEWS"
