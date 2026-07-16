from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.cross_database.fixtures import (
    CrossDatabaseMixin,
    assert_cross_db_relation_exists,
)


_TABLE_MODEL = """
{{ config(materialized='table') }}
select 1 as id, 'Alice' as name
union all select 2, 'Bob'
"""


class TestCrossDatabaseTable(CrossDatabaseMixin):
    """Test creating and recreating a table in another database."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cross_db_table.sql": _TABLE_MODEL,
        }

    def test_create_and_replace_table(self, project):
        # First run — creates table
        results = run_dbt(["run"])
        assert len(results) == 1
        assert_cross_db_relation_exists(project.adapter, project.test_schema, "cross_db_table")
        # Second run — drops and recreates
        results = run_dbt(["run"])
        assert len(results) == 1
        assert_cross_db_relation_exists(project.adapter, project.test_schema, "cross_db_table")
