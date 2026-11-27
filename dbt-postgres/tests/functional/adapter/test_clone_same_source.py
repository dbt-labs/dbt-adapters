import pytest

from dbt.tests.adapter.dbt_clone import fixtures
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseCloneSameSourceAndTarget


# Postgres can't clone tables, so pretend it can to reach the native clone path.
# Source and target are the same relation, so no clone statement is executed.
can_clone_table_true_macros_sql = """
{% macro can_clone_table() %}
    {{ return(True) }}
{% endmacro %}
"""


class TestCloneSameSourceAndTarget(BaseCloneSameSourceAndTarget):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": fixtures.macros_sql,
            "my_can_clone_tables.sql": can_clone_table_true_macros_sql,
            "infinite_macros.sql": fixtures.infinite_macros_sql,
            "get_schema_name.sql": fixtures.get_schema_name_sql,
        }
