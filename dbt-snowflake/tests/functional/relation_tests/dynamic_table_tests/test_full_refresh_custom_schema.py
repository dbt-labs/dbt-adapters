import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

from tests.functional.relation_tests.dynamic_table_tests import models
from tests.functional.utils import query_relation_type


class TestDynamicTableFullRefreshCustomSchema:

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {
            "simple_model.sql": models.SIMPLE_MODEL,
            "custom_schema_dynamic_table.sql": models.DYNAMIC_TABLE_CUSTOM_SCHEMA,
        }

    def test_dynamic_table_full_refresh_with_custom_schema(self, project):
        """Test that dynamic table full refresh works with custom schema configuration."""
        # Initial run to create the dynamic table
        run_dbt(["seed"])
        run_dbt(["run"])

        # Run full refresh - this was failing before the fix
        run_dbt(["run", "--full-refresh"])
        run_dbt(["run", "--full-refresh"])
