"""
Functional tests for Data Catalog Views (Multi Dialect Views).

Requires a live Athena environment with Lake Formation permissions
configured to allow creating protected multi dialect views.
"""

import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt


data_catalog_view_model_sql = """
{{ config(
    materialized='view',
    is_data_catalog_view=True
) }}

select 1 as id, 'test' as name
"""

standard_view_model_sql = """
{{ config(
    materialized='view'
) }}

select 1 as id, 'test' as name
"""


@pytest.mark.skip(reason="requires Lake Formation permissions not available in CI")
class TestDataCatalogView:
    """Verify that is_data_catalog_view=True produces a protected multi dialect view."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "data_catalog_view.sql": data_catalog_view_model_sql,
            "standard_view.sql": standard_view_model_sql,
        }

    def test_data_catalog_view_creation(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        for result in results:
            assert result.status == RunStatus.Success

    def test_data_catalog_view_idempotent(self, project):
        first_run = run_dbt(["run"])
        assert len(first_run) == 2

        second_run = run_dbt(["run"])
        assert len(second_run) == 2
        for result in second_run:
            assert result.status == RunStatus.Success
