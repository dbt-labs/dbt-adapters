import pytest

from dbt.tests.adapter.utils import base_utils, fixture_generate_series


class BaseGenerateSeries(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_generate_series.yml": fixture_generate_series.models__test_generate_series_yml,
            "test_generate_series.sql": self.interpolate_macro_namespace(
                fixture_generate_series.models__test_generate_series_sql, "generate_series"
            ),
        }


class TestGenerateSeries(BaseGenerateSeries):
    pass
