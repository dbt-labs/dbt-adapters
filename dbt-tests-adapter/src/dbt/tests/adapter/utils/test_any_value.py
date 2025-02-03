import pytest

from dbt.tests.adapter.utils import base_utils, fixture_any_value


class BaseAnyValue(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_any_value.csv": fixture_any_value.seeds__data_any_value_csv,
            "data_any_value_expected.csv": fixture_any_value.seeds__data_any_value_expected_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_any_value.yml": fixture_any_value.models__test_any_value_yml,
            "test_any_value.sql": self.interpolate_macro_namespace(
                fixture_any_value.models__test_any_value_sql, "any_value"
            ),
        }


class TestAnyValue(BaseAnyValue):
    pass
