import pytest

from dbt.tests.adapter.utils import base_utils, fixture_bool_or


class BaseBoolOr(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_bool_or.csv": fixture_bool_or.seeds__data_bool_or_csv,
            "data_bool_or_expected.csv": fixture_bool_or.seeds__data_bool_or_expected_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_bool_or.yml": fixture_bool_or.models__test_bool_or_yml,
            "test_bool_or.sql": self.interpolate_macro_namespace(
                fixture_bool_or.models__test_bool_or_sql, "bool_or"
            ),
        }


class TestBoolOr(BaseBoolOr):
    pass
