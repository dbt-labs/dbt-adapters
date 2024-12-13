import pytest

from dbt.tests.adapter.utils import base_utils, fixture_safe_cast


class BaseSafeCast(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_safe_cast.csv": fixture_safe_cast.seeds__data_safe_cast_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_safe_cast.yml": fixture_safe_cast.models__test_safe_cast_yml,
            "test_safe_cast.sql": self.interpolate_macro_namespace(
                self.interpolate_macro_namespace(
                    fixture_safe_cast.models__test_safe_cast_sql, "safe_cast"
                ),
                "type_string",
            ),
        }


class TestSafeCast(BaseSafeCast):
    pass
