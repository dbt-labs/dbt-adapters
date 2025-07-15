import pytest

from dbt.tests.adapter.utils import base_utils, fixture_cast


class BaseCast(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_cast.csv": fixture_cast.seeds__data_cast_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_cast.yml": fixture_cast.models__test_cast_yml,
            "test_cast.sql": self.interpolate_macro_namespace(
                self.interpolate_macro_namespace(fixture_cast.models__test_cast_sql, "cast"),
                "type_string",
            ),
        }


class TestCast(BaseCast):
    pass
