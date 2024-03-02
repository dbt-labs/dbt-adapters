import pytest

from dbt.tests.adapter.utils import base_utils, fixture_length


class BaseLength(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_length.csv": fixture_length.seeds__data_length_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_length.yml": fixture_length.models__test_length_yml,
            "test_length.sql": self.interpolate_macro_namespace(
                fixture_length.models__test_length_sql, "length"
            ),
        }


class TestLength(BaseLength):
    pass
