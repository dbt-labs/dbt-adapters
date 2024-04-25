import pytest

from dbt.tests.adapter.utils import base_utils, fixture_right


class BaseRight(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_right.csv": fixture_right.seeds__data_right_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_right.yml": fixture_right.models__test_right_yml,
            "test_right.sql": self.interpolate_macro_namespace(
                fixture_right.models__test_right_sql, "right"
            ),
        }


class TestRight(BaseRight):
    pass
