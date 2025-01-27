import pytest

from dbt.tests.adapter.utils import base_utils, fixture_position


class BasePosition(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_position.csv": fixture_position.seeds__data_position_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_position.yml": fixture_position.models__test_position_yml,
            "test_position.sql": self.interpolate_macro_namespace(
                fixture_position.models__test_position_sql, "position"
            ),
        }


class TestPosition(BasePosition):
    pass
