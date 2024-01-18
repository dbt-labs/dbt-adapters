import pytest

from dbt.tests.adapter.utils import base_utils, fixture_last_day


class BaseLastDay(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_last_day.csv": fixture_last_day.seeds__data_last_day_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_last_day.yml": fixture_last_day.models__test_last_day_yml,
            "test_last_day.sql": self.interpolate_macro_namespace(
                fixture_last_day.models__test_last_day_sql, "last_day"
            ),
        }


class TestLastDay(BaseLastDay):
    pass
