import pytest

from dbt.tests.adapter.utils import base_utils, fixture_get_intervals_between


class BaseGetIntervalsBetween(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_get_intervals_between.yml": fixture_get_intervals_between.models__test_get_intervals_between_yml,
            "test_get_intervals_between.sql": self.interpolate_macro_namespace(
                fixture_get_intervals_between.models__test_get_intervals_between_sql,
                "get_intervals_between",
            ),
        }


class TestGetIntervalsBetween(BaseGetIntervalsBetween):
    pass
