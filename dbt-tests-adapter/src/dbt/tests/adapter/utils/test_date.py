import pytest

from dbt.tests.adapter.utils import base_utils, fixture_date


class BaseDate(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date.yml": fixture_date.models__test_date_yml,
            "test_date.sql": self.interpolate_macro_namespace(
                fixture_date.models__test_date_sql, "date"
            ),
        }


class TestDate(BaseDate):
    pass
