import pytest

from dbt.tests.adapter.utils import base_utils, fixture_date_spine


class BaseDateSpine(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_spine.yml": fixture_date_spine.models__test_date_spine_yml,
            "test_date_spine.sql": self.interpolate_macro_namespace(
                fixture_date_spine.models__test_date_spine_sql, "date_spine"
            ),
        }


class TestDateSpine(BaseDateSpine):
    pass
