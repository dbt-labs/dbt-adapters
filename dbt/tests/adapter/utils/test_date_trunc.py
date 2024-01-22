import pytest

from dbt.tests.adapter.utils import base_utils, fixture_date_trunc


class BaseDateTrunc(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_date_trunc.csv": fixture_date_trunc.seeds__data_date_trunc_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": fixture_date_trunc.models__test_date_trunc_yml,
            "test_date_trunc.sql": self.interpolate_macro_namespace(
                fixture_date_trunc.models__test_date_trunc_sql, "date_trunc"
            ),
        }


class TestDateTrunc(BaseDateTrunc):
    pass
