import pytest

from dbt.tests.adapter.functions.test_tvfs import BasicSQLTVF
from tests.functional.functions import files


class TestBigQuerySQLTVF(BasicSQLTVF):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "generate_double_series.sql": files.MY_TVF_SQL,
            "generate_double_series.yml": files.MY_TVF_YML,
        }
