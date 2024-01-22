import pytest

from dbt.tests.adapter.utils import base_array_utils, fixture_array_append


class BaseArrayAppend(base_array_utils.BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": fixture_array_append.models__array_append_actual_sql,
            "expected.sql": fixture_array_append.models__array_append_expected_sql,
        }


class TestArrayAppend(BaseArrayAppend):
    pass
