import pytest

from dbt.tests.adapter.utils import base_array_utils, fixture_array_construct


class BaseArrayConstruct(base_array_utils.BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": fixture_array_construct.models__array_construct_actual_sql,
            "expected.sql": fixture_array_construct.models__array_construct_expected_sql,
        }


class TestArrayConstruct(BaseArrayConstruct):
    pass
