import pytest

from dbt.tests.adapter.utils import base_array_utils, fixture_array_concat


class BaseArrayConcat(base_array_utils.BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": fixture_array_concat.models__array_concat_actual_sql,
            "expected.sql": fixture_array_concat.models__array_concat_expected_sql,
        }


class TestArrayConcat(BaseArrayConcat):
    pass
