import pytest

from dbt.tests.adapter.utils import base_utils, fixture_get_powers_of_two


class BaseGetPowersOfTwo(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_get_powers_of_two.yml": fixture_get_powers_of_two.models__test_get_powers_of_two_yml,
            "test_get_powers_of_two.sql": self.interpolate_macro_namespace(
                fixture_get_powers_of_two.models__test_get_powers_of_two_sql, "get_powers_of_two"
            ),
        }


class TestGetPowersOfTwo(BaseGetPowersOfTwo):
    pass
