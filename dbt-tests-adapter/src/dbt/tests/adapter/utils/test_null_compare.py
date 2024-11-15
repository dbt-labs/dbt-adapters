import pytest

from dbt.tests.adapter.utils import base_utils, fixture_null_compare
from dbt.tests.util import run_dbt


class BaseMixedNullCompare(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_mixed_null_compare.yml": fixture_null_compare.MODELS__TEST_MIXED_NULL_COMPARE_YML,
            "test_mixed_null_compare.sql": fixture_null_compare.MODELS__TEST_MIXED_NULL_COMPARE_SQL,
        }

    def test_build_assert_equal(self, project):
        run_dbt()
        run_dbt(["test"], expect_pass=False)


class BaseNullCompare(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_null_compare.yml": fixture_null_compare.MODELS__TEST_NULL_COMPARE_YML,
            "test_null_compare.sql": fixture_null_compare.MODELS__TEST_NULL_COMPARE_SQL,
        }


class TestMixedNullCompare(BaseMixedNullCompare):
    pass


class TestNullCompare(BaseNullCompare):
    pass
