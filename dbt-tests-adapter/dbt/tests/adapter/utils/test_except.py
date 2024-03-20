import pytest

from dbt.tests.adapter.utils import base_utils, fixture_except
from dbt.tests.util import check_relations_equal, run_dbt


class BaseExcept(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_except_a.csv": fixture_except.seeds__data_except_a_csv,
            "data_except_b.csv": fixture_except.seeds__data_except_b_csv,
            "data_except_a_minus_b.csv": fixture_except.seeds__data_except_a_minus_b_csv,
            "data_except_b_minus_a.csv": fixture_except.seeds__data_except_b_minus_a_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "data_except_empty.sql": self.interpolate_macro_namespace(
                fixture_except.models__data_except_empty_sql, "except"
            ),
            "test_except_a_minus_b.sql": self.interpolate_macro_namespace(
                fixture_except.models__test_except_a_minus_b_sql, "except"
            ),
            "test_except_b_minus_a.sql": self.interpolate_macro_namespace(
                fixture_except.models__test_except_b_minus_a_sql, "except"
            ),
            "test_except_a_minus_a.sql": self.interpolate_macro_namespace(
                fixture_except.models__test_except_a_minus_a_sql, "except"
            ),
            "test_except_a_minus_empty.sql": self.interpolate_macro_namespace(
                fixture_except.models__test_except_a_minus_empty_sql, "except"
            ),
            "test_except_empty_minus_a.sql": self.interpolate_macro_namespace(
                fixture_except.models__test_except_empty_minus_a_sql, "except"
            ),
            "test_except_empty_minus_empty.sql": self.interpolate_macro_namespace(
                fixture_except.models__test_except_empty_minus_empty_sql, "except"
            ),
        }

    def test_build_assert_equal(self, project):
        run_dbt(["deps"])
        run_dbt(["build"])

        check_relations_equal(
            project.adapter,
            ["test_except_a_minus_b", "data_except_a_minus_b"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_b_minus_a", "data_except_b_minus_a"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_a_minus_a", "data_except_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_a_minus_empty", "data_except_a"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_empty_minus_a", "data_except_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_empty_minus_empty", "data_except_empty"],
        )


class TestExcept(BaseExcept):
    pass
