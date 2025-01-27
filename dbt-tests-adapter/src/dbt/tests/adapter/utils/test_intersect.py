import pytest

from dbt.tests.adapter.utils import base_utils, fixture_intersect
from dbt.tests.util import check_relations_equal, run_dbt


class BaseIntersect(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_intersect_a.csv": fixture_intersect.seeds__data_intersect_a_csv,
            "data_intersect_b.csv": fixture_intersect.seeds__data_intersect_b_csv,
            "data_intersect_a_overlap_b.csv": fixture_intersect.seeds__data_intersect_a_overlap_b_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "data_intersect_empty.sql": self.interpolate_macro_namespace(
                fixture_intersect.models__data_intersect_empty_sql, "intersect"
            ),
            "test_intersect_a_overlap_b.sql": self.interpolate_macro_namespace(
                fixture_intersect.models__test_intersect_a_overlap_b_sql, "intersect"
            ),
            "test_intersect_b_overlap_a.sql": self.interpolate_macro_namespace(
                fixture_intersect.models__test_intersect_b_overlap_a_sql, "intersect"
            ),
            "test_intersect_a_overlap_a.sql": self.interpolate_macro_namespace(
                fixture_intersect.models__test_intersect_a_overlap_a_sql, "intersect"
            ),
            "test_intersect_a_overlap_empty.sql": self.interpolate_macro_namespace(
                fixture_intersect.models__test_intersect_a_overlap_empty_sql, "intersect"
            ),
            "test_intersect_empty_overlap_a.sql": self.interpolate_macro_namespace(
                fixture_intersect.models__test_intersect_empty_overlap_a_sql, "intersect"
            ),
            "test_intersect_empty_overlap_empty.sql": self.interpolate_macro_namespace(
                fixture_intersect.models__test_intersect_empty_overlap_empty_sql, "intersect"
            ),
        }

    def test_build_assert_equal(self, project):
        run_dbt(["deps"])
        run_dbt(["build"])

        check_relations_equal(
            project.adapter,
            ["test_intersect_a_overlap_b", "data_intersect_a_overlap_b"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_b_overlap_a", "data_intersect_a_overlap_b"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_a_overlap_a", "data_intersect_a"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_a_overlap_empty", "data_intersect_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_empty_overlap_a", "data_intersect_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_empty_overlap_empty", "data_intersect_empty"],
        )


class TestIntersect(BaseIntersect):
    pass
