import pytest

from dbt.tests.util import (
    check_relations_equal,
    check_table_does_not_exist,
    rm_file,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)
import models
import seeds


class BaseConcurrency:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds.seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid.sql": models.invalid_sql,
            "table_a.sql": models.table_a_sql,
            "table_b.sql": models.table_b_sql,
            "view_model.sql": models.view_model_sql,
            "dep.sql": models.dep_sql,
            "view_with_conflicting_cascade.sql": models.view_with_conflicting_cascade_sql,
            "skip.sql": models.skip_sql,
        }

    @staticmethod
    def check_results(adapter, results):
        assert len(results) == 7
        check_relations_equal(adapter, ["seed", "view_model"])
        check_relations_equal(adapter, ["seed", "dep"])
        check_relations_equal(adapter, ["seed", "table_a"])
        check_relations_equal(adapter, ["seed", "table_b"])
        check_table_does_not_exist(adapter, "invalid")
        check_table_does_not_exist(adapter, "skip")

    def test_concurrency(self, project):
        run_dbt(["seed", "--select", "seed"])
        results = run_dbt(["run"], expect_pass=False)
        self.check_results(project.adapter, results)

        rm_file(project.project_root, "seeds", "seed.csv")
        write_file(seeds.update_csv, project.project_root, "seeds", "seed.csv")

        results, output = run_dbt_and_capture(["run"], expect_pass=False)
        self.check_results(project.adapter, results)
        assert "PASS=5 WARN=0 ERROR=1 SKIP=1 TOTAL=7" in output
