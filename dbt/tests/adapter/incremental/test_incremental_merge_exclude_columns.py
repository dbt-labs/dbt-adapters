from collections import namedtuple

import pytest

from dbt.tests.util import check_relations_equal, run_dbt
import models
import seeds


ResultHolder = namedtuple(
    "ResultHolder",
    [
        "seed_count",
        "model_count",
        "seed_rows",
        "inc_test_model_count",
        "relation",
    ],
)


class MergeExcludeColumns:
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_exclude_columns.sql": models.merge_exclude_columns_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_merge_exclude_columns.csv": seeds.expected_merge_exclude_columns_csv}

    def update_incremental_model(self, incremental_model):
        """update incremental model after the seed table has been updated"""
        model_result_set = run_dbt(["run", "--select", incremental_model])
        return len(model_result_set)

    def get_test_fields(self, project, seed, incremental_model, update_sql_file):
        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))

        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = "select * from {}.{}".format(project.test_schema, seed)

        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

        return ResultHolder(seed_count, model_count, seed_rows, inc_test_model_count, relation)

    def check_scenario_correctness(self, expected_fields, test_case_fields, project):
        """Invoke assertions to verify correct build functionality"""
        # 1. test seed(s) should build afresh
        assert expected_fields.seed_count == test_case_fields.seed_count
        # 2. test model(s) should build afresh
        assert expected_fields.model_count == test_case_fields.model_count
        # 3. seeds should have intended row counts post update
        assert expected_fields.seed_rows == test_case_fields.seed_rows
        # 4. incremental test model(s) should be updated
        assert expected_fields.inc_test_model_count == test_case_fields.inc_test_model_count
        # 5. result table should match intended result set (itself a relation)
        check_relations_equal(
            project.adapter, [expected_fields.relation, test_case_fields.relation]
        )

    def test__merge_exclude_columns(self, project):
        """seed should match model after two incremental runs"""

        expected_fields = ResultHolder(
            seed_count=1,
            model_count=1,
            inc_test_model_count=1,
            seed_rows=3,
            relation="expected_merge_exclude_columns",
        )

        test_case_fields = self.get_test_fields(
            project,
            seed="expected_merge_exclude_columns",
            incremental_model="merge_exclude_columns",
            update_sql_file=None,
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)
