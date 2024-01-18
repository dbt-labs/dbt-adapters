import pytest

import fixtures
from dbt.tests.util import relation_from_name, run_dbt


class BaseTestEmpty:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_source.csv": fixtures.raw_source_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_input.sql": fixtures.model_input_sql,
            "ephemeral_model_input.sql": fixtures.ephemeral_model_input_sql,
            "model.sql": fixtures.model_sql,
            "sources.yml": fixtures.schema_sources_yml,
        }

    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == expected_row_count

    def test_run_with_empty(self, project):
        # create source from seed
        run_dbt(["seed"])

        # run without empty - 3 expected rows in output - 1 from each input
        run_dbt(["run"])
        self.assert_row_count(project, "model", 3)

        # run with empty - 0 expected rows in output
        run_dbt(["run", "--empty"])
        self.assert_row_count(project, "model", 0)
