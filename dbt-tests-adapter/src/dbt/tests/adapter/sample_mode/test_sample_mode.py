import os
from pprint import pformat
from typing import Dict
from unittest import mock
import freezegun
import pytest

from dbt.tests.adapter.sample_mode import fixtures
from dbt.tests.util import relation_from_name, run_dbt


class BaseSampleModeTest:
    @pytest.fixture(scope="class")
    def model_that_samples_input_sql(self) -> str:
        """
        This is the SQL that references the input_model which can be sampled
        """
        return fixtures.model_that_samples_input_sql

    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        """
        This is the SQL that defines the input model to be sampled, including any {{ config(..) }}.
        event_time is a required configuration of this input
        """
        return fixtures.input_model_sql

    @pytest.fixture(scope="class")
    def models(self, model_that_samples_input_sql: str, input_model_sql: str) -> Dict[str, str]:
        return {
            "input_model.sql": input_model_sql,
            "model_that_samples_input_sql.sql": model_that_samples_input_sql,
        }

    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select * from {relation}", fetch="all")

        assert len(result) == expected_row_count, f"{relation_name}:{pformat(result)}"

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_SAMPLE_MODE": "True"})
    @freezegun.freeze_time("2025-01-03T02:03:0Z")
    def test_sample_mode(self, project) -> None:
        _ = run_dbt(["run"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_samples_input_sql",
            expected_row_count=3,
        )

        _ = run_dbt(["run", "--sample=1 day"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_samples_input_sql",
            expected_row_count=2,
        )
