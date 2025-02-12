import pytest

from dbt.tests.adapter.utils import base_utils, fixture_equals
from dbt.tests.util import relation_from_name, run_dbt


class BaseEquals(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_equals.csv": fixture_equals.SEEDS__DATA_EQUALS_CSV,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "equal_values.sql": fixture_equals.MODELS__EQUAL_VALUES_SQL,
            "not_equal_values.sql": fixture_equals.MODELS__NOT_EQUAL_VALUES_SQL,
        }

    def test_equal_values(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        # There are 9 cases total; 3 are equal and 6 are not equal

        # 3 are equal
        relation = relation_from_name(project.adapter, "equal_values")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation} where expected = 'same'", fetch="one"
        )
        assert result[0] == 3

        # 6 are not equal
        relation = relation_from_name(project.adapter, "not_equal_values")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation} where expected = 'different'",
            fetch="one",
        )
        assert result[0] == 6


class TestEquals(BaseEquals):
    pass
