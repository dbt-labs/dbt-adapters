from dbt.adapters.base import Column
from dbt.tests.util import run_dbt, run_dbt_and_capture
import pytest

from dbt.adapters.redshift import RedshiftRelation


class ColumnsInRelation:

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1.23 as my_num, 'a' as my_char"}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["run"])

    @pytest.fixture(scope="class")
    def expected_columns(self):
        return []

    def test_columns_in_relation(self, project, expected_columns):
        my_relation = RedshiftRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="my_model",
            type=RedshiftRelation.View,
        )
        with project.adapter.connection_named("_test"):
            actual_columns = project.adapter.get_columns_in_relation(my_relation)
        assert actual_columns == expected_columns


class TestColumnsInRelationBehaviorFlagOff(ColumnsInRelation):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {}}

    @pytest.fixture(scope="class")
    def expected_columns(self):
        # the SDK query returns "varchar" whereas our custom query returns "character varying"
        return [
            Column(column="my_num", dtype="numeric", numeric_precision=3, numeric_scale=2),
            Column(column="my_char", dtype="character varying", char_size=1),
        ]


@pytest.mark.skip(
    """
    There is a discrepancy between our custom query and the get_columns SDK call.
    This test should be skipped for now, but re-enabled once get_columns is implemented.
"""
)
class TestColumnsInRelationBehaviorFlagOn(ColumnsInRelation):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"restrict_direct_pg_catalog_access": True}}

    @pytest.fixture(scope="class")
    def expected_columns(self):
        # the SDK query returns "varchar" whereas our custom query returns "character varying"
        return [
            Column(column="my_num", dtype="numeric", numeric_precision=3, numeric_scale=2),
            Column(column="my_char", dtype="varchar", char_size=1),
        ]


ONE_CHECK = """
select 1 as id
-- {{ adapter.get_columns_in_relation(this) }}
"""


TWO_CHECK = """
select 1 as id
-- {{ adapter.get_columns_in_relation(this) }}
-- {{ adapter.get_columns_in_relation(this) }}
"""


@pytest.mark.skip(
    """
    There is a discrepancy between our custom query and the get_columns SDK call.
    This test should be skipped for now, but re-enabled once get_columns is implemented.
"""
)
class TestBehaviorFlagFiresOnce:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"restrict_direct_pg_catalog_access": False}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"one_check.sql": ONE_CHECK, "two_check.sql": TWO_CHECK}

    def test_warning_fires_once(self, project):
        msg = "https://docs.getdbt.com/reference/global-configs/behavior-changes#redshift-restrict_direct_pg_catalog_access"

        # trigger the evaluation once, we get one warning
        _, logs = run_dbt_and_capture(["--debug", "run", "--models", "one_check"])
        assert logs.count(msg) == 1

        # trigger the evaluation twice, we still get one warning
        _, logs = run_dbt_and_capture(["--debug", "run", "--models", "one_check"])
        assert logs.count(msg) == 1

        # trigger the evaluation three times, across two models, we still get one warning
        _, logs = run_dbt_and_capture(["--debug", "run", "--full-refresh"])
        assert logs.count(msg) == 1

        # note, we still got a warning in the second call, so it's once per invocation
