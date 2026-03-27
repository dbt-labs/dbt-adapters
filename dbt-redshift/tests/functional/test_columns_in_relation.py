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


class TestColumnsInRelationDatasharingOff(ColumnsInRelation):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_columns_in_relation_datasharing_off",
        }

    @pytest.fixture(scope="class")
    def expected_columns(self):
        # the SDK query returns "varchar" whereas our custom query returns "character varying"
        return [
            Column(column="my_num", dtype="numeric", numeric_precision=3, numeric_scale=2),
            Column(column="my_char", dtype="character varying", char_size=1),
        ]


class TestColumnsInRelationDatasharingOn(ColumnsInRelation):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_columns_in_relation_datasharing_on",
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["datasharing"] = True
        return outputs

    @pytest.fixture(scope="class")
    def expected_columns(self):
        return [
            Column(column="my_num", dtype="numeric", numeric_precision=3, numeric_scale=2),
            Column(column="my_char", dtype="character varying", char_size=1),
        ]
