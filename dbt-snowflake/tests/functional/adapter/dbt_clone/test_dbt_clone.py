import pytest
import shutil
import os
from copy import deepcopy
from dbt.tests.util import run_dbt
from dbt.tests.adapter.dbt_clone.test_dbt_clone import (
    BaseClonePossible,
    BaseCloneSameSourceAndTarget,
)


class TestSnowflakeClonePossible(BaseClonePossible):
    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_SEEDS"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass


def copy_state(project_root):
    state_path = os.path.join(project_root, "state")
    if not os.path.exists(state_path):
        os.makedirs(state_path)
    shutil.copyfile(
        os.path.join(project_root, "target", "manifest.json"),
        os.path.join(project_root, "state", "manifest.json"),
    )


def run_and_save_state(project_root):
    results = run_dbt(["run"])
    assert len(results) == 1
    copy_state(project_root)


table_model_1_sql = """
    {{ config(
        materialized='table',
        transient=true,
    ) }}

    select 1 as fun
    """


class TestSnowflakeCloneTrainsentTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model_1_sql,
        }

    @pytest.fixture(scope="class")
    def other_schema(self, unique_schema):
        return unique_schema + "_other"

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
        outputs = {"default": dbt_profile_target, "otherschema": deepcopy(dbt_profile_target)}
        outputs["default"]["schema"] = unique_schema
        outputs["otherschema"]["schema"] = other_schema
        return {"test": {"outputs": outputs, "target": "default"}}

    def test_can_clone_transient_table(self, project, other_schema):
        project.create_test_schema(other_schema)
        run_and_save_state(project.project_root)

        clone_args = [
            "clone",
            "--state",
            "state",
            "--target",
            "otherschema",
        ]

        results = run_dbt(clone_args)
        assert len(results) == 1


ICEBERG_EXTERNAL_VOLUME = os.getenv("SNOWFLAKE_TEST_ICEBERG_EXTERNAL_VOLUME", "s3_iceberg_snow")

iceberg_table_model_sql = f"""
    {{{{ config(
        materialized='table',
        table_format='iceberg',
        external_volume='{ICEBERG_EXTERNAL_VOLUME}',
        base_location_subpath='clone_test',
    ) }}}}

    select 1 as id
    """


class TestSnowflakeCloneIcebergTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "iceberg_model.sql": iceberg_table_model_sql,
        }

    @pytest.fixture(scope="class")
    def other_schema(self, unique_schema):
        return unique_schema + "_other"

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
        outputs = {"default": dbt_profile_target, "otherschema": deepcopy(dbt_profile_target)}
        outputs["default"]["schema"] = unique_schema
        outputs["otherschema"]["schema"] = other_schema
        return {"test": {"outputs": outputs, "target": "default"}}

    def test_can_clone_iceberg_table(self, project, other_schema):
        project.create_test_schema(other_schema)
        run_and_save_state(project.project_root)

        clone_args = [
            "clone",
            "--state",
            "state",
            "--target",
            "otherschema",
        ]

        results = run_dbt(clone_args)
        assert len(results) == 1

        # Verify the cloned relation is also an Iceberg table
        with project.adapter.connection_named("__test"):
            schema_relations = project.adapter.list_relations(
                database=project.database, schema=other_schema
            )
            assert len(schema_relations) == 1
            assert schema_relations[0].is_iceberg_format


class TestSnowflakeCloneSameSourceAndTarget(BaseCloneSameSourceAndTarget):
    pass
