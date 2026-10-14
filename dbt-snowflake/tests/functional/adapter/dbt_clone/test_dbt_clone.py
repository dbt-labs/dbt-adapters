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


iceberg_model_yml = """
version: 2
models:
  - name: iceberg_model
    description: "An Iceberg model"
    config:
      persist_docs:
        relation: true
        columns: true
    columns:
      - name: id
        description: "The id"
"""


class BaseSnowflakeCloneIceberg:
    # A tuple, because run_dbt appends --project-dir/--profiles-dir to the list it is given
    clone_args = ("clone", "--state", "state", "--target", "otherschema")

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "iceberg_model.sql": iceberg_table_model_sql,
            "schema.yml": iceberg_model_yml,
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

    def assert_cloned_relation_is_iceberg(self, project, other_schema):
        # Query Snowflake directly (SHOW OBJECTS) so the assertion reflects the real
        # object rather than dbt's relations cache.
        with project.adapter.connection_named("__test"):
            schema_relation = project.adapter.Relation.create(
                database=project.database, schema=other_schema, identifier=""
            ).without_identifier()
            schema_relations = project.adapter.list_relations_without_caching(schema_relation)
            assert len(schema_relations) == 1
            assert schema_relations[0].is_iceberg_format


class TestSnowflakeCloneIcebergTable(BaseSnowflakeCloneIceberg):
    def test_can_clone_iceberg_table(self, project, other_schema):
        project.create_test_schema(other_schema)
        run_and_save_state(project.project_root)

        # persist_docs runs after the clone, so this also covers the Iceberg comment DDL
        results = run_dbt([*self.clone_args])
        assert len(results) == 1

        self.assert_cloned_relation_is_iceberg(project, other_schema)


class TestSnowflakeCloneIcebergTableFullRefresh(BaseSnowflakeCloneIceberg):
    def test_full_refresh_replaces_regular_table(self, project, other_schema):
        project.create_test_schema(other_schema)
        run_and_save_state(project.project_root)

        # A regular table already exists at the clone target; switching it to Iceberg
        # requires a drop because create or replace cannot change the table format.
        project.run_sql(
            f"create table {project.database}.{other_schema}.iceberg_model as select 1 as id"
        )

        results = run_dbt([*self.clone_args, "--full-refresh"])
        assert len(results) == 1

        self.assert_cloned_relation_is_iceberg(project, other_schema)


class TestSnowflakeCloneSameSourceAndTarget(BaseCloneSameSourceAndTarget):
    pass
