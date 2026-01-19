from copy import deepcopy

import pytest

from dbt.tests.adapter.dbt_clone import fixtures
from dbt.tests.adapter.dbt_clone.test_dbt_clone import (
    BaseClone,
    BaseClonePossible,
    BaseCloneSameSourceAndTarget,
    BaseCloneNotPossible,
)
from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.util import run_dbt, run_dbt_and_capture, get_connection


class TestBigQueryClonePossible(BaseClonePossible):
    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_seeds"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass


class TestBigQueryCloneSameSourceAndTarget(BaseCloneSameSourceAndTarget):
    pass


class TestBigQueryCloneNotPossible(BaseCloneNotPossible):
    pass


class TestBigQueryCloneDropsOnPartitionMismatch(BaseClone):
    """Test that BigQuery drops target table when partition spec differs from source."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "partitioned_model.sql": fixtures.partitioned_table_model_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(scope="class")
    def other_schema(self, unique_schema):
        return unique_schema + "_other"

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
        outputs = {"default": dbt_profile_target, "otherschema": deepcopy(dbt_profile_target)}
        outputs["default"]["schema"] = unique_schema
        outputs["otherschema"]["schema"] = other_schema
        return {"test": {"outputs": outputs, "target": "default"}}

    @pytest.fixture(autouse=True)
    def clean_up(self, project, other_schema):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=other_schema
            )
            project.adapter.drop_schema(relation)

    def test_clone_drops_on_partition_mismatch(self, project, unique_schema, other_schema):
        """When target has different partition spec, it should be dropped and recreated."""
        project.create_test_schema(other_schema)

        # Run partitioned model
        results = run_dbt(["run"])
        assert len(results) == 1

        # Save state
        self.copy_state(project.project_root)

        # Create table with DIFFERENT partition in target schema (using MONTH instead of DAY)
        project.run_sql(
            f"""
            CREATE OR REPLACE TABLE `{project.database}`.`{other_schema}`.`partitioned_model`
            PARTITION BY DATE_TRUNC(created_at, MONTH)
            AS SELECT current_date() as created_at, 1 as id
        """
        )

        # Clone with full-refresh - should drop due to partition mismatch
        clone_args = ["clone", "--state", "state", "--target", "otherschema", "--full-refresh"]
        results, output = run_dbt_and_capture(clone_args)

        # Verify drop message appears due to partition mismatch
        assert "Dropping relation" in output
        assert "partition/clustering spec differs from source" in output


class BaseGrantsBigQueryClone(BaseGrants):
    """Mixin for BigQuery grant privilege name overrides."""

    def privilege_grantee_name_overrides(self):
        return {
            "select": "roles/bigquery.dataViewer",
            "insert": "roles/bigquery.dataEditor",
        }


class TestBigQueryClonePreservesGrants(BaseGrantsBigQueryClone, BaseClone):
    """Test that grants are preserved when cloning tables."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": fixtures.table_model_with_grants_sql,
            "schema.yml": self.interpolate_name_overrides(
                fixtures.clone_model_with_grants_schema_yml
            ),
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(scope="class")
    def other_schema(self, unique_schema):
        return unique_schema + "_other"

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
        outputs = {"default": dbt_profile_target, "otherschema": deepcopy(dbt_profile_target)}
        outputs["default"]["schema"] = unique_schema
        outputs["otherschema"]["schema"] = other_schema
        return {"test": {"outputs": outputs, "target": "default"}}

    @pytest.fixture(autouse=True)
    def clean_up(self, project, other_schema):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=other_schema
            )
            project.adapter.drop_schema(relation)

    def get_grants_on_relation_in_schema(self, project, relation_name, schema):
        """Get grants on a relation in a specific schema."""
        adapter = project.adapter
        relation = adapter.Relation.create(
            database=project.database,
            schema=schema,
            identifier=relation_name,
        )
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            _, grant_table = adapter.execute(show_grant_sql, fetch=True)
            actual_grants = adapter.standardize_grants_dict(grant_table)
        return actual_grants

    def test_clone_preserves_grants(self, project, unique_schema, other_schema, get_test_users):
        """Grants configured on model should be applied to cloned relation."""
        if len(get_test_users) == 0:
            pytest.skip("DBT_TEST_USER_1 environment variable not set")

        project.create_test_schema(other_schema)
        select_privilege = self.privilege_grantee_name_overrides()["select"]

        # Run model with grants
        results = run_dbt(["run"])
        assert len(results) == 1

        # Verify grants on source
        expected_grants = {select_privilege: [get_test_users[0]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected_grants)

        # Save state
        self.copy_state(project.project_root)

        # Clone to other schema
        clone_args = ["clone", "--state", "state", "--target", "otherschema", "--full-refresh"]
        results = run_dbt(clone_args)
        assert len(results) == 1

        # Verify grants on cloned relation in the other schema
        actual_grants = self.get_grants_on_relation_in_schema(project, "my_model", other_schema)
        # Check that the expected privilege exists on the cloned relation
        assert select_privilege in actual_grants
        assert get_test_users[0] in actual_grants[select_privilege]
