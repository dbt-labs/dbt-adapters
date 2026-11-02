import os
import re
import shutil
from copy import deepcopy

import pytest
from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.util import relation_from_name, run_dbt

# Snowflake BCR-2433 (bundle 2026_08) makes a bare CREATE OR REPLACE copy grants from the replaced
# object. dbt must keep its no-copy default, so it emits IGNORE GRANTS unless copy_grants is true.
# These tests assert the resulting grants after a re-run, so they hold with the bundle enabled
# or disabled; run them against both states.

_MODELS = {
    "table_unset.sql": ("table", ""),
    "table_false.sql": ("table", "copy_grants = false,"),
    "table_copy.sql": ("table", "copy_grants = true,"),
    "view_unset.sql": ("view", ""),
    "view_false.sql": ("view", "copy_grants = false,"),
    "view_copy.sql": ("view", "copy_grants = true,"),
    "incremental_unset.sql": ("incremental", ""),
    "incremental_copy.sql": ("incremental", "copy_grants = true,"),
}

_MODEL_TEMPLATE = """
{{{{ config(
    materialized = '{materialized}',
    {extra}
) }}}}

select 1 as id
"""


def _models():
    return {
        name: _MODEL_TEMPLATE.format(materialized=materialized, extra=extra)
        for name, (materialized, extra) in _MODELS.items()
    }


def _cleaned_ddl(model_file: str) -> str:
    with open(f"target/run/test/models/{model_file}", "r") as ddl_file:
        return re.sub(r"\s+", " ", ddl_file.read()).lower()


class TestReplaceGrants(BaseGrants):
    @pytest.fixture(scope="class")
    def models(self):
        return _models()

    @pytest.fixture(scope="class")
    def test_user(self, get_test_users):
        if not get_test_users:
            pytest.skip("DBT_TEST_USER_1 is not set")
        return get_test_users[0]

    def _grant_out_of_band(self, project, user):
        for name, (materialized, _) in _MODELS.items():
            model = name.removesuffix(".sql")
            relation = relation_from_name(project.adapter, model)
            object_type = "view" if materialized == "view" else "table"
            project.run_sql(f"grant select on {object_type} {relation} to role {user}")

    def _has_select(self, project, model, user) -> bool:
        grants = self.get_grants_on_relation(project, model)
        return user.lower() in [grantee.lower() for grantee in grants.get("select", [])]

    def test_replace_ignores_grants_unless_copy_grants(self, project, test_user):
        # first run creates the relations, then grant to a role that dbt does not manage
        run_dbt(["run"])
        self._grant_out_of_band(project, test_user)
        for model in ("table_unset", "view_unset", "incremental_unset"):
            assert self._has_select(project, model, test_user)

        # tables and views are replaced on every run, incremental only on full refresh
        run_dbt(["run", "--full-refresh"])

        for model in (
            "table_unset",
            "table_false",
            "view_unset",
            "view_false",
            "incremental_unset",
        ):
            assert not self._has_select(project, model, test_user), model
            assert "ignore grants" in _cleaned_ddl(f"{model}.sql"), model

        for model in ("table_copy", "view_copy", "incremental_copy"):
            assert self._has_select(project, model, test_user), model
            ddl = _cleaned_ddl(f"{model}.sql")
            assert "copy grants" in ddl, model
            assert "ignore grants" not in ddl, model


def _copy_state(project_root):
    state_path = os.path.join(project_root, "state")
    os.makedirs(state_path, exist_ok=True)
    shutil.copyfile(
        os.path.join(project_root, "target", "manifest.json"),
        os.path.join(state_path, "manifest.json"),
    )


class TestCloneReplaceGrants(BaseGrants):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "clone_unset.sql": _MODEL_TEMPLATE.format(materialized="table", extra=""),
            "clone_copy.sql": _MODEL_TEMPLATE.format(
                materialized="table", extra="copy_grants = true,"
            ),
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

    def test_clone_replace_ignores_grants_unless_copy_grants(
        self, project, other_schema, get_test_users
    ):
        if not get_test_users:
            pytest.skip("DBT_TEST_USER_1 is not set")
        user = get_test_users[0]
        clone_args = ["clone", "--state", "state", "--target", "otherschema", "--full-refresh"]

        project.create_test_schema(other_schema)
        run_dbt(["run"])
        _copy_state(project.project_root)
        run_dbt(clone_args)

        clones = {
            model: project.adapter.Relation.create(
                database=project.database, schema=other_schema, identifier=model
            )
            for model in ("clone_unset", "clone_copy")
        }
        for relation in clones.values():
            project.run_sql(f"grant select on table {relation} to role {user}")

        # replacing an existing clone target must not silently keep its grants
        run_dbt(clone_args)

        def has_select(relation):
            with project.adapter.connection_named("__test"):
                _, table = project.adapter.execute(
                    project.adapter.execute_macro(
                        "get_show_grant_sql", kwargs={"relation": relation}
                    ),
                    fetch=True,
                )
                grants = project.adapter.standardize_grants_dict(table)
            return user.lower() in [g.lower() for g in grants.get("select", [])]

        assert not has_select(clones["clone_unset"])
        assert has_select(clones["clone_copy"])
