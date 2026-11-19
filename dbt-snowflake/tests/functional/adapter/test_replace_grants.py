import os
import re
import shutil
from copy import deepcopy

import pytest
from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.util import relation_from_name, run_dbt, write_file

# A bare CREATE OR REPLACE keeps grants on some accounts (BCR-2433 preview) and drops them on
# others, so these tests only assert grants where dbt emits an explicit clause.

_MODELS = {
    "table_unset.sql": ("table", ""),
    "table_false.sql": ("table", "copy_grants = false,"),
    "table_copy.sql": ("table", "copy_grants = true,"),
    "view_unset.sql": ("view", ""),
    "view_false.sql": ("view", "copy_grants = false,"),
    "view_copy.sql": ("view", "copy_grants = true,"),
    "incremental_unset.sql": ("incremental", ""),
    "incremental_false.sql": ("incremental", "copy_grants = false,"),
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


def _has_select(grants: dict, user: str) -> bool:
    # Snowflake returns privilege names uppercased (e.g. "SELECT")
    grantees = [
        grantee
        for privilege, users in grants.items()
        if privilege.lower() == "select"
        for grantee in users
    ]
    return user.lower() in [grantee.lower() for grantee in grantees]


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
        return _has_select(self.get_grants_on_relation(project, model), user)

    def test_replace_grants_clause_follows_copy_grants(self, project, test_user):
        # first run creates the relations, then grant to a role that dbt does not manage
        run_dbt(["run"])
        self._grant_out_of_band(project, test_user)
        for model in ("table_false", "view_false", "incremental_false"):
            assert self._has_select(project, model, test_user)

        # tables and views are replaced on every run, incremental only on full refresh
        run_dbt(["run", "--full-refresh"])

        for model in ("table_unset", "view_unset", "incremental_unset"):
            ddl = _cleaned_ddl(f"{model}.sql")
            assert "create or replace" in ddl, model
            assert "copy grants" not in ddl, model
            assert "ignore grants" not in ddl, model

        for model in ("table_false", "view_false", "incremental_false"):
            assert not self._has_select(project, model, test_user), model
            ddl = _cleaned_ddl(f"{model}.sql")
            assert "ignore grants" in ddl, model
            assert "copy grants" not in ddl, model

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
            "clone_false.sql": _MODEL_TEMPLATE.format(
                materialized="table", extra="copy_grants = false,"
            ),
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

    def test_clone_replace_ignores_grants_when_copy_grants_false(
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
            for model in ("clone_false", "clone_copy")
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
            return _has_select(grants, user)

        assert not has_select(clones["clone_false"])
        assert has_select(clones["clone_copy"])


_REVOKE_MODEL_TEMPLATE = """
{{{{ config(
    materialized = '{materialized}',
    grants = {{'select': ['{user}']}},
) }}}}

select 1 as id
"""


class TestReplaceRevokesStaleGrants(BaseGrants):
    """Unset copy_grants may keep grants on replace, so removed grants must be revoked."""

    _MATERIALIZATIONS = ("table", "view")

    @pytest.fixture(scope="class")
    def models(self):
        return {f"{m}_revoke.sql": "select 1 as id" for m in self._MATERIALIZATIONS}

    def _write_models(self, project, user):
        for materialized in self._MATERIALIZATIONS:
            write_file(
                _REVOKE_MODEL_TEMPLATE.format(materialized=materialized, user=user),
                project.project_root,
                "models",
                f"{materialized}_revoke.sql",
            )

    def test_replace_revokes_grants_removed_from_config(self, project, get_test_users):
        if len(get_test_users) < 2:
            pytest.skip("DBT_TEST_USER_1 and DBT_TEST_USER_2 are not set")
        first_user, second_user = get_test_users[:2]

        self._write_models(project, first_user)
        run_dbt(["run"])
        self._write_models(project, second_user)
        run_dbt(["run"])

        for materialized in self._MATERIALIZATIONS:
            model = f"{materialized}_revoke"
            grants = self.get_grants_on_relation(project, model)
            assert not _has_select(grants, first_user), model
            assert _has_select(grants, second_user), model
