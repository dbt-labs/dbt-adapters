import json
import os

from dbt.tests.util import check_result_nodes_by_name, run_dbt
import pytest

from tests.functional.projects import GraphSelection


selectors_yml = """
selectors:
- name: bi_selector
  description: This is a BI selector
  definition:
    method: tag
    value: bi
"""


def assert_correct_schemas(project):
    adapter = project.adapter
    with adapter.connection_named("__test"):
        exists = adapter.check_schema_exists(project.database, project.test_schema)
        assert exists

        schema = project.test_schema + "_and_then"
        exists = adapter.check_schema_exists(project.database, schema)
        assert not exists


def clear_schema(project):
    project.run_sql("drop schema if exists {schema} cascade")
    project.run_sql("create schema {schema}")


class TestGraphSelection(GraphSelection):
    # The tests here aiming to test whether the correct node is selected,
    # we don't need the run to pass
    @pytest.fixture(scope="class")
    def selectors(self):
        return selectors_yml

    def test_specific_model(self, project):
        results = run_dbt(["run", "--select", "users"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])
        assert_correct_schemas(project)

    def test_tags(self, project, project_root):
        results = run_dbt(["run", "--selector", "bi_selector"], expect_pass=False)
        check_result_nodes_by_name(results, ["users", "users_rollup"])
        assert_correct_schemas(project)
        manifest_path = project_root.join("target/manifest.json")
        assert os.path.exists(manifest_path)
        with open(manifest_path) as fp:
            manifest = json.load(fp)
            assert "selectors" in manifest

    def test_tags_and_children(self, project):
        results = run_dbt(["run", "--select", "tag:base+"], expect_pass=False)
        check_result_nodes_by_name(
            results,
            [
                "emails_alt",
                "users_rollup",
                "users",
                "alternative.users",
                "users_rollup_dependency",
            ],
        )
        assert_correct_schemas(project)

    def test_tags_and_children_limited(self, project):
        results = run_dbt(["run", "--select", "tag:base+2"], expect_pass=False)
        check_result_nodes_by_name(
            results, ["emails_alt", "users_rollup", "users", "alternative.users"]
        )
        assert_correct_schemas(project)

    def test_group(self, project):
        expected = ["test.unique_users_id", "test.users"]
        results = run_dbt(["ls", "--select", "group:users_group"])
        assert sorted(results) == expected

    def test_specific_model_and_children(self, project):
        results = run_dbt(["run", "--select", "users+"], expect_pass=False)
        check_result_nodes_by_name(
            results, ["users", "users_rollup", "emails_alt", "users_rollup_dependency"]
        )
        assert_correct_schemas(project)

    def test_specific_model_and_children_limited(self, project):
        results = run_dbt(["run", "--select", "users+1"], expect_pass=False)
        check_result_nodes_by_name(results, ["users", "users_rollup", "emails_alt"])
        assert_correct_schemas(project)

    def test_specific_model_and_parents(self, project):
        results = run_dbt(["run", "--select", "+users_rollup"], expect_pass=False)
        check_result_nodes_by_name(results, ["users_rollup", "users"])
        assert_correct_schemas(project)

    def test_specific_model_and_parents_limited(self, project):
        results = run_dbt(["run", "--select", "1+users_rollup"], expect_pass=False)
        check_result_nodes_by_name(results, ["users", "users_rollup"])
        assert_correct_schemas(project)

    def test_specific_model_with_exclusion(self, project):
        results = run_dbt(
            [
                "run",
                "--select",
                "+users_rollup",
                "--exclude",
                "models/users_rollup.sql",
            ],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users"])
        assert_correct_schemas(project)

    def test_locally_qualified_name(self, project):
        results = run_dbt(["run", "--select", "test.subdir"])
        check_result_nodes_by_name(results, ["nested_users", "subdir", "versioned"])
        assert_correct_schemas(project)

        os.chdir(
            project.profiles_dir
        )  # Change to random directory to test that Path selector works with project-dir
        results = run_dbt(
            ["run", "--project-dir", str(project.project_root), "--select", "models/test/subdir*"]
        )
        check_result_nodes_by_name(results, ["nested_users", "subdir", "versioned"])
        assert_correct_schemas(project)

        results = run_dbt(
            [
                "build",
                "--project-dir",
                str(project.project_root),
                "--select",
                "models/patch_path_selection_schema.yml",
            ]
        )
        check_result_nodes_by_name(results, ["subdir"])
        assert_correct_schemas(project)

        # Check that list command works
        os.chdir(
            project.profiles_dir
        )  # Change to random directory to test that Path selector works with project-dir
        results = run_dbt(
            [
                "-q",
                "ls",
                "-s",
                "path:models/test/subdir.sql",
                "--project-dir",
                str(project.project_root),
            ]
            #           ["list", "--project-dir", str(project.project_root), "--select", "models/test/subdir*"]
        )
        assert len(results) == 1

    def test_locally_qualified_name_model_with_dots(self, project):
        results = run_dbt(["run", "--select", "alternative.users"], expect_pass=False)
        check_result_nodes_by_name(results, ["alternative.users"])
        assert_correct_schemas(project)

        results = run_dbt(["run", "--select", "models/alternative.*"], expect_pass=False)
        check_result_nodes_by_name(results, ["alternative.users"])
        assert_correct_schemas(project)

    def test_childrens_parents(self, project):
        results = run_dbt(["run", "--select", "@base_users"], expect_pass=False)
        check_result_nodes_by_name(
            results,
            [
                "alternative.users",
                "users_rollup",
                "users",
                "emails_alt",
                "users_rollup_dependency",
            ],
        )

        results = run_dbt(["test", "--select", "test_name:not_null"], expect_pass=False)
        check_result_nodes_by_name(results, ["not_null_emails_email"])

    def test_more_childrens_parents(self, project):
        results = run_dbt(["run", "--select", "@users"], expect_pass=False)
        check_result_nodes_by_name(
            results, ["users_rollup", "users", "emails_alt", "users_rollup_dependency"]
        )

        results = run_dbt(["test", "--select", "test_name:unique"], expect_pass=False)
        check_result_nodes_by_name(results, ["unique_users_id", "unique_users_rollup_gender"])

    def test_concat(self, project):
        results = run_dbt(["run", "--select", "@emails_alt", "users_rollup"], expect_pass=False)
        check_result_nodes_by_name(results, ["users_rollup", "users", "emails_alt"])

    def test_concat_multiple(self, project):
        results = run_dbt(
            ["run", "--select", "@emails_alt", "--select", "users_rollup"], expect_pass=False
        )
        check_result_nodes_by_name(results, ["users_rollup", "users", "emails_alt"])

    def test_concat_exclude(self, project):
        results = run_dbt(
            [
                "run",
                "--select",
                "@emails_alt",
                "users_rollup",
                "--exclude",
                "emails_alt",
            ],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users_rollup", "users"])

    def test_concat_exclude_multiple(self, project):
        results = run_dbt(
            [
                "run",
                "--select",
                "@emails_alt",
                "users_rollup",
                "--exclude",
                "users",
                "--exclude",
                "emails_alt",
            ],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users_rollup"])

    def test_concat_exclude_concat(self, project):
        results = run_dbt(
            [
                "run",
                "--select",
                "@emails_alt",
                "users_rollup",
                "--exclude",
                "emails_alt",
                "users_rollup",
            ],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users"])

        results = run_dbt(
            [
                "test",
                "--select",
                "@emails_alt",
                "users_rollup",
                "--exclude",
                "emails_alt",
                "users_rollup",
            ],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["unique_users_id"])

    def test_exposure_parents(self, project):
        results = run_dbt(["ls", "--select", "+exposure:seed_ml_exposure"])
        assert sorted(results) == [
            "exposure:test.seed_ml_exposure",
            "source:test.raw.seed",
        ]
        results = run_dbt(["ls", "--select", "1+exposure:user_exposure"])
        assert sorted(results) == [
            "exposure:test.user_exposure",
            "test.unique_users_id",
            "test.unique_users_rollup_gender",
            "test.users",
            "test.users_rollup",
            "test.versioned.v3",
        ]
        results = run_dbt(["run", "-m", "+exposure:user_exposure"], expect_pass=False)
        check_result_nodes_by_name(
            results,
            [
                "users_rollup",
                "users",
            ],
        )


class TestListPathGraphSelection(GraphSelection):
    def test_list_select_with_project_dir(self, project):
        # Check that list command works
        os.chdir(
            project.profiles_dir
        )  # Change to random directory to test that Path selector works with project-dir
        results = run_dbt(
            [
                "-q",
                "ls",
                "-s",
                "path:models/test/subdir.sql",
                "--project-dir",
                str(project.project_root),
            ]
        )
        assert results == ["test.test.subdir"]
