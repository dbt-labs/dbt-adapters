from dbt.tests.util import run_dbt
import pytest

from tests.functional.projects.graph_selection import (
    read_data,
    read_model,
    read_schema,
)


selectors_yml = """
selectors:
  - name: group_specified_as_string_str
    definition: group:users_group
  - name: group_specified_as_string_dict
    definition:
      method: group
      value: users_group
  - name: users_grouped_childrens_parents
    definition:
      method: group
      value: users_group
      childrens_parents: true
"""


class TestGroupSelection:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": read_schema("schema"),
            "base_users.sql": read_model("base_users"),
            "users.sql": read_model("users"),
            "users_rollup.sql": read_model("users_rollup"),
            "versioned_v3.sql": read_model("base_users"),
            "users_rollup_dependency.sql": read_model("users_rollup_dependency"),
            "emails.sql": read_model("emails"),
            "emails_alt.sql": read_model("emails_alt"),
            "alternative.users.sql": read_model("alternative_users"),
            "never_selected.sql": read_model("never_selected"),
            "test": {
                "subdir.sql": read_model("subdir"),
                "subdir": {"nested_users.sql": read_model("nested_users")},
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        return {
            "properties.yml": read_schema("properties"),
            "seed.csv": read_data("seed"),
            "summary_expected.csv": read_data("summary_expected"),
        }

    @pytest.fixture(scope="class")
    def selectors(self):
        return selectors_yml

    def test_select_models_by_group(self, project):
        results = run_dbt(["ls", "--model", "group:users_group"])
        assert sorted(results) == ["test.users"]

    def test_select_group_selector_str(self, project):
        results = run_dbt(["ls", "--selector", "group_specified_as_string_str"])
        assert sorted(results) == ["test.unique_users_id", "test.users"]

    def test_select_group_selector_dict(self, project):
        results = run_dbt(["ls", "--selector", "group_specified_as_string_dict"])
        assert sorted(results) == ["test.unique_users_id", "test.users"]

    def test_select_models_by_group_and_children(self, project):  # noqa
        results = run_dbt(["ls", "--models", "+group:users_group+"])
        assert sorted(results) == [
            "test.base_users",
            "test.emails_alt",
            "test.users",
            "test.users_rollup",
            "test.users_rollup_dependency",
        ]

    def test_select_group_and_children(self, project):  # noqa
        expected = [
            "exposure:test.user_exposure",
            "source:test.raw.seed",
            "test.base_users",
            "test.emails_alt",
            "test.unique_users_id",
            "test.unique_users_rollup_gender",
            "test.users",
            "test.users_rollup",
            "test.users_rollup_dependency",
        ]
        results = run_dbt(["ls", "--select", "+group:users_group+"])
        assert sorted(results) == expected

    def test_select_group_and_children_selector_str(self, project):  # noqa
        expected = [
            "exposure:test.user_exposure",
            "source:test.raw.seed",
            "test.base_users",
            "test.emails_alt",
            "test.unique_users_id",
            "test.unique_users_rollup_gender",
            "test.users",
            "test.users_rollup",
            "test.users_rollup_dependency",
            "test.versioned.v3",
        ]
        results = run_dbt(["ls", "--selector", "users_grouped_childrens_parents"])
        assert sorted(results) == expected

    # 2 groups
    def test_select_models_two_groups(self, project):
        expected = ["test.base_users", "test.emails", "test.users"]
        results = run_dbt(["ls", "--models", "@group:emails_group group:users_group"])
        assert sorted(results) == expected
