from dbt.tests.util import run_dbt
import pytest

from tests.functional.projects.graph_selection import (
    read_data,
    read_model,
    read_schema,
)


selectors_yml = """
selectors:
  - name: version_specified_as_string_str
    definition: version:latest
  - name: version_specified_as_string_dict
    definition:
      method: version
      value: latest
  - name: version_childrens_parents
    definition:
      method: version
      value: latest
      childrens_parents: true
"""


class TestVersionSelection:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": read_schema("schema"),
            "versioned_v1.sql": read_model("users"),
            "versioned_v2.sql": read_model("users"),
            "versioned_v3.sql": read_model("users"),
            "versioned_v4.5.sql": read_model("users"),
            "versioned_v5.0.sql": read_model("users"),
            "versioned_v21.sql": read_model("users"),
            "versioned_vtest.sql": read_model("users"),
            "base_users.sql": read_model("base_users"),
            "users.sql": read_model("users"),
            "users_rollup.sql": read_model("users_rollup"),
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

    def test_select_none_versions(self, project):
        results = run_dbt(["ls", "--select", "version:none"])
        assert sorted(results) == [
            "test.base_users",
            "test.unique_users_id",
            "test.unique_users_rollup_gender",
            "test.users",
            "test.users_rollup",
        ]

    def test_select_latest_versions(self, project):
        results = run_dbt(["ls", "--select", "version:latest"])
        assert sorted(results) == ["test.versioned.v2"]

    def test_select_old_versions(self, project):
        results = run_dbt(["ls", "--select", "version:old"])
        assert sorted(results) == ["test.versioned.v1"]

    def test_select_prerelease_versions(self, project):
        results = run_dbt(["ls", "--select", "version:prerelease"])
        assert sorted(results) == [
            "test.versioned.v21",
            "test.versioned.v3",
            "test.versioned.v4.5",
            "test.versioned.v5.0",
            "test.versioned.vtest",
        ]

    def test_select_version_selector_str(self, project):
        results = run_dbt(["ls", "--selector", "version_specified_as_string_str"])
        assert sorted(results) == ["test.versioned.v2"]

    def test_select_version_selector_dict(self, project):
        results = run_dbt(["ls", "--selector", "version_specified_as_string_dict"])
        assert sorted(results) == ["test.versioned.v2"]

    def test_select_models_by_version_and_children(self, project):  # noqa
        results = run_dbt(["ls", "--models", "+version:latest+"])
        assert sorted(results) == ["test.base_users", "test.versioned.v2"]

    def test_select_version_and_children(self, project):  # noqa
        expected = ["source:test.raw.seed", "test.base_users", "test.versioned.v2"]
        results = run_dbt(["ls", "--select", "+version:latest+"])
        assert sorted(results) == expected

    def test_select_group_and_children_selector_str(self, project):  # noqa
        expected = ["source:test.raw.seed", "test.base_users", "test.versioned.v2"]
        results = run_dbt(["ls", "--selector", "version_childrens_parents"])
        assert sorted(results) == expected

    # 2 versions
    def test_select_models_two_versions(self, project):
        results = run_dbt(["ls", "--models", "version:latest version:old"])
        assert sorted(results) == ["test.versioned.v1", "test.versioned.v2"]


my_model_yml = """
models:
  - name: my_model
    versions:
      - v: 0
"""


class TestVersionZero:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as id",
            "another.sql": "select * from {{ ref('my_model') }}",
            "schema.yml": my_model_yml,
        }

    def test_version_zero(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
