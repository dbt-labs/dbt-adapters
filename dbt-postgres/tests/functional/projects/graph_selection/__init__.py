from functools import partial

import pytest

from tests.functional.projects.utils import read


read_data = partial(read, "graph_selection", "data")
read_model = partial(read, "graph_selection", "models")
read_schema = partial(read, "graph_selection", "schemas")


class GraphSelection:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": read_schema("schema"),
            "patch_path_selection_schema.yml": read_schema("patch_path_selection"),
            "base_users.sql": read_model("base_users"),
            "users.sql": read_model("users"),
            "versioned_v3.sql": read_model("base_users"),
            "users_rollup.sql": read_model("users_rollup"),
            "users_rollup_dependency.sql": read_model("users_rollup_dependency"),
            "emails.sql": read_model("emails"),
            "emails_alt.sql": read_model("emails_alt"),
            "alternative.users.sql": read_model("alternative_users"),
            "never_selected.sql": read_model("never_selected"),
            "test": {
                "subdir.sql": read_model("subdir"),
                "versioned_v2.sql": read_model("subdir"),
                "subdir": {
                    "nested_users.sql": read_model("nested_users"),
                    "versioned_v1.sql": read_model("nested_users"),
                },
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        return {
            "properties.yml": read_schema("properties"),
            "seed.csv": read_data("seed"),
            "summary_expected.csv": read_data("summary_expected"),
        }
