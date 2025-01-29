from functools import partial

from tests.functional.projects.utils import read


read_macro = partial(read, "dbt_integration", "macros")
read_model = partial(read, "dbt_integration", "models")
read_schema = partial(read, "dbt_integration", "schemas")


def dbt_integration():
    return {
        "dbt_project.yml": read_schema("project"),
        "macros": {"do_something.sql": read_macro("do_something")},
        "models": {
            "schema.yml": read_schema("schema"),
            "incremental.sql": read_model("incremental"),
            "table_model.sql": read_model("table"),
            "view_model.sql": read_model("view"),
        },
    }
