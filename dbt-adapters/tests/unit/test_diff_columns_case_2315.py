from pathlib import Path

from jinja2 import Environment
from dbt.adapters.base.column import Column


MACRO_PATH = (
    Path(__file__).resolve().parents[2]
    / "src/dbt/include/global_project/macros/materializations/models/incremental/column_helpers.sql"
)


def test_default_diff_columns_preserves_case_sensitive_matching():
    source = MACRO_PATH.read_text()
    start = source.index("{% macro default__diff_columns(")
    end = source.index("{% endmacro %}", start) + len("{% endmacro %}")

    captured = []

    def return_value(value):
        captured.append(value)
        return ""

    env = Environment(extensions=["jinja2.ext.do"])
    env.globals["return"] = return_value
    template = env.from_string(source[start:end])

    for source_name, target_name in [("id", "ID"), ("ID", "id")]:
        captured.clear()
        template.module.default__diff_columns(
            [Column(source_name, "integer")],
            [Column(target_name, "integer")],
        )
        assert [col.name for col in captured[-1]] == [source_name]


def test_bigquery_diff_columns_case_insensitive():
    macro_path = (
        Path(__file__).resolve().parents[3]
        / "dbt-bigquery/src/dbt/include/bigquery/macros/materializations/models/incremental/on_schema_change.sql"
    )
    source = macro_path.read_text()
    start = source.index("{% macro bigquery__diff_columns(")
    end = source.index("{% endmacro %}", start) + len("{% endmacro %}")

    captured = []

    def return_value(value):
        captured.append(value)
        return ""

    env = Environment(extensions=["jinja2.ext.do"])
    env.globals["return"] = return_value
    macro = env.from_string(source[start:end]).module.bigquery__diff_columns

    for source_name, target_name in [("id", "ID"), ("ID", "id")]:
        captured.clear()
        macro(
            [Column(source_name, "integer")],
            [Column(target_name, "integer")],
        )
        assert captured[-1] == []


def test_bigquery_diff_column_data_types():
    macro_path = (
        Path(__file__).resolve().parents[3]
        / "dbt-bigquery/src/dbt/include/bigquery/macros/materializations/models/incremental/on_schema_change.sql"
    )
    source = macro_path.read_text()
    start = source.index("{% macro bigquery__diff_column_data_types(")
    end = source.index("{% endmacro %}", start) + len("{% endmacro %}")

    captured = []

    def return_value(value):
        captured.append(value)
        return ""

    env = Environment(extensions=["jinja2.ext.do"])
    env.globals["return"] = return_value
    macro = env.from_string(source[start:end]).module.bigquery__diff_column_data_types

    cases = [
        ("id", "ID", "integer", "integer", []),
        ("ID", "id", "integer", "integer", []),
        ("id", "ID", "string", "integer", [{"column_name": "ID", "new_type": "string"}]),
    ]

    for source_name, target_name, source_type, target_type, expected in cases:
        captured.clear()
        macro(
            [Column(source_name, source_type)],
            [Column(target_name, target_type)],
        )
        assert captured[-1] == expected
