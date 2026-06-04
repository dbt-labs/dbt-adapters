"""
Unit tests for the athena__py_save_table_as macro, focused on the
"model returns None" branch that skips adapter materialize().
"""

import os
from types import SimpleNamespace

import jinja2
import pytest

_MACRO_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        "src",
        "dbt",
        "include",
        "athena",
        "macros",
        "adapters",
    )
)


def _render(optional_args, compiled_code="def model(dbt, spark):\n    return spark"):
    target_relation = SimpleNamespace(schema="my_schema", identifier="my_table")
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_MACRO_DIR),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("python_submissions.sql")
    return template.module.athena__py_save_table_as(compiled_code, target_relation, optional_args)


class TestSkipMaterializeWhenModelReturnsNone:
    """Rendered template must branch on `df is None` and only call
    materialize() when the user returned a DataFrame."""

    def test_renders_none_branch_with_tableExists_guard(self):
        rendered = _render({"location": "s3://b/p"})
        assert "if df is None:" in rendered
        assert "spark.catalog.tableExists(target_fqn)" in rendered
        assert "else:" in rendered
        assert "materialize(spark, df, dbt.this)" in rendered

    def _exec_trailing_block(self, table_exists, model_returns):
        """Execute the rendered trailing block (after the materialize def)
        with stubbed model(), dbt, spark — return whether materialize() was
        called."""
        rendered = _render({"location": "s3://b/p"})
        # Pull the trailing `dbt = SparkdbtObj() ... materialize(...)` block.
        marker = "dbt = SparkdbtObj()"
        body = rendered[rendered.index(marker) :]
        # Strip the trailing endmacro artifacts if any.
        body = body.split("\n\n{%")[0]

        materialize_calls = []

        def fake_materialize(spark_session, df, target):
            materialize_calls.append((df, target))

        spark_stub = SimpleNamespace(
            catalog=SimpleNamespace(tableExists=lambda fqn: table_exists),
        )
        ns = {
            "SparkdbtObj": lambda: SimpleNamespace(
                this=SimpleNamespace(schema="s", identifier="t"),
            ),
            "model": lambda dbt, spark: model_returns,
            "spark": spark_stub,
            "materialize": fake_materialize,
        }
        exec(body, ns)
        return materialize_calls

    def test_none_with_existing_target_skips_materialize(self):
        calls = self._exec_trailing_block(table_exists=True, model_returns=None)
        assert calls == []

    def test_none_with_missing_target_raises(self):
        with pytest.raises(Exception, match="model\\(\\) returned None"):
            self._exec_trailing_block(table_exists=False, model_returns=None)

    def test_dataframe_return_still_materializes(self):
        sentinel = object()
        calls = self._exec_trailing_block(table_exists=True, model_returns=sentinel)
        assert len(calls) == 1
        assert calls[0][0] is sentinel
