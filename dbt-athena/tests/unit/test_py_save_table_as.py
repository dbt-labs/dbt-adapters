"""
Unit tests for the athena__py_save_table_as macro, focused on the
"model returns None" branch that skips adapter materialize().
"""

import os
from types import SimpleNamespace
from unittest import mock

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


def _render(
    optional_args,
    *,
    compiled_code="def model(dbt, spark):\n    return spark",
    target_identifier="my_table__dbt_tmp",
    this_identifier="my_table",
    schema="my_schema",
):
    """Render athena__py_save_table_as with stubbed dbt context.

    ``target_identifier`` defaults to a ``__dbt_tmp`` suffix to mirror the
    incremental Python flow, where create_table_as is invoked with the
    intermediate temp relation. ``this_identifier`` is the final relation
    that the guard must look up — regression test for the case where the
    two diverge.
    """
    target_relation = SimpleNamespace(schema=schema, identifier=target_identifier)
    context = {
        "target": SimpleNamespace(
            assume_role_arn=None,
            assume_role_external_id=None,
            assume_role_session_name=None,
            region_name="us-east-1",
        ),
        "this": SimpleNamespace(schema=schema, identifier=this_identifier),
    }
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_MACRO_DIR),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("python_submissions.sql", globals=context)
    return template.module.athena__py_save_table_as(compiled_code, target_relation, optional_args)


class TestSkipMaterializeWhenModelReturnsNone:
    """Rendered template must branch on ``df is None`` and only call
    materialize() when the user returned a DataFrame."""

    def test_renders_none_branch_with_glue_guard(self):
        rendered = _render({"location": "s3://b/p"})
        assert "if df is None:" in rendered
        assert "glue.get_table(" in rendered
        assert "else:" in rendered
        assert "materialize(spark, df, dbt.this)" in rendered

    def test_guard_uses_final_relation_not_tmp(self):
        """The guard must look up the final (``this``) relation, never the
        ``__dbt_tmp`` intermediate. Otherwise incremental Python models
        that write the final relation directly always raise on first run."""
        rendered = _render(
            {"location": "s3://b/p"},
            target_identifier="my_table__dbt_tmp",
            this_identifier="my_table",
        )
        assert 'Name="my_table"' in rendered
        assert "my_table__dbt_tmp" not in rendered.split("if df is None:", 1)[1]

    def _exec_trailing_block(self, *, table_exists, model_returns):
        """Execute the rendered ``if df is None:`` ... ``else: materialize(...)``
        block with stubbed dbt / model / spark / boto3."""
        rendered = _render({"location": "s3://b/p"})
        marker = "dbt = SparkdbtObj()"
        body = rendered[rendered.index(marker) :]

        materialize_calls = []

        def fake_materialize(spark_session, df, target):
            materialize_calls.append((df, target))

        glue_client = mock.Mock()
        if table_exists:
            glue_client.get_table.return_value = {}
        else:
            from botocore.exceptions import ClientError

            glue_client.get_table.side_effect = ClientError(
                error_response={"Error": {"Code": "EntityNotFoundException"}},
                operation_name="GetTable",
            )

        ns = {
            "SparkdbtObj": lambda: SimpleNamespace(
                this=SimpleNamespace(schema="my_schema", identifier="my_table"),
            ),
            "model": lambda dbt, spark: model_returns,
            "spark": mock.Mock(),
            "materialize": fake_materialize,
        }
        with mock.patch("boto3.client", return_value=glue_client):
            exec(compile(body, "<rendered>", "exec"), ns)
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
