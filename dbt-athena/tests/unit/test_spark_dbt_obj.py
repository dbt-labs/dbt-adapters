"""
Renders the ``athena__py_get_spark_dbt_object`` macro and execs the
resulting Python so we can drive the SparkdbtObj wrappers directly.

Guards against regressing the kwargs shim: dbt-core's source() / ref()
accept ``v=`` and ``version=``, and the wrappers must forward them
rather than dropping them on the floor.
"""

import os
from unittest.mock import MagicMock

import jinja2

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


def _build_spark_dbt_obj(source_stub, ref_stub, spark_stub):
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_MACRO_DIR),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("python_submissions.sql")
    rendered = template.module.athena__py_get_spark_dbt_object()

    class _StubDbtObj:
        def __init__(self, load_df_function):
            self.load_df_function = load_df_function

    namespace = {
        "dbtObj": _StubDbtObj,
        "source": source_stub,
        "ref": ref_stub,
        "spark": spark_stub,
    }
    exec(rendered, namespace)
    return namespace["SparkdbtObj"]()


def test_ref_forwards_positional_and_keyword_arguments():
    ref_stub = MagicMock(return_value="ref-result")
    obj = _build_spark_dbt_obj(MagicMock(), ref_stub, MagicMock())

    result = obj.ref("my_model", v=1)

    assert result == "ref-result"
    ref_stub.assert_called_once()
    args, kwargs = ref_stub.call_args
    assert args == ("my_model",)
    assert kwargs["v"] == 1
    assert "dbt_load_df_function" in kwargs


def test_ref_forwards_package_and_version_keyword():
    ref_stub = MagicMock(return_value="versioned")
    obj = _build_spark_dbt_obj(MagicMock(), ref_stub, MagicMock())

    obj.ref("test", "my_versioned_sql_model", version=1)

    args, kwargs = ref_stub.call_args
    assert args == ("test", "my_versioned_sql_model")
    assert kwargs["version"] == 1


def test_source_forwards_keyword_arguments():
    source_stub = MagicMock(return_value="source-result")
    obj = _build_spark_dbt_obj(source_stub, MagicMock(), MagicMock())

    result = obj.source("test_source", "test_table", some_kwarg="x")

    assert result == "source-result"
    args, kwargs = source_stub.call_args
    assert args == ("test_source", "test_table")
    assert kwargs["some_kwarg"] == "x"
    assert "dbt_load_df_function" in kwargs
