import os
from types import SimpleNamespace

import jinja2
import pytest

from dbt.adapters.athena import AthenaAdapter
from dbt.adapters.athena.config import AthenaSparkSessionConfig, is_spark_connect_engine_version

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

_CASES = [
    ("3.5", True),
    (3.5, True),
    ("3.5.1", False),
    ("3.4", False),
    ("", False),
    (None, False),
]


@pytest.mark.parametrize("value, expected", _CASES)
def test_is_spark_connect_engine_version(value, expected):
    assert is_spark_connect_engine_version(value) is expected


@pytest.mark.parametrize("value, expected", _CASES)
def test_config_property_agrees(value, expected):
    config = {} if value is None else {"spark_engine_version": value}
    assert AthenaSparkSessionConfig(config).is_spark_connect is expected


@pytest.mark.parametrize("value, expected", _CASES)
def test_adapter_method_agrees(value, expected):
    assert AthenaAdapter.is_spark_connect_engine(None, value) is expected


def _render_save_table_as(engine_version):
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_MACRO_DIR),
        extensions=["jinja2.ext.do"],
    )
    env.globals["adapter"] = SimpleNamespace(
        is_spark_connect_engine=lambda v: AthenaAdapter.is_spark_connect_engine(None, v)
    )
    relation = SimpleNamespace(schema="s", identifier="t")
    template = env.get_template("python_submissions.sql")
    return template.module.athena__py_save_table_as(
        "", relation, {"spark_engine_version": engine_version}
    )


@pytest.mark.parametrize("value, expected", _CASES)
def test_macro_branch_agrees(value, expected):
    rendered = _render_save_table_as(value)
    assert ("pyspark.sql.connect.dataframe.DataFrame" in rendered) is expected
    assert ("isinstance(df, pyspark.sql.dataframe.DataFrame)" in rendered) is not expected
