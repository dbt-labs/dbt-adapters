"""
Unit tests for the athena__py_save_table_as macro, focused on the
use_iceberg_write_to branch.

Renders the macro end-to-end with jinja2.FileSystemLoader, following
the pattern in test_get_partition_batches.py.
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


class TestUseIcebergWriteToRendering:
    """Verify the use_iceberg_write_to branch generates correct Python."""

    def test_renders_writeto_with_native_partition_transforms(self):
        rendered = _render(
            {
                "location": "s3://bucket/path",
                "use_iceberg_write_to": True,
                "partitioned_by": ["day(created_at)", "bucket(user_id, 4)"],
            }
        )
        assert '.writeTo("my_schema.my_table")' in rendered
        assert '.using("iceberg")' in rendered
        assert ".createOrReplace()" in rendered
        # partitioned_by values are passed through tojson, so they appear
        # as Python string literals consumed by _parse_iceberg_partition.
        assert '_parse_iceberg_partition("day(created_at)")' in rendered
        assert '_parse_iceberg_partition("bucket(user_id, 4)")' in rendered
        # spark_ctas branch must NOT be reached.
        assert "spark_session.sql" not in rendered

    def test_renders_writeto_without_partitions(self):
        rendered = _render({"location": "s3://bucket/path", "use_iceberg_write_to": True})
        assert ".writeTo(" in rendered
        assert ".createOrReplace()" in rendered
        # No partitionedBy call when partitioned_by is omitted.
        assert ".partitionedBy(" not in rendered

    def test_extra_table_properties_use_tojson_escaping(self):
        rendered = _render(
            {
                "location": "s3://bucket/path",
                "use_iceberg_write_to": True,
                "extra_table_properties": {
                    "format-version": "2",
                    'key"with"quotes': "value\\backslash",
                },
            }
        )
        # location goes through tojson together with the trailing slash.
        assert '_writer.tableProperty("location", "s3://bucket/path/")' in rendered
        # Plain property: emitted as JSON-escaped Python string literals.
        assert '_writer.tableProperty("format-version", "2")' in rendered
        # Special characters are escaped (no raw injection).
        assert 'key\\"with\\"quotes' in rendered
        assert "value\\\\backslash" in rendered

    def test_extra_table_properties_coerces_non_string_values(self):
        # Booleans / ints sometimes appear in user configs; |string|tojson
        # must coerce them to a Python string literal rather than blow up.
        rendered = _render(
            {
                "location": "s3://bucket/path",
                "use_iceberg_write_to": True,
                "extra_table_properties": {"format-version": 2},
            }
        )
        assert '_writer.tableProperty("format-version", "2")' in rendered

    def test_falls_back_to_spark_ctas_when_disabled(self):
        rendered = _render(
            {
                "location": "s3://bucket/path",
                "use_iceberg_write_to": False,
                "spark_ctas": "create table foo using iceberg as",
            }
        )
        assert "writeTo" not in rendered
        assert "spark_session.sql" in rendered
        assert "create table foo using iceberg as" in rendered

    def test_falls_back_to_save_as_table_when_no_iceberg_or_ctas(self):
        # The third branch (the original Hive saveAsTable path) must still
        # fire when neither use_iceberg_write_to nor spark_ctas is set.
        rendered = _render(
            {
                "location": "s3://bucket/path",
                "format": "parquet",
                "partitioned_by": None,
                "bucketed_by": None,
                "sorted_by": None,
            }
        )
        assert "writeTo" not in rendered
        assert "spark_session.sql" not in rendered
        assert "writer.saveAsTable" in rendered

    def test_target_relation_quotes_are_backticked(self):
        # Python identifiers in writeTo() must use backticks; quotes
        # cause AnalysisException in Spark.
        target = SimpleNamespace(schema='"q_schema"', identifier='"q_table"')
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(_MACRO_DIR))
        template = env.get_template("python_submissions.sql")
        rendered = template.module.athena__py_save_table_as(
            "", target, {"location": "s3://b/p", "use_iceberg_write_to": True}
        )
        assert '.writeTo("`q_schema`.`q_table`")' in rendered


class TestParseIcebergPartition:
    """Execute the inline _parse_iceberg_partition function from the
    rendered Python code against a stub pyspark.sql.functions module to
    cover its dispatch and arity validation."""

    @pytest.fixture
    def parse_fn(self):
        rendered = _render({"location": "s3://b/p", "use_iceberg_write_to": True})

        # Pull just the _parse_iceberg_partition definition out of the
        # rendered template and exec it with a stubbed F (pyspark.sql.functions)
        # injected directly, so we don't need a real Spark context.
        marker = "def _parse_iceberg_partition(expr_str):"
        start = rendered.index(marker)
        end = rendered.index("_writer = df.writeTo", start)
        # Strip the leading 4-space indent from each line (the function
        # lives inside materialize()).
        body = "\n".join(
            line[4:] if line.startswith("    ") else line
            for line in rendered[start:end].splitlines()
        )

        F_stub = SimpleNamespace(
            col=lambda c: ("col", c),
            days=lambda c: ("days", c),
            months=lambda c: ("months", c),
            years=lambda c: ("years", c),
            hours=lambda c: ("hours", c),
            bucket=lambda n, c: ("bucket", n, c),
            truncate=lambda n, c: ("truncate", n, c),
        )
        import re as re_module

        ns = {"re": re_module, "F": F_stub}
        exec(body, ns)
        return ns["_parse_iceberg_partition"]

    @pytest.mark.parametrize(
        "expr,expected",
        [
            ("day(created_at)", ("days", ("col", "created_at"))),
            ("days(created_at)", ("days", ("col", "created_at"))),
            ("month(ts)", ("months", ("col", "ts"))),
            ("year(ts)", ("years", ("col", "ts"))),
            ("hour(ts)", ("hours", ("col", "ts"))),
            ("bucket(user_id, 256)", ("bucket", 256, ("col", "user_id"))),
            ("truncate(name, 10)", ("truncate", 10, ("col", "name"))),
            ("plain_col", ("col", "plain_col")),
        ],
    )
    def test_dispatch(self, parse_fn, expr, expected):
        assert parse_fn(expr) == expected

    def test_bucket_with_missing_arg_raises_clear_error(self, parse_fn):
        with pytest.raises(ValueError, match="requires 2 arguments"):
            parse_fn("bucket(user_id)")

    def test_truncate_with_missing_arg_raises_clear_error(self, parse_fn):
        with pytest.raises(ValueError, match="requires 2 arguments"):
            parse_fn("truncate(name)")

    def test_unknown_transform_raises_value_error(self, parse_fn):
        with pytest.raises(ValueError, match="Unknown Iceberg partition transform"):
            parse_fn("md5(name)")
