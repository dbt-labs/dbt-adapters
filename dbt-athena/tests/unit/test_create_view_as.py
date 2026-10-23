"""
Unit tests for the athena__create_view_as macro.

Verifies DDL output for standard views vs Data Catalog Views (Multi Dialect Views)
using jinja2.FileSystemLoader with stubbed dbt context, following the pattern
used in test_get_partition_batches.py and the Spark adapter.
"""

import os
import re
from types import SimpleNamespace
from unittest import mock

import jinja2

_VIEW_MACROS_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        "src",
        "dbt",
        "include",
        "athena",
        "macros",
        "materializations",
        "models",
        "view",
    )
)


def _render_create_view_as(relation, sql, config_overrides=None):
    """Load and render athena__create_view_as with a stubbed dbt context.

    Args:
        relation: The relation name/object to pass to the macro.
        sql: The SQL body of the view.
        config_overrides: Dict of config values (is_data_catalog_view, contract, etc.).

    Returns:
        Rendered DDL string with whitespace normalised.
    """
    config_values = {
        "contract": SimpleNamespace(enforced=False),
        "is_data_catalog_view": False,
    }
    if config_overrides:
        config_values.update(config_overrides)

    context = {
        "config": mock.Mock(),
        "get_assert_columns_equivalent": mock.Mock(return_value=""),
    }
    context["config"].get = lambda key, *args, **kwargs: config_values.get(
        key, args[0] if args else kwargs.get("default")
    )

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_VIEW_MACROS_DIR),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("create_view_as.sql", globals=context)
    raw = template.module.athena__create_view_as(relation, sql)
    return re.sub(r"\s+", " ", raw).strip()


class TestCreateViewAsStandard:
    """Tests for standard (non-data-catalog) view creation."""

    def test_default_creates_standard_view(self):
        result = _render_create_view_as("my_schema.my_view", "select 1 as id")
        assert result == "create or replace view my_schema.my_view as select 1 as id"

    def test_explicit_false_creates_standard_view(self):
        result = _render_create_view_as(
            "my_schema.my_view",
            "select 1 as id",
            config_overrides={"is_data_catalog_view": False},
        )
        assert result == "create or replace view my_schema.my_view as select 1 as id"


class TestCreateViewAsDataCatalog:
    """Tests for Data Catalog View (Multi Dialect View) creation."""

    def test_data_catalog_view_ddl(self):
        result = _render_create_view_as(
            "my_schema.my_view",
            "select 1 as id",
            config_overrides={"is_data_catalog_view": True},
        )
        assert result == (
            "create or replace protected multi dialect view"
            " my_schema.my_view"
            " security definer"
            " as"
            " select 1 as id"
        )

    def test_data_catalog_view_with_complex_sql(self):
        sql = "select a, b, c from my_table where a > 1"
        result = _render_create_view_as(
            "my_catalog.my_schema.my_view",
            sql,
            config_overrides={"is_data_catalog_view": True},
        )
        assert "protected multi dialect view" in result
        assert "security definer" in result
        assert sql in result


class TestCreateViewAsContract:
    """Tests that contract enforcement calls get_assert_columns_equivalent."""

    def test_contract_enforced_standard_view(self):
        config_overrides = {
            "contract": SimpleNamespace(enforced=True),
            "is_data_catalog_view": False,
        }
        result = _render_create_view_as("my_schema.my_view", "select 1 as id", config_overrides)
        assert result == "create or replace view my_schema.my_view as select 1 as id"

    def test_contract_enforced_data_catalog_view(self):
        config_overrides = {
            "contract": SimpleNamespace(enforced=True),
            "is_data_catalog_view": True,
        }
        result = _render_create_view_as("my_schema.my_view", "select 1 as id", config_overrides)
        assert "protected multi dialect view" in result
        assert "security definer" in result
