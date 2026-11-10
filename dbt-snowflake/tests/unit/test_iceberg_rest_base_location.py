"""BASE_LOCATION for Iceberg REST catalog-linked databases (CLDs).

Required by Snowflake for CLDs that vend credentials. Opt-in: no default is applied.
"""

import os
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from jinja2 import Environment

from dbt.adapters.catalogs import InvalidCatalogIntegrationConfigError
from dbt.adapters.exceptions.compilation import InvalidRelationConfigError
from dbt.adapters.snowflake.catalogs._iceberg_rest import IcebergRestCatalogIntegration

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


def _integration(**adapter_properties):
    return IcebergRestCatalogIntegration(
        SimpleNamespace(
            name="test_iceberg_rest",
            catalog_name="POLARIS",
            catalog_type="iceberg_rest",
            external_volume=None,
            adapter_properties={"catalog_linked_database": "cld", **adapter_properties},
        )
    )


def _model(config=None):
    model = Mock()
    model.config = config or {}
    model.schema = "my_schema"
    model.identifier = "my_table"
    return model


class TestBaseLocationResolution:
    def test_no_base_location_by_default(self):
        relation = _integration().build_relation(_model())
        assert relation.base_location_root is None
        assert relation.base_location_subpath is None

    def test_catalog_root(self):
        relation = _integration(base_location_root="s3://bucket/prefix").build_relation(_model())
        assert relation.base_location_root == "s3://bucket/prefix"
        assert relation.base_location_subpath is None

    def test_model_root_overrides_catalog_root(self):
        relation = _integration(base_location_root="s3://bucket/a").build_relation(
            _model({"base_location_root": "s3://bucket/b"})
        )
        assert relation.base_location_root == "s3://bucket/b"

    def test_model_root_and_subpath(self):
        relation = _integration().build_relation(
            _model({"base_location_root": "s3://bucket/b", "base_location_subpath": "v1"})
        )
        assert (relation.base_location_root, relation.base_location_subpath) == (
            "s3://bucket/b",
            "v1",
        )

    def test_trailing_slashes_and_whitespace_stripped(self):
        relation = _integration(base_location_root=" s3://bucket/a// ").build_relation(
            _model({"base_location_subpath": "/v1/"})
        )
        assert (relation.base_location_root, relation.base_location_subpath) == (
            "s3://bucket/a",
            "v1",
        )

    @pytest.mark.parametrize("subpath", ["", "  ", "/", " // "])
    def test_empty_subpath_is_unset(self, subpath):
        relation = _integration(base_location_root="s3://bucket/a").build_relation(
            _model({"base_location_subpath": subpath})
        )
        assert relation.base_location_subpath is None

    @pytest.mark.parametrize("subpath", ["v1", "/", "  "])
    def test_subpath_without_root_errors_unless_empty(self, subpath):
        if not subpath.strip().strip("/"):
            # an empty subpath is unset, so there is nothing to complain about
            assert (
                _integration()
                .build_relation(_model({"base_location_subpath": subpath}))
                .base_location_root
                is None
            )
        else:
            with pytest.raises(InvalidRelationConfigError):
                _integration().build_relation(_model({"base_location_subpath": subpath}))

    @pytest.mark.parametrize("root", ["", "  ", "/", " // "])
    def test_empty_model_root_errors(self, root):
        with pytest.raises(InvalidRelationConfigError):
            _integration().build_relation(_model({"base_location_root": root}))

    @pytest.mark.parametrize("root", ["", "  ", "/"])
    def test_empty_catalog_root_errors(self, root):
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            _integration(base_location_root=root)


def _read(*parts):
    with open(os.path.join(MACROS_DIR, *parts)) as f:
        return f.read()


class _Relation:
    def __init__(self, identifier, schema="my_schema", database="cld"):
        self.identifier = identifier
        self.schema = schema
        self.database = database

    def __str__(self):
        return f"{self.database}.{self.schema}.{self.identifier}"


class TestBaseLocationInDDL:
    """Render the real create macros, since the emitted clause is the point of the feature."""

    def setup_method(self):
        # one template so that the macros can call each other like they do under dbt
        source = "\n".join(
            [
                _read("relations", "table", "create.sql"),
                _read("utils", "optional.sql"),
                _read("utils", "escape_single_quotes.sql"),
            ]
        )
        self.model = _model()
        self.catalog_relation = None

        config = Mock()
        config.model = self.model
        config.get = lambda key, default=None, **kwargs: (
            SimpleNamespace(enforced=False) if key == "contract" else default
        )

        adapter = Mock()
        adapter.build_catalog_relation = lambda model: self.catalog_relation
        adapter.get_relation = lambda **kwargs: None
        adapter.quote = lambda name: f'"{name}"'

        env = Environment(extensions=["jinja2.ext.do"])
        self.template = env.from_string(source)
        self.template.globals.update(
            {
                "config": config,
                "adapter": adapter,
                # `return` only matters for get_partition_by_keys, which is None in these tests;
                # rendering it as an empty string is falsy, like the None dbt would hand back
                "return": lambda value: value or "",
                "get_column_schema_from_query": lambda sql: [
                    SimpleNamespace(name="ID", data_type="FIXED")
                ],
            }
        )
        # `dbt.escape_single_quotes` dispatches to the snowflake implementation
        escape = env.from_string(_read("utils", "escape_single_quotes.sql")).module
        self.template.globals["dbt"] = SimpleNamespace(
            escape_single_quotes=escape.snowflake__escape_single_quotes
        )

    def _build(self, model_config=None, **adapter_properties):
        self.model.config = model_config or {}
        self.catalog_relation = _integration(**adapter_properties).build_relation(self.model)

    def _render(self, macro, relation):
        sql = macro(relation, "select 1 as id")
        return " ".join(str(sql).split())

    def _rest(self, relation):
        return self._render(
            self.template.module.snowflake__create_table_iceberg_rest_sql, relation
        )

    def _glue(self, relation):
        macro = self.template.module.snowflake__create_table_iceberg_rest_with_glue
        return " ".join(str(macro(relation, "select 1 as id", self.catalog_relation)).split())

    @pytest.mark.parametrize("render", ["_rest", "_glue"])
    def test_no_clause_without_root(self, render):
        self._build()
        assert "base_location" not in getattr(self, render)(_Relation("my_table"))

    @pytest.mark.parametrize("render", ["_rest", "_glue"])
    def test_clause_from_catalog_root(self, render):
        self._build(base_location_root="s3://bucket/prefix")
        sql = getattr(self, render)(_Relation("my_table"))
        assert "base_location = 's3://bucket/prefix/my_schema/my_table'" in sql

    @pytest.mark.parametrize("render", ["_rest", "_glue"])
    def test_clause_with_model_root_and_subpath(self, render):
        self._build({"base_location_root": "s3://bucket/b", "base_location_subpath": "v1"})
        sql = getattr(self, render)(_Relation("my_table"))
        assert "base_location = 's3://bucket/b/my_schema/my_table/v1'" in sql

    @pytest.mark.parametrize("render", ["_rest", "_glue"])
    def test_incremental_tmp_relation_gets_its_own_location(self, render):
        """The staging table must not share the target table's storage path."""
        self._build(base_location_root="s3://bucket/prefix")
        target = getattr(self, render)(_Relation("my_table"))
        tmp = getattr(self, render)(_Relation("my_table__dbt_tmp"))
        assert "base_location = 's3://bucket/prefix/my_schema/my_table'" in target
        assert "base_location = 's3://bucket/prefix/my_schema/my_table__dbt_tmp'" in tmp

    @pytest.mark.parametrize("render", ["_rest", "_glue"])
    def test_single_quotes_are_escaped(self, render):
        self._build(base_location_root="s3://bucket/team's")
        sql = getattr(self, render)(_Relation("my_table"))
        assert "base_location = 's3://bucket/team\\'s/my_schema/my_table'" in sql
