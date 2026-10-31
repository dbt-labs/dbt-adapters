import os
import re
import unittest
from types import SimpleNamespace
from unittest import mock

from dbt_common.clients.jinja import get_environment

from dbt.adapters.snowflake import constants
from dbt.adapters.snowflake.relation import SnowflakeRelation

APPLY_GRANTS_SQL = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "../../src/dbt/include/snowflake/macros/apply_grants.sql",
    )
)
CLONE_SQL = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "../../src/dbt/include/snowflake/macros/materializations/clone.sql",
    )
)
ICEBERG = constants.ICEBERG_TABLE_FORMAT
DEFAULT = constants.INFO_SCHEMA_TABLE_FORMAT


class _MacroReturn(Exception):
    """Stand-in for dbt's `return`, which unwinds the macro rather than emitting a value."""

    def __init__(self, value):
        self.value = value


def _relation(database, type="table", table_format=DEFAULT, **quote_policy):
    return SnowflakeRelation.create(
        database=database,
        schema="analytics",
        identifier="my_model",
        type=type,
        table_format=table_format,
    ).quote(**quote_policy)


class TestSnowflakeCloneMacros(unittest.TestCase):
    def setUp(self):
        self.config = {}
        self.cached = {}
        self.dropped = []
        self.statements = {}
        self.persisted = []
        self.flags = SimpleNamespace(FULL_REFRESH=True)

        def _return(value):
            raise _MacroReturn(value)

        def _statement(name, caller):
            self.statements[name] = re.sub(r"\s+", " ", caller()).strip()
            return ""

        self.context = {
            "return": _return,
            "config": mock.Mock(),
            "log": lambda *args, **kwargs: "",
            "flags": self.flags,
            "model": SimpleNamespace(unique_id="model.test.my_model"),
            "load_cached_relation": lambda relation: self.cached.get(relation.render()),
            "can_clone_table": lambda: True,
            "drop_relation_if_exists": lambda relation: self.dropped.append(relation) or "",
            "statement": _statement,
            "should_revoke": lambda *args, **kwargs: False,
            "apply_grants": lambda *args, **kwargs: None,
            "persist_docs": lambda relation, model: self.persisted.append(relation),
            "snowflake__get_clone_target_relation": self._macro(
                "snowflake__get_clone_target_relation"
            ),
            "snowflake__clone_target_drop_reason": self._macro(
                "snowflake__clone_target_drop_reason"
            ),
            "create_or_replace_clone": self._macro("snowflake__create_or_replace_clone"),
        }
        self.context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )
        flag = mock.Mock()
        type(flag).no_warn = mock.PropertyMock(return_value=False)
        self.context["adapter"] = SimpleNamespace(
            behavior=SimpleNamespace(snowflake_copy_grants_default=flag)
        )
        env = get_environment()
        with open(APPLY_GRANTS_SQL) as f:
            apply_grants = env.from_string(f.read(), globals=self.context).module
        self.context["snowflake__copy_grants_clause"] = (
            apply_grants.dbt_macro__snowflake__copy_grants_clause
        )
        with open(CLONE_SQL) as f:
            self.module = env.from_string(f.read(), globals=self.context).module

    def _macro(self, name):
        def call(*args, **kwargs):
            try:
                return getattr(self.module, f"dbt_macro__{name}")(*args, **kwargs)
            except _MacroReturn as macro_return:
                return macro_return.value

        return call

    def _clone(self, source, existing=None):
        this = _relation("target_db", type=None, identifier=True)
        self.cached = {source.render(): source}
        if existing is not None:
            self.cached[this.render()] = existing
        self.context.update(this=this, defer_relation=source)
        return self._macro("materialization_clone_snowflake")()

    def test_target_inherits_source_format_with_target_path_and_quoting(self):
        this = _relation("target_db", type=None, identifier=True)
        for table_format in (ICEBERG, DEFAULT):
            with self.subTest(table_format=table_format):
                source = _relation("source_db", table_format=table_format)
                target = self._macro("snowflake__get_clone_target_relation")(this, source)

                self.assertEqual(target.table_format, table_format)
                self.assertEqual(target.type, "table")
                self.assertEqual(target.path, this.path)
                self.assertEqual(target.quote_policy, this.quote_policy)

    def test_drop_reason(self):
        drop_reason = self._macro("snowflake__clone_target_drop_reason")
        cases = [
            ("view", DEFAULT, DEFAULT, "it is of type view"),
            (
                "table",
                DEFAULT,
                ICEBERG,
                "it is a regular table and the clone source is an Iceberg table",
            ),
            (
                "table",
                ICEBERG,
                DEFAULT,
                "it is an Iceberg table and the clone source is a regular table",
            ),
            ("table", ICEBERG, ICEBERG, None),
            ("table", DEFAULT, DEFAULT, None),
        ]
        for existing_type, existing_format, target_format, expected in cases:
            with self.subTest(existing=(existing_type, existing_format), target=target_format):
                existing = _relation("target_db", type=existing_type, table_format=existing_format)
                target = _relation("target_db", table_format=target_format)
                self.assertEqual(drop_reason(existing, target), expected)

    def test_create_or_replace_clone_ddl(self):
        create_or_replace_clone = self._macro("snowflake__create_or_replace_clone")
        source = _relation("source_db")
        cases = [
            (ICEBERG, {}, "create or replace iceberg table"),
            (DEFAULT, {}, "create or replace transient table"),
            (DEFAULT, {"transient": False}, "create or replace table"),
        ]
        for table_format, config, expected in cases:
            with self.subTest(table_format=table_format, config=config):
                self.config = config
                target = _relation("target_db", table_format=table_format)
                sql = re.sub(r"\s+", " ", create_or_replace_clone(target, source)).strip()
                self.assertTrue(sql.startswith(f"{expected} target_db.analytics.my_model"), sql)

    def test_clone_grants_clause(self):
        create_or_replace_clone = self._macro("snowflake__create_or_replace_clone")
        source = _relation("source_db")
        target = _relation("target_db")
        cases = [
            ({}, "ignore grants"),
            ({"copy_grants": False}, "ignore grants"),
            ({"copy_grants": True}, "copy grants"),
        ]
        for config, expected in cases:
            with self.subTest(config=config):
                self.config = config
                sql = re.sub(r"\s+", " ", create_or_replace_clone(target, source)).strip()
                self.assertIn(f"clone source_db.analytics.my_model {expected}", sql)

    def test_full_refresh_drops_regular_table_and_clones_as_iceberg(self):
        existing = _relation("target_db")
        result = self._clone(_relation("source_db", table_format=ICEBERG), existing)

        self.assertEqual(self.dropped, [existing])
        self.assertIn("create or replace iceberg table", self.statements["main"])
        (target,) = result["relations"]
        self.assertTrue(target.is_iceberg_format)
        self.assertEqual(self.persisted, [target])

    def test_full_refresh_drops_iceberg_table_and_clones_as_regular(self):
        existing = _relation("target_db", table_format=ICEBERG)
        result = self._clone(_relation("source_db"), existing)

        self.assertEqual(self.dropped, [existing])
        self.assertIn("create or replace transient table", self.statements["main"])
        self.assertFalse(result["relations"][0].is_iceberg_format)

    def test_full_refresh_same_format_replaces_without_drop(self):
        result = self._clone(
            _relation("source_db", table_format=ICEBERG),
            _relation("target_db", table_format=ICEBERG),
        )

        self.assertEqual(self.dropped, [])
        self.assertIn("create or replace iceberg table", self.statements["main"])
        self.assertTrue(result["relations"][0].is_iceberg_format)

    def test_existing_target_without_full_refresh_is_noop(self):
        self.flags.FULL_REFRESH = False
        result = self._clone(_relation("source_db", table_format=ICEBERG), _relation("target_db"))

        self.assertEqual(result, {"relations": []})
        self.assertEqual(self.statements, {})
        self.assertEqual(self.dropped, [])
