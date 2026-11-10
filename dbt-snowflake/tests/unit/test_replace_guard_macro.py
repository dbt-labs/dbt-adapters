import os
import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


class _Behavior:
    """Stands in for dbt_common's Behavior and records whether the flag was evaluated,
    because evaluating it (without no_warn) is what fires the behavior-change notice."""

    def __init__(self, enabled):
        self._enabled = enabled
        self.evaluated = False

    @property
    def snowflake_fail_on_unexpected_existing_relation(self):
        self.evaluated = True
        return self._enabled


class TestShouldReplaceOnCreate(unittest.TestCase):
    """The macro decides whether CTAS may use OR REPLACE. Returns (replace, flag_evaluated)."""

    @staticmethod
    def _relation(database="DB", schema="SCH", identifier="MODEL"):
        return mock.Mock(database=database, schema=schema, identifier=identifier)

    def _decide(self, materialized, cached, full_refresh=False, flag=True, relation=None):
        returned = []
        behavior = _Behavior(flag)
        config_values = {"materialized": materialized}
        context = {
            "config": mock.Mock(get=lambda key, default=None: config_values.get(key, default)),
            "adapter": mock.Mock(behavior=behavior),
            "should_full_refresh": lambda: full_refresh,
            "load_cached_relation": lambda relation: mock.Mock() if cached else None,
            "return": returned.append,
            "this": self._relation(),
        }
        env = Environment(loader=FileSystemLoader(MACROS_DIR), extensions=["jinja2.ext.do"])
        template = env.get_template("relations/table/replace_guard.sql", globals=context)
        template.module.snowflake__should_replace_on_create(relation or self._relation())
        self.assertEqual(len(returned), 1, "macro must return exactly once")
        return returned[0], behavior.evaluated

    def test_incremental_not_in_cache_creates_without_replace(self):
        self.assertEqual(self._decide("incremental", cached=False), (False, True))

    def test_snapshot_not_in_cache_creates_without_replace(self):
        self.assertEqual(self._decide("snapshot", cached=False), (False, True))

    def test_flag_off_keeps_replace_and_evaluates_flag_for_notice(self):
        self.assertEqual(self._decide("incremental", cached=False, flag=False), (True, True))

    def test_cached_relation_is_replaced_without_reading_flag(self):
        self.assertEqual(self._decide("incremental", cached=True), (True, False))

    def test_incremental_full_refresh_is_replaced_without_reading_flag(self):
        self.assertEqual(
            self._decide("incremental", cached=False, full_refresh=True), (True, False)
        )

    def test_snapshot_ignores_full_refresh(self):
        self.assertEqual(self._decide("snapshot", cached=False, full_refresh=True), (False, True))

    def test_table_model_is_replaced_without_reading_flag(self):
        self.assertEqual(self._decide("table", cached=False), (True, False))

    def test_store_failures_is_replaced_without_reading_flag(self):
        self.assertEqual(self._decide("test", cached=False), (True, False))

    def test_auxiliary_relation_is_replaced_without_reading_flag(self):
        helper = self._relation(identifier="MODEL__HELPER")
        self.assertEqual(self._decide("incremental", cached=False, relation=helper), (True, False))

    def test_relation_in_other_schema_is_replaced_without_reading_flag(self):
        elsewhere = self._relation(schema="OTHER")
        self.assertEqual(self._decide("snapshot", cached=False, relation=elsewhere), (True, False))

    def test_target_matched_case_insensitively(self):
        target = self._relation(database="db", schema="sch", identifier="model")
        self.assertEqual(self._decide("incremental", cached=False, relation=target), (False, True))


class TestShouldReplaceOnCreateDispatcher(unittest.TestCase):
    """The unprefixed macro is the overridable entry point and dispatches like other dbt macros."""

    def test_dispatches_to_adapter_implementation(self):
        returned = []
        relation = mock.Mock()
        implementation = mock.Mock(return_value=False)
        adapter = mock.Mock(**{"dispatch.return_value": implementation})
        env = Environment(loader=FileSystemLoader(MACROS_DIR), extensions=["jinja2.ext.do"])
        template = env.get_template(
            "relations/table/replace_guard.sql",
            globals={"adapter": adapter, "return": returned.append},
        )
        template.module.should_replace_on_create(relation)
        adapter.dispatch.assert_called_once_with("should_replace_on_create", "dbt")
        implementation.assert_called_once_with(relation)
        self.assertEqual(returned, [False])
