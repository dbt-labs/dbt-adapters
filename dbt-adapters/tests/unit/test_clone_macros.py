import os
import re
import unittest
from types import SimpleNamespace
from unittest import mock

from dbt_common.clients.jinja import get_environment

from dbt.adapters.base.relation import BaseRelation

CLONE_SQL = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "../../src/dbt/include/global_project/macros/materializations/models/clone/clone.sql",
    )
)


class _MacroReturn(Exception):
    """Stand-in for dbt's `return`, which unwinds the macro rather than emitting a value."""

    def __init__(self, value):
        self.value = value


def _relation(database, type="table"):
    return BaseRelation.create(
        database=database,
        schema="analytics",
        identifier="my_model",
        type=type,
    )


class TestDefaultCloneMacros(unittest.TestCase):
    def setUp(self):
        self.config = {}
        self.cached = {}
        self.dropped = []
        self.statements = {}
        self.persisted = []
        self.view_calls = []
        self.logs = []
        self.can_clone = True
        self.flags = SimpleNamespace(FULL_REFRESH=True)

        def _return(value):
            raise _MacroReturn(value)

        def _statement(name, caller):
            self.statements[name] = re.sub(r"\s+", " ", caller()).strip()
            return ""

        def _view_macro():
            self.view_calls.append(1)
            return {"relations": ["view-relation"]}

        self.context = {
            "return": _return,
            "config": mock.Mock(),
            "log": lambda *args, **kwargs: self.logs.append(args[0]) or "",
            "flags": self.flags,
            "model": SimpleNamespace(unique_id="model.test.my_model"),
            "load_cached_relation": lambda relation: self.cached.get(relation.render()),
            "can_clone_table": lambda: self.can_clone,
            "drop_relation_if_exists": lambda relation: self.dropped.append(relation) or "",
            "statement": _statement,
            "should_revoke": lambda *args, **kwargs: False,
            "apply_grants": lambda *args, **kwargs: None,
            "persist_docs": lambda relation, model: self.persisted.append(relation),
            "create_or_replace_clone": lambda target, source: (
                f"create or replace table {target} clone {source}"
            ),
            "adapter": SimpleNamespace(type=lambda: "postgres"),
            "context": {"materialization_view_default": _view_macro},
        }
        self.context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )
        env = get_environment()
        with open(CLONE_SQL) as f:
            self.module = env.from_string(f.read(), globals=self.context).module

    def _macro(self, name):
        def call(*args, **kwargs):
            try:
                return getattr(self.module, f"dbt_macro__{name}")(*args, **kwargs)
            except _MacroReturn as macro_return:
                return macro_return.value

        return call

    def _clone(self, source, existing=None, can_clone=True):
        self.can_clone = can_clone
        this = _relation("target_db")
        self.cached = {source.render(): source}
        if existing is not None:
            self.cached[this.render()] = existing
        self.context.update(this=this, defer_relation=source)
        return self._macro("materialization_clone_default")()

    def test_same_target_without_native_clone_skips_view_fallback(self):
        result = self._clone(_relation("target_db"), _relation("target_db"), can_clone=False)

        self.assertEqual(self.view_calls, [])
        self.assertEqual(self.statements, {})
        self.assertEqual(self.dropped, [])
        self.assertTrue(any("skipping clone for relation" in log for log in self.logs), self.logs)
        (target,) = result["relations"]
        self.assertTrue(target.is_view)
        self.assertEqual(target.render(), _relation("target_db").render())
        self.assertEqual(self.persisted, [target])

    def test_same_target_with_native_clone_skips_clone(self):
        result = self._clone(_relation("target_db"), _relation("target_db"))

        self.assertEqual(self.view_calls, [])
        self.assertEqual(self.statements, {})
        self.assertEqual(self.dropped, [])
        self.assertTrue(any("skipping clone for relation" in log for log in self.logs), self.logs)
        (target,) = result["relations"]
        self.assertTrue(target.is_table)
        self.assertEqual(self.persisted, [target])

    def test_same_target_does_not_drop_existing_view(self):
        self._clone(_relation("target_db"), _relation("target_db", type="view"), can_clone=True)

        self.assertEqual(self.dropped, [])
        self.assertTrue(any("skipping clone for relation" in log for log in self.logs), self.logs)

    def test_different_target_without_native_clone_uses_view_fallback(self):
        result = self._clone(_relation("source_db"), _relation("target_db"), can_clone=False)

        self.assertEqual(self.view_calls, [1])
        self.assertEqual(result, {"relations": ["view-relation"]})

    def test_different_target_with_native_clone_runs_clone_statement(self):
        result = self._clone(_relation("source_db"), _relation("target_db"))

        self.assertEqual(self.view_calls, [])
        self.assertIn("create or replace table", self.statements["main"])
        self.assertIn("clone", self.statements["main"])
        (target,) = result["relations"]
        self.assertTrue(target.is_table)

    def test_different_target_with_native_clone_but_view_source_uses_view_fallback(self):
        result = self._clone(
            _relation("source_db", type="view"), _relation("target_db"), can_clone=True
        )

        self.assertEqual(self.view_calls, [1])
        self.assertEqual(result, {"relations": ["view-relation"]})

    def test_existing_target_without_full_refresh_is_noop(self):
        self.flags.FULL_REFRESH = False
        result = self._clone(_relation("source_db"), _relation("target_db"))

        self.assertEqual(result, {"relations": []})
        self.assertEqual(self.statements, {})
        self.assertEqual(self.view_calls, [])
        self.assertEqual(self.dropped, [])

    def test_no_defer_relation_is_noop(self):
        self.context.update(this=_relation("target_db"), defer_relation=None)
        result = self._macro("materialization_clone_default")()

        self.assertEqual(result, {"relations": []})
        self.assertEqual(self.statements, {})
        self.assertEqual(self.view_calls, [])
