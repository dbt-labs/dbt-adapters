import os
import re
import unittest
from unittest import mock

from dbt_common.clients.jinja import get_environment

APPLY_GRANTS_SQL = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "../../src/dbt/include/snowflake/macros/apply_grants.sql",
    )
)

UNSET = object()


class _MacroReturn(Exception):
    """Stand-in for dbt's `return`, which unwinds the macro rather than emitting a value."""

    def __init__(self, value):
        self.value = value


def _return(value):
    raise _MacroReturn(value)


class TestSnowflakeCopyGrantsMacros(unittest.TestCase):
    def setUp(self):
        self.config = {}
        config = mock.Mock()
        config.get = lambda key, default=None, **kwargs: self.config.get(key, default)
        context = {"config": config, "return": _return}
        with open(APPLY_GRANTS_SQL) as f:
            self.module = get_environment().from_string(f.read(), globals=context).module

    def _copy_grants(self):
        try:
            self.module.dbt_macro__snowflake__copy_grants()
        except _MacroReturn as returned:
            return returned.value
        self.fail("snowflake__copy_grants did not return a value")

    def _set(self, copy_grants):
        self.config = {} if copy_grants is UNSET else {"copy_grants": copy_grants}

    def test_copy_grants_clause(self):
        cases = [(UNSET, ""), (None, ""), (True, "copy grants"), (False, "ignore grants")]
        for copy_grants, expected in cases:
            with self.subTest(copy_grants=copy_grants):
                self._set(copy_grants)
                clause = re.sub(
                    r"\s+", " ", self.module.dbt_macro__snowflake__copy_grants_clause()
                ).strip()
                self.assertEqual(clause, expected)

    def test_copy_grants_drives_revoke_unless_grants_were_ignored(self):
        cases = [(UNSET, True), (None, True), (True, True), (False, False)]
        for copy_grants, expected in cases:
            with self.subTest(copy_grants=copy_grants):
                self._set(copy_grants)
                self.assertIs(self._copy_grants(), expected)
