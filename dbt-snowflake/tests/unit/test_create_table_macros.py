import os
import re
import unittest
from types import SimpleNamespace
from unittest import mock

from dbt_common.clients.jinja import get_environment

CREATE_SQL = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "../../src/dbt/include/snowflake/macros/relations/table/create.sql",
    )
)
OPTIONAL_SQL = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "../../src/dbt/include/snowflake/macros/utils/optional.sql",
    )
)
APPLY_GRANTS_SQL = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "../../src/dbt/include/snowflake/macros/apply_grants.sql",
    )
)


class TestSnowflakeCreateTableInfoSchemaSql(unittest.TestCase):
    def setUp(self):
        self.config = {}
        catalog_relation = SimpleNamespace(
            is_transient=False, cluster_by=None, change_tracking=True
        )
        self.copy_grants_default = False
        adapter = SimpleNamespace(
            build_catalog_relation=lambda model: catalog_relation,
        )
        flag = mock.Mock()
        type(flag).no_warn = mock.PropertyMock(side_effect=lambda: self.copy_grants_default)
        adapter.behavior = SimpleNamespace(snowflake_copy_grants_default=flag)
        self.context = {
            "adapter": adapter,
            "config": mock.Mock(),
            "model": SimpleNamespace(),
        }
        self.context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )
        env = get_environment()
        with open(OPTIONAL_SQL) as f:
            optional = env.from_string(f.read(), globals=self.context).module
        self.context["optional"] = optional.dbt_macro__optional
        with open(APPLY_GRANTS_SQL) as f:
            apply_grants = env.from_string(f.read(), globals=self.context).module
        self.context["snowflake__copy_grants_clause"] = (
            apply_grants.dbt_macro__snowflake__copy_grants_clause
        )
        with open(CREATE_SQL) as f:
            self.module = env.from_string(f.read(), globals=self.context).module

    def _render(self):
        sql = self.module.dbt_macro__snowflake__create_table_info_schema_sql(
            "db.schema.tbl", "select 1 as id"
        )
        return re.sub(r"\s+", " ", sql).strip().lower()

    def test_change_tracking_precedes_policies_and_copy_clauses(self):
        self.config = {
            "copy_grants": True,
            "copy_tags": True,
            "row_access_policy": "my_policy on (id)",
            "table_tag": "env = 'prod'",
        }
        sql = self._render()

        positions = [
            sql.index(clause)
            for clause in (
                "change_tracking",
                "copy grants",
                "copy tags",
                "with row access policy",
                "with tag",
                "as (",
            )
        ]
        self.assertEqual(positions, sorted(positions))

    def test_copy_grants_true_emits_copy_grants(self):
        self.config = {"copy_grants": True}
        sql = self._render()
        self.assertIn("copy grants", sql)
        self.assertNotIn("ignore grants", sql)

    def test_copy_grants_false_emits_ignore_grants(self):
        self.config = {"copy_grants": False}
        sql = self._render()
        self.assertIn("ignore grants", sql)
        self.assertNotIn("copy grants", sql)

    def test_unset_copy_grants_emits_ignore_grants_by_default(self):
        sql = self._render()
        self.assertIn("ignore grants", sql)

    def test_unset_copy_grants_defers_to_snowflake_when_flag_enabled(self):
        self.copy_grants_default = True
        sql = self._render()
        self.assertNotIn("ignore grants", sql)
        self.assertNotIn("copy grants", sql)

    def test_explicit_false_still_emits_ignore_grants_when_flag_enabled(self):
        self.copy_grants_default = True
        self.config = {"copy_grants": False}
        self.assertIn("ignore grants", self._render())

    def test_ignore_grants_precedes_copy_tags(self):
        self.config = {"copy_tags": True}
        sql = self._render()
        self.assertLess(sql.index("ignore grants"), sql.index("copy tags"))
