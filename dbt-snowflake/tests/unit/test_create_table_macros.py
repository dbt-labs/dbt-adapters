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


class TestSnowflakeCreateTableInfoSchemaSql(unittest.TestCase):
    def setUp(self):
        self.config = {}
        catalog_relation = SimpleNamespace(
            is_transient=False, cluster_by=None, change_tracking=True
        )
        adapter = SimpleNamespace(build_catalog_relation=lambda model: catalog_relation)
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
