import os
import re
import unittest
from types import SimpleNamespace
from unittest import mock

from dbt_common.clients.jinja import get_environment

from dbt.adapters.snowflake.column import SnowflakeColumn

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


class TestSnowflakeCreateTableIcebergRestWithGlue(unittest.TestCase):
    def setUp(self):
        self.sql_columns = []
        adapter = SimpleNamespace(
            get_relation=lambda **kwargs: None,
            quote=lambda identifier: f'"{identifier}"',
        )
        self.context = {
            "adapter": adapter,
            "config": mock.Mock(),
            "get_column_schema_from_query": lambda sql: self.sql_columns,
            "get_partition_by_keys": lambda catalog_relation: None,
        }
        self.context["config"].get = lambda key, default=None, **kwargs: default
        env = get_environment()
        with open(OPTIONAL_SQL) as f:
            optional = env.from_string(f.read(), globals=self.context).module
        self.context["optional"] = optional.dbt_macro__optional
        with open(CREATE_SQL) as f:
            self.module = env.from_string(f.read(), globals=self.context).module

    def _render(self):
        catalog_relation = SimpleNamespace(
            external_volume=None,
            iceberg_version=None,
            target_file_size=None,
            auto_refresh=None,
            max_data_extension_time_in_days=None,
        )
        sql = self.module.dbt_macro__snowflake__create_table_iceberg_rest_with_glue(
            "glue_db.schema.tbl", "select 1", catalog_relation
        )
        return re.sub(r"\s+", " ", sql).strip()

    def test_fixed_columns_keep_precision_and_scale(self):
        self.sql_columns = [
            SnowflakeColumn("PRICE", "FIXED", numeric_precision=38, numeric_scale=2),
            SnowflakeColumn("AMOUNT", "FIXED", numeric_precision=38, numeric_scale=0),
            SnowflakeColumn.create("NAME", "TEXT"),
            SnowflakeColumn.create("UPDATED_AT", "TIMESTAMP_LTZ"),
        ]
        sql = self._render()

        self.assertIn(
            'create iceberg table glue_db.schema.tbl ( "price" DECIMAL(38,2), '
            '"amount" DECIMAL(38,0), "name" STRING, "updated_at" TIMESTAMP_LTZ)',
            sql,
        )
        self.assertNotIn(" INT", sql)
