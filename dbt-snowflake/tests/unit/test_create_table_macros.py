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
        adapter = SimpleNamespace(
            build_catalog_relation=lambda model: catalog_relation,
        )
        self.context = {
            "adapter": adapter,
            "config": mock.Mock(),
            "model": SimpleNamespace(),
            "should_replace_on_create": lambda relation: True,
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

    def test_unset_copy_grants_emits_ignore_grants(self):
        sql = self._render()
        self.assertIn("ignore grants", sql)

    def test_ignore_grants_precedes_copy_tags(self):
        self.config = {"copy_tags": True}
        sql = self._render()
        self.assertLess(sql.index("ignore grants"), sql.index("copy tags"))


class TestSnowflakeCreateTableBuiltInSql(unittest.TestCase):
    """CREATE ICEBERG TABLE (Snowflake as the Iceberg catalog) places COPY GRANTS and
    COPY TAGS before the ROW ACCESS POLICY and TAG clauses:
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake
    """

    def setUp(self):
        self.config = {}
        catalog_relation = SimpleNamespace(
            external_volume="my_volume",
            base_location="my/base/location",
            iceberg_version=None,
            storage_serialization_policy=None,
            max_data_extension_time_in_days=None,
            data_retention_time_in_days=None,
            change_tracking=None,
            cluster_by=None,
            automatic_clustering=False,
        )
        adapter = SimpleNamespace(
            build_catalog_relation=lambda model: catalog_relation,
        )
        self.context = {
            "adapter": adapter,
            "config": mock.Mock(),
            "model": SimpleNamespace(),
            "get_partition_by_keys": lambda catalog_relation: None,
            "should_replace_on_create": lambda relation: True,
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
        sql = self.module.dbt_macro__snowflake__create_table_built_in_sql(
            "db.schema.tbl", "select 1 as id"
        )
        return re.sub(r"\s+", " ", sql).strip().lower()

    def test_copy_clauses_precede_policies(self):
        self.config = {
            "copy_grants": True,
            "copy_tags": True,
            "row_access_policy": "my_policy on (id)",
            "table_tag": "env = 'prod'",
        }
        sql = self._render()

        self.assertIn("create or replace iceberg table db.schema.tbl", sql)
        positions = [
            sql.index(clause)
            for clause in (
                "base_location",
                "copy grants",
                "copy tags",
                "with row access policy my_policy on (id)",
                "with tag (env = 'prod')",
                "as (",
            )
        ]
        self.assertEqual(positions, sorted(positions), sql)

    def test_ignore_grants_precedes_policies(self):
        self.config = {
            "row_access_policy": "my_policy on (id)",
            "table_tag": "env = 'prod'",
        }
        sql = self._render()

        self.assertNotIn("copy grants", sql)
        self.assertLess(sql.index("ignore grants"), sql.index("with row access policy"))
        self.assertLess(sql.index("ignore grants"), sql.index("with tag"))


class TestSnowflakeCreateTableIcebergRestSql(unittest.TestCase):
    """CREATE ICEBERG TABLE in a catalog-linked database places COPY GRANTS before the
    ROW ACCESS POLICY and TAG clauses:
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-rest
    """

    def setUp(self):
        self.config = {}
        catalog_relation = SimpleNamespace(
            external_volume=None,
            iceberg_version=None,
            target_file_size=None,
            auto_refresh=None,
            max_data_extension_time_in_days=None,
        )
        adapter = SimpleNamespace(
            build_catalog_relation=lambda model: catalog_relation,
            get_relation=lambda **kwargs: None,
        )
        self.context = {
            "adapter": adapter,
            "config": mock.Mock(),
            "model": SimpleNamespace(),
            "get_partition_by_keys": lambda catalog_relation: None,
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
        sql = self.module.dbt_macro__snowflake__create_table_iceberg_rest_sql(
            "cld.schema.tbl", "select 1 as id"
        )
        return re.sub(r"\s+", " ", sql).strip().lower()

    def test_copy_grants_precedes_policies(self):
        self.config = {
            "copy_grants": True,
            "row_access_policy": "my_policy on (id)",
            "table_tag": "env = 'prod'",
        }
        sql = self._render()

        self.assertIn("create iceberg table cld.schema.tbl", sql)
        positions = [
            sql.index(clause)
            for clause in (
                "copy grants",
                "with row access policy my_policy on (id)",
                "with tag (env = 'prod')",
                "as (",
            )
        ]
        self.assertEqual(positions, sorted(positions), sql)

    def test_without_copy_grants(self):
        self.config = {"table_tag": "env = 'prod'"}
        sql = self._render()

        self.assertNotIn("copy grants", sql)
        self.assertIn("with tag (env = 'prod') as (", sql)


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
        self.config = {}
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
            'create iceberg table glue_db.schema.tbl ( "price" NUMBER(38,2), '
            '"amount" NUMBER(38,0), "name" STRING, "updated_at" TIMESTAMP_LTZ)',
            sql,
        )
        self.assertNotIn(" INT", sql)

    def test_copy_grants_precedes_policies(self):
        self.sql_columns = [SnowflakeColumn.create("ID", "NUMBER")]
        self.config = {
            "copy_grants": True,
            "row_access_policy": "my_policy on (id)",
            "table_tag": "env = 'prod'",
        }
        sql = self._render()

        positions = [
            sql.index(clause)
            for clause in (
                "copy grants",
                "with row access policy my_policy on (id)",
                "with tag (env = 'prod')",
                "; insert into",
            )
        ]
        self.assertEqual(positions, sorted(positions), sql)
