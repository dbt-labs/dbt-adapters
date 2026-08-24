import unittest
from unittest import mock
import re
from types import SimpleNamespace
from jinja2 import Environment, FileSystemLoader

from dbt.adapters.spark import SparkRelation


class TestSparkMacros(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader("src/dbt/include/spark/macros"),
            extensions=[
                "jinja2.ext.do",
            ],
        )

        self.config = {}
        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            "return": lambda r: r,
        }
        self.default_context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )

    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run_macro(self, template, name, temporary, relation, sql):
        self.default_context["model"].alias = relation

        def dispatch(macro_name, macro_namespace=None, packages=None):
            return getattr(template.module, f"spark__{macro_name}")

        self.default_context["adapter"].dispatch = dispatch

        value = getattr(template.module, name)(temporary, relation, sql)
        return re.sub(r"\s\s+", " ", value)

    def test_macros_load(self):
        self.jinja_env.get_template("adapters.sql")

    def test_list_relations_uses_catalog_qualified_namespace(self):
        def statement(*args, caller=None, **kwargs):
            return caller()

        context = {
            **self.default_context,
            "statement": statement,
            "load_result": lambda name: SimpleNamespace(table=""),
        }
        template = self.jinja_env.get_template("adapters.sql", globals=context)

        relation = mock.Mock()
        relation.without_identifier.return_value = "catalog.analytics"
        sql = template.module.spark__list_relations_without_caching(relation)
        fallback_sql = template.module.list_relations_show_tables_without_caching(relation)

        self.assertIn("show table extended in catalog.analytics like '*'", sql)
        self.assertIn("show tables in catalog.analytics like '*'", fallback_sql)
        self.assertEqual(relation.without_identifier.call_count, 2)

        string_sql = template.module.spark__list_relations_without_caching("catalog.analytics")
        self.assertIn("show table extended in catalog.analytics like '*'", string_sql)

    def test_list_schemas_uses_quoted_catalog(self):
        def statement(*args, caller=None, **kwargs):
            return caller()

        context = {
            **self.default_context,
            "statement": statement,
            "load_result": lambda name: SimpleNamespace(table=""),
        }
        context["adapter"].quote.return_value = "`catalog-name`"
        template = self.jinja_env.get_template("adapters.sql", globals=context)

        sql = template.module.spark__list_schemas("catalog-name")
        prequoted_sql = template.module.spark__list_schemas("`catalog-name`")
        default_sql = template.module.spark__list_schemas(None)

        self.assertIn("show namespaces in `catalog-name`", sql)
        self.assertIn("show namespaces in `catalog-name`", prequoted_sql)
        self.assertNotIn("``catalog-name``", prequoted_sql)
        self.assertIn("show databases", default_sql)

    def test_generate_database_name_uses_default_implementation(self):
        default_generate_database_name = mock.Mock(return_value="catalog")
        context = {
            **self.default_context,
            "default__generate_database_name": default_generate_database_name,
        }
        template = self.jinja_env.get_template("adapters.sql", globals=context)
        node = mock.Mock()

        template.module.spark__generate_database_name("catalog", node)

        default_generate_database_name.assert_called_once_with("catalog", node)

    def test_snapshot_staging_relation_preserves_catalog(self):
        captured = {}

        def statement(*args, caller=None, **kwargs):
            return caller()

        def create_view_as(relation, sql):
            captured["relation"] = relation
            return f"create view {relation} as {sql}"

        context = {
            **self.default_context,
            "api": SimpleNamespace(Relation=SparkRelation),
            "statement": statement,
            "snapshot_staging_table": lambda strategy, sql, target: "select 1",
            "create_view_as": create_view_as,
        }
        source, _, _ = self.jinja_env.loader.get_source(
            self.jinja_env, "materializations/snapshot.sql"
        )
        helper_macros = source.split("{% materialization", maxsplit=1)[0]
        template = self.jinja_env.from_string(helper_macros, globals=context)
        target_relation = SparkRelation.create(
            database="catalog-name",
            schema="analytics-name",
            identifier="events-snapshot",
            type="table",
            quote_policy={"database": True, "schema": True, "identifier": True},
        )

        template.module.spark_build_snapshot_staging_table(
            mock.Mock(), "select 1", target_relation
        )

        self.assertEqual(
            str(captured["relation"]),
            "`catalog-name`.`analytics-name`.`events-snapshot__dbt_tmp`",
        )
        self.assertEqual(captured["relation"].quote_policy, target_relation.quote_policy)

    def test_iceberg_snapshot_uses_quoted_unqualified_staging_relation(self):
        captured = {}

        def statement(*args, caller=None, **kwargs):
            return caller()

        def create_view_as(relation, sql):
            captured["relation"] = relation
            return f"create view {relation} as {sql}"

        context = {
            **self.default_context,
            "statement": statement,
            "snapshot_staging_table": lambda strategy, sql, target: "select 1",
            "create_view_as": create_view_as,
        }
        source, _, _ = self.jinja_env.loader.get_source(
            self.jinja_env, "materializations/snapshot.sql"
        )
        helper_macros = source.split("{% materialization", maxsplit=1)[0]
        template = self.jinja_env.from_string(helper_macros, globals=context)
        target_relation = SparkRelation.create(
            database="catalog-name",
            schema="analytics-name",
            identifier="events-snapshot",
            type="table",
            is_iceberg=True,
            quote_policy={"database": True, "schema": True, "identifier": True},
        )

        template.module.spark_build_snapshot_staging_table(
            mock.Mock(), "select 1", target_relation
        )

        staging_relation = captured["relation"]
        self.assertEqual(str(staging_relation), "`events-snapshot__dbt_tmp`")
        self.assertIsNone(staging_relation.database)
        self.assertIsNone(staging_relation.schema)
        self.assertEqual(staging_relation.quote_policy, target_relation.quote_policy)

        self.config["snapshot_table_column_names"] = SimpleNamespace(
            dbt_scd_id="dbt_scd_id",
            dbt_valid_to="dbt_valid_to",
        )
        merge_sql = template.module.spark__snapshot_merge_sql(
            target_relation,
            staging_relation,
            ["id"],
        )
        self.assertIn(
            "using `events-snapshot__dbt_tmp` as DBT_INTERNAL_SOURCE",
            merge_sql,
        )

    def test_macros_create_table_as(self):
        template = self.__get_template("adapters.sql")
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()

        self.assertEqual(sql, "create table my_table as select 1")

    def test_macros_create_table_as_file_format(self):
        template = self.__get_template("adapters.sql")

        self.config["file_format"] = "delta"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create or replace table my_table using delta as select 1")

        self.config["file_format"] = "hudi"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create table my_table using hudi as select 1")

    def test_macros_create_table_as_options(self):
        template = self.__get_template("adapters.sql")

        self.config["file_format"] = "delta"
        self.config["options"] = {"compression": "gzip"}
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            'create or replace table my_table using delta options (compression "gzip" ) as select 1',
        )

        self.config["file_format"] = "hudi"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql, 'create table my_table using hudi options (compression "gzip" ) as select 1'
        )

    def test_macros_create_table_as_hudi_options(self):
        template = self.__get_template("adapters.sql")

        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "id"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1 as id"
        ).strip()
        self.assertEqual(
            sql, 'create table my_table using hudi options (primaryKey "id" ) as select 1 as id'
        )

        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "id"
        self.config["options"] = {"primaryKey": "id"}
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1 as id"
        ).strip()
        self.assertEqual(
            sql, 'create table my_table using hudi options (primaryKey "id" ) as select 1 as id'
        )

        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "uuid"
        self.config["options"] = {"primaryKey": "id"}
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1 as id"
        )
        self.assertIn("mock.raise_compiler_error()", sql)

    def test_macros_create_table_as_partition(self):
        template = self.__get_template("adapters.sql")

        self.config["partition_by"] = "partition_1"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create table my_table partitioned by (partition_1) as select 1")

    def test_macros_create_table_as_partitions(self):
        template = self.__get_template("adapters.sql")

        self.config["partition_by"] = ["partition_1", "partition_2"]
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql, "create table my_table partitioned by (partition_1,partition_2) as select 1"
        )

    def test_macros_create_table_as_cluster(self):
        template = self.__get_template("adapters.sql")

        self.config["clustered_by"] = "cluster_1"
        self.config["buckets"] = "1"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql, "create table my_table clustered by (cluster_1) into 1 buckets as select 1"
        )

    def test_macros_create_table_as_clusters(self):
        template = self.__get_template("adapters.sql")

        self.config["clustered_by"] = ["cluster_1", "cluster_2"]
        self.config["buckets"] = "1"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create table my_table clustered by (cluster_1,cluster_2) into 1 buckets as select 1",
        )

    def test_macros_create_table_as_location(self):
        template = self.__get_template("adapters.sql")

        self.config["location_root"] = "/mnt/root"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create table my_table location '/mnt/root/my_table' as select 1")

    def test_macros_create_table_as_comment(self):
        template = self.__get_template("adapters.sql")

        self.config["persist_docs"] = {"relation": True}
        self.default_context["model"].description = "Description Test"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create table my_table comment 'Description Test' as select 1")

    def test_macros_create_table_as_all(self):
        template = self.__get_template("adapters.sql")

        self.config["file_format"] = "delta"
        self.config["location_root"] = "/mnt/root"
        self.config["partition_by"] = ["partition_1", "partition_2"]
        self.config["clustered_by"] = ["cluster_1", "cluster_2"]
        self.config["buckets"] = "1"
        self.config["persist_docs"] = {"relation": True}
        self.default_context["model"].description = "Description Test"

        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table using delta partitioned by (partition_1,partition_2) clustered by (cluster_1,cluster_2) into 1 buckets location '/mnt/root/my_table' comment 'Description Test' as select 1",
        )

        self.config["file_format"] = "hudi"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create table my_table using hudi partitioned by (partition_1,partition_2) clustered by (cluster_1,cluster_2) into 1 buckets location '/mnt/root/my_table' comment 'Description Test' as select 1",
        )
