from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import google.api_core.exceptions
import jinja2
import pytest

from dbt.adapters.bigquery import BigQueryAdapter
from dbt.adapters.bigquery.relation import BigQueryRelation, is_lakehouse_schema
from dbt.adapters.contracts.relation import RelationType
from dbt_common.exceptions import CompilationError, DbtDatabaseError, DbtRuntimeError


def lakehouse_relation(identifier="my_table", type=RelationType.Table):
    return BigQueryRelation.create(
        database="my-project",
        schema="my_catalog.my_namespace",
        identifier=identifier,
        type=type,
    )


def standard_relation(identifier="my_table", type=RelationType.Table):
    return BigQueryRelation.create(
        database="my-project",
        schema="my_dataset",
        identifier=identifier,
        type=type,
    )


class TestLakehouseRelation:
    def test_is_lakehouse_for_dotted_schema(self):
        assert lakehouse_relation().is_lakehouse is True

    def test_is_not_lakehouse_for_regular_schema(self):
        assert standard_relation().is_lakehouse is False

    def test_is_not_lakehouse_without_schema(self):
        relation = BigQueryRelation.create(database="my-project", identifier="my_table")
        assert relation.is_lakehouse is False

    def test_is_lakehouse_schema_predicate(self):
        assert is_lakehouse_schema("my_catalog.my_namespace") is True
        assert is_lakehouse_schema("my_dataset") is False
        assert is_lakehouse_schema(None) is False

    def test_render_matches_stock_form(self):
        # BigQuery normalizes backticked paths, so the stock per-component
        # rendering is a valid four-part reference
        assert lakehouse_relation().render() == "`my-project`.`my_catalog.my_namespace`.`my_table`"

    def test_lakehouse_table_cannot_be_renamed(self):
        assert standard_relation().can_be_renamed is True
        assert lakehouse_relation().can_be_renamed is False

    @pytest.mark.parametrize(
        "schema",
        [
            ".test_namespace",
            "test_catalog.",
            ".",
            "a..b",
            "a.b.c",
            "test_catalog. test_namespace",
        ],
    )
    def test_invalid_dotted_schema_fails_when_relation_is_created(self, schema):
        with pytest.raises(CompilationError, match="catalog.namespace"):
            BigQueryRelation.create(
                database="my-project",
                schema=schema,
                identifier="my_table",
            )


class TestLakehouseSchemaNaming:
    @staticmethod
    def _generate_schema_name():
        macro_root = Path(__file__).parents[2] / "src/dbt/include/bigquery/macros"
        environment = jinja2.Environment(
            loader=jinja2.FileSystemLoader(macro_root),
            extensions=["jinja2.ext.do"],
        )
        template = environment.get_template("get_custom_name/get_custom_schema.sql")

        def default_generate_schema_name(custom_schema_name, _node):
            if custom_schema_name is None:
                return "target_schema"
            return f"target_schema_{custom_schema_name.strip()}"

        def raise_compiler_error(message):
            raise DbtRuntimeError(message)

        module = template.make_module(
            {
                "default__generate_schema_name": default_generate_schema_name,
                "exceptions": SimpleNamespace(raise_compiler_error=raise_compiler_error),
            }
        )
        return module.bigquery__generate_schema_name

    @pytest.mark.parametrize(
        ("custom_schema_name", "expected"),
        [
            (None, "target_schema"),
            ("analytics", "target_schema_analytics"),
            (" test_catalog.test_namespace ", "test_catalog.test_namespace"),
        ],
    )
    def test_schema_name_generation(self, custom_schema_name, expected):
        result = self._generate_schema_name()(custom_schema_name, None)
        assert result.strip() == expected

    @pytest.mark.parametrize(
        "schema_name",
        [
            ".test_namespace",
            "test_catalog.",
            ".",
            "a..b",
            "a.b.c",
            "test_catalog. test_namespace",
            "test_catalog .test_namespace",
        ],
    )
    def test_invalid_lakehouse_schema_raises(self, schema_name):
        with pytest.raises(DbtRuntimeError, match="catalog.namespace"):
            self._generate_schema_name()(schema_name, None)


class TestLakehouseTempSchemaValidation:
    @staticmethod
    def _validate(configured_temp_schema, target_schema="scratch_dataset"):
        macro_root = Path(__file__).parents[2] / "src/dbt/include/bigquery/macros"
        environment = jinja2.Environment(
            loader=jinja2.FileSystemLoader(macro_root),
            extensions=["jinja2.ext.do"],
        )
        template = environment.get_template("adapters.sql")

        def raise_compiler_error(message):
            raise DbtRuntimeError(message)

        config = {}
        if configured_temp_schema is not None:
            config["temp_schema"] = configured_temp_schema
        module = template.make_module(
            {
                "config": config,
                "target": SimpleNamespace(schema=target_schema),
                "exceptions": SimpleNamespace(raise_compiler_error=raise_compiler_error),
            }
        )
        module.bigquery__validate_lakehouse_temp_schema(SimpleNamespace(is_lakehouse=True))

    @pytest.mark.parametrize("temp_schema", ["scratch_dataset", "scratch_123"])
    def test_plain_temp_schema_is_accepted(self, temp_schema):
        self._validate(temp_schema)

    @pytest.mark.parametrize(
        "temp_schema",
        ["catalog.namespace", "", " scratch", "scratch ", 123],
    )
    def test_invalid_explicit_temp_schema_is_rejected(self, temp_schema):
        with pytest.raises(DbtRuntimeError, match="regular dataset"):
            self._validate(temp_schema)

    def test_dotted_default_target_schema_is_rejected(self):
        with pytest.raises(DbtRuntimeError, match="regular dataset"):
            self._validate(None, target_schema="catalog.namespace")


class TestLakehouseGuards:
    """The guards are the first statements of their methods, so they can be
    exercised as unbound calls with a mocked adapter instance."""

    def test_rename_relation_raises(self):
        with pytest.raises(DbtRuntimeError, match="rename"):
            BigQueryAdapter.rename_relation(
                MagicMock(), lakehouse_relation(), standard_relation("other")
            )

    def test_copy_table_raises_for_scalar_source(self):
        with pytest.raises(DbtRuntimeError, match="copy"):
            BigQueryAdapter.copy_table(
                MagicMock(), standard_relation(), lakehouse_relation(), "table"
            )

    def test_copy_table_raises_for_list_source(self):
        # the copy materialization passes `source` as a list of relations;
        # the guard must inspect the elements, not the list object
        with pytest.raises(DbtRuntimeError, match="copy"):
            BigQueryAdapter.copy_table(
                MagicMock(), [lakehouse_relation()], standard_relation("dest"), "table"
            )

    def test_drop_schema_raises(self):
        with pytest.raises(DbtRuntimeError, match="not supported"):
            BigQueryAdapter.drop_schema(MagicMock(), lakehouse_relation())


class TestLakehouseSchemaLifecycle:
    def test_create_schema_noops_when_namespace_exists(self):
        mock_self = MagicMock()
        mock_self.check_schema_exists.return_value = True

        BigQueryAdapter.create_schema(mock_self, lakehouse_relation())

        mock_self.check_schema_exists.assert_called_once_with(
            "my-project", "my_catalog.my_namespace"
        )
        mock_self.execute_macro.assert_not_called()

    def test_create_schema_raises_when_namespace_missing(self):
        mock_self = MagicMock()
        mock_self.check_schema_exists.return_value = False

        with pytest.raises(DbtRuntimeError, match="does not exist"):
            BigQueryAdapter.create_schema(mock_self, lakehouse_relation())

    def test_check_schema_exists_uses_datasets_get_for_lakehouse(self):
        # a tables.list probe is unreliable for EMPTY namespaces
        mock_self = MagicMock()
        client = mock_self.connections.get_thread_connection.return_value.handle

        assert (
            BigQueryAdapter.check_schema_exists(mock_self, "my-project", "my_catalog.my_ns")
            is True
        )
        client.get_dataset.assert_called_once()
        client.list_tables.assert_not_called()

        client.get_dataset.side_effect = google.api_core.exceptions.NotFound("missing")
        assert (
            BigQueryAdapter.check_schema_exists(mock_self, "my-project", "my_catalog.my_ns")
            is False
        )

    def test_get_relation_skips_routine_fallback(self):
        mock_self = MagicMock()
        mock_self._schema_is_cached.return_value = False
        mock_self.connections.get_bq_table.side_effect = google.api_core.exceptions.NotFound(
            "missing"
        )

        # RoutineReference.from_string would raise ValueError on a four-part id;
        # reaching `None` proves the fallback was skipped
        result = BigQueryAdapter.get_relation(
            mock_self, "my-project", "my_catalog.my_namespace", "my_table"
        )

        assert result is None


class TestLakehouseReplaceability:
    def test_is_replaceable_true_without_metadata_probe(self):
        # Iceberg tables report no partition metadata via the tables API, so a
        # config comparison would force a destructive drop+create on every run
        mock_self = MagicMock()

        result = BigQueryAdapter.is_replaceable(
            mock_self, lakehouse_relation(), MagicMock(), ["col"]
        )

        assert result is True
        mock_self.connections.get_bq_table.assert_not_called()


class TestLakehouseFreshnessRouting:
    def test_batch_freshness_routes_lakehouse_sources_individually(self):
        mock_self = MagicMock()
        freshness = object()
        mock_self.calculate_freshness_from_metadata.return_value = (None, freshness)
        source = lakehouse_relation()

        (
            responses,
            freshness_responses,
        ) = BigQueryAdapter.calculate_freshness_from_metadata_batch(mock_self, [source])

        mock_self.calculate_freshness_from_metadata.assert_called_once_with(source, None)
        assert freshness_responses == {source: freshness}
        # the batch macro path must not run when only lakehouse sources exist
        mock_self.execute_macro.assert_not_called()

    def test_batch_freshness_isolates_failing_lakehouse_source(self):
        # one bad lakehouse source must not raise out of the batch method:
        # dbt-core would discard the whole project's metadata freshness cache
        mock_self = MagicMock()
        mock_self.calculate_freshness_from_metadata.side_effect = (
            google.api_core.exceptions.NotFound("gone")
        )
        source = lakehouse_relation()

        (
            responses,
            freshness_responses,
        ) = BigQueryAdapter.calculate_freshness_from_metadata_batch(mock_self, [source])

        assert freshness_responses == {}
        mock_self.execute_macro.assert_not_called()


class TestLakehouseDropRelation:
    def test_drop_relation_uses_sql_ddl(self):
        mock_self = MagicMock()
        mock_self._schema_is_cached.return_value = False
        relation = lakehouse_relation()

        BigQueryAdapter.drop_relation(mock_self, relation)

        mock_self.execute.assert_called_once_with(f"drop table if exists {relation}")
        mock_self.connections.get_thread_connection.assert_not_called()

    def test_drop_relation_tolerates_missing_namespace(self):
        # parity with delete_table(not_found_ok=True), which swallowed a 404
        # for a namespace dropped by the catalog-managing engine
        mock_self = MagicMock()
        mock_self._schema_is_cached.return_value = False
        mock_self.execute.side_effect = DbtDatabaseError("Not found: Dataset my-project:x")

        BigQueryAdapter.drop_relation(mock_self, lakehouse_relation())

    def test_drop_relation_reraises_other_errors(self):
        mock_self = MagicMock()
        mock_self._schema_is_cached.return_value = False
        mock_self.execute.side_effect = DbtDatabaseError("Access Denied")

        with pytest.raises(DbtDatabaseError, match="Access Denied"):
            BigQueryAdapter.drop_relation(mock_self, lakehouse_relation())


class TestLakehouseSchemaEvolution:
    def _mock_self_with_empty_table(self):
        mock_self = MagicMock()
        client = mock_self.connections.get_thread_connection.return_value.handle
        client.get_table.return_value.schema = []
        mock_self.quote.side_effect = lambda name: f"`{name}`"
        return mock_self, client

    def test_alter_table_add_columns_uses_ddl(self):
        mock_self, client = self._mock_self_with_empty_table()
        relation = lakehouse_relation()
        add_columns = [SimpleNamespace(name="new_col", data_type="STRING")]

        BigQueryAdapter.alter_table_add_remove_columns(mock_self, relation, add_columns, None)

        executed_sql = mock_self.execute.call_args[0][0]
        assert executed_sql == f"alter table {relation.render()} add column `new_col` STRING"
        client.update_table.assert_not_called()

    def test_alter_table_add_nested_column_raises(self):
        mock_self, client = self._mock_self_with_empty_table()
        add_columns = [SimpleNamespace(name="parent.child", data_type="STRING")]

        with pytest.raises(DbtRuntimeError, match="nested STRUCT"):
            BigQueryAdapter.alter_table_add_remove_columns(
                mock_self, lakehouse_relation(), add_columns, None
            )
        client.update_table.assert_not_called()

    def test_nested_addition_is_rejected_before_any_drop(self):
        mock_self, client = self._mock_self_with_empty_table()
        add_columns = [SimpleNamespace(name="parent.child", data_type="STRING")]
        remove_columns = [SimpleNamespace(name="old_col", data_type="STRING")]

        with pytest.raises(DbtRuntimeError, match="nested STRUCT"):
            BigQueryAdapter.alter_table_add_remove_columns(
                mock_self, lakehouse_relation(), add_columns, remove_columns
            )

        mock_self.execute.assert_not_called()
        client.get_table.assert_not_called()

    def test_drop_and_readd_same_column_raises(self):
        # documented Lakehouse limitation: DROP COLUMN then re-adding the same
        # name fails server-side; the guard must fire before the drop executes
        mock_self, client = self._mock_self_with_empty_table()
        add_columns = [SimpleNamespace(name="x", data_type="STRING")]
        remove_columns = [SimpleNamespace(name="x", data_type="INT64")]

        with pytest.raises(DbtRuntimeError, match="re-add"):
            BigQueryAdapter.alter_table_add_remove_columns(
                mock_self, lakehouse_relation(), add_columns, remove_columns
            )
        mock_self.execute.assert_not_called()
        client.update_table.assert_not_called()

    def test_update_columns_skips_lakehouse(self):
        mock_self = MagicMock()

        BigQueryAdapter.update_columns(mock_self, lakehouse_relation(), {"col": {}})

        mock_self.connections.get_thread_connection.assert_not_called()

    def test_update_table_description_skips_lakehouse(self):
        mock_self = MagicMock()

        BigQueryAdapter.update_table_description(
            mock_self, "my-project", "my_catalog.my_namespace", "my_table", "desc"
        )

        mock_self.connections.get_thread_connection.assert_not_called()


class TestLakehouseOptionValidation:
    def _validate(self, config, node=None, model=None):
        adapter = BigQueryAdapter.__new__(BigQueryAdapter)
        node = node or {
            "schema": "my_catalog.my_namespace",
            "database": "my-project",
            "alias": "t",
        }
        config_mock = MagicMock()
        config_mock.get.side_effect = lambda key, default=None: config.get(key, default)
        config_mock.model = model
        return adapter._validate_lakehouse_options(config_mock, node)

    def test_table_options_are_left_to_bigquery(self):
        self._validate(
            {
                "cluster_by": ["col"],
                "hours_to_expiration": 24,
                "kms_key_name": "key",
                "require_partition_filter": True,
                "partition_expiration_days": 7,
                "enable_change_history": True,
                "grants": {"select": ["user:reader@example.com"]},
                "partition_by": {"field": "day", "data_type": "date"},
            }
        )

    @pytest.mark.parametrize(
        "partition_by",
        [
            {
                "field": "id",
                "data_type": "int64",
                "range": {"start": 0, "end": 100, "interval": 10},
            },
            {"field": "day", "data_type": "date", "granularity": "hour"},
            {"field": "ts", "data_type": "timestamp", "granularity": "week"},
        ],
    )
    def test_rendered_partition_variants_are_left_to_bigquery(self, partition_by):
        self._validate({"partition_by": partition_by})

    def test_copy_partitions_inside_partition_by_raises(self):
        # the canonical spelling lives inside partition_by, not at the top level
        partition_by = {"field": "day", "data_type": "date", "copy_partitions": True}
        with pytest.raises(DbtRuntimeError, match="`copy_partitions` uses BigQuery copy jobs"):
            self._validate({"partition_by": partition_by})

    def test_string_partitioning_raises(self):
        # the partition_by macro renders NO clause for unknown data types; on
        # lakehouse that silently creates an unpartitioned table the user can
        # never re-spec through CREATE OR REPLACE
        partition_by = {"field": "country", "data_type": "string"}
        with pytest.raises(DbtRuntimeError, match="cannot render a partition clause"):
            self._validate({"partition_by": partition_by})

    def test_non_lakehouse_node_skips_validation(self):
        node = {"schema": "my_dataset", "database": "my-project", "alias": "t"}
        self._validate({"cluster_by": ["col"]}, node=node)

    @pytest.mark.parametrize("config_key", ["catalog_name", "catalog"])
    def test_biglake_catalog_config_conflict_raises(self, config_key):
        # a biglake write integration would emit storage_uri/WITH CONNECTION
        # into lakehouse DDL
        model = SimpleNamespace(config={config_key: "managed_iceberg"})
        with pytest.raises(DbtRuntimeError, match="cannot be combined"):
            self._validate({}, model=model)

    def test_no_catalog_config_passes(self):
        model = SimpleNamespace(config={})
        self._validate({}, model=model)

    def test_python_target_is_rejected_by_early_validator(self):
        adapter = BigQueryAdapter.__new__(BigQueryAdapter)
        config_mock = MagicMock()
        config_mock.get.side_effect = lambda key, default=None: default
        config_mock.model = None
        node = {
            "schema": "my_catalog.my_namespace",
            "database": "my-project",
            "alias": "t",
            "language": "python",
        }

        with pytest.raises(DbtRuntimeError, match="Python models cannot target"):
            adapter.validate_lakehouse_target(config_mock, node, "python")


class TestLakehouseOptionEmission:
    def _table_options(self, config, node):
        mock_self = MagicMock()
        mock_self.get_common_options.return_value = {}
        mock_self.build_catalog_relation.return_value = None
        config_mock = MagicMock()
        config_mock.get.side_effect = lambda key, default=None: config.get(key, default)
        config_mock.model = None
        return BigQueryAdapter.get_table_options(mock_self, config_mock, node, temporary=False)

    def test_table_options_are_not_silently_suppressed_for_lakehouse(self):
        node = {
            "schema": "my_catalog.my_namespace",
            "database": "my-project",
            "alias": "t",
        }
        config = {
            "partition_by": {"field": "day", "data_type": "date"},
            "require_partition_filter": False,
            "partition_expiration_days": 0,
            "enable_change_history": False,
        }

        opts = self._table_options(config, node)

        assert opts["require_partition_filter"] is False
        assert opts["partition_expiration_days"] == 0
        assert opts["enable_change_history"] is False


class TestLakehouseCommonOptions:
    def _common_options(self, config, node):
        mock_self = MagicMock()
        config_mock = MagicMock()
        config_mock.get.side_effect = lambda key, default=None: config.get(key, default)
        config_mock.persist_relation_docs.return_value = False
        return BigQueryAdapter.get_common_options(mock_self, config_mock, node)

    def test_hours_to_expiration_is_not_silently_suppressed_for_lakehouse(self):
        node = {
            "schema": "my_catalog.my_namespace",
            "database": "my-project",
            "alias": "t",
        }
        opts = self._common_options({"hours_to_expiration": 0}, node)
        assert "expiration_timestamp" in opts
