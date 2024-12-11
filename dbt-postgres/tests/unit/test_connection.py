from multiprocessing import get_context
from unittest import TestCase, mock

import pytest
from dbt.context.query_header import generate_query_header_context
from dbt.context.providers import generate_runtime_macro_context
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import ManifestStateCheck
from dbt.task.debug import DebugTask
from dbt_common.exceptions import DbtConfigError
from psycopg2 import DatabaseError, extensions as psycopg2_extensions

from dbt.adapters.postgres import Plugin as PostgresPlugin, PostgresAdapter
from tests.unit.utils import (
    clear_plugin,
    config_from_parts_or_dicts,
    inject_adapter,
    load_internal_manifest_macros,
)


class TestPostgresConnection(TestCase):
    def setUp(self):
        self.target_dict = {
            "type": "postgres",
            "dbname": "postgres",
            "user": "root",
            "host": "thishostshouldnotexist",
            "pass": "password",
            "port": 5432,
            "schema": "public",
        }

        profile_cfg = {
            "outputs": {
                "test": self.target_dict,
            },
            "target": "test",
        }
        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self.mp_context = get_context("spawn")

        self.handle = mock.MagicMock(spec=psycopg2_extensions.connection)
        self.cursor = self.handle.cursor.return_value
        self.mock_execute = self.cursor.execute
        self.patcher = mock.patch("dbt.adapters.postgres.connections.psycopg2")
        self.psycopg2 = self.patcher.start()

        # Create the Manifest.state_check patcher
        @mock.patch("dbt.parser.manifest.ManifestLoader.build_manifest_state_check")
        def _mock_state_check(self):
            all_projects = self.all_projects
            return ManifestStateCheck(
                vars_hash=FileHash.from_contents("vars"),
                project_hashes={name: FileHash.from_contents(name) for name in all_projects},
                profile_hash=FileHash.from_contents("profile"),
            )

        self.load_state_check = mock.patch(
            "dbt.parser.manifest.ManifestLoader.build_manifest_state_check"
        )
        self.mock_state_check = self.load_state_check.start()
        self.mock_state_check.side_effect = _mock_state_check

        self.psycopg2.connect.return_value = self.handle
        self.adapter = PostgresAdapter(self.config, self.mp_context)
        self.adapter.set_macro_resolver(load_internal_manifest_macros(self.config))
        self.adapter.set_macro_context_generator(generate_runtime_macro_context)
        self.adapter.connections.set_query_header(
            generate_query_header_context(self.config, self.adapter.get_macro_resolver())
        )
        self.qh_patch = mock.patch.object(self.adapter.connections.query_header, "add")
        self.mock_query_header_add = self.qh_patch.start()
        self.mock_query_header_add.side_effect = lambda q: "/* dbt */\n{}".format(q)
        self.adapter.acquire_connection()
        inject_adapter(self.adapter, PostgresPlugin)

    def tearDown(self):
        # we want a unique self.handle every time.
        self.adapter.cleanup_connections()
        self.qh_patch.stop()
        self.patcher.stop()
        self.load_state_check.stop()
        clear_plugin(PostgresPlugin)

    def test_quoting_on_drop_schema(self):
        relation = self.adapter.Relation.create(
            database="postgres",
            schema="test_schema",
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.drop_schema(relation)

        self.mock_execute.assert_has_calls(
            [mock.call('/* dbt */\ndrop schema if exists "test_schema" cascade', None)]
        )

    def test_quoting_on_drop(self):
        relation = self.adapter.Relation.create(
            database="postgres",
            schema="test_schema",
            identifier="test_table",
            type="table",
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.drop_relation(relation)
        self.mock_execute.assert_has_calls(
            [
                mock.call(
                    '/* dbt */\ndrop table if exists "postgres"."test_schema".test_table cascade',
                    None,
                )
            ]
        )

    def test_quoting_on_truncate(self):
        relation = self.adapter.Relation.create(
            database="postgres",
            schema="test_schema",
            identifier="test_table",
            type="table",
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.truncate_relation(relation)
        self.mock_execute.assert_has_calls(
            [mock.call('/* dbt */\ntruncate table "postgres"."test_schema".test_table', None)]
        )

    def test_quoting_on_rename(self):
        from_relation = self.adapter.Relation.create(
            database="postgres",
            schema="test_schema",
            identifier="table_a",
            type="table",
            quote_policy=self.adapter.config.quoting,
        )
        to_relation = self.adapter.Relation.create(
            database="postgres",
            schema="test_schema",
            identifier="table_b",
            type="table",
            quote_policy=self.adapter.config.quoting,
        )

        self.adapter.rename_relation(from_relation=from_relation, to_relation=to_relation)
        self.mock_execute.assert_has_calls(
            [
                mock.call(
                    '/* dbt */\nalter table "postgres"."test_schema".table_a rename to table_b',
                    None,
                )
            ]
        )

    @pytest.mark.skip(
        """
        We moved from __version__ to __about__ when establishing `hatch` as our build tool.
        However, `adapters.factory.register_adapter` assumes __version__ when determining
        the adapter version. This test causes an import error
    """
    )
    def test_debug_connection_ok(self):
        DebugTask.validate_connection(self.target_dict)
        self.mock_execute.assert_has_calls([mock.call("/* dbt */\nselect 1 as id", None)])

    def test_debug_connection_fail_nopass(self):
        del self.target_dict["pass"]
        with self.assertRaises(DbtConfigError):
            DebugTask.validate_connection(self.target_dict)

    @pytest.mark.skip(
        """
        We moved from __version__ to __about__ when establishing `hatch` as our build tool.
        However, `adapters.factory.register_adapter` assumes __version__ when determining
        the adapter version. This test causes an import error
    """
    )
    def test_connection_fail_select(self):
        self.mock_execute.side_effect = DatabaseError()
        with self.assertRaises(DbtConfigError):
            DebugTask.validate_connection(self.target_dict)
        self.mock_execute.assert_has_calls([mock.call("/* dbt */\nselect 1 as id", None)])

    def test_dbname_verification_is_case_insensitive(self):
        # Override adapter settings from setUp()
        self.target_dict["dbname"] = "Postgres"
        profile_cfg = {
            "outputs": {
                "test": self.target_dict,
            },
            "target": "test",
        }
        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }
        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self.mp_context = get_context("spawn")
        self.adapter.cleanup_connections()
        self._adapter = PostgresAdapter(self.config, self.mp_context)
        self.adapter.verify_database("postgres")
