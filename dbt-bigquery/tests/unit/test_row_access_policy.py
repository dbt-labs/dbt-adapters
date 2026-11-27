import unittest
from unittest.mock import MagicMock, patch, call

import dbt_common.exceptions

from dbt.adapters.bigquery import BigQueryAdapter


class TestPersistRowAccessPolicy(unittest.TestCase):
    def setUp(self):
        self.adapter = BigQueryAdapter.__new__(BigQueryAdapter)
        self.mock_client = MagicMock()
        self.mock_conn = MagicMock()
        self.mock_conn.handle = self.mock_client
        self.adapter.connections = MagicMock()
        self.adapter.connections.get_thread_connection.return_value = self.mock_conn

        self.mock_query_job = MagicMock()
        self.mock_client.query.return_value = self.mock_query_job

        self.relation = MagicMock()
        self.relation.__str__ = lambda self: "`project.dataset.my_table`"

    def test_no_config_is_noop(self):
        self.adapter.persist_row_access_policy(self.relation, None)
        self.mock_client.query.assert_not_called()

    def test_empty_config_is_noop(self):
        self.adapter.persist_row_access_policy(self.relation, {})
        self.mock_client.query.assert_not_called()

    def test_missing_filter_using_raises(self):
        config = {"grantees": ["user:ai@example.com"]}
        with self.assertRaises(dbt_common.exceptions.DbtRuntimeError):
            self.adapter.persist_row_access_policy(self.relation, config)

    def test_missing_grantees_raises(self):
        config = {"filter_using": "org_id NOT IN (SELECT org_id FROM optouts)"}
        with self.assertRaises(dbt_common.exceptions.DbtRuntimeError):
            self.adapter.persist_row_access_policy(self.relation, config)

    def test_applies_policy_with_default_name(self):
        config = {
            "filter_using": "org_id NOT IN (SELECT org_id FROM optouts)",
            "grantees": ["user:ai@example.com", "group:data-team@example.com"],
        }
        self.adapter.persist_row_access_policy(self.relation, config)

        calls = self.mock_client.query.call_args_list
        assert len(calls) == 2

        drop_sql = calls[0][0][0]
        assert "DROP ROW ACCESS POLICY IF EXISTS `dbt_row_access`" in drop_sql
        assert "`project.dataset.my_table`" in drop_sql

        create_sql = calls[1][0][0]
        assert "CREATE ROW ACCESS POLICY `dbt_row_access`" in create_sql
        assert "`project.dataset.my_table`" in create_sql
        assert '"user:ai@example.com"' in create_sql
        assert '"group:data-team@example.com"' in create_sql
        assert "org_id NOT IN (SELECT org_id FROM optouts)" in create_sql

    def test_custom_policy_name(self):
        config = {
            "filter_using": "TRUE",
            "grantees": ["user:bot@example.com"],
            "policy_name": "my_custom_policy",
        }
        self.adapter.persist_row_access_policy(self.relation, config)

        drop_sql = self.mock_client.query.call_args_list[0][0][0]
        assert "DROP ROW ACCESS POLICY IF EXISTS `my_custom_policy`" in drop_sql

        create_sql = self.mock_client.query.call_args_list[1][0][0]
        assert "CREATE ROW ACCESS POLICY `my_custom_policy`" in create_sql

    def test_string_grantee_is_accepted(self):
        config = {
            "filter_using": "TRUE",
            "grantees": "user:single@example.com",
        }
        self.adapter.persist_row_access_policy(self.relation, config)

        create_sql = self.mock_client.query.call_args_list[1][0][0]
        assert '"user:single@example.com"' in create_sql

    def test_query_results_are_awaited(self):
        self.adapter.persist_row_access_policy(
            self.relation,
            {
                "filter_using": "TRUE",
                "grantees": ["user:a@example.com"],
            },
        )
        assert self.mock_query_job.result.call_count == 2
