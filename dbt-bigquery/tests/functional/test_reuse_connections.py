import pytest
from dbt.tests.util import run_dbt_and_capture


REUSE_MESSAGE = "BigQuery client reuse is enabled"
REOPENED_MESSAGE = "Opening a new connection, currently in state closed"

# more models than threads, so every thread runs more than one node
_MODEL_COUNT = 8
_MODEL_SQL = """
{{ config(materialized="table") }}
select 1 as id
"""


class BaseReuseConnections:
    @pytest.fixture(scope="class")
    def models(self):
        return {f"model_{index}.sql": _MODEL_SQL for index in range(_MODEL_COUNT)}

    def run_and_capture(self):
        results, stdout = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == _MODEL_COUNT
        return stdout


class TestReuseConnectionsEnabled(BaseReuseConnections):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        return {**dbt_profile_target, "reuse_connections": True}

    def test_clients_are_reused_between_nodes(self, project):
        stdout = self.run_and_capture()
        assert stdout.count(REUSE_MESSAGE) == 1
        assert REOPENED_MESSAGE not in stdout


class TestReuseConnectionsNotSet(BaseReuseConnections):
    def test_a_new_client_is_opened_for_each_node(self, project):
        stdout = self.run_and_capture()
        assert REUSE_MESSAGE not in stdout
        assert REOPENED_MESSAGE in stdout
