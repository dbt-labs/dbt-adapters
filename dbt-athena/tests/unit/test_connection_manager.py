from multiprocessing import get_context
from unittest import mock

import pytest
from pyathena.model import AthenaQueryExecution

from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.connections import AthenaAdapterResponse
from dbt.adapters.athena.spark_connect.session import SparkConnectSessionPool


class TestAthenaConnectionManager:
    @pytest.mark.parametrize(
        ("state", "result"),
        (
            pytest.param(AthenaQueryExecution.STATE_SUCCEEDED, "OK"),
            pytest.param(AthenaQueryExecution.STATE_CANCELLED, "ERROR"),
        ),
    )
    def test_get_response(self, state, result):
        cursor = mock.MagicMock()
        cursor.rowcount = 1
        cursor.state = state
        cursor.data_scanned_in_bytes = 123
        cm = AthenaConnectionManager(mock.MagicMock(), get_context("spawn"))
        response = cm.get_response(cursor)
        assert isinstance(response, AthenaAdapterResponse)
        assert response.code == result
        assert response.rows_affected == 1
        assert response.data_scanned_in_bytes == 123

    def test_data_type_code_to_name(self):
        cm = AthenaConnectionManager(mock.MagicMock(), get_context("spawn"))
        assert cm.data_type_code_to_name("array<string>") == "ARRAY"
        assert cm.data_type_code_to_name("map<int, boolean>") == "MAP"
        assert cm.data_type_code_to_name("DECIMAL(3, 7)") == "DECIMAL"

    def test_cleanup_all_terminates_only_current_invocation_sessions(self):
        SparkConnectSessionPool._reset_for_tests()
        try:
            pool = SparkConnectSessionPool()
            mine_client = mock.MagicMock()
            other_client = mock.MagicMock()
            pool._sessions["sid-mine"] = {
                "key": ("inv-current", "fp"),
                "client": mine_client,
                "load": 0,
            }
            pool._sessions["sid-other"] = {
                "key": ("inv-other", "fp"),
                "client": other_client,
                "load": 0,
            }

            cm = AthenaConnectionManager(mock.MagicMock(), get_context("spawn"))
            with (
                mock.patch(
                    "dbt_common.invocation.get_invocation_id",
                    return_value="inv-current",
                ),
                mock.patch.object(AthenaConnectionManager.__bases__[0], "cleanup_all"),
            ):
                cm.cleanup_all()

            mine_client.terminate_session.assert_called_once_with(SessionId="sid-mine")
            other_client.terminate_session.assert_not_called()
            assert "sid-mine" not in pool._snapshot()
            assert "sid-other" in pool._snapshot()
        finally:
            SparkConnectSessionPool._reset_for_tests()
