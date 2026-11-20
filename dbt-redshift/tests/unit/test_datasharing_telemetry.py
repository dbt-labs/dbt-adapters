from multiprocessing import get_context
from unittest import mock

from dbt.adapters.redshift import RedshiftAdapter
from tests.unit.utils import config_from_parts_or_dicts


_BASE_PROFILE = {
    "outputs": {
        "test": {
            "type": "redshift",
            "dbname": "redshift",
            "user": "root",
            "host": "thishostshouldnotexist.test.us-east-1",
            "pass": "password",
            "port": 5439,
            "schema": "public",
        }
    },
    "target": "test",
}

_BASE_PROJECT = {
    "name": "X",
    "version": "0.1",
    "profile": "test",
    "project-root": "/tmp/dbt/does-not-exist",
    "quoting": {"identifier": False, "schema": True},
    "config-version": 2,
}


def _make_config(extra_creds=None):
    from copy import deepcopy

    profile = deepcopy(_BASE_PROFILE)
    if extra_creds:
        profile["outputs"]["test"].update(extra_creds)
    return config_from_parts_or_dicts(_BASE_PROJECT, profile)


def _info_calls(mock_logger):
    return [str(c) for c in mock_logger.info.call_args_list]


def test_datasharing_enabled_emits_info_log():
    config = _make_config({"datasharing": True})
    with mock.patch("dbt.adapters.redshift.impl.logger") as mock_logger:
        RedshiftAdapter(config, get_context("spawn"))
    assert any("datasharing" in c.lower() for c in _info_calls(mock_logger))


def test_datasharing_disabled_no_datasharing_info_log():
    config = _make_config({"datasharing": False})
    with mock.patch("dbt.adapters.redshift.impl.logger") as mock_logger:
        RedshiftAdapter(config, get_context("spawn"))
    assert not any("datasharing" in c.lower() for c in _info_calls(mock_logger))


def test_datasharing_log_includes_connection_method():
    config = _make_config({"datasharing": True, "method": "database"})
    with mock.patch("dbt.adapters.redshift.impl.logger") as mock_logger:
        RedshiftAdapter(config, get_context("spawn"))
    datasharing_calls = [c for c in _info_calls(mock_logger) if "datasharing" in c.lower()]
    assert datasharing_calls
    assert "database" in datasharing_calls[0]


def test_datasharing_log_serverless_true():
    config = _make_config({"datasharing": True, "is_serverless": True})
    with mock.patch("dbt.adapters.redshift.impl.logger") as mock_logger:
        RedshiftAdapter(config, get_context("spawn"))
    # logger.info is called as info(fmt_string, method, serverless, ra3_node)
    # c[0] is the positional-args tuple; c[0][0] is the format string, c[0][2] is serverless
    datasharing_calls = [
        c for c in mock_logger.info.call_args_list if "datasharing" in c[0][0].lower()
    ]
    assert datasharing_calls
    assert datasharing_calls[0][0][2] is True  # serverless arg


def test_datasharing_log_serverless_false_for_provisioned():
    config = _make_config({"datasharing": True})
    with mock.patch("dbt.adapters.redshift.impl.logger") as mock_logger:
        RedshiftAdapter(config, get_context("spawn"))
    datasharing_calls = [
        c for c in mock_logger.info.call_args_list if "datasharing" in c[0][0].lower()
    ]
    assert datasharing_calls
    assert datasharing_calls[0][0][2] is False  # serverless arg


def test_datasharing_disabled_emits_debug_log():
    config = _make_config({"datasharing": False})
    with mock.patch("dbt.adapters.redshift.impl.logger") as mock_logger:
        RedshiftAdapter(config, get_context("spawn"))
    debug_calls = [str(c) for c in mock_logger.debug.call_args_list]
    assert any("datasharing" in c.lower() for c in debug_calls)
