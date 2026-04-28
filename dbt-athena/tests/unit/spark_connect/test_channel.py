"""Tests for the Athena Spark Connect channel builder.

The real ``ChannelBuilder`` lives in pyspark, which is an optional
dependency. We stub the parts we need so the production module can be
imported and exercised without pyspark installed.
"""

import sys
import threading
import time
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtRuntimeError


class _StubChannelBuilder:
    """Minimal stand-in for pyspark's ChannelBuilder."""

    def __init__(self, url: str) -> None:
        self.url = url

    def metadata(self):
        # Include a non-auth header to verify filtering preserves it,
        # and an existing auth header to verify it gets replaced.
        return [("user-agent", "dbt-athena"), ("x-aws-proxy-auth", "stale")]


class _StubReattachIterator:
    _release_thread_pool = None

    @classmethod
    def shutdown(cls):
        pass


@pytest.fixture
def fake_pyspark(monkeypatch):
    """Inject fake pyspark modules so the channel module can import them."""
    fake_pyspark = types.ModuleType("pyspark")
    fake_sql = types.ModuleType("pyspark.sql")
    fake_connect = types.ModuleType("pyspark.sql.connect")
    fake_client = types.ModuleType("pyspark.sql.connect.client")
    fake_core = types.ModuleType("pyspark.sql.connect.client.core")
    fake_core.ChannelBuilder = _StubChannelBuilder
    fake_reattach = types.ModuleType("pyspark.sql.connect.client.reattach")
    fake_reattach.ExecutePlanResponseReattachableIterator = _StubReattachIterator

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect", fake_connect)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.client", fake_client)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.client.core", fake_core)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.client.reattach", fake_reattach)
    return fake_core


@pytest.fixture(autouse=True)
def _reset_module_state():
    """Force a fresh ``_channel_impl`` import against each test's fake pyspark."""
    import dbt.adapters.athena.spark_connect.pyspark_patches as patch_mod

    sys.modules.pop("dbt.adapters.athena.spark_connect._channel_impl", None)
    patch_mod._patches_applied = False
    yield
    sys.modules.pop("dbt.adapters.athena.spark_connect._channel_impl", None)
    patch_mod._patches_applied = False


def _future(seconds: int) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def test_url_is_rewritten_to_spark_connect_scheme(fake_pyspark):
    from dbt.adapters.athena.spark_connect.channel import create_athena_channel_builder

    builder = create_athena_channel_builder(
        athena_client=Mock(),
        session_id="sid",
        endpoint_url="https://athena.us-east-1.amazonaws.com",
        initial_auth_token="tok",
        initial_token_expiry=_future(3600),
    )

    assert builder.url == "sc://athena.us-east-1.amazonaws.com:443/;use_ssl=true"


def test_metadata_appends_current_auth_token_and_drops_stale(fake_pyspark):
    from dbt.adapters.athena.spark_connect.channel import create_athena_channel_builder

    builder = create_athena_channel_builder(
        athena_client=Mock(),
        session_id="sid",
        endpoint_url="https://x",
        initial_auth_token="fresh-token",
        initial_token_expiry=_future(3600),
    )

    md = builder.metadata()

    auth_headers = [v for k, v in md if k == "x-aws-proxy-auth"]
    assert auth_headers == ["fresh-token"]
    # Non-auth headers from the parent are preserved.
    assert ("user-agent", "dbt-athena") in md


def test_token_within_refresh_margin_triggers_refresh(fake_pyspark):
    from dbt.adapters.athena.spark_connect.channel import (
        _TOKEN_REFRESH_MARGIN_SECONDS,
        create_athena_channel_builder,
    )

    athena_client = Mock()
    athena_client.get_session_endpoint.return_value = {
        "AuthToken": "refreshed-token",
        "AuthTokenExpirationTime": _future(3600),
    }

    builder = create_athena_channel_builder(
        athena_client=athena_client,
        session_id="sid",
        endpoint_url="https://x",
        initial_auth_token="stale-token",
        initial_token_expiry=_future(_TOKEN_REFRESH_MARGIN_SECONDS - 1),
    )

    md = builder.metadata()

    athena_client.get_session_endpoint.assert_called_once_with(SessionId="sid")
    assert ("x-aws-proxy-auth", "refreshed-token") in md


def test_token_outside_refresh_margin_does_not_refresh(fake_pyspark):
    from dbt.adapters.athena.spark_connect.channel import (
        _TOKEN_REFRESH_MARGIN_SECONDS,
        create_athena_channel_builder,
    )

    athena_client = Mock()

    builder = create_athena_channel_builder(
        athena_client=athena_client,
        session_id="sid",
        endpoint_url="https://x",
        initial_auth_token="fresh-token",
        initial_token_expiry=_future(_TOKEN_REFRESH_MARGIN_SECONDS + 60),
    )

    builder.metadata()

    athena_client.get_session_endpoint.assert_not_called()


def test_missing_initial_token_forces_refresh(fake_pyspark):
    from dbt.adapters.athena.spark_connect.channel import create_athena_channel_builder

    athena_client = Mock()
    athena_client.get_session_endpoint.return_value = {
        "AuthToken": "newly-fetched",
        "AuthTokenExpirationTime": _future(3600),
    }

    builder = create_athena_channel_builder(
        athena_client=athena_client,
        session_id="sid",
        endpoint_url="https://x",
        initial_auth_token=None,
        initial_token_expiry=None,
    )

    md = builder.metadata()

    athena_client.get_session_endpoint.assert_called_once_with(SessionId="sid")
    assert ("x-aws-proxy-auth", "newly-fetched") in md


def test_refresh_raises_when_endpoint_returns_no_token(fake_pyspark):
    from dbt.adapters.athena.spark_connect.channel import create_athena_channel_builder

    athena_client = Mock()
    athena_client.get_session_endpoint.return_value = {"AuthToken": None}

    builder = create_athena_channel_builder(
        athena_client=athena_client,
        session_id="sid-xyz",
        endpoint_url="https://x",
        initial_auth_token=None,
        initial_token_expiry=None,
    )

    with pytest.raises(DbtRuntimeError, match="GetSessionEndpoint returned no AuthToken"):
        builder.metadata()


def test_concurrent_metadata_only_refreshes_once(fake_pyspark):
    """Two simultaneous metadata() calls trigger exactly one refresh."""
    from dbt.adapters.athena.spark_connect.channel import create_athena_channel_builder

    barrier = threading.Barrier(2)
    call_count = 0

    def slow_refresh(SessionId):
        nonlocal call_count
        call_count += 1
        # Hold the lock long enough that the second thread is guaranteed
        # to be blocked on it before this returns.
        time.sleep(0.05)
        return {
            "AuthToken": "post-refresh",
            "AuthTokenExpirationTime": _future(3600),
        }

    athena_client = Mock()
    athena_client.get_session_endpoint.side_effect = slow_refresh

    builder = create_athena_channel_builder(
        athena_client=athena_client,
        session_id="sid",
        endpoint_url="https://x",
        initial_auth_token=None,
        initial_token_expiry=None,
    )

    results = []

    def call_metadata():
        barrier.wait()
        results.append(builder.metadata())

    threads = [threading.Thread(target=call_metadata) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert call_count == 1
    for md in results:
        assert ("x-aws-proxy-auth", "post-refresh") in md


def test_class_is_cached_across_calls(fake_pyspark):
    """Two builds against the same fake pyspark produce the same class object."""
    from dbt.adapters.athena.spark_connect.channel import create_athena_channel_builder

    first = create_athena_channel_builder(
        athena_client=Mock(),
        session_id="sid",
        endpoint_url="https://x",
        initial_auth_token="tok",
        initial_token_expiry=_future(3600),
    )
    second = create_athena_channel_builder(
        athena_client=Mock(),
        session_id="sid",
        endpoint_url="https://x",
        initial_auth_token="tok",
        initial_token_expiry=_future(3600),
    )
    assert type(first) is type(second)
