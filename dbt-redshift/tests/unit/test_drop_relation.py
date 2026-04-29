from contextlib import contextmanager
from multiprocessing import get_context
from unittest import TestCase, mock

from dbt.adapters.sql import SQLAdapter

from dbt.adapters.redshift import (
    Plugin as RedshiftPlugin,
    RedshiftAdapter,
)
from tests.unit.utils import (
    config_from_parts_or_dicts,
    inject_adapter,
)

BASE_OUTPUT = {
    "type": "redshift",
    "dbname": "redshift",
    "user": "root",
    "host": "thishostshouldnotexist.test.us-east-1",
    "pass": "password",
    "port": 5439,
    "schema": "public",
}

PROJECT_CFG = {
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


def make_adapter(extra_credentials=None):
    output = {**BASE_OUTPUT, **(extra_credentials or {})}
    profile_cfg = {"outputs": {"test": output}, "target": "test"}
    config = config_from_parts_or_dicts(PROJECT_CFG, profile_cfg)
    adapter = RedshiftAdapter(config, get_context("spawn"))
    inject_adapter(adapter, RedshiftPlugin)
    return adapter


class TestDropRelationLock(TestCase):

    def test_drop_relation_uses_lock_by_default(self):
        """allow_concurrent_drops defaults to False: fresh_transaction() must be called."""
        adapter = make_adapter()
        relation = mock.MagicMock()

        fresh_txn_entered = []

        @contextmanager
        def fake_fresh_transaction():
            fresh_txn_entered.append(True)
            yield

        with mock.patch.object(adapter.connections, "fresh_transaction", fake_fresh_transaction):
            with mock.patch.object(SQLAdapter, "drop_relation", return_value=None):
                adapter.drop_relation(relation)

        self.assertTrue(fresh_txn_entered, "fresh_transaction() should have been entered")

    def test_drop_relation_skips_lock_when_allow_concurrent_drops(self):
        """allow_concurrent_drops=True: fresh_transaction_without_lock() is used, not fresh_transaction()."""
        adapter = make_adapter({"allow_concurrent_drops": True})
        relation = mock.MagicMock()

        locked_txn_called = []
        unlocked_txn_called = []

        @contextmanager
        def fake_fresh_transaction():
            locked_txn_called.append(True)
            yield

        @contextmanager
        def fake_fresh_transaction_without_lock():
            unlocked_txn_called.append(True)
            yield

        with mock.patch.object(adapter.connections, "fresh_transaction", fake_fresh_transaction):
            with mock.patch.object(
                adapter.connections,
                "fresh_transaction_without_lock",
                fake_fresh_transaction_without_lock,
            ):
                with mock.patch.object(SQLAdapter, "drop_relation", return_value=None):
                    adapter.drop_relation(relation)

        self.assertFalse(
            locked_txn_called, "fresh_transaction() (locked) should NOT have been called"
        )
        self.assertTrue(
            unlocked_txn_called, "fresh_transaction_without_lock() should have been called"
        )


class TestFreshTransactionRetryResilience(TestCase):
    """Regression test for GitHub issue #1698.

    When execute()'s retry resets transaction_open to False during yield,
    fresh_transaction must restore the flag so commit() does not crash.
    """

    def _run_fresh_transaction_with_retry(self, extra_credentials=None, use_lock=True):
        adapter = make_adapter(extra_credentials)
        mgr = adapter.connections
        conn = mock.MagicMock()
        conn.transaction_open = True

        def mock_begin():
            conn.transaction_open = True

        def mock_commit():
            if conn.transaction_open is False:
                from dbt_common.exceptions import DbtRuntimeError

                raise DbtRuntimeError("Tried to commit but no open transaction!")
            conn.transaction_open = False

        ctx = mgr.fresh_transaction if use_lock else mgr.fresh_transaction_without_lock

        with (
            mock.patch.object(mgr, "begin", side_effect=mock_begin),
            mock.patch.object(mgr, "commit", side_effect=mock_commit),
            mock.patch.object(mgr, "get_thread_connection", return_value=conn),
        ):
            with ctx():
                # Simulate retry: close/open resets transaction_open
                conn.transaction_open = False

    def test_fresh_transaction_no_crash_when_retry_resets_transaction(self):
        self._run_fresh_transaction_with_retry()

    def test_fresh_transaction_without_lock_no_crash_when_retry_resets_transaction(self):
        self._run_fresh_transaction_with_retry(
            extra_credentials={"allow_concurrent_drops": True}, use_lock=False
        )
