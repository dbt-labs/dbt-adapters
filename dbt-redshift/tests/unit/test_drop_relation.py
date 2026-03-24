from contextlib import contextmanager
from multiprocessing import get_context
from unittest import TestCase, mock

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
            with mock.patch(
                "dbt.adapters.protocol.AdapterProtocol.drop_relation", return_value=None
            ):
                adapter.drop_relation(relation)

        self.assertTrue(fresh_txn_entered, "fresh_transaction() should have been entered")

    def test_drop_relation_skips_lock_when_allow_concurrent_drops(self):
        """allow_concurrent_drops=True: fresh_transaction() must NOT be called."""
        adapter = make_adapter({"allow_concurrent_drops": True})
        relation = mock.MagicMock()

        fresh_txn_called = []

        @contextmanager
        def fake_fresh_transaction():
            fresh_txn_called.append(True)
            yield

        with mock.patch.object(adapter.connections, "fresh_transaction", fake_fresh_transaction):
            with mock.patch(
                "dbt.adapters.protocol.AdapterProtocol.drop_relation", return_value=None
            ):
                adapter.drop_relation(relation)

        self.assertFalse(fresh_txn_called, "fresh_transaction() should NOT have been called")
