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


class TestRenameRelationLock(TestCase):
    """Regression test for CORE-724.

    A conflict on the target->backup rename rolls back the build transaction,
    discarding the intermediate (__dbt_tmp) relation created earlier in that
    same transaction. Wrapping rename_relation in fresh_transaction(), like
    drop_relation already does for DROP...CASCADE, commits the intermediate
    relation before the risky rename is attempted, so a conflict there can no
    longer take the intermediate relation down with it.
    """

    def test_rename_relation_uses_fresh_transaction(self):
        adapter = make_adapter()
        from_relation = mock.MagicMock()
        to_relation = mock.MagicMock()

        fresh_txn_entered = []

        @contextmanager
        def fake_fresh_transaction():
            fresh_txn_entered.append(True)
            yield

        with mock.patch.object(adapter.connections, "fresh_transaction", fake_fresh_transaction):
            with mock.patch.object(SQLAdapter, "rename_relation", return_value=None):
                adapter.rename_relation(from_relation, to_relation)

        self.assertTrue(fresh_txn_entered, "fresh_transaction() should have been entered")
