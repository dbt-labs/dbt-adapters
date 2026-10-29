from unittest import mock

import pytest

from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.sql import SQLAdapter
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.lakecompute.impl import LakeComputeAdapter


class TestConstraintSupport:
    # BaseAdapter's Postgres defaults would render constraint clauses into DDL.

    def test_every_constraint_type_is_declared(self):
        assert set(LakeComputeAdapter.CONSTRAINT_SUPPORT) == set(ConstraintType) - {
            ConstraintType.custom
        }

    def test_nothing_is_supported(self):
        assert set(LakeComputeAdapter.CONSTRAINT_SUPPORT.values()) == {
            ConstraintSupport.NOT_SUPPORTED
        }


class TestGetColumnsInRelation:
    """A missing relation has no columns; every other failure still raises.

    The messages are what the live service returned for `describe` (2026-10-28).
    """

    @pytest.fixture
    def adapter(self):
        # get_columns_in_relation touches nothing on the instance before calling super()
        return LakeComputeAdapter.__new__(LakeComputeAdapter)

    @pytest.mark.parametrize(
        "message",
        [
            "INTERNAL: [CatalogException] Catalog Error: Table with name missing_t does not "
            'exist!\nDid you mean "stg_high_scores"?',
            'INTERNAL: [CatalogException] Catalog Error: Table with name "missing_s.t" does not '
            'exist because schema "missing_s" does not exist.',
            # a never-created D.S.T falls back to a catalog named D
            'INTERNAL: [BinderException] Binder Error: Catalog "lake_db" does not exist!',
            'INTERNAL: [BinderException] Binder Error: Catalog "LAKE_DB" does not exist!',
        ],
    )
    def test_missing_table_or_schema_returns_no_columns(self, adapter, message):
        with mock.patch.object(
            SQLAdapter, "get_columns_in_relation", side_effect=DbtDatabaseError(message)
        ):
            assert adapter.get_columns_in_relation(mock.Mock(database="lake_db")) == []

    @pytest.mark.parametrize(
        "message",
        [
            # a catalog other than the relation's own is a real failure
            'INTERNAL: [BinderException] Binder Error: Catalog "missing_c" does not exist!',
            "lakecompute query internal: ... ended with status Canceled",
            "lakecompute rejected the credentials for auth method 'fivetran': 401",
        ],
    )
    def test_other_failures_raise(self, adapter, message):
        with mock.patch.object(
            SQLAdapter, "get_columns_in_relation", side_effect=DbtDatabaseError(message)
        ):
            with pytest.raises(DbtDatabaseError):
                adapter.get_columns_in_relation(mock.Mock(database="lake_db"))


class TestGetRelation:
    """A listing miss falls back to a `describe` probe, as v2 always does."""

    @pytest.fixture
    def adapter(self):
        adapter = LakeComputeAdapter.__new__(LakeComputeAdapter)
        adapter.cache = mock.Mock()
        return adapter

    def test_listing_hit_does_not_probe(self, adapter):
        listed = mock.Mock()
        with (
            mock.patch.object(SQLAdapter, "get_relation", return_value=listed),
            mock.patch.object(LakeComputeAdapter, "get_columns_in_relation") as probe,
        ):
            assert adapter.get_relation("db", "s", "t") is listed
        probe.assert_not_called()

    def test_listing_miss_probes_and_caches_a_table(self, adapter):
        with (
            mock.patch.object(SQLAdapter, "get_relation", return_value=None),
            mock.patch.object(
                LakeComputeAdapter, "get_columns_in_relation", return_value=[mock.Mock()]
            ) as probe,
        ):
            relation = adapter.get_relation("db", "s", "MixedCase")
        assert relation.render() == "db.s.MixedCase"
        assert relation.type == "table"
        probe.assert_called_once_with(relation)
        adapter.cache.add.assert_called_once_with(relation)

    def test_probe_miss_is_none(self, adapter):
        with (
            mock.patch.object(SQLAdapter, "get_relation", return_value=None),
            mock.patch.object(LakeComputeAdapter, "get_columns_in_relation", return_value=[]),
        ):
            assert adapter.get_relation("db", "s", "t") is None
        adapter.cache.add.assert_not_called()

    def test_stays_available_to_jinja(self):
        assert "get_relation" in LakeComputeAdapter._available_
