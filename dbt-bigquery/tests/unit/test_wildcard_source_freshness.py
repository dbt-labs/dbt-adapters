from types import SimpleNamespace

import pytest

import dbt_common.exceptions
from dbt.adapters.bigquery import BigQueryAdapter
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.contracts.relation import RelationType


def _make_relation(identifier: str) -> BigQueryRelation:
    return BigQueryRelation.create(
        database="test-project",
        schema="test_dataset",
        identifier=identifier,
        type=RelationType.Table,
    )


def _make_adapter_stub(flag_enabled: bool):
    """Create a minimal stub with the behavior flag for _check_for_wildcard_identifier."""
    return SimpleNamespace(
        behavior=SimpleNamespace(
            bigquery_reject_wildcard_source_freshness=flag_enabled,
        ),
    )


class TestCheckForWildcardIdentifierFlagEnabled:
    """When the behavior flag is enabled, wildcard identifiers raise an error."""

    def test_raises_on_wildcard_identifier(self):
        stub = _make_adapter_stub(flag_enabled=True)
        relation = _make_relation("events_*")
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="wildcard table"):
            BigQueryAdapter._check_for_wildcard_identifier(stub, relation)

    def test_raises_on_embedded_wildcard(self):
        stub = _make_adapter_stub(flag_enabled=True)
        relation = _make_relation("table_2024*")
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="wildcard table"):
            BigQueryAdapter._check_for_wildcard_identifier(stub, relation)

    def test_passes_for_normal_identifier(self):
        stub = _make_adapter_stub(flag_enabled=True)
        relation = _make_relation("events_table")
        BigQueryAdapter._check_for_wildcard_identifier(stub, relation)

    def test_passes_for_none_identifier(self):
        stub = _make_adapter_stub(flag_enabled=True)
        relation = BigQueryRelation.create(
            database="test-project",
            schema="test_dataset",
            identifier=None,
        )
        BigQueryAdapter._check_for_wildcard_identifier(stub, relation)


class TestCheckForWildcardIdentifierFlagDisabled:
    """When the behavior flag is disabled, wildcard identifiers do NOT raise."""

    def test_no_error_on_wildcard_identifier(self):
        stub = _make_adapter_stub(flag_enabled=False)
        relation = _make_relation("events_*")
        # Should not raise when flag is off
        BigQueryAdapter._check_for_wildcard_identifier(stub, relation)

    def test_no_error_on_embedded_wildcard(self):
        stub = _make_adapter_stub(flag_enabled=False)
        relation = _make_relation("table_2024*")
        BigQueryAdapter._check_for_wildcard_identifier(stub, relation)
