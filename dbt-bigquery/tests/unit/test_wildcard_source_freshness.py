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


class TestCheckForWildcardIdentifier:
    def test_raises_on_wildcard_identifier(self):
        relation = _make_relation("events_*")
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="wildcard table"):
            BigQueryAdapter._check_for_wildcard_identifier(relation)

    def test_raises_on_embedded_wildcard(self):
        relation = _make_relation("table_2024*")
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="wildcard table"):
            BigQueryAdapter._check_for_wildcard_identifier(relation)

    def test_passes_for_normal_identifier(self):
        relation = _make_relation("events_table")
        # Should not raise
        BigQueryAdapter._check_for_wildcard_identifier(relation)

    def test_passes_for_none_identifier(self):
        relation = BigQueryRelation.create(
            database="test-project",
            schema="test_dataset",
            identifier=None,
        )
        # Should not raise
        BigQueryAdapter._check_for_wildcard_identifier(relation)
