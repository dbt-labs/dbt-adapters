from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dbt.adapters.bigquery.impl import BigQueryAdapter
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.contracts.relation import RelationType


@pytest.fixture
def adapter():
    return BigQueryAdapter(MagicMock(), MagicMock())


def _make_relation(database: str, schema: str, identifier: str) -> BigQueryRelation:
    return BigQueryRelation.create(
        database=database, schema=schema, identifier=identifier, type=RelationType.Table
    )


class TestCalculateFreshnessFromMetadataBatch:
    def test_empty_sources_returns_early_without_querying(self, adapter, mocker):
        execute_macro = mocker.patch.object(adapter, "execute_macro")

        adapter_responses, freshness_responses = adapter.calculate_freshness_from_metadata_batch(
            sources=[]
        )

        assert adapter_responses == []
        assert freshness_responses == {}
        execute_macro.assert_not_called()

    def test_sources_sharing_a_schema_across_databases_query_separately(self, adapter, mocker):
        """Two sources with the same schema/identifier but different databases must
        not be batched into the same query (#2107): the query only ever reaches
        whichever project relations[0] names, so batching them together would let
        the wrong project's table answer for one of the sources.
        """
        mocker.patch.object(
            adapter, "_behavior", SimpleNamespace(bigquery_use_batch_source_freshness=True)
        )
        stale_source = _make_relation("project-a", "stripe", "plans")
        fresh_source = _make_relation("project-b", "stripe", "plans")

        # Each fake query result's "table" is just the queried database's own
        # name, standing in for the single row that database's real query
        # would come back with: both sources share a schema/identifier, so
        # nothing but which batch a row came from can tell them apart.
        freshness_by_database = {
            "project-a": {"max_loaded_at": "stale", "snapshotted_at": "now", "age": 1.0},
            "project-b": {"max_loaded_at": "fresh", "snapshotted_at": "now", "age": 2.0},
        }

        def fake_execute_macro(_name, kwargs, **_kw):
            database = kwargs["relations"][0].database
            return MagicMock(response=f"adapter-response-{database}", table=[database])

        execute_macro = mocker.patch.object(
            adapter, "execute_macro", side_effect=fake_execute_macro
        )
        mocker.patch.object(
            adapter,
            "_parse_freshness_row",
            side_effect=lambda row, _table: (("stripe", "plans"), freshness_by_database[row]),
        )

        _adapter_responses, freshness_responses = adapter.calculate_freshness_from_metadata_batch(
            sources=[stale_source, fresh_source]
        )

        assert (
            execute_macro.call_count == 2
        ), "each database should get its own batch query, not one shared query"
        assert freshness_responses[stale_source]["max_loaded_at"] == "stale"
        assert freshness_responses[fresh_source]["max_loaded_at"] == "fresh"
